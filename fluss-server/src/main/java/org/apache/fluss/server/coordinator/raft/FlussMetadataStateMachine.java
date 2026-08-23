/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator.raft;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.shaded.ratis.org.apache.ratis.io.MD5Hash;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.Message;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroupId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.protocol.TermIndex;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.storage.RaftStorage;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.TransactionContext;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.MD5FileUtil;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

/**
 * Deterministic apply target for the replicated metadata log. Follows the KRaft commit-apply model:
 * state is mutated ONLY here, after Ratis commits an entry, on EVERY node including the leader.
 * This makes the in-memory store a pure function of the committed log — no pre-apply, no divergence
 * risk.
 *
 * <p>Apply is single-threaded (Ratis serializes {@code applyTransaction}). See §5.6 (Thread Model)
 * for the strict rule that no apply-thread code may synchronously wait on a new append.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.4.
 */
@Internal
public class FlussMetadataStateMachine extends BaseStateMachine {

    private static final CompletableFuture<Message> EMPTY =
            CompletableFuture.completedFuture(Message.EMPTY);

    private final RaftMetadataStore store;

    // Manages on-disk snapshot files (snapshot.<term>_<index> + .md5) under the Ratis storage dir.
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    // Set after construction to break the RaftLeaderSelector <-> state-machine construction cycle.
    @Nullable private volatile RaftLeaderSelector leaderSelector;

    // Fired from the apply loop after each mutation advances lastAppliedIndex, carrying the applied
    // metadata offset. MUST be a cheap, non-blocking wakeup only (e.g.
    // MetadataStreamService.onApplied
    // enqueue onto the metadata-stream pool) — it runs on the single apply thread and must never
    // block it, scan the Raft log, or complete RPC futures (§5.6, §5.8.2).
    @Nullable private volatile LongConsumer applyListener;

    @GuardedBy("this")
    private long lastAppliedIndex = -1;

    // Futures awaiting a given applied index (used by RaftJournalWriter.append to know when the
    // committed entry has become locally visible). Keyed by target index.
    @GuardedBy("this")
    private final NavigableMap<Long, CompletableFuture<Long>> appliedWaiters = new TreeMap<>();

    public FlussMetadataStateMachine(RaftMetadataStore store) {
        this.store = store;
    }

    /** Wires the leader selector once it has been created (see {@link RaftLeaderSelector}). */
    public void setLeaderSelector(RaftLeaderSelector leaderSelector) {
        this.leaderSelector = leaderSelector;
    }

    /**
     * Registers the apply-loop wakeup listener (typically {@code
     * MetadataStreamService::onApplied}). The listener fires on the single apply thread after each
     * mutation advances the applied offset; it MUST be a cheap, non-blocking notification (§5.8.2)
     * — it may not complete RPCs, scan the Raft log, or wait on a new append (§5.6). Passing {@code
     * null} detaches the listener.
     */
    public void setApplyListener(@Nullable LongConsumer applyListener) {
        this.applyListener = applyListener;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            LogEntryProto entry = trx.getLogEntry();
            byte[] data = trx.getStateMachineLogEntry().getLogData().toByteArray();
            applyEntry(MetadataRecord.fromBytes(data), entry.getIndex());
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return EMPTY;
        } catch (Exception e) {
            CompletableFuture<Message> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }
    }

    /**
     * Serialized by Ratis; the ONLY mutation path for the store on all nodes.
     *
     * <p>The store mutation and the {@link #lastAppliedIndex} advance happen under {@code this};
     * the resolved apply waiters are completed <em>after</em> the monitor is released (analysis
     * §8.9.4 C3). Completing them inside the lock would run every caller's {@code
     * thenCompose}/{@code whenComplete} continuation on the apply thread <em>while holding the
     * state-machine monitor</em> — e.g. {@link RaftJournalWriter#appendAll}'s batch bookkeeping —
     * which both inflates apply-loop latency (every microsecond here is charged to the writer via
     * {@code awaitApplied}) and risks lock-order inversions (§5.6).
     */
    @VisibleForTesting
    void applyEntry(MetadataRecord record, long index) {
        final List<CompletableFuture<Long>> resolved;
        final LongConsumer listener;
        synchronized (this) {
            if (index <= lastAppliedIndex) {
                return; // duplicate (retried write)
            }
            store.apply(record); // deterministic mutation of the KvStore + fire local watches
            lastAppliedIndex = index;
            resolved = drainAppliedWaiters(index);
            listener = applyListener;
        }
        // Outside the monitor: caller continuations must not run under the state-machine lock.
        completeAll(resolved, index);
        // Wake cross-process long-poll subscribers parked at the high watermark (§5.8.2). This is a
        // pure notification: the listener re-reads the freshly-appended tail from the Raft log.
        if (listener != null) {
            listener.accept(index);
        }
    }

    public synchronized long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /**
     * Captures a live terminal-state image of the KvStore tagged with the current applied offset,
     * both read atomically under {@code this} so the image and its {@code snapshot_metadata_offset}
     * can never disagree (the seam correctness argument in §5.8.1 point 4). Unlike {@link
     * #takeSnapshot()}, this does NOT touch disk — it serves {@code FetchMetadataSnapshot} directly
     * from the in-memory store and is always available (does not depend on an auto-triggered
     * snapshot having run). The offset uses the {@link #lastAppliedIndex} field (advanced inside
     * {@link #applyEntry} together with the store mutation), so image and offset are exactly
     * consistent — never off by an entry.
     */
    public synchronized MetadataSnapshot captureSnapshot() {
        return new MetadataSnapshot(
                lastAppliedIndex, MetadataSnapshot.serializeEntries(store.snapshotEntries()));
    }

    /**
     * Returns a future completing when {@code index} has been applied locally. Called from {@code
     * RaftJournalWriter.append} (NOT from the apply thread). Completes immediately if already
     * applied, including Raft configuration / metadata log entries that advance {@link
     * #getLastAppliedTermIndex()} via {@link #notifyTermIndexUpdated} without going through {@link
     * #applyEntry}.
     */
    public synchronized CompletableFuture<Long> awaitApplied(long index) {
        if (lastAppliedIndex >= index) {
            return CompletableFuture.completedFuture(lastAppliedIndex);
        }
        TermIndex ratisApplied = getLastAppliedTermIndex();
        if (ratisApplied != null && ratisApplied.getIndex() >= index) {
            return CompletableFuture.completedFuture(ratisApplied.getIndex());
        }
        return appliedWaiters.computeIfAbsent(index, k -> new CompletableFuture<>());
    }

    /**
     * Ratis invokes this for {@code MetadataProto} / {@code RaftConfigurationProto} entries, which
     * never enter {@link #applyTransaction}. Leader catch-up waits on the Raft commit index, which
     * after an election is often one of these entries; complete waiters here so {@link
     * #awaitApplied} does not hang until {@code coordinator.raft.catchup.timeout}.
     */
    @Override
    public void notifyTermIndexUpdated(long term, long index) {
        super.notifyTermIndexUpdated(term, index);
        final List<CompletableFuture<Long>> resolved;
        synchronized (this) {
            resolved = drainAppliedWaiters(index);
        }
        completeAll(resolved, index); // outside the monitor (§8.9.4 C3)
    }

    /**
     * Removes and returns every waiter whose target index is {@code <= index}. Callers MUST invoke
     * {@link #completeAll} outside the monitor.
     */
    @GuardedBy("this")
    private List<CompletableFuture<Long>> drainAppliedWaiters(long index) {
        if (appliedWaiters.isEmpty()) {
            return Collections.emptyList();
        }
        NavigableMap<Long, CompletableFuture<Long>> done = appliedWaiters.headMap(index, true);
        if (done.isEmpty()) {
            return Collections.emptyList();
        }
        List<CompletableFuture<Long>> resolved = new ArrayList<>(done.values());
        done.clear();
        return resolved;
    }

    private static void completeAll(List<CompletableFuture<Long>> waiters, long index) {
        for (int i = 0; i < waiters.size(); i++) {
            waiters.get(i).complete(index);
        }
    }

    // ---- snapshot / log compaction (§5.2.5) ----

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
            throws IOException {
        super.initialize(server, groupId, raftStorage);
        storage.init(raftStorage);
        load(storage.getLatestSnapshot());
    }

    @Override
    public void reinitialize() throws IOException {
        load(storage.getLatestSnapshot());
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    /**
     * Serializes the terminal KvStore state into a single snapshot file tagged with the last
     * applied term/index (the {@code snapshot_metadata_offset} required by §5.8.1). The store
     * contents and the offset tag are captured atomically under {@code this} so the image and its
     * offset never disagree; the (small) metadata store is copied under the lock and the file is
     * written outside it. Returns the snapshot index, or {@link RaftLog#INVALID_LOG_INDEX} if
     * nothing is applied yet.
     */
    @Override
    public long takeSnapshot() {
        final TermIndex lastApplied;
        final List<byte[][]> entries;
        synchronized (this) {
            // Tag the file with the SM applied index, not Ratis getLastAppliedTermIndex().
            // Configuration log entries can advance the Ratis applied index without going
            // through applyEntry, which would otherwise snapshot user state at N under index N+1.
            if (lastAppliedIndex < 0) {
                return RaftLog.INVALID_LOG_INDEX;
            }
            TermIndex ratisApplied = getLastAppliedTermIndex();
            long term = ratisApplied != null ? ratisApplied.getTerm() : 0;
            lastApplied = TermIndex.valueOf(term, lastAppliedIndex);
            entries = store.snapshotEntries();
        }
        File snapshotFile = storage.getSnapshotFile(lastApplied.getTerm(), lastApplied.getIndex());
        try {
            writeEntries(snapshotFile, entries);
            MD5Hash md5 = MD5FileUtil.computeMd5ForFile(snapshotFile);
            MD5FileUtil.saveMD5File(snapshotFile, md5);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to take metadata snapshot at " + lastApplied, e);
        }
        return lastApplied.getIndex();
    }

    /** Restores the KvStore and the applied index from {@code snapshot} (no-op if none exists). */
    private void load(@Nullable SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            return; // fresh cluster, nothing to restore
        }
        File file = snapshot.getFile().getPath().toFile();
        if (!file.exists()) {
            throw new FileNotFoundException("Snapshot file not found: " + file);
        }
        List<byte[][]> entries = readEntries(file);
        synchronized (this) {
            store.restoreFromSnapshot(entries);
            TermIndex ti = snapshot.getTermIndex();
            lastAppliedIndex = ti.getIndex();
            setLastAppliedTermIndex(ti);
        }
    }

    /**
     * Snapshot file format: {@code [int count]} then {@code count} × {@code [int keyLen][key][int
     * valLen][val]}. Package-private and static so the round-trip can be unit-tested without Ratis.
     */
    @VisibleForTesting
    static void writeEntries(File file, List<byte[][]> entries) throws IOException {
        try (DataOutputStream out =
                new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            out.writeInt(entries.size());
            for (byte[][] entry : entries) {
                byte[] key = entry[0];
                byte[] value = entry[1];
                out.writeInt(key.length);
                out.write(key);
                out.writeInt(value.length);
                out.write(value);
            }
        }
    }

    @VisibleForTesting
    static List<byte[][]> readEntries(File file) throws IOException {
        try (DataInputStream in =
                new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            int count = in.readInt();
            List<byte[][]> entries = new ArrayList<>(Math.max(0, count));
            for (int i = 0; i < count; i++) {
                byte[] key = new byte[in.readInt()];
                in.readFully(key);
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                entries.add(new byte[][] {key, value});
            }
            return entries;
        }
    }

    // ---- leadership callbacks (Ratis EventApi / LeaderEventApi) ----

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId leaderId) {
        RaftLeaderSelector selector = this.leaderSelector;
        if (selector != null) {
            boolean isLeader = groupMemberId.getPeerId().equals(leaderId);
            selector.notifyLeadershipChanged(isLeader);
        }
    }

    @Override
    public void notifyNotLeader(Collection<TransactionContext> pendingEntries) {
        RaftLeaderSelector selector = this.leaderSelector;
        if (selector != null) {
            selector.notifyLeadershipChanged(false);
        }
    }
}
