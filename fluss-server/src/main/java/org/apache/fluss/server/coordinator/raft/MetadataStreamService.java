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
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Leader-side service that streams committed metadata records to subscribers (TabletServers,
 * standby Coordinators, clients) by metadata offset — the KRaft {@code MetadataLoader} model as a
 * Fluss service. Replaces cross-process ZooKeeper watches (§5.8).
 *
 * <p><b>Authoritative source = the Raft log itself, read back by index.</b> The metadata offset of
 * a record IS its Raft log index; because the log is replicated identically to every voter, any
 * elected leader can serve any historical {@code [fromOffset, high]} range from its own log without
 * depending on in-memory state accumulated while it was a follower — this is what makes the stream
 * leader-failover-safe. No in-memory delta buffer is kept.
 *
 * <p>{@link #onApplied(long)} is a pure wakeup signal (fired from the apply loop via {@code
 * MetadataWatchService}); it never carries record content. Long-poll waiters parked at the high
 * watermark are woken to re-read the freshly-appended tail from the log.
 *
 * <p>Records too old to be in the log (compacted into a snapshot) are reported via {@link
 * FetchResult#snapshotRequired()} so the subscriber bootstraps from a snapshot (§5.2.5) and then
 * resumes streaming at {@code snapshotOffset + 1}.
 *
 * <p>Thread-safe. Runs on the metadata-stream pool (§5.6); read-only w.r.t. the log.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public class MetadataStreamService implements MetadataStreamGateway {

    /** Default cap on records returned per fetch when the caller does not specify one. */
    public static final int DEFAULT_MAX_RECORDS = 1000;

    private final RaftLog raftLog;
    private final LongSupplier currentEpoch; // Raft leader term (coordinator epoch, §5.1.6)
    // Captures a terminal-state KvStore image + its metadata offset, atomically (§5.8.1 point 4).
    // Null when this service is constructed without snapshot support (fetchSnapshot then throws).
    @Nullable private final Supplier<MetadataSnapshot> snapshotSupplier;
    private final Object wakeupLock = new Object();

    // False once this node is no longer the leader; parked long-polls return promptly so
    // subscribers
    // re-target the new leader.
    private volatile boolean serving = true;

    /**
     * Creates a service without snapshot-bootstrap support; {@link #fetchSnapshot()} will throw.
     * Useful for tests/callers that only exercise the delta stream.
     */
    public MetadataStreamService(RaftLog raftLog, LongSupplier currentEpoch) {
        this(raftLog, currentEpoch, null);
    }

    /**
     * @param raftLog authoritative source read back by index (§5.8.1)
     * @param currentEpoch supplies the current Raft leader term (coordinator epoch, §5.1.6)
     * @param snapshotSupplier captures the KvStore terminal-state image + its metadata offset for
     *     {@link #fetchSnapshot()} (typically {@code FlussMetadataStateMachine::captureSnapshot});
     *     {@code null} disables snapshot bootstrap
     */
    public MetadataStreamService(
            RaftLog raftLog,
            LongSupplier currentEpoch,
            @Nullable Supplier<MetadataSnapshot> snapshotSupplier) {
        this.raftLog = raftLog;
        this.currentEpoch = currentEpoch;
        this.snapshotSupplier = snapshotSupplier;
    }

    /**
     * Fetches an ordered, contiguous run of committed metadata records starting at {@code
     * fromOffset}. Long-polls up to {@code maxWaitMs} when the subscriber is already caught up.
     *
     * @param fromOffset the next metadata offset (Raft index) the subscriber wants (applied + 1)
     * @param maxRecords maximum records to return ({@code <= 0} uses {@link #DEFAULT_MAX_RECORDS})
     * @param maxWaitMs long-poll budget in millis when no new records are available ({@code <= 0} =
     *     return immediately)
     */
    @Override
    public FetchResult fetch(long fromOffset, int maxRecords, long maxWaitMs) throws IOException {
        int limit = maxRecords <= 0 ? DEFAULT_MAX_RECORDS : maxRecords;
        long epoch = currentEpoch.getAsLong();

        // Records at or below the log start were compacted into a snapshot: the subscriber must
        // bootstrap from a snapshot before it can resume the delta stream (§5.8.1 point 3/4).
        if (fromOffset < raftLog.getStartIndex()) {
            return new FetchResult(
                    epoch,
                    raftLog.getLastCommittedIndex(),
                    Collections.emptyList(),
                    fromOffset,
                    true);
        }

        // Caught up: park until a new commit wakes us or the deadline/leadership change fires.
        if (fromOffset > raftLog.getLastCommittedIndex() && maxWaitMs > 0) {
            awaitNewData(fromOffset, maxWaitMs);
        }

        long high = raftLog.getLastCommittedIndex();
        List<StreamRecord> records = new ArrayList<>();
        long scannedThrough = fromOffset - 1;
        for (long index = fromOffset; index <= high && records.size() < limit; index++) {
            LogEntryProto entry = raftLog.get(index);
            if (entry.hasStateMachineLogEntry()) {
                byte[] data = entry.getStateMachineLogEntry().getLogData().toByteArray();
                records.add(new StreamRecord(index, data));
            }
            // Non-state-machine entries (Raft config/metadata) carry no metadata record but still
            // consume an offset; the subscriber advances past them via nextOffset below.
            scannedThrough = index;
        }
        long nextOffset = scannedThrough + 1;
        return new FetchResult(epoch, high, records, nextOffset, false);
    }

    /**
     * Serves a full metadata snapshot for a subscriber whose {@code fromOffset} was reported {@link
     * FetchResult#snapshotRequired() snapshot_required} (records too old, already compacted out of
     * the Raft log). The returned image is a terminal-state KvStore dump tagged with {@code
     * snapshot_metadata_offset} (§5.8.1 point 4). After loading it, the subscriber sets its applied
     * offset to that value and resumes the delta stream at {@code snapshot_metadata_offset + 1};
     * because the image and its offset are captured atomically (see {@link
     * FlussMetadataStateMachine#captureSnapshot()}), the snapshot→delta seam is never off-by-one.
     *
     * @throws UnsupportedOperationException if this service was constructed without snapshot
     *     support
     */
    @Override
    public FetchSnapshotResult fetchSnapshot() {
        if (snapshotSupplier == null) {
            throw new UnsupportedOperationException(
                    "MetadataStreamService was constructed without snapshot support");
        }
        long epoch = currentEpoch.getAsLong();
        MetadataSnapshot snapshot = snapshotSupplier.get();
        return new FetchSnapshotResult(epoch, snapshot.snapshotMetadataOffset(), snapshot.image());
    }

    /**
     * Wakeup hook, called from the apply loop (via {@code MetadataWatchService}) after a record is
     * committed+applied. Content is NOT passed here — parked waiters re-read the log tail.
     */
    public void onApplied(long metadataOffset) {
        synchronized (wakeupLock) {
            wakeupLock.notifyAll();
        }
    }

    /**
     * Signals that this node lost leadership; wakes all parked long-polls so they return and the
     * subscribers re-target the new leader (the response epoch will reflect the change).
     */
    public void notifyLeadershipLost() {
        serving = false;
        synchronized (wakeupLock) {
            wakeupLock.notifyAll();
        }
    }

    private void awaitNewData(long fromOffset, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        long startEpoch = currentEpoch.getAsLong();
        synchronized (wakeupLock) {
            while (serving
                    && fromOffset > raftLog.getLastCommittedIndex()
                    && currentEpoch.getAsLong() == startEpoch) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return;
                }
                try {
                    wakeupLock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** A single metadata record on the stream: its offset (Raft index) and serialized bytes. */
    @Internal
    public static final class StreamRecord {
        private final long offset;
        private final byte[] bytes;

        public StreamRecord(long offset, byte[] bytes) {
            this.offset = offset;
            this.bytes = bytes;
        }

        public long offset() {
            return offset;
        }

        public byte[] bytes() {
            return bytes;
        }
    }

    /** Result of {@link #fetch}. */
    @Internal
    public static final class FetchResult {
        private final long leaderEpoch;
        private final long highMetadataOffset;
        private final List<StreamRecord> records;
        private final long nextOffset;
        private final boolean snapshotRequired;

        public FetchResult(
                long leaderEpoch,
                long highMetadataOffset,
                List<StreamRecord> records,
                long nextOffset,
                boolean snapshotRequired) {
            this.leaderEpoch = leaderEpoch;
            this.highMetadataOffset = highMetadataOffset;
            this.records = records;
            this.nextOffset = nextOffset;
            this.snapshotRequired = snapshotRequired;
        }

        public long leaderEpoch() {
            return leaderEpoch;
        }

        public long highMetadataOffset() {
            return highMetadataOffset;
        }

        public List<StreamRecord> records() {
            return records;
        }

        /** The offset the subscriber should request next (skips consumed non-record entries). */
        public long nextOffset() {
            return nextOffset;
        }

        public boolean snapshotRequired() {
            return snapshotRequired;
        }
    }

    /** Result of {@link #fetchSnapshot}: the terminal-state image and the offset it reflects. */
    @Internal
    public static final class FetchSnapshotResult {
        private final long leaderEpoch;
        private final long snapshotMetadataOffset;
        private final byte[] image;

        public FetchSnapshotResult(long leaderEpoch, long snapshotMetadataOffset, byte[] image) {
            this.leaderEpoch = leaderEpoch;
            this.snapshotMetadataOffset = snapshotMetadataOffset;
            this.image = image;
        }

        public long leaderEpoch() {
            return leaderEpoch;
        }

        /** The offset the subscriber must resume the delta stream from, minus one. */
        public long snapshotMetadataOffset() {
            return snapshotMetadataOffset;
        }

        /** Serialized KvStore terminal state; see {@link MetadataSnapshot} for the wire format. */
        public byte[] image() {
            return image;
        }
    }
}
