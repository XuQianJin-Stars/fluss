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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;
import java.util.function.LongConsumer;

/**
 * Subscriber-side counterpart of {@link MetadataStreamService} (§5.8): pulls the ordered metadata
 * stream from the current leader by offset and replays it into a local {@link RaftMetadataStore},
 * exactly the deterministic apply already used by Raft followers (§5.1.4) — reused verbatim. This
 * is the cross-process replacement for ZooKeeper watches on TabletServers / standby Coordinators /
 * clients.
 *
 * <p>Cursor model (§5.8.1): the subscriber owns its cursor. {@link #nextFetchOffset} is the next
 * metadata offset it wants; everything strictly below it is applied to the local store. On each
 * round it calls {@link MetadataStreamGateway#fetch}, applies the returned records in order, and
 * advances the cursor to {@link MetadataStreamService.FetchResult#nextOffset()} (which also skips
 * non-record Raft config entries).
 *
 * <p>Three recovery paths, all owned here:
 *
 * <ul>
 *   <li><b>Snapshot bootstrap</b> — when {@code fetch} reports {@code snapshot_required} (the
 *       requested offset was compacted out of the log), the subscriber pulls a full terminal-state
 *       image via {@link MetadataStreamGateway#fetchSnapshot()}, restores the local store, sets its
 *       applied offset to {@code snapshot_metadata_offset}, and resumes at {@code + 1}. The atomic
 *       image↔offset binding (§5.8.1 point 4) makes this seam never off-by-one.
 *   <li><b>Reconnect</b> — an {@link IOException} from the gateway (leader unreachable / changed)
 *       is caught by {@link #runLoop()}, which backs off and retries from the same cursor
 *       (idempotent, because the leader re-serves the same range from its identical replicated
 *       log).
 *   <li><b>Fencing</b> — when the response {@code leaderEpoch} advances past the last one seen, a
 *       coordinator failover happened; {@link #leaderEpochListener} is notified so the caller can
 *       revalidate routing/leadership.
 * </ul>
 *
 * <p>Single-consumer: {@link #pollOnce()} / {@link #runLoop()} must be driven by one thread. Cursor
 * fields are {@code volatile} only so observability getters can be read from another thread.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
@NotThreadSafe
public class MetadataStreamSubscriber implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStreamSubscriber.class);

    /** Metadata offsets are Raft indices; the first possible record index is 1. */
    public static final long INITIAL_FETCH_OFFSET = 1L;

    private final MetadataStreamGateway gateway;
    private final RaftMetadataStore localStore;
    private final int maxRecordsPerFetch;
    private final long longPollMs;
    private final long reconnectBackoffMs;
    @Nullable private final LongConsumer leaderEpochListener;

    private volatile long nextFetchOffset = INITIAL_FETCH_OFFSET;
    private volatile long knownLeaderEpoch = -1L;
    private volatile boolean needSnapshot = false;
    private volatile boolean running = false;

    public MetadataStreamSubscriber(
            MetadataStreamGateway gateway,
            RaftMetadataStore localStore,
            int maxRecordsPerFetch,
            long longPollMs,
            long reconnectBackoffMs,
            @Nullable LongConsumer leaderEpochListener) {
        this.gateway = gateway;
        this.localStore = localStore;
        this.maxRecordsPerFetch = maxRecordsPerFetch;
        this.longPollMs = longPollMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.leaderEpochListener = leaderEpochListener;
    }

    /** The next metadata offset the subscriber will request (its resume point). */
    public long nextFetchOffset() {
        return nextFetchOffset;
    }

    /** The highest metadata offset fully applied to the local store ({@code -1} if none yet). */
    public long appliedOffset() {
        return nextFetchOffset - 1;
    }

    /** The most recent leader epoch (Raft term) observed on the stream ({@code -1} if none yet). */
    public long knownLeaderEpoch() {
        return knownLeaderEpoch;
    }

    /**
     * Bootstraps the local store from a full leader snapshot and aligns the cursor so the delta
     * stream resumes exactly at {@code snapshot_metadata_offset + 1}. Used on cold start and to
     * recover from a {@code snapshot_required} response.
     */
    public void bootstrapFromSnapshot() throws IOException {
        MetadataStreamService.FetchSnapshotResult snap = gateway.fetchSnapshot();
        List<byte[][]> entries = MetadataSnapshot.deserializeEntries(snap.image());
        localStore.restoreFromSnapshot(entries);
        nextFetchOffset = snap.snapshotMetadataOffset() + 1;
        needSnapshot = false;
        maybeAdvanceEpoch(snap.leaderEpoch());
        LOG.info(
                "Bootstrapped metadata subscriber from snapshot: {} entries, snapshotMetadataOffset={}, resuming at {}",
                entries.size(),
                snap.snapshotMetadataOffset(),
                nextFetchOffset);
    }

    /**
     * Performs one fetch+replay round from the current cursor and returns the number of records
     * applied. Long-polls up to the configured budget when caught up. A {@code snapshot_required}
     * response applies no records and arms a snapshot bootstrap for the next call.
     */
    public int pollOnce() throws IOException {
        if (needSnapshot) {
            bootstrapFromSnapshot();
            return 0;
        }
        MetadataStreamService.FetchResult result =
                gateway.fetch(nextFetchOffset, maxRecordsPerFetch, longPollMs);
        maybeAdvanceEpoch(result.leaderEpoch());
        if (result.snapshotRequired()) {
            needSnapshot = true;
            LOG.info(
                    "Leader reported snapshot_required at fromOffset={}; will bootstrap from snapshot",
                    nextFetchOffset);
            return 0;
        }
        for (MetadataStreamService.StreamRecord record : result.records()) {
            localStore.apply(MetadataRecord.fromBytes(record.bytes()));
        }
        nextFetchOffset = result.nextOffset();
        return result.records().size();
    }

    /**
     * Runs the replay loop until {@link #close()}. Transport failures ({@link IOException}) trigger
     * a bounded backoff and retry from the same cursor (reconnect). Intended to run on a dedicated
     * subscriber thread.
     */
    public void runLoop() {
        running = true;
        LOG.info("Metadata stream subscriber loop started at offset {}", nextFetchOffset);
        while (running) {
            try {
                pollOnce();
            } catch (IOException e) {
                if (!running) {
                    break;
                }
                LOG.warn(
                        "Metadata stream fetch failed at offset {}; reconnecting after {}ms",
                        nextFetchOffset,
                        reconnectBackoffMs,
                        e);
                sleepBackoff();
            } catch (RuntimeException e) {
                LOG.error("Unexpected error replaying metadata stream; stopping subscriber", e);
                throw e;
            }
        }
        LOG.info("Metadata stream subscriber loop stopped at offset {}", nextFetchOffset);
    }

    private void sleepBackoff() {
        try {
            Thread.sleep(reconnectBackoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private void maybeAdvanceEpoch(long epoch) {
        if (epoch > knownLeaderEpoch) {
            long previous = knownLeaderEpoch;
            knownLeaderEpoch = epoch;
            if (previous >= 0) {
                LOG.info(
                        "Observed leader epoch change {} -> {} on metadata stream",
                        previous,
                        epoch);
            }
            LongConsumer listener = leaderEpochListener;
            if (listener != null) {
                listener.accept(epoch);
            }
        }
    }

    @Override
    public void close() {
        running = false;
    }
}
