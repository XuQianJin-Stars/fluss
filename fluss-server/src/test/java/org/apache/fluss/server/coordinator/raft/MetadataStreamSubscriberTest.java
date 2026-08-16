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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MetadataStreamSubscriber} against a scriptable in-memory {@link
 * MetadataStreamGateway} fake. Covers the four recovery paths owned by the subscriber (§5.8):
 * ordered replay, snapshot bootstrap with exact cursor alignment, {@code snapshot_required}
 * re-bootstrap, leader-epoch fencing notification, and reconnect after a transport failure.
 */
class MetadataStreamSubscriberTest {

    @Test
    void testReplayAppliesRecordsInOrderAndAdvancesCursor() throws Exception {
        FakeGateway gateway = new FakeGateway(7L);
        gateway.appendRecord(1L, tableRecord("t1", "v1"));
        gateway.appendRecord(2L, tableRecord("t2", "v2"));
        gateway.appendRecord(3L, tableRecord("t3", "v3"));

        InMemoryMetadataKvStore kv = new InMemoryMetadataKvStore();
        RaftMetadataStore store = new RaftMetadataStore(kv, new MetadataWatchService());
        MetadataStreamSubscriber subscriber = newSubscriber(gateway, store);

        int applied = subscriber.pollOnce();

        assertThat(applied).isEqualTo(3);
        assertThat(kv.size()).isEqualTo(3);
        assertThat(subscriber.appliedOffset()).isEqualTo(3L);
        assertThat(subscriber.nextFetchOffset()).isEqualTo(4L);
        assertThat(subscriber.knownLeaderEpoch()).isEqualTo(7L);
        assertThat(store.get(MetadataKeys.keyOf(tableRecord("t2", "v2"))))
                .isEqualTo("v2".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testSnapshotBootstrapAlignsCursorToOffsetPlusOne() throws Exception {
        FakeGateway gateway = new FakeGateway(4L);
        gateway.setSnapshot(
                10L, tableRecord("a", "1"), tableRecord("b", "2"), tableRecord("c", "3"));

        InMemoryMetadataKvStore kv = new InMemoryMetadataKvStore();
        RaftMetadataStore store = new RaftMetadataStore(kv, new MetadataWatchService());
        MetadataStreamSubscriber subscriber = newSubscriber(gateway, store);

        subscriber.bootstrapFromSnapshot();

        assertThat(kv.size()).isEqualTo(3);
        // applied offset == snapshot_metadata_offset, resume exactly one past it (no off-by-one)
        assertThat(subscriber.appliedOffset()).isEqualTo(10L);
        assertThat(subscriber.nextFetchOffset()).isEqualTo(11L);
        assertThat(subscriber.knownLeaderEpoch()).isEqualTo(4L);
    }

    @Test
    void testSnapshotRequiredTriggersBootstrapThenResumesDelta() throws Exception {
        FakeGateway gateway = new FakeGateway(2L);
        // Log starts at offset 100 (everything below was compacted); a fresh subscriber requesting
        // from 1 must be told snapshot_required.
        gateway.setLogStartOffset(100L);
        gateway.setSnapshot(99L, tableRecord("old", "x"));
        gateway.appendRecord(100L, tableRecord("new", "y"));

        InMemoryMetadataKvStore kv = new InMemoryMetadataKvStore();
        RaftMetadataStore store = new RaftMetadataStore(kv, new MetadataWatchService());
        MetadataStreamSubscriber subscriber = newSubscriber(gateway, store);

        // 1st poll from offset 1 -> snapshot_required, no records applied, bootstrap armed.
        assertThat(subscriber.pollOnce()).isEqualTo(0);
        assertThat(kv.size()).isEqualTo(0);

        // 2nd poll -> bootstraps from the snapshot (offset 99), cursor -> 100.
        assertThat(subscriber.pollOnce()).isEqualTo(0);
        assertThat(kv.size()).isEqualTo(1);
        assertThat(subscriber.nextFetchOffset()).isEqualTo(100L);

        // 3rd poll -> resumes the delta stream and applies the record at 100.
        assertThat(subscriber.pollOnce()).isEqualTo(1);
        assertThat(kv.size()).isEqualTo(2);
        assertThat(subscriber.appliedOffset()).isEqualTo(100L);
    }

    @Test
    void testLeaderEpochChangeNotifiesListener() throws Exception {
        FakeGateway gateway = new FakeGateway(5L);
        gateway.appendRecord(1L, tableRecord("t1", "v1"));

        List<Long> observedEpochs = new ArrayList<>();
        InMemoryMetadataKvStore kv = new InMemoryMetadataKvStore();
        RaftMetadataStore store = new RaftMetadataStore(kv, new MetadataWatchService());
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(gateway, store, 100, 0L, 5L, observedEpochs::add);

        subscriber.pollOnce();
        assertThat(observedEpochs).containsExactly(5L);

        // simulate a coordinator failover: epoch advances, and a fetch from a stale cursor is fine.
        gateway.setLeaderEpoch(9L);
        gateway.appendRecord(2L, tableRecord("t2", "v2"));
        subscriber.pollOnce();

        assertThat(observedEpochs).containsExactly(5L, 9L);
        assertThat(subscriber.knownLeaderEpoch()).isEqualTo(9L);
    }

    @Test
    void testRunLoopReconnectsAfterTransportFailure() {
        FakeGateway gateway = new FakeGateway(3L);
        gateway.failNextFetches(2); // first two fetches throw IOException -> reconnect+retry
        gateway.appendRecord(1L, tableRecord("t1", "v1"));
        gateway.appendRecord(2L, tableRecord("t2", "v2"));

        InMemoryMetadataKvStore kv = new InMemoryMetadataKvStore();
        RaftMetadataStore store = new RaftMetadataStore(kv, new MetadataWatchService());
        // short long-poll so the loop spins; short backoff so reconnect is quick.
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(gateway, store, 100, 5L, 10L, null);

        Thread loop = new Thread(subscriber::runLoop, "subscriber-loop");
        loop.setDaemon(true);
        loop.start();
        try {
            long deadline = System.currentTimeMillis() + 5_000;
            while (kv.size() < 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertThat(kv.size()).isEqualTo(2);
            assertThat(gateway.fetchAttempts()).isGreaterThanOrEqualTo(3); // 2 failed + >=1 success
            assertThat(subscriber.appliedOffset()).isEqualTo(2L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            subscriber.close();
        }
    }

    private static MetadataStreamSubscriber newSubscriber(
            MetadataStreamGateway gateway, RaftMetadataStore store) {
        return new MetadataStreamSubscriber(gateway, store, 100, 0L, 5L, null);
    }

    private static MetadataRecord tableRecord(String key, String payload) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_TABLE,
                key.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    /** Scriptable in-memory {@link MetadataStreamGateway} with offset-addressed records. */
    private static final class FakeGateway implements MetadataStreamGateway {
        private final List<long[]> offsets = new ArrayList<>(); // parallel to records
        private final List<MetadataStreamService.StreamRecord> records = new ArrayList<>();
        private volatile long leaderEpoch;
        private volatile long logStartOffset = 1L;
        private volatile long snapshotMetadataOffset = -1L;
        private byte[] snapshotImage = MetadataSnapshot.serializeEntries(new ArrayList<>());
        private final AtomicInteger failFetches = new AtomicInteger(0);
        private final AtomicInteger fetchAttempts = new AtomicInteger(0);

        FakeGateway(long leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
        }

        void setLeaderEpoch(long epoch) {
            this.leaderEpoch = epoch;
        }

        void setLogStartOffset(long start) {
            this.logStartOffset = start;
        }

        void failNextFetches(int n) {
            failFetches.set(n);
        }

        int fetchAttempts() {
            return fetchAttempts.get();
        }

        void appendRecord(long offset, MetadataRecord record) {
            offsets.add(new long[] {offset});
            records.add(new MetadataStreamService.StreamRecord(offset, record.toBytes()));
        }

        void setSnapshot(long offset, MetadataRecord... entries) {
            this.snapshotMetadataOffset = offset;
            List<byte[][]> kv = new ArrayList<>();
            for (MetadataRecord r : entries) {
                kv.add(new byte[][] {MetadataKeys.keyOf(r), r.payload()});
            }
            this.snapshotImage = MetadataSnapshot.serializeEntries(kv);
        }

        @Override
        public MetadataStreamService.FetchResult fetch(
                long fromOffset, int maxRecords, long maxWaitMs) throws IOException {
            fetchAttempts.incrementAndGet();
            if (failFetches.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw new IOException("simulated leader unreachable");
            }
            if (fromOffset < logStartOffset) {
                return new MetadataStreamService.FetchResult(
                        leaderEpoch, highOffset(), new ArrayList<>(), fromOffset, true);
            }
            List<MetadataStreamService.StreamRecord> out = new ArrayList<>();
            long next = fromOffset;
            for (int i = 0; i < records.size() && out.size() < maxRecords; i++) {
                long off = offsets.get(i)[0];
                if (off >= fromOffset) {
                    out.add(records.get(i));
                    next = off + 1;
                }
            }
            return new MetadataStreamService.FetchResult(
                    leaderEpoch, highOffset(), out, next, false);
        }

        @Override
        public MetadataStreamService.FetchSnapshotResult fetchSnapshot() {
            return new MetadataStreamService.FetchSnapshotResult(
                    leaderEpoch, snapshotMetadataOffset, snapshotImage);
        }

        private long highOffset() {
            long high = logStartOffset - 1;
            for (long[] o : offsets) {
                high = Math.max(high, o[0]);
            }
            return high;
        }
    }
}
