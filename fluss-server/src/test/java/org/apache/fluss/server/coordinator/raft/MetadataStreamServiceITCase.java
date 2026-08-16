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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for {@link MetadataStreamService} against a real single-node embedded Ratis
 * quorum (§5.8): (1) a contiguous fetch returns committed metadata records read back from the log
 * by index, and (2) a long-poll parked at the high watermark is woken by {@code onApplied} after a
 * new record is committed, returning that record.
 */
class MetadataStreamServiceITCase {

    @TempDir private Path raftDir;

    @Test
    void testContiguousFetchAndLongPollWakeup() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, sm);

        try {
            server.start();
            waitUntilLeaderReady(server, 20_000);

            RaftLog raftLog =
                    server.getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getRaftLog();
            MetadataStreamService stream =
                    new MetadataStreamService(
                            raftLog, () -> currentTerm(server), sm::captureSnapshot);
            // Wire the apply-loop wakeup: parked long-polls are woken automatically when a record
            // commits+applies, WITHOUT the test calling onApplied by hand (§5.8.2, part b).
            sm.setApplyListener(stream::onApplied);

            RaftProperties clientProps = server.buildRaftProperties();
            try (RaftClient client =
                            RaftClient.newBuilder()
                                    .setClientId(ClientId.randomId())
                                    .setRaftGroup(group)
                                    .setProperties(clientProps)
                                    .build();
                    RaftJournalWriter writer = new RaftJournalWriter(client, sm)) {

                append(writer, "db.a", "1");
                append(writer, "db.b", "2");
                append(writer, "db.c", "3");

                // ---- 1. contiguous fetch returns all three metadata records, in order ----
                MetadataStreamService.FetchResult r = stream.fetch(1L, 100, 0);
                assertThat(r.snapshotRequired()).isFalse();
                assertThat(r.leaderEpoch()).isPositive();
                List<MetadataStreamService.StreamRecord> recs = r.records();
                assertThat(recs).hasSize(3);
                assertThat(decodeKey(recs.get(0))).isEqualTo("db.a");
                assertThat(decodeKey(recs.get(1))).isEqualTo("db.b");
                assertThat(decodeKey(recs.get(2))).isEqualTo("db.c");
                // offsets are strictly increasing Raft indices
                assertThat(recs.get(0).offset()).isLessThan(recs.get(1).offset());
                assertThat(recs.get(1).offset()).isLessThan(recs.get(2).offset());
                assertThat(r.nextOffset()).isGreaterThan(recs.get(2).offset());

                // drain to a stable tail (absorbs any trailing Raft config/commit-index settling)
                long from = drainToTail(stream);

                // ---- 2a. a caught-up long-poll parks for ~the budget and returns no records ----
                long t0 = System.currentTimeMillis();
                MetadataStreamService.FetchResult caughtUp = stream.fetch(from, 100, 600);
                long waited = System.currentTimeMillis() - t0;
                assertThat(caughtUp.records()).isEmpty();
                assertThat(waited)
                        .isGreaterThanOrEqualTo(500L); // it actually parked, not busy-return
                from = caughtUp.nextOffset();

                // ---- 2b. a parked long-poll is woken by onApplied when a new record commits ----
                final long parkFrom = from;
                CompletableFuture<MetadataStreamService.FetchResult> parked =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return stream.fetch(parkFrom, 100, 15_000);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                Thread.sleep(300); // let it park
                append(writer, "db.d", "4");
                // NOTE: no manual stream.onApplied(...) here — the apply-loop listener wired via
                // sm.setApplyListener(stream::onApplied) must wake the parked poll on its own.

                MetadataStreamService.FetchResult woken = parked.get(15, TimeUnit.SECONDS);
                // db.d is visible on the stream from parkFrom onward (drain past any config
                // entries)
                assertThat(collectKeysFrom(stream, woken)).contains("db.d");

                // ---- 3. fetchSnapshot serves the terminal-state image tagged with the applied
                // offset (part a). The offset must equal the state machine's lastAppliedIndex, and
                // the image must reflect exactly the four registered tables. ----
                MetadataStreamService.FetchSnapshotResult snap = stream.fetchSnapshot();
                assertThat(snap.leaderEpoch()).isPositive();
                assertThat(snap.snapshotMetadataOffset()).isEqualTo(sm.lastAppliedIndex());
                assertThat(MetadataSnapshot.deserializeEntries(snap.image())).hasSize(4);
            }
        } finally {
            server.close();
        }
    }

    /**
     * Fetches (no wait) advancing the cursor until caught up and stable, returns the next offset.
     */
    private static long drainToTail(MetadataStreamService stream) throws Exception {
        long from = 1L;
        while (true) {
            MetadataStreamService.FetchResult r = stream.fetch(from, 1000, 0);
            if (r.records().isEmpty() && r.nextOffset() == from) {
                return from;
            }
            from = r.nextOffset();
        }
    }

    /**
     * Collects record keys starting from an already-fetched result, draining any remaining tail.
     */
    private static java.util.List<String> collectKeysFrom(
            MetadataStreamService stream, MetadataStreamService.FetchResult first)
            throws Exception {
        java.util.List<String> keys = new java.util.ArrayList<>();
        for (MetadataStreamService.StreamRecord rec : first.records()) {
            keys.add(decodeKey(rec));
        }
        long cursor = first.nextOffset();
        while (true) {
            MetadataStreamService.FetchResult r = stream.fetch(cursor, 1000, 0);
            for (MetadataStreamService.StreamRecord rec : r.records()) {
                keys.add(decodeKey(rec));
            }
            if (r.records().isEmpty() && r.nextOffset() == cursor) {
                return keys;
            }
            cursor = r.nextOffset();
        }
    }

    private static void append(RaftJournalWriter writer, String key, String payload)
            throws Exception {
        writer.append(
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLE,
                                key.getBytes(StandardCharsets.UTF_8),
                                payload.getBytes(StandardCharsets.UTF_8)))
                .get(15, TimeUnit.SECONDS);
    }

    private static String decodeKey(MetadataStreamService.StreamRecord record) {
        MetadataRecord decoded = MetadataRecord.fromBytes(record.bytes());
        return new String(decoded.key(), StandardCharsets.UTF_8);
    }

    private static long currentTerm(RaftMetadataServer server) {
        try {
            return server.getRaftServer()
                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                    .getInfo()
                    .getCurrentTerm();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitUntilLeaderReady(RaftMetadataServer server, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ready =
                    server.getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .isLeaderReady();
            if (ready) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Raft leader was not ready within " + timeoutMs + "ms");
    }
}
