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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link MetadataStreamSubscriber} replaying from a real single-node embedded
 * Ratis leader through {@link MetadataStreamService} (§5.8, part d). Verifies (1) delta replay
 * rebuilds the exact leader KvStore on the subscriber, and (2) snapshot bootstrap aligns the cursor
 * so a subsequent delta fetch picks up new records with no gap or duplicate.
 */
class MetadataStreamSubscriberITCase {

    @TempDir private Path raftDir;

    @Test
    void testReplayAndSnapshotBootstrapAgainstRealLeader() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataStore leaderStore =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(leaderStore);
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

                // ---- 1. delta replay rebuilds the leader store on the subscriber ----
                InMemoryMetadataKvStore subKv = new InMemoryMetadataKvStore();
                RaftMetadataStore subStore =
                        new RaftMetadataStore(subKv, new MetadataWatchService());
                MetadataStreamSubscriber subscriber =
                        new MetadataStreamSubscriber(stream, subStore, 100, 0L, 20L, null);

                drainSubscriber(subscriber, subKv, 3);
                assertThat(subKv.size()).isEqualTo(3);
                assertThat(subscriber.knownLeaderEpoch()).isPositive();
                assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("db.b", "2"))))
                        .isEqualTo("2".getBytes(StandardCharsets.UTF_8));

                // ---- 2. a fresh subscriber cold-starts via snapshot bootstrap, then resumes ----
                InMemoryMetadataKvStore bootKv = new InMemoryMetadataKvStore();
                RaftMetadataStore bootStore =
                        new RaftMetadataStore(bootKv, new MetadataWatchService());
                MetadataStreamSubscriber bootSubscriber =
                        new MetadataStreamSubscriber(stream, bootStore, 100, 0L, 20L, null);

                bootSubscriber.bootstrapFromSnapshot();
                assertThat(bootKv.size()).isEqualTo(3);
                assertThat(bootSubscriber.appliedOffset()).isEqualTo(sm.lastAppliedIndex());

                // new record after the snapshot must be picked up by the resumed delta stream.
                append(writer, "db.d", "4");
                drainSubscriber(bootSubscriber, bootKv, 4);
                assertThat(bootKv.size()).isEqualTo(4);
                assertThat(bootStore.get(MetadataKeys.keyOf(tableRecord("db.d", "4"))))
                        .isEqualTo("4".getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            server.close();
        }
    }

    /** Polls until the subscriber's local store reaches {@code expectedSize} entries. */
    private static void drainSubscriber(
            MetadataStreamSubscriber subscriber, InMemoryMetadataKvStore kv, int expectedSize)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            subscriber.pollOnce();
            if (kv.size() >= expectedSize) {
                return;
            }
        }
        throw new IllegalStateException(
                "Subscriber did not reach " + expectedSize + " entries; got " + kv.size());
    }

    private static void append(RaftJournalWriter writer, String key, String payload)
            throws Exception {
        writer.append(tableRecord(key, payload)).get(15, TimeUnit.SECONDS);
    }

    private static MetadataRecord tableRecord(String key, String payload) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_TABLE,
                key.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8));
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
