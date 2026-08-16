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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link MetadataStreamCacheApplier} (§5.8, T6-2 first slice): a {@link
 * CoordinatorMetadataCache} is kept in sync purely by consuming the metadata stream from a real
 * single-node embedded Ratis leader — the cross-process replacement for the ZooKeeper-watch feed.
 * Verifies (1) tablet-server registrations + the coordinator leader project into the cache, and (2)
 * a {@code FENCE_TABLET_SERVER} removes a server from the cache's alive set.
 */
class MetadataStreamCacheApplierITCase {

    private static final String LISTENER = "FLUSS";

    @TempDir private Path raftDir;

    @Test
    void testCacheProjectedFromStream() throws Exception {
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
        RaftLeaderSelector leaderSelector = new RaftLeaderSelector("n1", server, sm, 20_000);
        sm.setLeaderSelector(leaderSelector);
        leaderSelector.startElectLeaderAsync(() -> {}, t -> {});

        RaftMetadataClient client = null;
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
            RaftClient raftClient =
                    RaftClient.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setRaftGroup(group)
                            .setProperties(clientProps)
                            .build();
            RaftJournalWriter writer = new RaftJournalWriter(raftClient, sm);
            client =
                    new RaftMetadataClient(
                            writer,
                            leaderStore,
                            leaderSelector,
                            new RaftSequenceIDCounter("table", writer, leaderStore, 1),
                            new RaftSequenceIDCounter("partition", writer, leaderStore, 1),
                            new RaftSequenceIDCounter("writer", writer, leaderStore, 1));

            // Leader writes realistic membership records.
            client.registerTabletServer(1, tabletServer("rack1", 1));
            client.registerTabletServer(2, tabletServer("rack2", 2));
            client.registerTabletServer(3, tabletServer("rack3", 3));
            client.registerCoordinatorLeader(
                    new CoordinatorAddress(
                            "100", Endpoint.fromListenersString(LISTENER + "://localhost:9124")));

            // Subscriber-fed follower store + the cache projected from it by the applier.
            InMemoryMetadataKvStore subKv = new InMemoryMetadataKvStore();
            RaftMetadataStore subStore = new RaftMetadataStore(subKv, new MetadataWatchService());
            MetadataStreamSubscriber subscriber =
                    new MetadataStreamSubscriber(stream, subStore, 100, 0L, 20L, null);
            CoordinatorMetadataCache cache = new CoordinatorMetadataCache();
            MetadataStreamCacheApplier applier = new MetadataStreamCacheApplier(subStore, cache);

            // ---- 1. project the streamed membership into the cache ----
            drainUntilCaughtUp(subscriber, sm);
            applier.refresh();
            assertThat(cache.isAliveTabletServer(1)).isTrue();
            assertThat(cache.isAliveTabletServer(2)).isTrue();
            assertThat(cache.isAliveTabletServer(3)).isTrue();
            assertThat(cache.getAllAliveTabletServers(LISTENER)).containsOnlyKeys(1, 2, 3);
            assertThat(cache.getCoordinatorServer(LISTENER)).isNotNull();

            // ---- 2. FENCE_TABLET_SERVER(2) removes it from the alive set ----
            writer.append(
                            new MetadataRecord(
                                    MetadataRecord.Type.FENCE_TABLET_SERVER,
                                    serverIdKey(2),
                                    new byte[0]))
                    .get(15, TimeUnit.SECONDS);
            drainUntilCaughtUp(subscriber, sm);
            applier.refresh();
            assertThat(cache.isAliveTabletServer(2)).isFalse();
            assertThat(cache.getAllAliveTabletServers(LISTENER)).containsOnlyKeys(1, 3);
        } finally {
            if (client != null) {
                client.close();
            }
            leaderSelector.close();
            server.close();
        }
    }

    /** Polls the subscriber until it has applied everything the leader has committed. */
    private static void drainUntilCaughtUp(
            MetadataStreamSubscriber subscriber, FlussMetadataStateMachine sm) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            subscriber.pollOnce();
            if (subscriber.appliedOffset() >= sm.lastAppliedIndex()) {
                return;
            }
        }
        throw new IllegalStateException(
                "Subscriber did not catch up: applied="
                        + subscriber.appliedOffset()
                        + " leader="
                        + sm.lastAppliedIndex());
    }

    private static TabletServerRegistration tabletServer(String rack, int id) {
        return new TabletServerRegistration(
                rack,
                Endpoint.fromListenersString(LISTENER + "://localhost:" + (9000 + id)),
                10_000L + id);
    }

    private static byte[] serverIdKey(int serverId) {
        return ByteBuffer.allocate(4).putInt(serverId).array();
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
