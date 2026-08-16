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
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DeadCoordinatorEvent;
import org.apache.fluss.server.coordinator.event.NewCoordinatorEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link RaftCoordinatorChangeWatcher} (§5.8, route-1 (c)): the coordinator
 * leader's coordinator-membership events are produced from the in-process apply-watch of a real
 * single-node embedded Ratis leader — the cross-backend replacement for the ZooKeeper
 * CoordinatorChangeWatcher. Verifies a register-leader produces a {@link NewCoordinatorEvent} and
 * an unregister-leader produces a {@link DeadCoordinatorEvent}, in commit order.
 */
class RaftCoordinatorChangeWatcherITCase {

    private static final String LISTENER = "FLUSS";

    @TempDir private Path raftDir;

    @Test
    void testCoordinatorEventsProducedFromApplyWatch() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        MetadataWatchService watchService = new MetadataWatchService();
        RaftMetadataStore leaderStore =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), watchService);
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(leaderStore);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, sm);
        RaftLeaderSelector leaderSelector = new RaftLeaderSelector("n1", server, sm, 20_000);
        sm.setLeaderSelector(leaderSelector);
        leaderSelector.startElectLeaderAsync(() -> {}, t -> {});

        RaftMetadataClient client = null;
        RaftCoordinatorChangeWatcher watcher = null;
        try {
            server.start();
            waitUntilLeaderReady(server, 20_000);

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

            TestingEventManager eventManager = new TestingEventManager();
            watcher = new RaftCoordinatorChangeWatcher(watchService, eventManager);
            watcher.start();

            // ---- register leader -> NewCoordinatorEvent ----
            client.registerCoordinatorLeader(coordinator("cs-1"));
            NewCoordinatorEvent newEvent =
                    (NewCoordinatorEvent) awaitEvent(eventManager, NewCoordinatorEvent.class);
            assertThat(newEvent.getServerId()).isEqualTo("cs-1");

            // ---- unregister leader -> DeadCoordinatorEvent ----
            client.unregisterCoordinatorLeader(coordinator("cs-1"));
            DeadCoordinatorEvent deadEvent =
                    (DeadCoordinatorEvent) awaitEvent(eventManager, DeadCoordinatorEvent.class);
            assertThat(deadEvent.getServerId()).isEqualTo("cs-1");

            // ---- after stop, no further events are produced ----
            watcher.stop();
            eventManager.clearEvents();
            client.registerCoordinatorLeader(coordinator("cs-2"));
            Thread.sleep(200);
            assertThat(eventManager.getEvents()).isEmpty();
        } finally {
            if (watcher != null) {
                watcher.stop();
            }
            if (client != null) {
                client.close();
            }
            leaderSelector.close();
            server.close();
        }
    }

    private static CoordinatorEvent awaitEvent(
            TestingEventManager eventManager, Class<? extends CoordinatorEvent> type)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorEvent> events = eventManager.getEvents();
            for (CoordinatorEvent event : events) {
                if (type.isInstance(event)) {
                    eventManager.clearEvents();
                    return event;
                }
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("No " + type.getSimpleName() + " event was produced");
    }

    private static CoordinatorAddress coordinator(String id) {
        return new CoordinatorAddress(
                id, Endpoint.fromListenersString(LISTENER + "://localhost:9123"));
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
