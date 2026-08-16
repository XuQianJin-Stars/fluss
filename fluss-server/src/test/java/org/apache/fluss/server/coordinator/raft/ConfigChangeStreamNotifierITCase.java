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
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.config.cluster.ServerReconfigurable;
import org.apache.fluss.server.DynamicConfigManager;
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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link ConfigChangeStreamNotifier} (§5.8, T6-2 config-change slice): a
 * Raft-backed {@link DynamicConfigManager}'s dynamic server config is kept in sync purely by
 * consuming the metadata stream from a real single-node embedded Ratis leader — the cross-process
 * replacement for the ZooKeeper {@code ZkNodeChangeNotificationWatcher}. Verifies (1) an entity
 * config written on the leader propagates through the follower store + notifier into the manager's
 * registered {@code ServerReconfigurable}, and (2) a subsequent overwrite propagates the new value.
 */
class ConfigChangeStreamNotifierITCase {

    private static final String RATE_LIMITER_KEY =
            ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key();

    @TempDir private Path raftDir;

    @Test
    void testDynamicConfigProjectedFromStream() throws Exception {
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

            MetadataStreamService stream =
                    new MetadataStreamService(
                            server.getRaftServer()
                                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                                    .getRaftLog(),
                            () -> currentTerm(server),
                            sm::captureSnapshot);
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

            // A Raft-backed DynamicConfigManager: no ZK watcher (neutral ctor), config fed purely
            // by the stream notifier. A capturing reconfigurable records the applied config.
            DynamicConfigManager configManager =
                    new DynamicConfigManager(client, new Configuration(), false);
            AtomicReference<MemorySize> applied = new AtomicReference<>();
            configManager.register(
                    new ServerReconfigurable() {
                        @Override
                        public void validate(Configuration newConfig) {}

                        @Override
                        public void reconfigure(Configuration newConfig) {
                            applied.set(
                                    newConfig.get(
                                            ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC));
                        }
                    });

            // Subscriber-fed follower store + the notifier projecting it into the manager.
            RaftMetadataStore subStore =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            MetadataStreamSubscriber subscriber =
                    new MetadataStreamSubscriber(stream, subStore, 100, 0L, 20L, null);
            ConfigChangeStreamNotifier notifier =
                    new ConfigChangeStreamNotifier(subStore, configManager::reloadDynamicConfig);

            // ---- 1. leader writes an entity config -> notifier applies it to the manager ----
            client.upsertServerEntityConfig(Collections.singletonMap(RATE_LIMITER_KEY, "200mb"));
            drainUntilCaughtUp(subscriber, sm);
            notifier.refresh();

            assertThat(applied.get()).isEqualTo(MemorySize.parse("200mb"));
            assertThat(configManager.describeConfigs())
                    .anySatisfy(
                            entry -> {
                                assertThat(entry.key()).isEqualTo(RATE_LIMITER_KEY);
                                assertThat(entry.value()).isEqualTo("200mb");
                            });

            // ---- 2. leader overwrites the entity config -> notifier applies the new value ----
            client.upsertServerEntityConfig(Collections.singletonMap(RATE_LIMITER_KEY, "500mb"));
            drainUntilCaughtUp(subscriber, sm);
            notifier.refresh();

            assertThat(applied.get()).isEqualTo(MemorySize.parse("500mb"));
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
