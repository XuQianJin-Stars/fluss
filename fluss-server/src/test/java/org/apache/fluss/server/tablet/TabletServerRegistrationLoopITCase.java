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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.coordinator.raft.RaftCoordinatorMetadataServices;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link TabletServerRegistrationLoop} against a real single-node embedded
 * Ratis leader (via {@link RaftCoordinatorMetadataServices}). Verifies the §5.4.2 client mechanics:
 * registration adds the server to the leader's membership, heartbeats renew the lease, and once the
 * leader fences a server whose lease expired the next heartbeat observes {@code registered=false}
 * and the loop re-registers.
 */
class TabletServerRegistrationLoopITCase {

    @TempDir private Path raftDir;

    /**
     * A gateway that routes only the registration/heartbeat RPCs to the real leader-side handler.
     */
    private static final class DelegatingGateway extends TestCoordinatorGateway {
        private final RaftCoordinatorMetadataServices services;

        DelegatingGateway(RaftCoordinatorMetadataServices services) {
            this.services = services;
        }

        @Override
        public CompletableFuture<RegisterTabletServerResponse> registerTabletServer(
                RegisterTabletServerRequest request) {
            return services.tabletServerRegistrationRpcServer().registerTabletServer(request);
        }

        @Override
        public CompletableFuture<TabletServerHeartbeatResponse> tabletServerHeartbeat(
                TabletServerHeartbeatRequest request) {
            return services.tabletServerRegistrationRpcServer().tabletServerHeartbeat(request);
        }
    }

    @Test
    void testRegisterHeartbeatAndReRegisterAfterFencing() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        long sessionTimeoutMs = 800L;
        long checkIntervalMs = 150L;
        conf.set(
                ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT,
                Duration.ofMillis(sessionTimeoutMs));
        conf.set(
                ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL,
                Duration.ofMillis(checkIntervalMs));

        RaftMetadataClient client =
                (RaftMetadataClient) MetadataStoreClientFactory.create(conf, null);
        RaftCoordinatorMetadataServices services =
                RaftCoordinatorMetadataServices.create(client, conf);

        TabletServerRegistrationLoop loop = null;
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");
            services.start();

            loop =
                    new TabletServerRegistrationLoop(
                            new DelegatingGateway(services),
                            /* serverId */ 1,
                            () ->
                                    new RegisterTabletServerRequest()
                                            .setServerId(1)
                                            .setRegisterTimestamp(System.currentTimeMillis())
                                            .setRack("rack1")
                                            .setListeners("FLUSS://localhost:9001"),
                            /* heartbeatIntervalMs */ 100L,
                            /* rpcTimeoutMs */ 5_000L);

            // ---- register + heartbeat: the server joins membership and the lease is renewed ----
            loop.start();
            assertThat(loop.isRegistered()).isTrue();
            assertThat(loop.leaderEpoch()).isPositive();
            waitUntil(
                    5_000,
                    () -> meta.getSortedTabletServerList().length == 1,
                    "server was not registered");
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);
            // heartbeats keep it in membership across several session windows
            Thread.sleep(sessionTimeoutMs + 3 * checkIntervalMs);
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);

            // ---- fencing + re-register: stop heartbeating, let the lease expire, then a manual
            // tick observes registered=false and re-registers ----
            loop.close(); // stop the heartbeat scheduler so the lease can expire
            waitUntil(
                    5_000,
                    () -> meta.getSortedTabletServerList().length == 0,
                    "server was not fenced after the lease expired");
            assertThat(meta.getSortedTabletServerList()).isEmpty();

            loop.heartbeatTick(); // next heartbeat sees registered=false -> re-registers
            assertThat(loop.isRegistered()).isTrue();
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);
        } finally {
            if (loop != null) {
                loop.close();
            }
            services.close();
            client.close();
        }
    }

    private interface Condition {
        boolean test() throws Exception;
    }

    private static void waitUntil(long timeoutMs, Condition condition, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
