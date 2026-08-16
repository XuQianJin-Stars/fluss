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
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link TabletServerRegistrationRpcServer} (T6/B2) against a real single-node
 * embedded Ratis leader. Verifies that the {@code RegisterTabletServer}/{@code
 * TabletServerHeartbeat} RPC logic appends the membership record, seeds + renews the liveness
 * lease, and correctly reports a fenced server as no longer registered.
 */
class TabletServerRegistrationRpcServerITCase {

    private static final long SESSION_TIMEOUT_MS = 10_000L;
    private static final long LEADER_EPOCH = 7L;

    @TempDir private Path raftDir;

    @Test
    void testRegisterHeartbeatAndFenceRoundTrip() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataClient client =
                (RaftMetadataClient) MetadataStoreClientFactory.create(conf, null);
        ManualClock clock = new ManualClock(0);
        AtomicBoolean leader = new AtomicBoolean(true);
        TabletServerLivenessManager liveness =
                new TabletServerLivenessManager(
                        client.writerForTesting(),
                        clock,
                        SESSION_TIMEOUT_MS,
                        /* checkIntervalMs */ 1_000L,
                        leader::get);
        TabletServerRegistrationRpcServer rpc =
                new TabletServerRegistrationRpcServer(client, liveness, () -> LEADER_EPOCH);
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");

            // ---- register: appends REGISTER_TABLET_SERVER + starts tracking the lease ----
            RegisterTabletServerResponse registerResponse =
                    rpc.registerTabletServer(
                                    new RegisterTabletServerRequest()
                                            .setServerId(1)
                                            .setRegisterTimestamp(10_001L)
                                            .setRack("rack1")
                                            .setListeners("FLUSS://localhost:9001"))
                            .get();
            assertThat(registerResponse.getLeaderEpoch()).isEqualTo(LEADER_EPOCH);
            assertThat(meta.getTabletServer(1)).isPresent();
            assertThat(meta.getTabletServer(1).get().getRack()).isEqualTo("rack1");
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);
            assertThat(liveness.isTracked(1)).isTrue();

            // ---- heartbeat for a registered server: registered=true, lease renewed ----
            clock.advanceTime(1_000L, TimeUnit.MILLISECONDS);
            TabletServerHeartbeatResponse hbRegistered =
                    rpc.tabletServerHeartbeat(
                                    new TabletServerHeartbeatRequest()
                                            .setServerId(1)
                                            .setLeaderEpoch(LEADER_EPOCH))
                            .get();
            assertThat(hbRegistered.getLeaderEpoch()).isEqualTo(LEADER_EPOCH);
            assertThat(hbRegistered.isRegistered()).isTrue();
            assertThat(liveness.isTracked(1)).isTrue();

            // ---- heartbeat for a never-registered server: registered=false, not tracked ----
            TabletServerHeartbeatResponse hbUnknown =
                    rpc.tabletServerHeartbeat(
                                    new TabletServerHeartbeatRequest()
                                            .setServerId(99)
                                            .setLeaderEpoch(LEADER_EPOCH))
                            .get();
            assertThat(hbUnknown.isRegistered()).isFalse();
            assertThat(liveness.isTracked(99)).isFalse();

            // ---- fence server 1 by lease expiry, then its heartbeat reports unregistered ----
            clock.advanceTime(SESSION_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS);
            liveness.checkExpiredServers();
            assertThat(liveness.isTracked(1)).isFalse();
            waitUntil(
                    10_000,
                    () -> !meta.getTabletServer(1).isPresent(),
                    "server 1 was not fenced from the store");

            TabletServerHeartbeatResponse hbFenced =
                    rpc.tabletServerHeartbeat(
                                    new TabletServerHeartbeatRequest()
                                            .setServerId(1)
                                            .setLeaderEpoch(LEADER_EPOCH))
                            .get();
            assertThat(hbFenced.isRegistered()).isFalse();
            // a fenced server must re-register before it is tracked again
            assertThat(liveness.isTracked(1)).isFalse();

            // ---- re-register brings it back ----
            rpc.registerTabletServer(
                            new RegisterTabletServerRequest()
                                    .setServerId(1)
                                    .setRegisterTimestamp(20_001L)
                                    .setListeners("FLUSS://localhost:9001"))
                    .get();
            assertThat(meta.getTabletServer(1)).isPresent();
            assertThat(liveness.isTracked(1)).isTrue();
        } finally {
            liveness.close();
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
