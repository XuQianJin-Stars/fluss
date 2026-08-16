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
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link RaftCoordinatorMetadataServices} (T6 final integration) against a real
 * single-node embedded Ratis leader. Verifies that the leader-side services assembled from a
 * bootstrapped {@link RaftMetadataClient} actually serve real data through the two RPC handlers the
 * coordinator gateway delegates to: the metadata stream returns committed records, and
 * tablet-server registration/heartbeat go through the liveness lease.
 */
class RaftCoordinatorMetadataServicesITCase {

    @TempDir private Path raftDir;

    @Test
    void testAssembledServicesServeStreamAndRegistration() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataClient client =
                (RaftMetadataClient)
                        org.apache.fluss.server.metadata.MetadataStoreClientFactory.create(
                                conf, null);
        RaftCoordinatorMetadataServices services =
                RaftCoordinatorMetadataServices.create(
                        client,
                        /* sessionTimeoutMs */ 10_000L,
                        /* checkIntervalMs */ 1_000L,
                        /* idBlockSize */ 1_000L,
                        SystemClock.getInstance());
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");
            services.start();

            // ---- register some metadata, then the stream handler serves it as real records ----
            meta.registerDatabase(
                    "db1", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
            meta.registerDatabase(
                    "db2", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));

            FetchMetadataResponse fetch =
                    services.metadataStreamRpcServer()
                            .fetchMetadata(
                                    MetadataStreamRpcs.buildFetchRequest(
                                            /* serverId */ 1,
                                            /* fromOffset */ 1L,
                                            /* leaderEpoch */ 0L,
                                            /* maxRecords */ 100,
                                            /* maxWaitMs */ 0))
                            .get();
            assertThat(fetch.getLeaderEpoch()).isPositive();
            assertThat(fetch.getRecordsList()).isNotEmpty(); // committed records are streamed back
            assertThat(fetch.getNextOffset()).isGreaterThan(1L);
            assertThat(fetch.isSnapshotRequired()).isFalse();

            // ---- tablet-server registration through the handler: membership + lease ----
            services.tabletServerRegistrationRpcServer()
                    .registerTabletServer(
                            new RegisterTabletServerRequest()
                                    .setServerId(1)
                                    .setRegisterTimestamp(10_001L)
                                    .setRack("rack1")
                                    .setListeners("FLUSS://localhost:9001"))
                    .get();
            assertThat(meta.getTabletServer(1)).isPresent();
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);
            assertThat(services.livenessManager().isTracked(1)).isTrue();

            // heartbeat for the registered server renews the lease
            assertThat(
                            services.tabletServerRegistrationRpcServer()
                                    .tabletServerHeartbeat(
                                            new TabletServerHeartbeatRequest()
                                                    .setServerId(1)
                                                    .setLeaderEpoch(0L))
                                    .get()
                                    .isRegistered())
                    .isTrue();

            // ---- fetchSnapshot serves a terminal-state image (snapshot bootstrap wired) ----
            assertThat(
                            services.metadataStreamRpcServer()
                                    .fetchMetadataSnapshot(
                                            MetadataStreamRpcs.buildSnapshotRequest(1, 0L))
                                    .get()
                                    .getSnapshotMetadataOffset())
                    .isNotNegative();
        } finally {
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
