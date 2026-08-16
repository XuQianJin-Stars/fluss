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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.server.coordinator.raft.MetadataStreamRpcs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Boots a real single-node {@link CoordinatorServer} on the Raft metadata backend and verifies the
 * §7.3.1 coordinator gateway wiring (Slice 1 of the TabletServer-integration milestone):
 *
 * <ol>
 *   <li>startup no longer throws — the previous {@code UnsupportedOperationException} gate is gone
 *       and the Raft path (with a null {@code zkClient}) starts cleanly through {@code
 *       initCoordinatorStandby};
 *   <li>the coordinator gateway delegates to the two Raft-only handlers wired there — TabletServer
 *       registration/heartbeat (§5.4.2) and the cross-process metadata stream (§5.8) — once the
 *       embedded Ratis node has elected itself leader, instead of failing with {@code
 *       raftBackendOnly(...)}.
 * </ol>
 */
class CoordinatorServerRaftBootITCase {

    @TempDir private Path raftDir;

    @Test
    void testCoordinatorBootsAndServesRaftGatewayHandlers() throws Exception {
        int raftPort = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.setString(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        conf.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://127.0.0.1:100");
        conf.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");

        CoordinatorServer server = new CoordinatorServer(conf);
        try {
            // (1) RAFT-mode startup must not throw (gate removed; no NPE from the null-zkClient
            // DynamicConfigManager path).
            assertThatCode(server::start).doesNotThrowAnyException();

            CoordinatorService gateway = server.getCoordinatorService();
            assertThat(gateway).isNotNull();

            // (2) once the single-node quorum elects itself leader, registration is served through
            // the liveness lease (proves setTabletServerRegistrationRpcServer wiring).
            RegisterTabletServerResponse register =
                    pollUntilServed(
                            () ->
                                    gateway.registerTabletServer(
                                                    new RegisterTabletServerRequest()
                                                            .setServerId(1)
                                                            .setRegisterTimestamp(1L)
                                                            .setRack("rack1")
                                                            .setListeners("FLUSS://localhost:9001"))
                                            .get());
            assertThat(register.getLeaderEpoch()).isPositive();

            // and the metadata stream handler streams back the committed records (proves
            // setMetadataStreamRpcServer wiring).
            FetchMetadataResponse fetch =
                    gateway.fetchMetadata(MetadataStreamRpcs.buildFetchRequest(1, 1L, 0L, 100, 0))
                            .get();
            assertThat(fetch.getLeaderEpoch()).isPositive();
            assertThat(fetch.getRecordsList()).isNotEmpty();
        } finally {
            server.close();
        }
    }

    private interface Call<T> {
        T get() throws Exception;
    }

    /** Retries a handler call until the Ratis node is leader and the call succeeds (bounded). */
    private static <T> T pollUntilServed(Call<T> call) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                return call.get();
            } catch (Exception e) {
                last = e;
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("Raft gateway handler never served within timeout", last);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
