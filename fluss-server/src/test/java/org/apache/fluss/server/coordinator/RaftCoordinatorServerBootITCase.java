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
import org.apache.fluss.server.metadata.MetadataStoreClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end boot test for a single-node {@link CoordinatorServer} on the RAFT metadata backend (no
 * ZooKeeper): the server bootstraps an embedded Ratis quorum, elects itself, runs {@code
 * initCoordinatorLeader} (registers the coordinator leader + starts the event processor + creates
 * the default database), and serves. This is the coordinator-leader-boot milestone that unblocks a
 * fully Raft-backed cluster (§5.1 / §5.5 / §7.3.1).
 */
class RaftCoordinatorServerBootITCase {

    @TempDir private Path raftDir;

    @TempDir private Path remoteDir;

    @Test
    void testCoordinatorBootsElectsAndCreatesDefaultDatabaseOnRaft() throws Exception {
        int raftPort = freePort();
        Configuration conf = new Configuration();
        // Presence of coordinator.seed.nodes + absence of zookeeper.address selects the RAFT
        // backend.
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.setString(ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        conf.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDir.toString());

        CoordinatorServer server = new CoordinatorServer(conf);
        try {
            server.start();

            MetadataStoreClient meta = server.getMetadataStoreClient();
            // The embedded Ratis server elects this node, then initCoordinatorLeader runs:
            // registerCoordinatorLeader() (redirect-exempt read) + createDefaultDatabase(). Poll
            // until both have been committed+applied (leader-guarded reads may briefly throw
            // NotCoordinatorLeaderException while leadership settles, so treat that as "not
            // ready").
            waitUntil(
                    30_000,
                    () ->
                            server.isLeader()
                                    && meta.getCoordinatorLeaderAddress().isPresent()
                                    && meta.listDatabases()
                                            .contains(CoordinatorServer.DEFAULT_DATABASE),
                    "coordinator did not boot as a Raft leader with the default database");

            assertThat(server.isLeader()).isTrue();
            assertThat(meta.getCoordinatorLeaderAddress()).isPresent();
            assertThat(meta.listDatabases()).contains(CoordinatorServer.DEFAULT_DATABASE);
        } finally {
            server.close();
        }
    }

    private interface Condition {
        boolean test() throws Exception;
    }

    private static void waitUntil(long timeoutMs, Condition condition, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.test()) {
                    return;
                }
            } catch (Exception ignored) {
                // leadership/store still settling; retry until the deadline
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
