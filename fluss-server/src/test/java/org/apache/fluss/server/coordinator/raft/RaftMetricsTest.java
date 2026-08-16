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
import org.apache.fluss.server.metrics.group.TestingMetricGroups;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies {@link RaftMetrics} reads live Ratis division state and registers gauges. */
class RaftMetricsTest {

    @TempDir Path tempDir;

    @Test
    void testRegistersGaugesFromLiveLeader() throws Exception {
        int port = freePort();
        String address = "127.0.0.1:" + port;
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS.key(), address);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID.key(), address);
        conf.setString(
                ConfigOptions.COORDINATOR_RAFT_DIR.key(), tempDir.resolve("raft").toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(100));

        try (RaftMetadataClient client = RaftMetadataClient.create(conf)) {
            long deadline = System.currentTimeMillis() + 30_000L;
            while (!client.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            assertThat(client.isLeader()).isTrue();

            assertThat(RaftMetrics.role(client)).isEqualTo(RaftMetrics.ROLE_LEADER);
            assertThat(RaftMetrics.term(client)).isGreaterThan(0L);
            // Commit/applied may be 0 or the Ratis sentinel (-1) before any user append.
            assertThat(RaftMetrics.commitIndex(client)).isGreaterThanOrEqualTo(-1L);
            assertThat(RaftMetrics.appliedIndex(client)).isGreaterThanOrEqualTo(-1L);
            assertThat(RaftMetrics.applyLag(client)).isGreaterThanOrEqualTo(-1L);
            assertThat(RaftMetrics.maxFollowerLag(client)).isGreaterThanOrEqualTo(0L);

            RaftMetrics.register(TestingMetricGroups.COORDINATOR_METRICS, client);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
