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
import org.apache.fluss.exception.InvalidUpdateVersionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for the §5.11 {@code metadata.version} feature gate: mixed-version voters cause
 * the leader to refuse an {@link MetadataRecord.Type#UPDATE_METADATA_VERSION} bump.
 */
class MetadataVersionFeatureGateITCase {

    @TempDir private Path baseDir;

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            try {
                toClose.get(i).close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        toClose.clear();
    }

    @Test
    void testMixedVersionRefuseThenAllow() throws Exception {
        int port = freePort();
        String addr = "127.0.0.1:" + port;
        Configuration conf = confFor(baseDir.resolve("n1"), addr);
        RaftMetadataClient client = RaftMetadataClient.create(conf);
        toClose.add(client);
        waitUntilLeader(client, 30_000);

        String localPeerId = RaftMetadataClient.peerIdForAddress(addr).toString();
        assertThat(client.voterMaxMetadataVersions())
                .containsEntry(localPeerId, MetadataVersions.CURRENT_METADATA_VERSION);

        // Unset cluster version; an old voter that only supports 0 blocks the bump to 1.
        assertThat(client.getMetadataVersion()).isEqualTo(MetadataVersions.UNSET_METADATA_VERSION);

        client.advertiseVoterMaxMetadataVersion("old_voter_0", 0);
        assertThatThrownBy(
                        () ->
                                client.updateMetadataVersion(
                                        MetadataVersions.CURRENT_METADATA_VERSION))
                .isInstanceOf(InvalidUpdateVersionException.class)
                .hasMessageContaining("old_voter_0")
                .hasMessageContaining("only supports up to 0");

        assertThat(client.getMetadataVersion()).isEqualTo(MetadataVersions.UNSET_METADATA_VERSION);

        // Re-advertise the old voter at CURRENT; bump succeeds and persists.
        client.advertiseVoterMaxMetadataVersion(
                "old_voter_0", MetadataVersions.CURRENT_METADATA_VERSION);
        client.updateMetadataVersion(MetadataVersions.CURRENT_METADATA_VERSION);
        assertThat(client.getMetadataVersion())
                .isEqualTo(MetadataVersions.CURRENT_METADATA_VERSION);

        // Idempotent re-bump.
        client.updateMetadataVersion(MetadataVersions.CURRENT_METADATA_VERSION);
        assertThat(client.getMetadataVersion())
                .isEqualTo(MetadataVersions.CURRENT_METADATA_VERSION);

        // Cannot bump above this binary's CURRENT.
        assertThatThrownBy(
                        () ->
                                client.updateMetadataVersion(
                                        MetadataVersions.CURRENT_METADATA_VERSION + 1))
                .isInstanceOf(InvalidUpdateVersionException.class)
                .hasMessageContaining("only supports up to");

        // Downgrade refused.
        assertThatThrownBy(() -> client.updateMetadataVersion(0))
                .isInstanceOf(InvalidUpdateVersionException.class)
                .hasMessageContaining("Downgrade");
    }

    private static Configuration confFor(Path raftDir, String address) {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, address);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, address);
        conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(100));
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(100));
        conf.set(ConfigOptions.COORDINATOR_RAFT_CATCHUP_TIMEOUT, Duration.ofSeconds(10));
        return conf;
    }

    private static void waitUntilLeader(RaftMetadataClient client, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (client.leaderSelector().isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Timed out waiting for Raft leadership");
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
