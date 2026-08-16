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
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link TabletServerLivenessManager} (T6-1) against a real single-node
 * embedded Ratis leader. Verifies heartbeat-lease fencing: a stale server is fenced (its {@code
 * REGISTER_TABLET_SERVER} membership key is removed via a replicated {@code FENCE_TABLET_SERVER}
 * record), a renewed server survives, and fencing runs only while leader.
 */
class TabletServerLivenessManagerITCase {

    private static final long SESSION_TIMEOUT_MS = 10_000L;

    @TempDir private Path raftDir;

    @Test
    void testExpiredServerIsFencedRenewedSurvivesAndLeaderGuard() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        // A factory-bootstrapped Raft client gives us a real leader + writer + store view.
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
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");

            meta.registerTabletServer(1, tabletServer("rack1", 1));
            meta.registerTabletServer(2, tabletServer("rack2", 2));
            assertThat(meta.getTabletServer(1)).isPresent();
            assertThat(meta.getTabletServer(2)).isPresent();

            // both contact the leader at t=0
            liveness.recordContact(1);
            liveness.recordContact(2);

            // ---- leader guard: when NOT leader, no fencing even if leases are expired ----
            leader.set(false);
            clock.advanceTime(SESSION_TIMEOUT_MS * 2, TimeUnit.MILLISECONDS); // both stale now
            liveness.checkExpiredServers();
            assertThat(liveness.isTracked(1)).isTrue();
            assertThat(liveness.isTracked(2)).isTrue();
            assertThat(meta.getTabletServer(1)).isPresent();
            assertThat(meta.getTabletServer(2)).isPresent();

            // ---- as leader: server 1 renews its lease, server 2 stays stale ----
            leader.set(true);
            liveness.recordContact(1); // renew 1 at the current (advanced) time
            liveness.checkExpiredServers(); // now-last[1]=0 (alive); now-last[2]=2*timeout
            // (expired)

            assertThat(liveness.isTracked(1)).isTrue();
            assertThat(liveness.isTracked(2)).isFalse();
            // the FENCE_TABLET_SERVER append is async; wait for it to be applied to the store.
            waitUntil(
                    10_000,
                    () -> !meta.getTabletServer(2).isPresent(),
                    "server 2 was not fenced from the store");
            assertThat(meta.getTabletServer(1)).isPresent();
        } finally {
            liveness.close();
            client.close();
        }
    }

    @Test
    void testSeededOfflineServerIsFencedAfterSessionTimeout() throws Exception {
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
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");

            // Simulate coordinator restart: REGISTER records exist in the store, but the new
            // leader has not yet observed heartbeats for them.
            meta.registerTabletServer(1, tabletServer("rack1", 1));
            meta.registerTabletServer(2, tabletServer("rack2", 2));
            liveness.seedRegisteredServers(Arrays.asList(1, 2));
            assertThat(liveness.isTracked(1)).isTrue();
            assertThat(liveness.isTracked(2)).isTrue();

            // Live tablet renews; offline tablet does not.
            clock.advanceTime(SESSION_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
            liveness.recordContact(1);
            clock.advanceTime(SESSION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            liveness.checkExpiredServers();

            assertThat(liveness.isTracked(1)).isTrue();
            assertThat(liveness.isTracked(2)).isFalse();
            waitUntil(
                    10_000,
                    () -> !meta.getTabletServer(2).isPresent(),
                    "seeded offline server 2 was not fenced");
            assertThat(meta.getTabletServer(1)).isPresent();
        } finally {
            liveness.close();
            client.close();
        }
    }

    private static TabletServerRegistration tabletServer(String rack, int id) {
        return new TabletServerRegistration(
                rack,
                Endpoint.fromListenersString("FLUSS://localhost:" + (9000 + id)),
                10_000L + id);
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
