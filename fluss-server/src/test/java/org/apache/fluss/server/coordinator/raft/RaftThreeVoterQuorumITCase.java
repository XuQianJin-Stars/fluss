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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies a 3-voter quorum assembled the same way the JMH harness assembles it: one {@link
 * RaftMetadataClient} plus two bare {@link RaftMetadataServer} followers sharing one initial group.
 *
 * <p>This is the fixture the analysis §7.2 measurements depend on, so it is worth a test of its
 * own: if the peer-id scheme or the start order regresses, the benchmark silently fails to elect a
 * leader rather than reporting a wrong number.
 */
class RaftThreeVoterQuorumITCase {

    @TempDir private Path baseDir;

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() {
        // Close the client (leader) last: followers losing their leader is the cheaper teardown.
        List<AutoCloseable> reversed = new ArrayList<>(toClose);
        Collections.reverse(reversed);
        for (AutoCloseable c : reversed) {
            try {
                c.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        toClose.clear();
    }

    @Test
    void testThreeVoterQuorumElectsLeaderAndCommits() throws Exception {
        int[] ports = freePorts(3);
        List<String> addresses = new ArrayList<>(3);
        for (int port : ports) {
            addresses.add("127.0.0.1:" + port);
        }

        // Grow 1 → 3 rather than bootstrapping all three at once. Starting every peer with the full
        // 3-voter conf leaves each of them waiting for a quorum that has never agreed on any
        // configuration, so no election ever completes (same trap noted in
        // RaftVoterReconfigurationITCase).
        Configuration leaderConf = confFor(baseDir.resolve("n0"), addresses.get(0));
        leaderConf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addresses.get(0));
        RaftMetadataClient client = RaftMetadataClient.create(leaderConf);
        toClose.add(client);
        waitUntil(30_000, client::isLeader, "single voter did not become leader");

        // Each joiner starts with the group it is about to enter.
        String twoVoters = addresses.get(0) + "," + addresses.get(1);
        startFollower(baseDir.resolve("n1"), addresses.get(1), twoVoters);
        assertThat(client.addVoter(addresses.get(1))).hasSize(2);

        String threeVoters = twoVoters + "," + addresses.get(2);
        startFollower(baseDir.resolve("n2"), addresses.get(2), threeVoters);
        assertThat(client.addVoter(addresses.get(2))).hasSize(3);

        waitUntil(60_000, client::isLeader, "leader lost after growing to 3 voters");

        // A write must actually commit through the 3-voter quorum, not just elect.
        TableBucket bucket = new TableBucket(1L, 0);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(
                        1, 10, Collections.singletonList(1), Collections.emptyList(), 0, 1000);
        client.registerLeaderAndIsr(bucket, leaderAndIsr, -1);

        assertThat(client.getLeaderAndIsr(bucket)).isPresent();
    }

    private void startFollower(Path dir, String address, String votersInTargetGroup)
            throws Exception {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        RaftMetadataServer follower =
                new RaftMetadataServer(
                        confFor(dir, address),
                        // Same peer-id scheme as the client, else the two disagree on identity.
                        RaftMetadataClient.peerIdForAddress(address),
                        RaftMetadataClient.resolveGroupForVoters(votersInTargetGroup),
                        new FlussMetadataStateMachine(store));
        follower.start();
        toClose.add(follower);
    }

    private static Configuration confFor(Path dir, String nodeId) {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, nodeId);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, dir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
        conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(100));
        return conf;
    }

    private static int[] freePorts(int count) throws Exception {
        List<ServerSocket> sockets = new ArrayList<>(count);
        try {
            int[] ports = new int[count];
            for (int i = 0; i < count; i++) {
                ServerSocket socket = new ServerSocket(0);
                sockets.add(socket);
                ports[i] = socket.getLocalPort();
            }
            return ports;
        } finally {
            for (ServerSocket socket : sockets) {
                socket.close();
            }
        }
    }

    private static void waitUntil(long timeoutMs, Condition condition, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(50L);
        }
        throw new AssertionError(message);
    }

    /** Boolean supplier that may throw. */
    private interface Condition {
        boolean get() throws Exception;
    }
}
