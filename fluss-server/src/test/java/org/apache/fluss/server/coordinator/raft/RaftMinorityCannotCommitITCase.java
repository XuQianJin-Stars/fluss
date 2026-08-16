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
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.retry.RetryPolicies;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.TimeDuration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §8.3: with only a minority of voters alive, a metadata append cannot commit (majority quorum).
 *
 * <p>Simulates isolation by killing the leader plus one follower so a single survivor remains —
 * that peer cannot form a majority of the 3-voter configuration, so client appends fail until a
 * second voter is restored.
 */
class RaftMinorityCannotCommitITCase {

    private static final int NUM_NODES = 3;

    @TempDir private Path baseDir;

    private final List<Node> nodes = new ArrayList<>();
    private RaftGroup group;

    @AfterEach
    void tearDown() {
        for (Node node : nodes) {
            try {
                node.server.close();
            } catch (Exception ignored) {
                // best-effort shutdown
            }
        }
        nodes.clear();
    }

    @Test
    void testIsolatedMinorityCannotCommit() throws Exception {
        startCluster();
        Node leader = waitForLeader(30_000);

        appendVia(
                leader,
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "ok".getBytes(StandardCharsets.UTF_8),
                        "1".getBytes(StandardCharsets.UTF_8)),
                20,
                TimeUnit.SECONDS);

        // Kill leader + one follower → one survivor (minority of the 3-voter set).
        List<Node> killed = new ArrayList<>();
        Node survivor = null;
        int killedFollowers = 0;
        for (Node node : new ArrayList<>(nodes)) {
            boolean isLeader = node.peerId.equals(leader.peerId);
            if (isLeader || killedFollowers < 1) {
                if (!isLeader) {
                    killedFollowers++;
                }
                node.server.close();
                nodes.remove(node);
                killed.add(node);
            } else {
                survivor = node;
            }
        }
        assertThat(nodes).hasSize(1);
        assertThat(survivor).isNotNull();
        assertThat(survivor.peerId).isNotEqualTo(leader.peerId);
        final Node minority = survivor;

        MetadataRecord blocked =
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "blocked".getBytes(StandardCharsets.UTF_8),
                        "2".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> appendVia(minority, blocked, 3, TimeUnit.SECONDS))
                .as("minority survivor must not successfully commit")
                .isInstanceOf(Exception.class);
        assertThat(minority.store.get(MetadataKeys.keyOf(blocked)))
                .as("minority must not apply an uncommitted append")
                .isNull();

        // Restore one killed peer → majority of remaining live set can elect and commit.
        Node restored = killed.get(0);
        final Node rejoined =
                startNode(restored.peerId, baseDir.resolve(restored.peerId.toString()));
        Node newLeader = waitForLeader(30_000);
        appendVia(
                newLeader,
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "after".getBytes(StandardCharsets.UTF_8),
                        "3".getBytes(StandardCharsets.UTF_8)),
                20,
                TimeUnit.SECONDS);

        final byte[] storeKey =
                MetadataKeys.keyOf(
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLE,
                                "after".getBytes(StandardCharsets.UTF_8),
                                new byte[0]));
        final byte[] afterPayload = "3".getBytes(StandardCharsets.UTF_8);
        waitUntil(
                30_000,
                () ->
                        java.util.Arrays.equals(rejoined.store.get(storeKey), afterPayload)
                                || java.util.Arrays.equals(
                                        minority.store.get(storeKey), afterPayload),
                "restored majority did not commit post-partition write");
    }

    private void startCluster() throws Exception {
        int[] ports = freePorts(NUM_NODES);
        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            peers.add(RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + ports[i]).build());
        }
        group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peers);
        for (RaftPeer peer : peers) {
            startNode(peer.getId(), baseDir.resolve(peer.getId().toString()));
        }
    }

    private Node startNode(RaftPeerId id, Path raftDir) throws Exception {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = new RaftMetadataServer(conf, id, group, sm);
        server.start();
        Node node = new Node(id, server, store, sm);
        nodes.add(node);
        return node;
    }

    private void appendVia(Node target, MetadataRecord record, long timeout, TimeUnit unit)
            throws Exception {
        RaftProperties props = target.server.buildRaftProperties();
        try (RaftClient client =
                        RaftClient.newBuilder()
                                .setClientId(ClientId.randomId())
                                .setRaftGroup(group)
                                .setLeaderId(target.peerId)
                                .setRetryPolicy(
                                        RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                                                2,
                                                TimeDuration.valueOf(100, TimeUnit.MILLISECONDS)))
                                .setProperties(props)
                                .build();
                RaftJournalWriter writer = new RaftJournalWriter(client, target.sm)) {
            writer.append(record).get(timeout, unit);
        }
    }

    private Node waitForLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Node node : nodes) {
                try {
                    if (node.server
                            .getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .isLeaderReady()) {
                        return node;
                    }
                } catch (Exception ignored) {
                    // division may not be ready yet
                }
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("No leader elected within " + timeoutMs + "ms");
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
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out: " + message);
    }

    private static int[] freePorts(int n) throws Exception {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) {
                    s.close();
                }
            }
        }
        return ports;
    }

    private static final class Node {
        final RaftPeerId peerId;
        final RaftMetadataServer server;
        final RaftMetadataStore store;
        final FlussMetadataStateMachine sm;

        Node(
                RaftPeerId peerId,
                RaftMetadataServer server,
                RaftMetadataStore store,
                FlussMetadataStateMachine sm) {
            this.peerId = peerId;
            this.server = server;
            this.store = store;
            this.sm = sm;
        }
    }
}
