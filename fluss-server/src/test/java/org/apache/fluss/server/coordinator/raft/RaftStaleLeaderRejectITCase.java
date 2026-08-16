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
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.fluss.shaded.ratis.org.apache.ratis.retry.RetryPolicies;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §8.3: after failover bumps the Raft term, an append forced to the former leader is rejected with
 * {@link NotLeaderException} (term fencing).
 */
class RaftStaleLeaderRejectITCase {

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
    void testAppendOnStaleLeaderRejectedAfterFailover() throws Exception {
        startCluster();
        Node firstLeader = waitForLeader(30_000);
        long termBefore =
                firstLeader
                        .server
                        .getRaftServer()
                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                        .getInfo()
                        .getCurrentTerm();

        appendToCurrentLeader(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "pre".getBytes(StandardCharsets.UTF_8),
                        "v1".getBytes(StandardCharsets.UTF_8)));

        // Crash the leader so a survivor is elected (term bump).
        RaftPeerId staleLeaderId = firstLeader.peerId;
        Path staleDir = baseDir.resolve(staleLeaderId.toString());
        firstLeader.server.close();
        nodes.remove(firstLeader);

        Node newLeader = waitForLeader(30_000);
        assertThat(newLeader.peerId).isNotEqualTo(staleLeaderId);
        long termAfter =
                newLeader
                        .server
                        .getRaftServer()
                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                        .getInfo()
                        .getCurrentTerm();
        assertThat(termAfter).isGreaterThan(termBefore);

        appendToCurrentLeader(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "post".getBytes(StandardCharsets.UTF_8),
                        "v2".getBytes(StandardCharsets.UTF_8)));

        // Former leader rejoins as a follower (same peer id / storage dir).
        Node restarted = startNode(staleLeaderId, staleDir);
        waitUntil(
                30_000,
                () -> {
                    try {
                        return restarted
                                        .server
                                        .getRaftServer()
                                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                                        .getInfo()
                                        .isAlive()
                                && !restarted
                                        .server
                                        .getRaftServer()
                                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                                        .getInfo()
                                        .isLeader();
                    } catch (Exception e) {
                        return false;
                    }
                },
                "restarted peer did not become a live follower");

        RaftProperties props = restarted.server.buildRaftProperties();
        try (RaftClient staleClient =
                        RaftClient.newBuilder()
                                .setClientId(ClientId.randomId())
                                .setRaftGroup(group)
                                .setLeaderId(staleLeaderId)
                                .setRetryPolicy(RetryPolicies.noRetry())
                                .setProperties(props)
                                .build();
                RaftJournalWriter writer = new RaftJournalWriter(staleClient, restarted.sm)) {
            assertThatThrownBy(
                            () ->
                                    writer.append(
                                                    new MetadataRecord(
                                                            MetadataRecord.Type.REGISTER_TABLE,
                                                            "stale"
                                                                    .getBytes(
                                                                            StandardCharsets.UTF_8),
                                                            "x".getBytes(StandardCharsets.UTF_8)))
                                            .get(15, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    // Ratis may wrap NotLeaderException in AlreadyClosedException after noRetry.
                    .hasRootCauseInstanceOf(NotLeaderException.class);
        }
    }

    private void startCluster() throws Exception {
        int[] ports = freePorts(NUM_NODES);
        List<RaftPeer> peerList = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            peerList.add(
                    RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + ports[i]).build());
        }
        group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peerList);
        for (RaftPeer peer : peerList) {
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

    private void appendToCurrentLeader(MetadataRecord record) throws Exception {
        Node leader = waitForLeader(30_000);
        RaftProperties props = leader.server.buildRaftProperties();
        try (RaftClient client =
                        RaftClient.newBuilder()
                                .setClientId(ClientId.randomId())
                                .setRaftGroup(group)
                                .setProperties(props)
                                .build();
                RaftJournalWriter writer = new RaftJournalWriter(client, leader.sm)) {
            writer.append(record).get(20, TimeUnit.SECONDS);
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
