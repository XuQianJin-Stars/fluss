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
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Chaos tests for the cross-process metadata stream (§5.8, T5-4): the properties that only a
 * Raft-log-backed stream survives.
 *
 * <ul>
 *   <li><b>Leader failover mid-stream</b> — a subscriber streaming from a 3-voter quorum keeps its
 *       cursor across a leader crash and resumes from the new leader with no gap or duplicate,
 *       because the metadata offset IS the Raft index and every voter holds an identical log.
 *   <li><b>Slow subscriber back-pressure</b> — a subscriber pulling one record at a time while the
 *       leader keeps appending eventually catches up with every record, in order, none lost (the
 *       pull model gives the subscriber ownership of its pace).
 *   <li><b>Snapshot boundary</b> — records committed before an on-disk snapshot and after it are
 *       both recoverable by a late subscriber via snapshot bootstrap + delta resume.
 * </ul>
 */
class MetadataStreamChaosITCase {

    private static final int NUM_NODES = 3;

    @TempDir private Path baseDir;

    private final List<Node> nodes = new CopyOnWriteArrayList<>();
    private final Map<RaftPeerId, MetadataStreamService> services = new ConcurrentHashMap<>();
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
        services.clear();
    }

    @Test
    void testLeaderFailoverMidStream() throws Exception {
        startCluster(NUM_NODES);
        Node firstLeader = waitForLeader(30_000);

        // pre-failover writes under the first leader
        appendToLeader(tableRecord("t1", "1"));
        appendToLeader(tableRecord("t2", "2"));

        // subscriber routes every fetch to the CURRENT leader; on a leader crash the routing
        // transport fails, the subscriber reconnects, and re-targets the new leader at the same
        // cursor (the new leader's identical log serves the same range).
        InMemoryMetadataKvStore subKv = new InMemoryMetadataKvStore();
        RaftMetadataStore subStore = new RaftMetadataStore(subKv, new MetadataWatchService());
        RpcMetadataStreamGateway gateway =
                new RpcMetadataStreamGateway(new LeaderRoutingTransport(), 1);
        // short long-poll so a poll against a dying leader returns quickly and re-resolves.
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(gateway, subStore, 100, 200L, 100L, null);
        Thread loop = new Thread(subscriber::runLoop, "chaos-subscriber");
        loop.setDaemon(true);
        loop.start();

        try {
            waitUntil(30_000, () -> subKv.size() >= 2, "subscriber did not catch up pre-failover");

            // crash the leader mid-stream
            firstLeader.server.close();
            nodes.remove(firstLeader);
            services.remove(firstLeader.peerId);

            Node newLeader = waitForLeader(30_000);
            assertThat(newLeader.peerId).isNotEqualTo(firstLeader.peerId);

            // post-failover writes under the new leader
            appendToLeader(tableRecord("t3", "3"));
            appendToLeader(tableRecord("t4", "4"));

            // subscriber must pick up the post-failover records with no gap / no loss
            waitUntil(
                    30_000,
                    () -> subKv.size() >= 4,
                    "subscriber did not resume from the new leader after failover");
            assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("t1", "1")))).isNotNull();
            assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("t4", "4"))))
                    .isEqualTo("4".getBytes(StandardCharsets.UTF_8));
        } finally {
            subscriber.close();
        }
    }

    @Test
    void testSlowSubscriberCatchesUpWithoutLoss() throws Exception {
        startCluster(1);
        waitForLeader(30_000);

        InMemoryMetadataKvStore subKv = new InMemoryMetadataKvStore();
        RaftMetadataStore subStore = new RaftMetadataStore(subKv, new MetadataWatchService());
        // slow subscriber: at most 2 records per fetch, so it lags a fast producer.
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(leaderGateway(), subStore, 2, 200L, 50L, null);
        Thread loop = new Thread(subscriber::runLoop, "slow-subscriber");
        loop.setDaemon(true);
        loop.start();

        int total = 25;
        try (RaftClient client = newLeaderClient();
                RaftJournalWriter writer = new RaftJournalWriter(client, currentLeaderSm())) {
            for (int i = 0; i < total; i++) {
                writer.append(tableRecord("k" + i, "v" + i)).get(20, TimeUnit.SECONDS);
                Thread.sleep(5); // produce faster than a 2-at-a-time consumer drains
            }

            waitUntil(
                    30_000,
                    () -> subKv.size() >= total,
                    "slow subscriber did not catch up to all " + total + " records");
            // every record present -> pull-based back-pressure lost nothing
            for (int i = 0; i < total; i++) {
                assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("k" + i, "v" + i))))
                        .isEqualTo(("v" + i).getBytes(StandardCharsets.UTF_8));
            }
        } finally {
            subscriber.close();
        }
    }

    @Test
    void testSnapshotBoundaryRecovery() throws Exception {
        startCluster(1);
        Node leader = waitForLeader(30_000);

        try (RaftClient client = newLeaderClient();
                RaftJournalWriter writer = new RaftJournalWriter(client, leader.sm)) {
            // records committed BEFORE the snapshot
            writer.append(tableRecord("before1", "b1")).get(20, TimeUnit.SECONDS);
            writer.append(tableRecord("before2", "b2")).get(20, TimeUnit.SECONDS);

            // take a real on-disk snapshot folding the current state (compaction boundary, §5.2.5)
            long snapshotIndex = leader.sm.takeSnapshot();
            assertThat(snapshotIndex).isGreaterThan(0L);

            // records committed AFTER the snapshot
            writer.append(tableRecord("after1", "a1")).get(20, TimeUnit.SECONDS);
        }

        // a late subscriber cold-starts via snapshot bootstrap (recovers the pre-snapshot state)
        // then resumes the delta stream to pick up the post-snapshot record.
        InMemoryMetadataKvStore subKv = new InMemoryMetadataKvStore();
        RaftMetadataStore subStore = new RaftMetadataStore(subKv, new MetadataWatchService());
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(leaderGateway(), subStore, 100, 0L, 50L, null);

        subscriber.bootstrapFromSnapshot();
        // snapshot reflected the two pre-snapshot records
        assertThat(subKv.size()).isGreaterThanOrEqualTo(2);
        assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("before1", "b1")))).isNotNull();

        // resume the delta stream past the snapshot boundary
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && subStore.get(MetadataKeys.keyOf(tableRecord("after1", "a1"))) == null) {
            subscriber.pollOnce();
        }
        assertThat(subStore.get(MetadataKeys.keyOf(tableRecord("after1", "a1"))))
                .isEqualTo("a1".getBytes(StandardCharsets.UTF_8));
    }

    // ---- stream wiring ----

    /** Routing transport that resolves the CURRENT leader on every call (failover-aware). */
    private final class LeaderRoutingTransport implements RpcMetadataStreamGateway.Transport {
        @Override
        public CompletableFuture<FetchMetadataResponse> sendFetch(FetchMetadataRequest request) {
            try {
                return leaderRpcServer().fetchMetadata(request);
            } catch (Exception e) {
                CompletableFuture<FetchMetadataResponse> f = new CompletableFuture<>();
                f.completeExceptionally(new java.io.IOException("leader unavailable", e));
                return f;
            }
        }

        @Override
        public CompletableFuture<FetchMetadataSnapshotResponse> sendFetchSnapshot(
                FetchMetadataSnapshotRequest request) {
            try {
                return leaderRpcServer().fetchMetadataSnapshot(request);
            } catch (Exception e) {
                CompletableFuture<FetchMetadataSnapshotResponse> f = new CompletableFuture<>();
                f.completeExceptionally(new java.io.IOException("leader unavailable", e));
                return f;
            }
        }
    }

    /** A gateway pinned to whatever node is currently leader (single-node convenience). */
    private RpcMetadataStreamGateway leaderGateway() {
        return new RpcMetadataStreamGateway(new LeaderRoutingTransport(), 1);
    }

    private MetadataStreamRpcServer leaderRpcServer() throws Exception {
        Node leader = waitForLeader(30_000);
        MetadataStreamService service =
                services.computeIfAbsent(leader.peerId, id -> buildService(leader));
        return new MetadataStreamRpcServer(service);
    }

    private MetadataStreamService buildService(Node node) {
        RaftLog raftLog;
        try {
            raftLog =
                    node.server
                            .getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getRaftLog();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MetadataStreamService service =
                new MetadataStreamService(
                        raftLog, () -> currentTerm(node), node.sm::captureSnapshot);
        node.sm.setApplyListener(service::onApplied);
        return service;
    }

    private FlussMetadataStateMachine currentLeaderSm() throws Exception {
        return waitForLeader(30_000).sm;
    }

    private RaftClient newLeaderClient() throws Exception {
        Node leader = waitForLeader(30_000);
        RaftProperties props = leader.server.buildRaftProperties();
        return RaftClient.newBuilder()
                .setClientId(ClientId.randomId())
                .setRaftGroup(group)
                .setProperties(props)
                .build();
    }

    // ---- cluster harness (mirrors RaftClusterITCase) ----

    private void startCluster(int numNodes) throws Exception {
        int[] ports = freePorts(numNodes);
        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            peers.add(RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + ports[i]).build());
        }
        group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peers);

        for (int i = 0; i < numNodes; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            Configuration conf = new Configuration();
            conf.setString(
                    ConfigOptions.COORDINATOR_RAFT_DIR, baseDir.resolve("n" + (i + 1)).toString());
            conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
            conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
            RaftMetadataStore store =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
            RaftMetadataServer server = new RaftMetadataServer(conf, id, group, sm);
            server.start();
            nodes.add(new Node(id, server, sm));
        }
    }

    private void appendToLeader(MetadataRecord record) throws Exception {
        Node leader = waitForLeader(30_000);
        try (RaftClient client = newLeaderClient();
                RaftJournalWriter writer = new RaftJournalWriter(client, leader.sm)) {
            writer.append(record).get(20, TimeUnit.SECONDS);
        }
    }

    private Node waitForLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Node node : nodes) {
                try {
                    boolean ready =
                            node.server
                                    .getRaftServer()
                                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                                    .getInfo()
                                    .isLeaderReady();
                    if (ready) {
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

    private static long currentTerm(Node node) {
        try {
            return node.server
                    .getRaftServer()
                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                    .getInfo()
                    .getCurrentTerm();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static MetadataRecord tableRecord(String key, String payload) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_TABLE,
                key.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8));
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
        final FlussMetadataStateMachine sm;

        Node(RaftPeerId peerId, RaftMetadataServer server, FlussMetadataStateMachine sm) {
            this.peerId = peerId;
            this.server = server;
            this.sm = sm;
        }
    }
}
