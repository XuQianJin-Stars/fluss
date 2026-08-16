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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.coordinator.raft.AllocateIdRpcServer;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end 2-process cluster test on the RAFT metadata backend over REAL Netty RPC (§5.4 / §5.8,
 * T6-3 + B1 slice 4). It boots a real single-node {@link CoordinatorServer} on the Raft backend
 * (embedded Ratis + real Netty {@code RpcServer}) AND a full real {@link TabletServer} whose {@code
 * coordinator.seed.nodes} points at the coordinator — no ZooKeeper, no in-process shortcuts.
 *
 * <p>This is the first test that boots a full {@code TabletServer} on RAFT: its metadata client is
 * a stream-fed READ-ONLY follower view (NOT an embedded Ratis voter), and the {@link
 * TabletServerRaftMetadataFeed} registers it + converges its {@link TabletServerMetadataCache} from
 * the metadata stream. It then exercises the write-side data plane:
 *
 * <ol>
 *   <li>the {@code TabletServer} boots on RAFT, registers with the coordinator (leader membership
 *       lists its id) and converges its cache (coordinator node + itself) purely from the stream;
 *   <li>DATA PLANE: calling the {@code TabletServer}'s {@code initWriter} RPC over real Netty
 *       reserves globally-unique writer ids from the coordinator leader via the {@code AllocateId}
 *       RPC (kind {@link AllocateIdRpcServer#KIND_WRITER}) — the Raft replacement for the ZooKeeper
 *       writer-id counter — returning strictly increasing ids across calls;
 *   <li>the other two id kinds a tablet server allocates on the Raft backend — kv auto-increment
 *       column ({@link AllocateIdRpcServer#KIND_AUTO_INCREMENT_COLUMN}) and bucket snapshot ({@link
 *       AllocateIdRpcServer#KIND_BUCKET_SNAPSHOT}) — are served by the same coordinator leader over
 *       the same gateway with contiguous, forward-only ranges (the kv/snapshot write paths inject
 *       exactly this transport; driving them to a physical kv tablet additionally needs bucket
 *       leadership assignment, a follow-on beyond this boot milestone).
 * </ol>
 */
class RaftTwoProcessClusterITCase {

    private static final String INTERNAL_LISTENER = "FLUSS";
    private static final int TABLET_SERVER_ID = 1;

    @TempDir private Path raftDir;
    @TempDir private Path coordinatorRemoteDir;
    @TempDir private Path tabletDataDir;
    @TempDir private Path tabletRemoteDir;

    @Test
    void testTwoProcessRaftClusterBootsAndAllocatesIdsOnTheDataPlane() throws Exception {
        int raftPort = freePort();
        Configuration coordinatorConf = new Configuration();
        // Presence of coordinator.seed.nodes + absence of zookeeper.address selects the RAFT
        // backend. The coordinator's Ratis transport uses coordinator.raft.voters (separate from
        // the Fluss RPC listeners below).
        coordinatorConf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + raftPort);
        coordinatorConf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + raftPort);
        coordinatorConf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        coordinatorConf.set(
                ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        coordinatorConf.set(
                ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        coordinatorConf.set(
                ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(150));
        coordinatorConf.set(
                ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT, Duration.ofMillis(5_000));
        coordinatorConf.set(
                ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT, Duration.ofMillis(100));
        // Bind CLIENT + FLUSS on real localhost ports; only CLIENT is advertised as a placeholder,
        // so the FLUSS internal listener advertises its real bound endpoint (what servers use).
        coordinatorConf.setString(
                ConfigOptions.BIND_LISTENERS,
                "CLIENT://localhost:0," + INTERNAL_LISTENER + "://localhost:0");
        coordinatorConf.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        coordinatorConf.set(ConfigOptions.REMOTE_DATA_DIR, coordinatorRemoteDir.toString());

        CoordinatorServer coordinator = new CoordinatorServer(coordinatorConf);
        TabletServer tabletServer = null;
        RpcClient rpcClient =
                RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance());
        try {
            coordinator.start();

            MetadataStoreClient leaderMeta = coordinator.getMetadataStoreClient();
            waitUntil(
                    30_000,
                    () ->
                            coordinator.isLeader()
                                    && leaderMeta.getCoordinatorLeaderAddress().isPresent()
                                    && leaderMeta
                                            .listDatabases()
                                            .contains(CoordinatorServer.DEFAULT_DATABASE),
                    "coordinator did not boot as a Raft leader with the default database");

            String seed = internalSeed(coordinator);

            // ---- boot a full real TabletServer on RAFT pointing at the coordinator ----
            Configuration tsConf = new Configuration();
            // The real FLUSS listener of the coordinator; selects RAFT and seeds discovery.
            tsConf.setString(ConfigOptions.COORDINATOR_SEED_NODES, seed);
            tsConf.set(ConfigOptions.TABLET_SERVER_ID, TABLET_SERVER_ID);
            tsConf.setString(ConfigOptions.DATA_DIR, tabletDataDir.toString());
            tsConf.set(ConfigOptions.REMOTE_DATA_DIR, tabletRemoteDir.toString());
            tsConf.setString(
                    ConfigOptions.BIND_LISTENERS,
                    "CLIENT://localhost:0," + INTERNAL_LISTENER + "://localhost:0");
            tsConf.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:101");
            tsConf.set(
                    ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT,
                    Duration.ofMillis(5_000));
            tsConf.set(ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT, Duration.ofMillis(100));
            tsConf.setString(ConfigOptions.INTERNAL_LISTENER_NAME, INTERNAL_LISTENER);

            tabletServer = new TabletServer(tsConf);
            // Boots on RAFT: a stream-fed follower metadata client (no embedded Ratis voter) + the
            // TabletServerRaftMetadataFeed that registers and converges the cache from the stream.
            tabletServer.start();

            // (1) registration over Netty joined the coordinator's membership.
            waitUntil(
                    20_000,
                    () -> {
                        int[] ids = leaderMeta.getSortedTabletServerList();
                        return ids.length == 1 && ids[0] == TABLET_SERVER_ID;
                    },
                    "coordinator leader did not see the tablet server registration");

            // (1b) the tablet server's own cache converged from the metadata stream.
            TabletServerMetadataCache cache = tabletServer.getMetadataCache();
            waitUntil(
                    20_000,
                    () ->
                            cache.isAliveTabletServer(TABLET_SERVER_ID)
                                    && cache.getCoordinatorServer(INTERNAL_LISTENER) != null,
                    "tablet server cache did not converge via the metadata stream");
            assertThat(cache.getAllAliveTabletServers(INTERNAL_LISTENER))
                    .containsKey(TABLET_SERVER_ID);

            // ---- (2) DATA PLANE: writer ids via the real TabletServer over Netty ----
            ServerNode tsNode = tabletServerNode(tabletServer);
            TabletServerGateway tsGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> tsNode, rpcClient, TabletServerGateway.class);
            long firstWriterId = tsGateway.initWriter(new InitWriterRequest()).get().getWriterId();
            long secondWriterId = tsGateway.initWriter(new InitWriterRequest()).get().getWriterId();
            // The tablet server reserved writer ids from the coordinator leader (KIND_WRITER) via
            // the AllocateId RPC — forward-only, no reuse.
            assertThat(firstWriterId).isGreaterThanOrEqualTo(0L);
            assertThat(secondWriterId).isGreaterThan(firstWriterId);

            // ---- (3) the other two tablet-server id kinds are served by the same leader ----
            ServerNode coordinatorNode = coordinatorNode(seed);
            CoordinatorGateway coordinatorGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> coordinatorNode, rpcClient, CoordinatorGateway.class);

            AllocateIdResponse col =
                    coordinatorGateway
                            .allocateId(
                                    new AllocateIdRequest()
                                            .setIdKind(
                                                    AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN)
                                            .setCount(5L)
                                            .setCounterKey("col".getBytes(StandardCharsets.UTF_8)))
                            .get();
            assertThat(col.getCount()).isEqualTo(5L);
            assertThat(col.getStartId()).isGreaterThanOrEqualTo(0L);
            // a second reservation for the same column continues strictly after the first range.
            AllocateIdResponse col2 =
                    coordinatorGateway
                            .allocateId(
                                    new AllocateIdRequest()
                                            .setIdKind(
                                                    AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN)
                                            .setCount(1L)
                                            .setCounterKey("col".getBytes(StandardCharsets.UTF_8)))
                            .get();
            assertThat(col2.getStartId()).isGreaterThanOrEqualTo(col.getStartId() + 5L);

            AllocateIdResponse snap =
                    coordinatorGateway
                            .allocateId(
                                    new AllocateIdRequest()
                                            .setIdKind(AllocateIdRpcServer.KIND_BUCKET_SNAPSHOT)
                                            .setCount(1L)
                                            .setCounterKey(
                                                    "bucket".getBytes(StandardCharsets.UTF_8)))
                            .get();
            assertThat(snap.getStartId()).isGreaterThanOrEqualTo(0L);
        } finally {
            if (tabletServer != null) {
                tabletServer.close();
            }
            rpcClient.close();
            coordinator.close();
        }
    }

    /** The coordinator's real (bound) FLUSS internal listener as a {@code host:port} seed entry. */
    private static String internalSeed(CoordinatorServer coordinator) {
        return internalEndpoint(coordinator.getRpcServer().getBindEndpoints());
    }

    private static ServerNode coordinatorNode(String seed) {
        String[] hostPort = seed.split(":");
        return new ServerNode(
                0, hostPort[0], Integer.parseInt(hostPort[1]), ServerType.COORDINATOR);
    }

    private static ServerNode tabletServerNode(TabletServer tabletServer) {
        String seed = internalEndpoint(tabletServer.getRpcServer().getBindEndpoints());
        String[] hostPort = seed.split(":");
        return new ServerNode(
                TABLET_SERVER_ID,
                hostPort[0],
                Integer.parseInt(hostPort[1]),
                ServerType.TABLET_SERVER);
    }

    private static String internalEndpoint(List<Endpoint> binds) {
        Endpoint internal =
                binds.stream()
                        .filter(e -> e.getListenerName().equals(INTERNAL_LISTENER))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "No " + INTERNAL_LISTENER + " listener bound."));
        return internal.getHost() + ":" + internal.getPort();
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
                // leadership / store / stream still settling; retry until the deadline
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
