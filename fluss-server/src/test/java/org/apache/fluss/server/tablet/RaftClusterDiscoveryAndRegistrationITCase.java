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
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.coordinator.CoordinatorServer;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.raft.InMemoryMetadataKvStore;
import org.apache.fluss.server.coordinator.raft.MetadataWatchService;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.coordinator.raft.RaftMetadataStore;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end 2-node (coordinator + tablet-side feed) test on the RAFT metadata backend over REAL
 * Netty RPC (§5.4.1 / §5.4.2). It boots a real single-node {@link CoordinatorServer} on the Raft
 * backend (embedded Ratis, real Netty {@code RpcServer}) and then drives the exact
 * tablet-server-side coordinator-facing handshake against it — WITHOUT ZooKeeper and WITHOUT the
 * in-process gateway shortcut {@link TabletServerRaftMetadataFeedITCase} uses:
 *
 * <ol>
 *   <li>{@link CoordinatorDiscovery} resolves the coordinator leader from {@code
 *       coordinator.seed.nodes} by issuing the real {@code metadata()} RPC over Netty and reading
 *       {@code MetadataResponse.coordinator_server} on the internal listener (the empty-cache
 *       bootstrap a freshly-started TabletServer performs);
 *   <li>a {@link TabletServerRaftMetadataFeed} built over a real leader-following {@code
 *       CoordinatorGateway} proxy registers this server (RegisterTabletServer over Netty) — the
 *       coordinator's leader-side {@code TabletServerRegistrationRpcServer} joins it to membership,
 *       verified from the leader's own store;
 *   <li>the background metadata stream ({@code FetchMetadata} over real Netty) converges the {@link
 *       TabletServerMetadataCache} with the coordinator node + this server, purely from the stream
 *       (the cross-process replacement for the ZooKeeper watches / {@code UpdateMetadata} push).
 * </ol>
 *
 * <p>This RUNS the previously-gated cross-process discovery + registration + metadata-stream path
 * end-to-end against a real coordinator over real Netty, the payoff the tablet-server Raft boot
 * branch (§5.4.2) was built for. It is the FIRST test to exercise the {@code FetchMetadata} record
 * path over real Netty serialization — the sibling {@link TabletServerRaftMetadataFeedITCase}
 * drives the stream through an in-process handler. It deliberately does NOT boot a full {@code
 * TabletServer} — that additionally requires the tablet-side metadata client to be a stream-fed
 * follower view rather than an embedded Ratis voter (a separate integration step) — so it isolates
 * and verifies just the coordinator-facing handshake.
 */
class RaftClusterDiscoveryAndRegistrationITCase {

    private static final String INTERNAL_LISTENER = "FLUSS";
    private static final int TABLET_SERVER_ID = 1;

    @TempDir private Path raftDir;

    @TempDir private Path remoteDir;

    @Test
    void testTabletServerDiscoversRegistersAndConvergesOverNetty() throws Exception {
        int raftPort = freePort();
        Configuration conf = new Configuration();
        // Presence of coordinator.seed.nodes + absence of zookeeper.address selects RAFT. The seed
        // list here is only a placeholder that selects the backend; the tablet side below discovers
        // the coordinator from its actual (bound) FLUSS listener, not from this value.
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + raftPort);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(150));
        conf.set(ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT, Duration.ofMillis(5_000));
        conf.set(ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT, Duration.ofMillis(100));
        // Bind CLIENT + FLUSS on real localhost ports. Only CLIENT is advertised (placeholder), so
        // the FLUSS internal listener advertises its real bound endpoint (reachable), which is what
        // TabletServers use to reach the coordinator.
        conf.setString(
                ConfigOptions.BIND_LISTENERS,
                "CLIENT://localhost:0," + INTERNAL_LISTENER + "://localhost:0");
        conf.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        conf.set(ConfigOptions.REMOTE_DATA_DIR, remoteDir.toString());

        CoordinatorServer coordinator = new CoordinatorServer(conf);
        RpcClient rpcClient =
                RpcClient.create(new Configuration(), TestingClientMetricGroup.newInstance());
        TabletServerRaftMetadataFeed feed = null;
        try {
            coordinator.start();

            MetadataStoreClient leaderMeta = coordinator.getMetadataStoreClient();
            // Wait until the coordinator has booted as a Raft leader AND initCoordinatorLeader ran
            // (default database created), which is also when the coordinator's metadata cache holds
            // the coordinator node — so metadata() can return it for discovery.
            waitUntil(
                    30_000,
                    () ->
                            coordinator.isLeader()
                                    && leaderMeta.getCoordinatorLeaderAddress().isPresent()
                                    && leaderMeta
                                            .listDatabases()
                                            .contains(CoordinatorServer.DEFAULT_DATABASE),
                    "coordinator did not boot as a Raft leader with the default database");

            // ---- (1) discover the coordinator from its real FLUSS internal listener over Netty
            // ----
            String seed = internalSeed(coordinator);
            CoordinatorDiscovery discovery =
                    new CoordinatorDiscovery(Collections.singletonList(seed), rpcClient);
            ServerNode coordinatorNode = discovery.discoverCoordinatorLeader(20_000, 500L);
            // The coordinator advertised its reachable FLUSS internal endpoint (not the CLIENT
            // placeholder), resolved over a real metadata() RPC.
            assertThat(coordinatorNode).isNotNull();
            assertThat(coordinatorNode.serverType()).isEqualTo(ServerType.COORDINATOR);
            assertThat(coordinatorNode.host() + ":" + coordinatorNode.port()).isEqualTo(seed);

            CoordinatorGateway coordinatorGateway =
                    GatewayClientProxy.createGatewayProxy(
                            () -> coordinatorNode, rpcClient, CoordinatorGateway.class);

            // The cache's MetadataManager is only used for on-demand table-metadata fallback (not
            // exercised here), so back it with a throwaway follower read view — no embedded Ratis
            // voter needed on the tablet side.
            RaftMetadataStore throwawayStore =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            TabletServerMetadataCache cache =
                    new TabletServerMetadataCache(
                            new MetadataManager(
                                    RaftMetadataClient.followerReadView(throwawayStore),
                                    conf,
                                    null));

            // ---- (2) register this server against the real coordinator gateway over Netty ----
            feed =
                    TabletServerRaftMetadataFeed.create(
                            conf,
                            TABLET_SERVER_ID,
                            coordinatorGateway,
                            cache,
                            () ->
                                    new RegisterTabletServerRequest()
                                            .setServerId(TABLET_SERVER_ID)
                                            .setRegisterTimestamp(System.currentTimeMillis())
                                            .setRack("rack1")
                                            .setListeners(INTERNAL_LISTENER + "://localhost:9001"),
                            null,
                            null);
            feed.start();

            // registration (over Netty) joined the coordinator's membership: the blocking first
            // RegisterTabletServer succeeded, and the leader's own store now lists this server id.
            assertThat(feed.registrationLoop().isRegistered()).isTrue();
            waitUntil(
                    15_000,
                    () -> {
                        int[] ids = leaderMeta.getSortedTabletServerList();
                        return ids.length == 1 && ids[0] == TABLET_SERVER_ID;
                    },
                    "coordinator leader did not see the tablet server registration");

            assertThat(leaderMeta.getSortedTabletServerList()).containsExactly(TABLET_SERVER_ID);

            // ---- (3) the background metadata stream (FetchMetadata over real Netty) converged the
            // cache with the coordinator node + this server — no ZooKeeper, no UpdateMetadata push,
            // no manual polling. This exercises the record path over real Netty serialization for
            // the first time (the sibling ITCase drives it through an in-process handler).
            waitUntil(
                    20_000,
                    () ->
                            cache.isAliveTabletServer(TABLET_SERVER_ID)
                                    && cache.getCoordinatorServer(INTERNAL_LISTENER) != null,
                    "tablet server cache did not converge via the metadata stream");

            assertThat(cache.getAllAliveTabletServers(INTERNAL_LISTENER))
                    .containsKey(TABLET_SERVER_ID);
            // the converged coordinator node carries the reachable internal endpoint discovered
            // above
            assertThat(
                            cache.getCoordinatorServer(INTERNAL_LISTENER).host()
                                    + ":"
                                    + cache.getCoordinatorServer(INTERNAL_LISTENER).port())
                    .isEqualTo(seed);
        } finally {
            if (feed != null) {
                feed.close();
            }
            rpcClient.close();
            coordinator.close();
        }
    }

    /** The coordinator's real (bound) FLUSS internal listener as a {@code host:port} seed entry. */
    private static String internalSeed(CoordinatorServer coordinator) {
        List<Endpoint> binds = coordinator.getRpcServer().getBindEndpoints();
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
