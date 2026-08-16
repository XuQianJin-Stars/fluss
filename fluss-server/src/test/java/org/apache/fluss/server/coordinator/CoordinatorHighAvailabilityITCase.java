/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.exception.InvalidCoordinatorException;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.CreateDatabaseRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.metrics.TestingClientMetricGroup;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.Watcher;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.ZooKeeper;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for Coordinator Server High Availability on ZooKeeper and Raft.
 *
 * <p>ZooKeeper runs two coordinators and expires the leader's session (LeaderLatch). Raft runs a
 * 3-voter quorum and closes the leader process (majority remains), then restarts it as a follower.
 *
 * <p>Verifies that only the leader processes RPC requests while standbys return {@link
 * NotCoordinatorLeaderException}, plus failover, re-entrant election, standby dynamic-config
 * tracking (#3625), and stale-epoch fencing.
 */
class CoordinatorHighAvailabilityITCase {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static ZooKeeperClient zookeeperClient;

    private MetadataBackend currentBackend;
    private final List<CoordinatorServer> coordinators = new ArrayList<>();
    private final List<Configuration> coordinatorConfs = new ArrayList<>();
    private TabletServer tabletServer;
    private RpcClient rpcClient;

    @TempDir Path tempDir;

    @BeforeAll
    static void baseBeforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
    }

    @BeforeEach
    void setUp() throws Exception {
        Configuration clientConf = new Configuration();
        rpcClient = RpcClient.create(clientConf, TestingClientMetricGroup.newInstance());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = coordinators.size() - 1; i >= 0; i--) {
            CoordinatorServer server = coordinators.get(i);
            if (server != null) {
                server.close();
            }
        }
        coordinators.clear();
        coordinatorConfs.clear();
        if (tabletServer != null) {
            tabletServer.close();
            tabletServer = null;
        }
        if (rpcClient != null) {
            rpcClient.close();
        }
        currentBackend = null;
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testLeaderAndStandbyRpcBehavior(MetadataBackend backend) throws Exception {
        startCoordinators(backend);

        waitUntilCoordinatorServerElected();

        CoordinatorAddress leaderAddress = currentLeaderAddress();
        CoordinatorServer leaderServer = findServerById(leaderAddress.getId());
        CoordinatorServer standbyServer = findServerByNotId(leaderAddress.getId());

        assertThat(leaderServer).isNotNull();
        assertThat(standbyServer).isNotNull();

        CoordinatorGateway leaderGateway = createGatewayForServer(leaderServer);
        CoordinatorGateway standbyGateway = createGatewayForServer(standbyServer);

        String testDbName = "test_ha_db_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest =
                new CreateDatabaseRequest().setDatabaseName(testDbName).setIgnoreIfExists(false);

        leaderGateway.createDatabase(createDbRequest).get();

        String testDbName2 = "test_ha_db2_" + System.currentTimeMillis();
        CreateDatabaseRequest createDbRequest2 =
                new CreateDatabaseRequest().setDatabaseName(testDbName2).setIgnoreIfExists(false);

        assertThatThrownBy(() -> standbyGateway.createDatabase(createDbRequest2).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(NotCoordinatorLeaderException.class)
                .hasMessageContaining("not the current leader");
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testLeaderLosesLeadershipAndReElected(MetadataBackend backend) throws Exception {
        startCoordinators(backend);

        waitUntilCoordinatorServerElected();
        CoordinatorAddress firstLeaderAddr = currentLeaderAddress();

        CoordinatorServer leader = findServerById(firstLeaderAddr.getId());
        CoordinatorServer standby = findServerByNotId(firstLeaderAddr.getId());
        assertThat(leader).isNotNull();
        assertThat(standby).isNotNull();

        verifyServerIsLeader(leader, "test_reentrant_db_1");
        verifyServerIsStandby(standby);

        CoordinatorServer former = stepDownAndRejoin(leader);
        CoordinatorServer newLeader = findLeaderServer();
        assertThat(newLeader).isNotNull();
        assertThat(newLeader.getServerId()).isNotEqualTo(firstLeaderAddr.getId());

        verifyServerIsLeader(newLeader, "test_reentrant_db_2");
        verifyServerIsStandby(former);

        CoordinatorServer originalAgain = stepDownAndRejoin(newLeader);
        CoordinatorServer reelected = findLeaderServer();
        assertThat(reelected).isNotNull();
        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThat(reelected.getServerId())
                    .as("Original leader should be re-elected, proving re-entrant election")
                    .isEqualTo(former.getServerId());
        }

        verifyServerIsLeader(reelected, "test_reentrant_db_3");
        createGatewayForServer(reelected).metadata(new MetadataRequest()).get();
        verifyServerIsStandby(originalAgain);
    }

    /**
     * Regression test for #3625: a standby coordinator must keep applying dynamic config changes so
     * that, after failover, the promoted leader uses the latest config rather than a stale snapshot
     * taken when it started up as standby.
     */
    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testStandbyTracksDynamicConfigAndNewLeaderUsesItAfterFailover(MetadataBackend backend)
            throws Exception {
        startCoordinators(backend);

        waitUntilCoordinatorServerElected();
        CoordinatorAddress firstLeaderAddr = currentLeaderAddress();

        CoordinatorServer leader = findServerById(firstLeaderAddr.getId());
        assertThat(leader).isNotNull();

        String configKey = ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER.key();

        leader.getDynamicConfigManager()
                .alterConfigs(
                        Collections.singletonList(
                                new AlterConfig(configKey, "3", AlterConfigOpType.SET)));

        for (CoordinatorServer server : liveCoordinators()) {
            if (server == leader) {
                continue;
            }
            waitUntil(
                    () -> "3".equals(dynamicConfigValue(server, configKey)),
                    Duration.ofSeconds(30),
                    "Standby coordinator did not apply dynamic config change while standby");
        }

        stepDownAndRejoin(leader);
        CoordinatorServer promoted = findLeaderServer();
        assertThat(promoted).isNotNull();
        assertThat(promoted.getServerId()).isNotEqualTo(firstLeaderAddr.getId());

        assertThat(dynamicConfigValue(promoted, configKey))
                .as("Newly promoted leader should use the dynamic config it tracked while standby")
                .isEqualTo("3");

        promoted.getDynamicConfigManager()
                .alterConfigs(
                        Collections.singletonList(
                                new AlterConfig(configKey, "5", AlterConfigOpType.SET)));
        assertThat(dynamicConfigValue(promoted, configKey)).isEqualTo("5");
    }

    /** Reads the effective value of a config key from a server's DynamicConfigManager. */
    private static String dynamicConfigValue(CoordinatorServer server, String configKey) {
        for (ConfigEntry entry : server.getDynamicConfigManager().describeConfigs()) {
            if (entry.key().equals(configKey)) {
                return entry.value();
            }
        }
        return null;
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testMultipleLeadershipLossAndRecovery(MetadataBackend backend) throws Exception {
        startCoordinators(backend);

        for (int i = 0; i < 3; i++) {
            waitUntilCoordinatorServerElected();
            CoordinatorServer currentLeader = findLeaderServer();
            assertThat(currentLeader).isNotNull();

            verifyServerIsLeader(currentLeader, "test_cycle_db_" + i);

            for (CoordinatorServer other : liveCoordinators()) {
                if (other != currentLeader) {
                    waitUntilServerReadyToRejoin(other);
                }
            }

            stepDownAndRejoin(currentLeader);
        }

        CoordinatorServer finalLeader = findLeaderServer();
        assertThat(finalLeader).isNotNull();
        createGatewayForServer(finalLeader).metadata(new MetadataRequest()).get();
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testTabletServerRejectsStaleCoordinatorEpochAfterLeaderSwitch(MetadataBackend backend)
            throws Exception {
        startCoordinators(backend);
        waitUntilCoordinatorServerElected();
        tabletServer = new TabletServer(createTabletServerConfiguration());
        tabletServer.start();

        CoordinatorAddress firstLeaderAddr = currentLeaderAddress();
        CoordinatorServer leader = findServerById(firstLeaderAddr.getId());

        int oldCoordinatorEpoch = leader.getCoordinatorEventProcessor().getCoordinatorEpoch();

        stepDownAndRejoin(leader);
        CoordinatorServer promoted = findLeaderServer();
        assertThat(promoted).isNotNull();
        assertThat(promoted.getServerId()).isNotEqualTo(firstLeaderAddr.getId());

        int newCoordinatorEpoch = promoted.getCoordinatorEventProcessor().getCoordinatorEpoch();
        waitUntil(
                () -> tabletServer.getReplicaManager().getCoordinatorEpoch() == newCoordinatorEpoch,
                Duration.ofSeconds(30),
                "Tablet server did not update to new coordinator epoch");

        TabletServerGateway tsGateway = createGatewayForTabletServer(tabletServer);

        assertThatThrownBy(
                        () ->
                                tsGateway
                                        .updateMetadata(
                                                new UpdateMetadataRequest()
                                                        .setCoordinatorEpoch(oldCoordinatorEpoch))
                                        .get())
                .satisfies(
                        t ->
                                assertThat(getRootCause(t))
                                        .isInstanceOf(InvalidCoordinatorException.class));
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testStaleCoordinatorRequestRejectedAfterLeaderSwitch(MetadataBackend backend)
            throws Exception {
        startCoordinators(backend);

        waitUntilCoordinatorServerElected();
        CoordinatorAddress firstLeaderAddr = currentLeaderAddress();

        CoordinatorServer leader = findServerById(firstLeaderAddr.getId());
        int oldLeaderEpochZkVersion =
                leader.getCoordinatorEventProcessor()
                        .getCoordinatorContext()
                        .getCoordinatorZkVersion();

        CoordinatorServer former = stepDownAndRejoin(leader);
        CoordinatorServer promoted = findLeaderServer();
        assertThat(promoted).isNotNull();

        TableBucket tableBucket = new TableBucket(1, 1);
        LeaderAndIsr leaderAndIsr =
                new LeaderAndIsr(0, 0, Arrays.asList(2, 3), Collections.emptyList(), 0, 0);

        MetadataStoreClient oldLeaderStore = former.getMetadataStoreClient();
        MetadataStoreClient newLeaderStore = promoted.getMetadataStoreClient();

        int newLeaderEpochZkVersion =
                promoted.getCoordinatorEventProcessor()
                        .getCoordinatorContext()
                        .getCoordinatorZkVersion();

        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThatThrownBy(
                            () ->
                                    oldLeaderStore.registerLeaderAndIsr(
                                            tableBucket, leaderAndIsr, oldLeaderEpochZkVersion))
                    .satisfies(
                            t ->
                                    assertThat(getRootCause(t))
                                            .isInstanceOf(
                                                    KeeperException.BadVersionException.class));
        } else {
            assertThatThrownBy(
                            () ->
                                    oldLeaderStore.registerLeaderAndIsr(
                                            tableBucket, leaderAndIsr, oldLeaderEpochZkVersion))
                    .satisfies(
                            t ->
                                    assertThat(getRootCause(t))
                                            .isInstanceOf(NotCoordinatorLeaderException.class));
        }
        newLeaderStore.registerLeaderAndIsr(tableBucket, leaderAndIsr, newLeaderEpochZkVersion);
        assertThat(newLeaderStore.getLeaderAndIsr(tableBucket)).hasValue(leaderAndIsr);
    }

    private TabletServerGateway createGatewayForTabletServer(TabletServer server) {
        List<Endpoint> endpoints = server.getRpcServer().getBindEndpoints();
        Endpoint endpoint = endpoints.get(0);
        ServerNode serverNode =
                new ServerNode(
                        server.getServerId(),
                        endpoint.getHost(),
                        endpoint.getPort(),
                        ServerType.TABLET_SERVER);

        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, TabletServerGateway.class);
    }

    private CoordinatorGateway createGatewayForServer(CoordinatorServer server) {
        List<Endpoint> endpoints = server.getRpcServer().getBindEndpoints();
        Endpoint endpoint = endpoints.get(0);
        ServerNode serverNode =
                new ServerNode(
                        server.hashCode(),
                        endpoint.getHost(),
                        endpoint.getPort(),
                        ServerType.COORDINATOR);

        return GatewayClientProxy.createGatewayProxy(
                () -> serverNode, rpcClient, CoordinatorGateway.class);
    }

    private void startCoordinators(MetadataBackend backend) throws Exception {
        currentBackend = backend;
        if (backend == MetadataBackend.RAFT) {
            int[] ports = freePorts(3);
            String[] addresses = new String[3];
            StringBuilder voters = new StringBuilder();
            for (int i = 0; i < 3; i++) {
                addresses[i] = "127.0.0.1:" + ports[i];
                if (i > 0) {
                    voters.append(',');
                }
                voters.append(addresses[i]);
            }
            for (int i = 0; i < 3; i++) {
                Configuration conf =
                        createRaftCoordinatorConfiguration(
                                voters.toString(), addresses[i], "raft-" + i);
                coordinatorConfs.add(conf);
                coordinators.add(new CoordinatorServer(conf));
            }
        } else {
            for (int i = 0; i < 2; i++) {
                Configuration conf = createZkConfiguration();
                coordinatorConfs.add(conf);
                coordinators.add(new CoordinatorServer(conf));
            }
        }
        for (CoordinatorServer server : coordinators) {
            server.start();
        }
    }

    private static Configuration createZkConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        applySharedServerConfig(configuration, true);
        configuration.set(ConfigOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_CONNECTION_TIMEOUT, Duration.ofSeconds(5));
        configuration.set(ConfigOptions.ZOOKEEPER_RETRY_WAIT, Duration.ofMillis(500));
        return configuration;
    }

    private Configuration createRaftCoordinatorConfiguration(
            String voters, String localAddress, String dirName) {
        Configuration configuration = new Configuration();
        configuration.setString(ConfigOptions.COORDINATOR_SEED_NODES, voters);
        configuration.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, voters);
        configuration.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, localAddress);
        configuration.setString(
                ConfigOptions.COORDINATOR_RAFT_DIR,
                tempDir.resolve(dirName).toAbsolutePath().toString());
        configuration.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
        configuration.set(
                ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
        configuration.set(
                ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(150));
        applySharedServerConfig(configuration, false);
        return configuration;
    }

    private static void applySharedServerConfig(
            Configuration configuration, boolean fakeAdvertisedListeners) {
        configuration.setString(
                ConfigOptions.BIND_LISTENERS, "CLIENT://localhost:0,FLUSS://localhost:0");
        if (fakeAdvertisedListeners) {
            configuration.setString(ConfigOptions.ADVERTISED_LISTENERS, "CLIENT://198.168.0.1:100");
        }
        configuration.set(ConfigOptions.REMOTE_DATA_DIR, "/tmp/fluss/remote-data");
    }

    private Configuration createTabletServerConfiguration() {
        Configuration configuration;
        if (currentBackend == MetadataBackend.RAFT) {
            configuration = new Configuration();
            // Discovery calls metadata() on these seeds. Include every bind endpoint (CLIENT +
            // FLUSS): the same CLIENT handshake waitUntilCoordinatorLeaderReady already uses, plus
            // the internal listener tablets normally target. Leader endpoints first so the first
            // attempt is the serving coordinator.
            List<String> seeds = new ArrayList<>();
            CoordinatorServer leader = findLeaderServer();
            addBindSeeds(seeds, leader);
            for (CoordinatorServer server : liveCoordinators()) {
                if (server != leader) {
                    addBindSeeds(seeds, server);
                }
            }
            configuration.setString(ConfigOptions.COORDINATOR_SEED_NODES, String.join(",", seeds));
            configuration.set(
                    ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT,
                    Duration.ofSeconds(30));
            configuration.set(
                    ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT, Duration.ofMillis(100));
            applySharedServerConfig(configuration, false);
        } else {
            configuration = createZkConfiguration();
        }
        configuration.set(ConfigOptions.TABLET_SERVER_ID, 0);
        configuration.setString(
                ConfigOptions.DATA_DIR, tempDir.resolve("tablet").toAbsolutePath().toString());
        return configuration;
    }

    private static void addBindSeeds(List<String> seeds, CoordinatorServer coordinator) {
        if (coordinator == null || coordinator.getRpcServer() == null) {
            return;
        }
        for (Endpoint endpoint : coordinator.getRpcServer().getBindEndpoints()) {
            seeds.add(endpoint.getHost() + ":" + endpoint.getPort());
        }
    }

    private void waitUntilCoordinatorServerElected() throws Exception {
        waitUntil(
                () -> findLeaderServer() != null && currentLeaderAddress() != null,
                Duration.ofMinutes(1),
                "Fail to wait coordinator server elected");
        waitUntilCoordinatorLeaderReady();
    }

    private void waitUntilCoordinatorLeaderReady() throws Exception {
        waitUntil(
                () -> {
                    CoordinatorServer currentLeader = findLeaderServer();
                    if (currentLeader == null
                            || currentLeader.getCoordinatorService() == null
                            || !currentLeader.getCoordinatorService().isLeader()) {
                        return false;
                    }
                    // isLeader() flips after catch-up; initCoordinatorLeader (event processor +
                    // metadata cache) finishes later. Tablet discovery calls metadata() and needs
                    // coordinator_server in the response.
                    try {
                        currentLeader.getCoordinatorEventProcessor();
                        return createGatewayForServer(currentLeader)
                                .metadata(new MetadataRequest())
                                .get(5, TimeUnit.SECONDS)
                                .hasCoordinatorServer();
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(60),
                "Coordinator leader did not recognize itself as leader");
    }

    private CoordinatorAddress currentLeaderAddress() throws Exception {
        CoordinatorServer leader = findLeaderServer();
        if (leader == null) {
            return null;
        }
        return leader.getMetadataStoreClient().getCoordinatorLeaderAddress().orElse(null);
    }

    private CoordinatorServer findLeaderServer() {
        for (CoordinatorServer server : coordinators) {
            if (server != null && server.isLeader()) {
                return server;
            }
        }
        return null;
    }

    private CoordinatorServer findServerById(String serverId) {
        for (CoordinatorServer server : coordinators) {
            if (server != null && server.getServerId().equals(serverId)) {
                return server;
            }
        }
        return null;
    }

    private CoordinatorServer findServerByNotId(String serverId) {
        for (CoordinatorServer server : coordinators) {
            if (server != null && !server.getServerId().equals(serverId)) {
                return server;
            }
        }
        return null;
    }

    private List<CoordinatorServer> liveCoordinators() {
        List<CoordinatorServer> live = new ArrayList<>();
        for (CoordinatorServer server : coordinators) {
            if (server != null) {
                live.add(server);
            }
        }
        return live;
    }

    private static Throwable getRootCause(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return t.getCause() != null ? t.getCause() : t;
        }
        return t;
    }

    /**
     * Forces the current leader to step down, waits for a successor, and returns a live instance of
     * the former leader that can rejoin election. ZooKeeper expires the session in-process. Raft
     * closes the leader (a 3-voter majority remains) and restarts it as a follower.
     */
    private CoordinatorServer stepDownAndRejoin(CoordinatorServer leader) throws Exception {
        String oldId = leader.getServerId();
        int idx = coordinators.indexOf(leader);
        if (currentBackend == MetadataBackend.RAFT) {
            Configuration conf = coordinatorConfs.get(idx);
            leader.close();
            coordinators.set(idx, null);
            waitUntilNewLeaderElected(oldId);
            CoordinatorServer restarted = new CoordinatorServer(conf);
            restarted.start();
            coordinators.set(idx, restarted);
            waitUntilServerReadyToRejoin(restarted);
            return restarted;
        }
        killZkSession(leader);
        waitUntilNewLeaderElected(oldId);
        waitUntilServerReadyToRejoin(leader);
        return leader;
    }

    /**
     * Kills the ZK session of a CoordinatorServer to simulate real session timeout.
     *
     * <p>This creates a duplicate ZK connection with the same session ID and immediately closes it,
     * which forces the ZK server to expire the original session. This causes:
     *
     * <ol>
     *   <li>All ephemeral nodes for that session to be deleted (LeaderLatch node, leader address)
     *   <li>The original client receives SESSION_EXPIRED event
     *   <li>Curator fires ConnectionState.LOST
     *   <li>LeaderLatch calls notLeader() callback
     *   <li>Curator reconnects with a new session and re-participates in election
     * </ol>
     */
    private void killZkSession(CoordinatorServer server) throws Exception {
        CuratorFramework curatorClient = server.getZooKeeperClient().getCuratorClient();
        ZooKeeper zk = curatorClient.getZookeeperClient().getZooKeeper();
        long sessionId = zk.getSessionId();
        byte[] sessionPasswd = zk.getSessionPasswd();
        String connectString = ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString();

        CountDownLatch connectedLatch = new CountDownLatch(1);
        ZooKeeper dupZk =
                new ZooKeeper(
                        connectString,
                        1000,
                        event -> {
                            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                                connectedLatch.countDown();
                            }
                        },
                        sessionId,
                        sessionPasswd);
        if (!connectedLatch.await(10, TimeUnit.SECONDS)) {
            dupZk.close();
            throw new RuntimeException(
                    "Failed to establish duplicate ZK connection for session kill");
        }
        dupZk.close();
    }

    /** Waits until a new coordinator leader is elected that is different from the given old one. */
    private void waitUntilNewLeaderElected(String oldLeaderId) throws Exception {
        waitUntil(
                () -> {
                    try {
                        CoordinatorServer leader = findLeaderServer();
                        return leader != null && !leader.getServerId().equals(oldLeaderId);
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofMinutes(1),
                "Fail to wait for new coordinator leader to be elected");
        waitUntilCoordinatorLeaderReady();
    }

    /** Verifies that the given server can process requests as a leader. */
    private void verifyServerIsLeader(CoordinatorServer server, String dbName) throws Exception {
        createGatewayForServer(server)
                .createDatabase(
                        new CreateDatabaseRequest().setDatabaseName(dbName).setIgnoreIfExists(true))
                .get();
    }

    /** Verifies that the given server rejects requests as a standby (non-leader). */
    private void verifyServerIsStandby(CoordinatorServer server) {
        assertThatThrownBy(
                        () ->
                                createGatewayForServer(server)
                                        .createDatabase(
                                                new CreateDatabaseRequest()
                                                        .setDatabaseName(
                                                                "standby_check_"
                                                                        + System.nanoTime())
                                                        .setIgnoreIfExists(false))
                                        .get())
                .satisfies(
                        t ->
                                assertThat(getRootCause(t))
                                        .isInstanceOf(NotCoordinatorLeaderException.class));
    }

    /**
     * Waits until a former leader is ready to win the next election: ZooKeeper re-registers the
     * ephemeral node; Raft has finished starting as a follower.
     */
    private void waitUntilServerReadyToRejoin(CoordinatorServer server) {
        if (currentBackend == MetadataBackend.RAFT) {
            waitUntil(
                    () ->
                            server.getCoordinatorService() != null
                                    && !server.isLeader()
                                    && !server.getCoordinatorService().isLeader(),
                    Duration.ofSeconds(30),
                    "Server " + server.getServerId() + " did not start as a Raft follower");
            return;
        }
        String path = "/coordinators/ids/" + server.getServerId();
        waitUntil(
                () -> {
                    try {
                        return zookeeperClient.getCuratorClient().checkExists().forPath(path)
                                != null;
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(30),
                "Server " + server.getServerId() + " did not re-register in ZK");
    }

    private static int[] freePorts(int count) throws Exception {
        ServerSocket[] sockets = new ServerSocket[count];
        int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket socket : sockets) {
                if (socket != null) {
                    socket.close();
                }
            }
        }
        return ports;
    }
}
