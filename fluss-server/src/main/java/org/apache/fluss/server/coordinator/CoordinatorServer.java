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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.RpcServer;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.netty.server.RequestsMetrics;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.ServerBase;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.authorizer.AuthorizerLoader;
import org.apache.fluss.server.coordinator.gossip.GossipDiscovery;
import org.apache.fluss.server.coordinator.gossip.GossipMember;
import org.apache.fluss.server.coordinator.gossip.GossipService;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.raft.RaftCoordinatorMetadataServices;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.coordinator.raft.RaftMetrics;
import org.apache.fluss.server.coordinator.rebalance.RebalanceManager;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
import org.apache.fluss.server.metadata.ServerMetadataCache;
import org.apache.fluss.server.metrics.ServerMetricUtils;
import org.apache.fluss.server.metrics.group.CoordinatorMetricGroup;
import org.apache.fluss.server.metrics.group.LakeTieringMetricGroup;
import org.apache.fluss.server.storage.DiskWriteLimitConfigValidator;
import org.apache.fluss.server.zk.ZkEpoch;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.ExecutorUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;
import org.apache.fluss.utils.concurrent.FutureUtils;
import org.apache.fluss.utils.concurrent.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.config.ConfigOptions.BACKGROUND_THREADS;
import static org.apache.fluss.config.FlussConfigUtils.validateCoordinatorConfigs;

/**
 * Coordinator server implementation. The coordinator server is responsible to:
 *
 * <ul>
 *   <li>manage the tablet servers
 *   <li>manage the metadata
 *   <li>coordinate the whole cluster, e.g. data re-balance, recover data when tablet servers down
 * </ul>
 */
public class CoordinatorServer extends ServerBase {

    public static final String DEFAULT_DATABASE = "fluss";
    private static final String SERVER_NAME = "CoordinatorServer";

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final CompletableFuture<Result> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);
    private final Clock clock;

    private final String serverId;

    @GuardedBy("lock")
    private MetricRegistry metricRegistry;

    @GuardedBy("lock")
    private CoordinatorMetricGroup serverMetricGroup;

    @GuardedBy("lock")
    private RpcServer rpcServer;

    @GuardedBy("lock")
    private RpcClient rpcClient;

    @GuardedBy("lock")
    private ClientMetricGroup clientMetricGroup;

    @GuardedBy("lock")
    private CoordinatorService coordinatorService;

    @GuardedBy("lock")
    private CoordinatorMetadataCache metadataCache;

    @GuardedBy("lock")
    private MetadataManager metadataManager;

    @GuardedBy("lock")
    private CoordinatorChannelManager coordinatorChannelManager;

    @GuardedBy("lock")
    private CoordinatorEventProcessor coordinatorEventProcessor;

    @GuardedBy("lock")
    @Nullable
    private ZooKeeperClient zkClient;

    // Backend-neutral metadata client (§5.5/§7.3.1). During the dual-support phase the ZooKeeper
    // backend returns a ZooKeeperClient, which is also assigned to zkClient above; downstream
    // managers still take the concrete ZooKeeperClient until they are migrated incrementally.
    @GuardedBy("lock")
    private MetadataStoreClient metaClient;

    @GuardedBy("lock")
    private AutoPartitionManager autoPartitionManager;

    @GuardedBy("lock")
    private LakeTableTieringManager lakeTableTieringManager;

    /** Shared scheduler for lightweight coordinator background tasks. */
    @GuardedBy("lock")
    private Scheduler scheduler;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    @GuardedBy("lock")
    @Nullable
    private Authorizer authorizer;

    private boolean authorizerStarted;

    @GuardedBy("lock")
    private DynamicConfigManager dynamicConfigManager;

    @GuardedBy("lock")
    private LakeCatalogDynamicLoader lakeCatalogDynamicLoader;

    @GuardedBy("lock")
    private RemoteDirDynamicLoader remoteDirDynamicLoader;

    @GuardedBy("lock")
    private LeaderElection coordinatorLeaderElection;

    @GuardedBy("lock")
    private KvSnapshotLeaseManager kvSnapshotLeaseManager;

    @GuardedBy("lock")
    private ReplicaCapacityController replicaCapacityController;

    // Leader-side Raft metadata services (cross-process metadata stream §5.8 + TabletServer
    // registration/heartbeat §5.4.2). Non-null ONLY on the Raft backend; null on ZooKeeper.
    @GuardedBy("lock")
    @Nullable
    private RaftCoordinatorMetadataServices raftMetadataServices;

    /** Optional gossip membership (§1.2 / §5.4.4). Null when gossip is disabled. */
    @GuardedBy("lock")
    @Nullable
    private GossipService gossipService;

    public CoordinatorServer(Configuration conf) {
        this(conf, SystemClock.getInstance());
    }

    public CoordinatorServer(Configuration conf, Clock clock) {
        super(conf);
        validateCoordinatorConfigs(conf);
        this.terminationFuture = new CompletableFuture<>();
        this.serverId = UUID.randomUUID().toString();
        this.clock = clock;
    }

    public static void main(String[] args) {
        Configuration configuration =
                loadConfiguration(args, CoordinatorServer.class.getSimpleName());
        applyServerDefaultConfigurations(configuration);
        CoordinatorServer coordinatorServer = new CoordinatorServer(configuration);
        startServer(coordinatorServer);
    }

    @Override
    protected void startServices() throws Exception {
        electCoordinatorLeaderAsync();
    }

    private void electCoordinatorLeaderAsync() throws Exception {
        initCoordinatorStandby();

        // start election (coordinatorLeaderElection is created inside initCoordinatorStandby
        // after zkClient is initialized)
        coordinatorLeaderElection.startElectLeaderAsync(
                () -> {
                    try {
                        initCoordinatorLeader();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                },
                (Throwable t) -> {
                    try {
                        cleanupCoordinatorLeader();
                    } catch (Exception e) {
                        LOG.error("Failed to cleanup coordinator leader services", e);
                    }
                });
    }

    protected void initCoordinatorStandby() throws Exception {
        // When a coordinator server starts, it first becomes a standby.
        // This method execute initialization for standby, opening necessary rpc server port.
        // Corresponding rpc methods will reject requests from clients
        // and just serve for health check.
        synchronized (lock) {
            LOG.info("Initializing Coordinator services as standby.");
            List<Endpoint> endpoints = Endpoint.loadBindEndpoints(conf, ServerType.COORDINATOR);

            this.scheduler = new FlussScheduler(conf.get(BACKGROUND_THREADS));
            scheduler.startup();

            // for metrics
            this.metricRegistry = MetricRegistry.create(conf, pluginManager);
            this.serverMetricGroup =
                    ServerMetricUtils.createCoordinatorGroup(
                            metricRegistry,
                            ServerMetricUtils.validateAndGetClusterId(conf),
                            endpoints.get(0).getHost(),
                            serverId);

            // Obtain the metadata client + leader election through the backend-neutral seam
            // (§7.3.1). For the ZooKeeper backend this yields exactly the objects created before
            // (a ZooKeeperClient and a CoordinatorLeaderElection), so behavior is unchanged. For
            // the Raft backend this bootstraps a RaftMetadataClient owning an embedded Ratis
            // server; its leader-side stream/registration handlers are wired onto the coordinator
            // gateway further below (§5.4.2/§5.8).
            MetadataBackend backend = MetadataBackend.from(conf);
            this.metaClient = MetadataStoreClientFactory.create(conf, this);
            // On the ZooKeeper backend the client IS a ZooKeeperClient; zkClient is kept for the
            // genuinely ZK-only calls that have no Raft analog (split-brain fencing, the session
            // re-init listener, DynamicConfigManager's config-change notification watcher). It is
            // null on the Raft backend.
            this.zkClient =
                    metaClient instanceof ZooKeeperClient ? (ZooKeeperClient) metaClient : null;

            // Leader election via the backend-neutral seam (CoordinatorLeaderElection on ZooKeeper,
            // RaftLeaderSelector on Raft).
            this.coordinatorLeaderElection = metaClient.newLeaderSelector(serverId);

            this.lakeCatalogDynamicLoader = new LakeCatalogDynamicLoader(conf, pluginManager, true);
            this.remoteDirDynamicLoader = new RemoteDirDynamicLoader(conf);
            this.metadataCache = new CoordinatorMetadataCache();
            this.replicaCapacityController =
                    new ReplicaCapacityController(conf, metadataCache, serverMetricGroup);

            // On ZooKeeper, config-change notifications reach tablet servers through a ZK
            // change-notification watcher; on Raft they are delivered via the metadata stream
            // (T6-2), so use the backend-neutral constructor (no watcher) there. zkClient is null
            // on the Raft backend, which would make the ZK watcher NPE at startup.
            if (zkClient != null) {
                this.dynamicConfigManager = new DynamicConfigManager(zkClient, conf);
            } else {
                this.dynamicConfigManager = new DynamicConfigManager(metaClient, conf, true);
            }

            this.authorizer = AuthorizerLoader.createAuthorizer(conf, metaClient, pluginManager);
            // Do not call authorizer.startup() here: DefaultAuthorizer.loadCache() reads ACL
            // keys via MetadataStoreClient which requireLeader() on Raft. Standby init runs
            // before (or racing) catch-up sets isLeader=true — that threw
            // NotCoordinatorLeaderException and failed CoordinatorServer.start when
            // authorizer.enabled=true. Startup is deferred to initCoordinatorLeader().

            this.lakeTableTieringManager =
                    new LakeTableTieringManager(
                            new LakeTieringMetricGroup(metricRegistry, serverMetricGroup));

            this.metadataManager = new MetadataManager(metaClient, conf, lakeCatalogDynamicLoader);
            this.ioExecutor =
                    Executors.newFixedThreadPool(
                            conf.get(ConfigOptions.SERVER_IO_POOL_SIZE),
                            new ExecutorThreadFactory("coordinator-io"));

            // Initialize and start the kv snapshot lease manager
            this.kvSnapshotLeaseManager =
                    new KvSnapshotLeaseManager(
                            conf.get(ConfigOptions.KV_SNAPSHOT_LEASE_EXPIRATION_CHECK_INTERVAL)
                                    .toMillis(),
                            metaClient,
                            conf.getString(ConfigOptions.REMOTE_DATA_DIR),
                            clock,
                            serverMetricGroup);
            kvSnapshotLeaseManager.start();

            this.coordinatorService =
                    new CoordinatorService(
                            conf,
                            remoteFileSystem,
                            metaClient,
                            this::getCoordinatorEventProcessor,
                            metadataCache,
                            metadataManager,
                            authorizer,
                            lakeCatalogDynamicLoader,
                            lakeTableTieringManager,
                            remoteDirDynamicLoader,
                            dynamicConfigManager,
                            ioExecutor,
                            kvSnapshotLeaseManager,
                            coordinatorLeaderElection,
                            replicaCapacityController);

            // On the Raft backend, assemble the leader-side metadata services (the cross-process
            // metadata stream §5.8 and TabletServer registration/heartbeat §5.4.2) from the
            // bootstrapped RaftMetadataClient and expose their RPC handlers through the coordinator
            // gateway. Only meaningful on the leader — the handlers self-guard with requireLeader()
            // and the liveness fencer checks isLeader — so it is safe to create and start them
            // while this node is still a standby.
            if (backend == MetadataBackend.RAFT) {
                this.raftMetadataServices =
                        RaftCoordinatorMetadataServices.create(
                                (RaftMetadataClient) metaClient,
                                conf,
                                dynamicConfigManager::reloadDynamicConfig);
                raftMetadataServices.start();
                wireRaftLeaderHandlers(coordinatorService, raftMetadataServices);
                // §5.12 Raft observability gauges (role/term/commit/applied/lag).
                RaftMetrics.register(serverMetricGroup, (RaftMetadataClient) metaClient);
            }

            this.rpcServer =
                    RpcServer.create(
                            conf,
                            endpoints,
                            coordinatorService,
                            serverMetricGroup,
                            RequestsMetrics.createCoordinatorServerRequestMetrics(
                                    serverMetricGroup));
            // Register server reconfigurable components
            dynamicConfigManager.register(lakeCatalogDynamicLoader);
            dynamicConfigManager.register(remoteDirDynamicLoader);
            dynamicConfigManager.register(replicaCapacityController);
            // Register stateless validators for coordinator-side upfront validation
            dynamicConfigManager.register(new HistoricalLookupCacheConfigValidator());
            dynamicConfigManager.register(new DiskWriteLimitConfigValidator());
            rpcServer.getServerReconfigurables().forEach(dynamicConfigManager::register);
            dynamicConfigManager.startup();

            rpcServer.start();

            startGossipIfEnabled();

            registerCoordinatorServer();
            // The ZooKeeper session re-init listener re-registers the coordinator ephemeral node
            // after a ZK session expiry; there is no such session on the Raft backend.
            if (zkClient != null) {
                ZooKeeperUtils.registerZookeeperClientReInitSessionListener(
                        zkClient, this::registerCoordinatorServer, this);
            }
        }
    }

    /**
     * Exposes the leader-side Raft metadata handlers (§5.4.2/§5.8/§5.3) through the coordinator
     * gateway on the Raft backend: the cross-process metadata stream, TabletServer
     * registration/heartbeat, and TabletServer id/sequence allocation (the AllocateId RPC that
     * replaces the three ZooKeeper-backed sequence generators — writer id / kv auto-increment
     * column id / bucket snapshot id). The handlers self-guard with {@code requireLeader()}, so it
     * is safe to set them while this node is still a standby. Package-private so the assembly test
     * drives the exact wiring {@link #initCoordinatorStandby} performs.
     */
    static void wireRaftLeaderHandlers(
            CoordinatorService coordinatorService,
            RaftCoordinatorMetadataServices raftMetadataServices) {
        coordinatorService.setMetadataStreamRpcServer(
                raftMetadataServices.metadataStreamRpcServer());
        coordinatorService.setTabletServerRegistrationRpcServer(
                raftMetadataServices.tabletServerRegistrationRpcServer());
        coordinatorService.setAllocateIdRpcServer(raftMetadataServices.allocateIdRpcServer());
    }

    protected void initCoordinatorLeader() throws Exception {
        // Establish the fencing epoch for this leadership term. On ZooKeeper this is a CAS on the
        // /coordinators/epoch znode (returns a ZkEpoch); on Raft the leader term IS the fence and
        // is
        // already reflected by the LeaderElection, so no extra write is needed (§5.1.6).
        CoordinatorEpoch coordinatorEpoch;
        if (zkClient != null) {
            ZkEpoch zkEpoch = zkClient.fenceBecomeCoordinatorLeader(serverId);
            // expose the freshly-fenced epoch through the LeaderElection contract (no extra ZK
            // access)
            ((CoordinatorLeaderElection) coordinatorLeaderElection).setCurrentEpoch(zkEpoch);
            coordinatorEpoch = zkEpoch;
        } else {
            coordinatorEpoch = coordinatorLeaderElection.currentEpoch();
        }
        registerCoordinatorLeader();

        synchronized (lock) {
            this.clientMetricGroup = new ClientMetricGroup(metricRegistry, SERVER_NAME);
            this.rpcClient = RpcClient.create(conf, clientMetricGroup);

            this.coordinatorChannelManager = new CoordinatorChannelManager(rpcClient);

            this.autoPartitionManager =
                    new AutoPartitionManager(
                            metadataCache,
                            metadataManager,
                            remoteDirDynamicLoader,
                            conf,
                            replicaCapacityController);
            autoPartitionManager.start();

            // start coordinator event processor after we register coordinator leader to zk
            // so that the event processor can get the coordinator leader node from zk during
            // start up. In HA for coordinator server, the processor also need to know the leader
            // node during start up
            CoordinatorContext coordinatorContext = new CoordinatorContext(coordinatorEpoch);
            this.coordinatorEventProcessor =
                    new CoordinatorEventProcessor(
                            metaClient,
                            metadataCache,
                            coordinatorChannelManager,
                            coordinatorContext,
                            replicaCapacityController,
                            autoPartitionManager,
                            lakeTableTieringManager,
                            serverMetricGroup,
                            conf,
                            ioExecutor,
                            metadataManager,
                            kvSnapshotLeaseManager,
                            scheduler,
                            clock);
            if (raftMetadataServices != null) {
                coordinatorEventProcessor.setTabletServerLivenessManager(
                        raftMetadataServices.livenessManager());
            }
            // Load ACL cache once we are a serving leader (requireLeader() succeeds). Idempotent
            // across re-elections: DefaultAuthorizer ZK watcher must not be started twice.
            if (authorizer != null && !authorizerStarted) {
                authorizer.startup();
                authorizerStarted = true;
            }
            // Reload kv-snapshot leases now that requireLeader() succeeds. start() may have run
            // before Raft leadership and skipped the load.
            if (kvSnapshotLeaseManager != null) {
                kvSnapshotLeaseManager.reloadLeasesFromStore();
            }
            coordinatorEventProcessor.startup();

            // As the active leader, this server is the sole writer of dynamic configs and holds the
            // latest values, so stop consuming change notifications to avoid rolling a value back.
            dynamicConfigManager.pauseListening();

            createDefaultDatabase();
        }
    }

    /**
     * Cleans up leader-specific resources when this server loses leadership.
     *
     * <p>This method is called by {@link CoordinatorLeaderElection} when the server transitions
     * from leader to standby. It cleans up leader-only resources while keeping the server running
     * as a standby, ready to participate in future elections.
     */
    protected void cleanupCoordinatorLeader() {
        synchronized (lock) {
            LOG.info("Cleaning up coordinator leader services.");

            try {
                // make sure the current coordinator leader node is unregistered.
                // Different from ZK disconnection,
                // when we actively release the Leader's election,
                // we need to manually delete the node
                unregisterCoordinatorLeader();
            } catch (Throwable t) {
                LOG.warn("Failed to unregister coordinator leader from Zookeeper", t);
            }

            // Clean up leader-specific resources in reverse order of initialization
            try {
                if (coordinatorEventProcessor != null) {
                    coordinatorEventProcessor.shutdown();
                    coordinatorEventProcessor = null;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to shutdown coordinator event processor", t);
            }

            try {
                if (kvSnapshotLeaseManager != null) {
                    kvSnapshotLeaseManager.clearInMemoryLeases();
                }
            } catch (Throwable t) {
                LOG.warn("Failed to clear kv snapshot lease manager state", t);
            }

            try {
                if (coordinatorChannelManager != null) {
                    coordinatorChannelManager.close();
                    coordinatorChannelManager = null;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to close coordinator channel manager", t);
            }

            try {
                if (autoPartitionManager != null) {
                    autoPartitionManager.close();
                    autoPartitionManager = null;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to close auto partition manager", t);
            }

            try {
                if (rpcClient != null) {
                    rpcClient.close();
                    rpcClient = null;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to close RPC client", t);
            }

            try {
                if (clientMetricGroup != null) {
                    clientMetricGroup.close();
                    clientMetricGroup = null;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to close client metric group", t);
            }

            try {
                // Back to standby: resume consuming config-change notifications and re-sync from
                // ZooKeeper to pick up any changes the new leader made while we were not listening.
                dynamicConfigManager.resumeListening();
            } catch (Throwable t) {
                LOG.warn("Failed to resume dynamic config listening", t);
            }

            LOG.info("Coordinator leader services cleaned up successfully.");
        }
    }

    @Override
    protected CompletableFuture<Result> closeAsync(Result result) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info("Shutting down Coordinator server ({}).", result);
            CompletableFuture<Void> serviceShutdownFuture = stopServices();

            serviceShutdownFuture.whenComplete(
                    ((Void ignored2, Throwable serviceThrowable) -> {
                        if (serviceThrowable != null) {
                            terminationFuture.completeExceptionally(serviceThrowable);
                        } else {
                            terminationFuture.complete(result);
                        }
                    }));
        }

        return terminationFuture;
    }

    private void registerCoordinatorServer() throws Exception {
        CoordinatorAddress coordinatorAddress = buildCoordinatorAddress();
        registerToZookeeperWithRetry(
                "coordinator server",
                () -> metaClient.registerCoordinatorServer(coordinatorAddress));
    }

    private void registerCoordinatorLeader() throws Exception {
        CoordinatorAddress coordinatorAddress = buildCoordinatorAddress();
        registerToZookeeperWithRetry(
                "coordinator leader",
                () -> metaClient.registerCoordinatorLeader(coordinatorAddress));
    }

    private void unregisterCoordinatorLeader() throws Exception {
        CoordinatorAddress coordinatorAddress = buildCoordinatorAddress();
        metaClient.unregisterCoordinatorLeader(coordinatorAddress);
    }

    private CoordinatorAddress buildCoordinatorAddress() {
        List<Endpoint> bindEndpoints = rpcServer.getBindEndpoints();
        return new CoordinatorAddress(
                this.serverId, Endpoint.loadAdvertisedEndpoints(bindEndpoints, conf));
    }

    /**
     * Starts optional gossip membership exchange when {@code coordinator.gossip.enabled} is true
     * (design §5.4.4). Advertises this coordinator's RPC endpoint so TabletServers can expand a
     * partial {@code coordinator.seed.nodes} list.
     */
    private void startGossipIfEnabled() throws Exception {
        if (!conf.getBoolean(ConfigOptions.COORDINATOR_GOSSIP_ENABLED)) {
            return;
        }
        List<Endpoint> advertised =
                Endpoint.loadAdvertisedEndpoints(rpcServer.getBindEndpoints(), conf);
        if (advertised.isEmpty()) {
            LOG.warn(
                    "coordinator.gossip.enabled is true but no advertised endpoint is available; "
                            + "skipping gossip.");
            return;
        }
        Endpoint ep = advertised.get(0);
        GossipMember self =
                new GossipMember(
                        serverId,
                        ep.getHost(),
                        ep.getPort(),
                        ep.getHost(),
                        conf.get(ConfigOptions.COORDINATOR_GOSSIP_PORT),
                        0L,
                        1);
        this.gossipService =
                GossipService.start(self, GossipDiscovery.seedsFromConfig(conf), conf, clock);
    }

    /**
     * Registers to ZooKeeper with retry logic to handle the case where the ephemeral node may still
     * exist for a while after ZK client reconnects.
     *
     * @param description a description of the registration for logging
     * @param registration the registration action to perform
     * @see <a href="https://issues.apache.org/jira/browse/ZOOKEEPER-2985">ZOOKEEPER-2985</a>
     */
    private void registerToZookeeperWithRetry(String description, ThrowingRunnable registration)
            throws Exception {
        long startTime = System.currentTimeMillis();
        while (true) {
            try {
                registration.run();
                break;
            } catch (KeeperException.NodeExistsException nodeExistsException) {
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime >= ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS) {
                    LOG.error(
                            "Registering {} to Zookeeper exceeded total retry time of {} ms. "
                                    + "Aborting registration attempts.",
                            description,
                            ZOOKEEPER_REGISTER_TOTAL_WAIT_TIME_MS);
                    throw nodeExistsException;
                }

                LOG.warn(
                        "Node for {} already exists in Zookeeper. "
                                + "Retrying register after {} ms....",
                        description,
                        ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                try {
                    Thread.sleep(ZOOKEEPER_REGISTER_RETRY_INTERVAL_MS);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /** A functional interface for actions that may throw checked exceptions. */
    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private void createDefaultDatabase() {
        MetadataManager metadataManager =
                new MetadataManager(metaClient, conf, lakeCatalogDynamicLoader);
        List<String> databases = metadataManager.listDatabases();
        if (databases.isEmpty()) {
            metadataManager.createDatabase(DEFAULT_DATABASE, DatabaseDescriptor.EMPTY, true);
            LOG.info("Created default database '{}' because no database exists.", DEFAULT_DATABASE);
        }
        // create Kafka default database if Kafka is enabled.
        if (conf.get(ConfigOptions.KAFKA_ENABLED)) {
            String kafkaDB = conf.get(ConfigOptions.KAFKA_DATABASE);
            if (!databases.contains(kafkaDB)) {
                metadataManager.createDatabase(kafkaDB, DatabaseDescriptor.EMPTY, true);
                LOG.info("Created default database '{}' for Kafka protocol.", kafkaDB);
            }
        }
    }

    /**
     * Get the coordinator event processor. Don't call this method directly as the coordinator event
     * processor is single threaded model.
     */
    @VisibleForTesting
    public CoordinatorEventProcessor getCoordinatorEventProcessor() {
        if (coordinatorEventProcessor != null) {
            return coordinatorEventProcessor;
        } else {
            throw new IllegalStateException("CoordinatorEventProcessor is not initialized yet.");
        }
    }

    @VisibleForTesting
    ReplicaCapacityController getReplicaCapacityController() {
        return replicaCapacityController;
    }

    @VisibleForTesting
    MetadataManager getMetadataManager() {
        return metadataManager;
    }

    CompletableFuture<Void> stopServices() {
        // Closing the leader election may wait for cleanupCoordinatorLeader(), which acquires
        // lock. Close it before entering the synchronized shutdown section to avoid deadlock.
        Throwable leaderElectionException = null;
        try {
            if (coordinatorLeaderElection != null) {
                coordinatorLeaderElection.close();
                coordinatorLeaderElection = null;
            }
        } catch (Throwable t) {
            leaderElectionException = t;
        }

        synchronized (lock) {
            Throwable exception = leaderElectionException;

            try {
                // We must shut down the scheduler early because otherwise, the scheduler could
                // touch other resources that might have been shutdown and cause exceptions.
                if (scheduler != null) {
                    scheduler.shutdown();
                    scheduler = null;
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (gossipService != null) {
                    gossipService.close();
                    gossipService = null;
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (serverMetricGroup != null) {
                    serverMetricGroup.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);
            try {
                if (metricRegistry != null) {
                    terminationFutures.add(metricRegistry.closeAsync());
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (autoPartitionManager != null) {
                    autoPartitionManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorEventProcessor != null) {
                    coordinatorEventProcessor.shutdown();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorChannelManager != null) {
                    coordinatorChannelManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (rpcServer != null) {
                    terminationFutures.add(rpcServer.closeAsync());
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (coordinatorService != null) {
                    coordinatorService.shutdown();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                // Release parked stream long-polls and stop the liveness scheduler before the
                // underlying Ratis server is shut down (via metaClient.close() below).
                if (raftMetadataServices != null) {
                    raftMetadataServices.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (ioExecutor != null) {
                    // shutdown io executor
                    ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, ioExecutor);
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (lakeTableTieringManager != null) {
                    lakeTableTieringManager.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (authorizer != null) {
                    authorizer.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (dynamicConfigManager != null) {
                    dynamicConfigManager.close();
                }

                if (lakeCatalogDynamicLoader != null) {
                    lakeCatalogDynamicLoader.close();
                }

                if (remoteDirDynamicLoader != null) {
                    remoteDirDynamicLoader.close();
                }

                if (kvSnapshotLeaseManager != null) {
                    kvSnapshotLeaseManager.close();
                }

            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (zkClient != null) {
                    zkClient.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                // On the Raft backend metaClient is a RaftMetadataClient owning the embedded Ratis
                // server and must be closed explicitly. On ZooKeeper metaClient == zkClient (closed
                // just above), so only close here when they differ, to avoid a double close.
                if (metaClient != null && metaClient != zkClient) {
                    metaClient.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            try {
                if (rpcClient != null) {
                    rpcClient.close();
                }

                if (clientMetricGroup != null) {
                    clientMetricGroup.close();
                }
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }
            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    protected CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    public CoordinatorService getCoordinatorService() {
        return coordinatorService;
    }

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }

    @VisibleForTesting
    public RpcServer getRpcServer() {
        return rpcServer;
    }

    @VisibleForTesting
    public String getServerId() {
        return serverId;
    }

    @VisibleForTesting
    public ServerMetadataCache getMetadataCache() {
        return metadataCache;
    }

    @VisibleForTesting
    public @Nullable Authorizer getAuthorizer() {
        return authorizer;
    }

    public DynamicConfigManager getDynamicConfigManager() {
        return dynamicConfigManager;
    }

    @VisibleForTesting
    public RebalanceManager getRebalanceManager() {
        return coordinatorEventProcessor.getRebalanceManager();
    }

    @VisibleForTesting
    public ZooKeeperClient getZooKeeperClient() {
        return zkClient;
    }

    @VisibleForTesting
    public boolean isLeader() {
        return coordinatorLeaderElection != null && coordinatorLeaderElection.isLeader();
    }

    @VisibleForTesting
    public MetadataStoreClient getMetadataStoreClient() {
        return metaClient;
    }
}
