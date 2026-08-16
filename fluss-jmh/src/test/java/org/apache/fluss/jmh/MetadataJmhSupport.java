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

package org.apache.fluss.jmh;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperTestUtils;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ServerTags;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.ZkVersion;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.ExitUtils;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.FileUtils;

import org.apache.curator.test.TestingServer;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Shared helpers for §8.4 ZooKeeper vs Raft metadata-path JMH benches.
 *
 * <p>Short-run defaults (warmup 2 × 1s, measurement 3 × 1s, 1 in-process fork) are intended to
 * finish in minutes on a laptop while still producing usable AverageTime numbers.
 */
final class MetadataJmhSupport {

    static final String ZK = "ZOOKEEPER";
    static final String RAFT = "RAFT";
    static final String REMOTE_DIR = "file:///tmp/fluss-jmh-remote";
    /** ZooKeeper {@code -1} (any version); Raft ignores CAS tokens. */
    static final int MATCH_ANY_VERSION = ZkVersion.MATCH_ANY_VERSION.getVersion();

    private static final Map<String, StoreSession> SHARED_STORES = new HashMap<>();
    private static final Map<String, AdminSession> SHARED_ADMINS = new HashMap<>();

    static {
        // Ratis GrpcService.startImpl() calls ExitUtils.terminate() on bind failure.
        ExitUtils.disableSystemExit();
    }

    private MetadataJmhSupport() {}

    /** Minimal 1-bucket table used by create/register benches. */
    static TableDescriptor benchmarkTableDescriptor() {
        return TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(1)
                .build();
    }

    /** 1-bucket partitioned log table (no auto-partition) for Admin create/drop partition. */
    static TableDescriptor benchmarkPartitionedTableDescriptor() {
        return TableDescriptor.builder()
                .schema(
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.STRING())
                                .build())
                .distributedBy(1)
                .partitionedBy("pt")
                .build();
    }

    static Schema benchmarkSchema() {
        return Schema.newBuilder().column("id", DataTypes.INT()).build();
    }

    static DatabaseRegistration databaseRegistration() {
        return DatabaseRegistration.of(DatabaseDescriptor.builder().comment("jmh").build());
    }

    static TableRegistration tableRegistration(long tableId, TableDescriptor descriptor) {
        return TableRegistration.newTable(tableId, REMOTE_DIR, descriptor);
    }

    static TableAssignment tableAssignment() {
        return TableAssignment.builder().add(0, BucketAssignment.of(1)).build();
    }

    static PartitionAssignment partitionAssignment(long tableId) {
        return new PartitionAssignment(tableId, tableAssignment().getBucketAssignments());
    }

    static LeaderAndIsr leaderAndIsr() {
        return new LeaderAndIsr(
                1, 10, Collections.singletonList(1), Collections.emptyList(), 0, 1000);
    }

    static TabletServerRegistration tabletServerRegistration(int tabletServerId) {
        return new TabletServerRegistration(
                "rack" + tabletServerId,
                Endpoint.fromListenersString("CLIENT://host" + tabletServerId + ":3456"),
                System.currentTimeMillis());
    }

    static CoordinatorAddress coordinatorAddress(String id) {
        return new CoordinatorAddress(id, Endpoint.fromListenersString("CLIENT://localhost:10012"));
    }

    static AccessControlEntry aclEntry() {
        return new AccessControlEntry(
                new FlussPrincipal("u1", "USER"), "*", OperationType.READ, PermissionType.ALLOW);
    }

    static Set<AccessControlEntry> aclEntries() {
        return Collections.singleton(aclEntry());
    }

    static ServerTags serverTags() {
        return new ServerTags(Collections.singletonMap(1, ServerTag.PERMANENT_OFFLINE));
    }

    static RebalanceTask rebalanceTask(String rebalanceId) {
        TableBucket bucket = new TableBucket(1L, 0);
        Map<TableBucket, RebalancePlanForBucket> plan = new HashMap<>();
        plan.put(
                bucket,
                new RebalancePlanForBucket(
                        bucket, 0, 1, Collections.singletonList(0), Collections.singletonList(1)));
        return new RebalanceTask(rebalanceId, RebalanceStatus.NOT_STARTED, plan);
    }

    static KvSnapshotLeaseMetadata kvLeaseMetadata() {
        Map<Long, FsPath> paths = new HashMap<>();
        paths.put(1L, new FsPath("/tmp/fluss-jmh-lease"));
        return new KvSnapshotLeaseMetadata(1000L, paths);
    }

    static ProducerOffsets producerOffsets() {
        return new ProducerOffsets(1_000L, Collections.emptyList());
    }

    static RemoteLogManifestHandle remoteLogManifestHandle() {
        return new RemoteLogManifestHandle(new FsPath("file:///tmp/fluss-jmh/m.manifest"), 100L);
    }

    static BucketSnapshot bucketSnapshot(long snapshotId) {
        return new BucketSnapshot(snapshotId, 0L, "file:///tmp/fluss-jmh/snap-" + snapshotId);
    }

    static LakeTable lakeTable() {
        return new LakeTable(Collections.emptyList());
    }

    static Map<String, String> entityConfig() {
        return Collections.singletonMap("jmh.key", "v");
    }

    static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Reuses one store backend per {@code backend} for the in-process {@code @Fork(0)} JVM so
     * successive JMH trials do not leak Ratis ports.
     */
    static StoreSession sharedStore(String backend, long raftIdBlockSize) throws Exception {
        synchronized (SHARED_STORES) {
            StoreSession existing = SHARED_STORES.get(backend);
            if (existing != null) {
                return existing;
            }
            StoreSession started = startStoreWithRetry(backend, raftIdBlockSize);
            SHARED_STORES.put(backend, started);
            return started;
        }
    }

    /**
     * Reuses one {@link FlussClusterExtension} per {@code backend} for Admin benches
     * ({@code @Fork(0)}).
     */
    static AdminSession sharedAdmin(String backend) throws Exception {
        synchronized (SHARED_ADMINS) {
            AdminSession existing = SHARED_ADMINS.get(backend);
            if (existing != null) {
                return existing;
            }
            FlussClusterExtension cluster =
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(1)
                            .setMetadataBackend(MetadataBackend.valueOf(backend))
                            .build();
            cluster.start();
            Connection connection = ConnectionFactory.createConnection(cluster.getClientConfig());
            AdminSession started = new AdminSession(cluster, connection, connection.getAdmin());
            SHARED_ADMINS.put(backend, started);
            return started;
        }
    }

    private static StoreSession startStoreWithRetry(String backend, long raftIdBlockSize)
            throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                return StoreSession.start(backend, raftIdBlockSize);
            } catch (Exception e) {
                last = e;
                Thread.sleep(50L * (attempt + 1));
            }
        }
        throw last;
    }

    /** Closes reused store/admin backends so the {@code @Fork(0)} JVM can exit. */
    static void closeShared() {
        synchronized (SHARED_STORES) {
            for (StoreSession session : SHARED_STORES.values()) {
                try {
                    session.close();
                } catch (Exception ignored) {
                    // best-effort shutdown
                }
            }
            SHARED_STORES.clear();
        }
        synchronized (SHARED_ADMINS) {
            for (AdminSession session : SHARED_ADMINS.values()) {
                try {
                    if (session.admin != null) {
                        session.admin.close();
                    }
                    if (session.connection != null) {
                        session.connection.close();
                    }
                    if (session.cluster != null) {
                        session.cluster.close();
                    }
                } catch (Exception ignored) {
                    // best-effort shutdown
                }
            }
            SHARED_ADMINS.clear();
        }
    }

    private static Configuration newStoreConf() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, "file:///tmp/fluss-jmh-remote");
        return conf;
    }

    static void waitUntilRaftLeader(RaftMetadataClient client) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!client.isLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        if (!client.isLeader()) {
            throw new IllegalStateException("Raft node did not become leader within 30s");
        }
    }

    /**
     * Short JMH flags used for the §8.4 baseline. {@code forks(0)} keeps the cluster in-process
     * (same pattern as {@link LogScannerBenchmark}).
     */
    static Options shortRunOptions(String includeRegex) {
        return new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(includeRegex)
                .warmupIterations(2)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(1))
                .forks(0)
                .timeUnit(TimeUnit.MICROSECONDS)
                .build();
    }

    /** Embedded ZooKeeper TestingServer or 1-voter Ratis, exposing {@link MetadataStoreClient}. */
    static final class StoreSession implements AutoCloseable {
        final String backend;
        final MetadataStoreClient store;
        boolean seeded;
        private final TestingServer zooKeeper;
        private final Path raftDir;

        private StoreSession(
                String backend, MetadataStoreClient store, TestingServer zooKeeper, Path raftDir) {
            this.backend = backend;
            this.store = store;
            this.zooKeeper = zooKeeper;
            this.raftDir = raftDir;
        }

        static StoreSession start(String backend, long raftIdBlockSize) throws Exception {
            if (ZK.equals(backend)) {
                TestingServer zk = ZooKeeperTestUtils.createAndStartZookeeperTestingServer();
                Configuration conf = newStoreConf();
                conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, zk.getConnectString());
                MetadataStoreClient client =
                        ZooKeeperUtils.startZookeeperClient(conf, NOPErrorHandler.INSTANCE);
                return new StoreSession(ZK, client, zk, null);
            }
            if (RAFT.equals(backend)) {
                Path dir = Files.createTempDirectory("jmh-raft-meta");
                int port = freePort();
                String address = "127.0.0.1:" + port;
                Configuration conf = newStoreConf();
                conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, address);
                conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, address);
                conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, dir.toString());
                conf.set(ConfigOptions.COORDINATOR_RAFT_ID_BLOCK_SIZE, raftIdBlockSize);
                conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
                conf.set(
                        ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT,
                        Duration.ofMillis(100));
                conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(100));
                RaftMetadataClient client = RaftMetadataClient.create(conf);
                waitUntilRaftLeader(client);
                return new StoreSession(RAFT, client, null, dir);
            }
            throw new IllegalArgumentException("Unknown backend: " + backend);
        }

        @Override
        public void close() throws Exception {
            if (store != null) {
                store.close();
            }
            if (zooKeeper != null) {
                zooKeeper.close();
            }
            if (raftDir != null) {
                FileUtils.deleteDirectoryQuietly(raftDir.toFile());
            }
        }
    }

    /** Shared Admin + cluster for {@link MetadataPathBenchmark} ({@code @Fork(0)} reuse). */
    static final class AdminSession {
        final FlussClusterExtension cluster;
        final Connection connection;
        final Admin admin;
        boolean seeded;

        AdminSession(FlussClusterExtension cluster, Connection connection, Admin admin) {
            this.cluster = cluster;
            this.connection = connection;
            this.admin = admin;
        }
    }
}
