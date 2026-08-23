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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.data.BucketSnapshot;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.RebalanceTask;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.server.zk.data.ServerTags;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableBucketAndManifest;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.utils.types.Tuple2;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Store-level metadata writes and reads on {@link MetadataStoreClient} (ZooKeeper vs 1-voter Raft).
 * Isolates consensus + local apply from coordinator DDL. Covers every store method that can run
 * without a tablet cluster; ISR churn / replica RPCs are out of scope.
 *
 * <p>Delete benches include a preceding create of a unique key (noted in §8.4). Raft {@code
 * registerCoordinatorServer} and {@code insertAclChangeNotification} are no-ops.
 *
 * <p>Raft id allocation uses the production {@code coordinator.raft.id-block-size} default (1000).
 * See {@link SequenceIdAllocationBenchmark} for block-size 1 vs 64.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
@Threads(1)
public class MetadataStoreBenchmark {

    private static final String STORE_DB = "jmh_store_db";
    private static final long SEED_TABLE_ID = 1L;
    private static final long SEED_PART_TABLE_ID = 2L;
    private static final long SEED_PARTITION_ID = 1L;
    private static final String SEED_PARTITION_NAME = "p_seed";
    private static final String SEED_LEASE = "lease-seed";
    private static final String SEED_PRODUCER = "prod-seed";
    private static final String SEED_ACL_DB = "jmhacl";

    /**
     * Buckets per ISR batch. Sized above {@link
     * org.apache.fluss.server.coordinator.raft.RaftJournalWriter}'s append chunk (64) so the
     * chunked-pipeline path is exercised, and in the range of a real partitioned table's bucket
     * count on failover (analysis §8.9.2).
     */
    private static final int ISR_BATCH_BUCKETS = 100;

    /** Bucket-id base for ISR batch rows; kept clear of the seeded/unique-bucket ranges. */
    private static final int ISR_BATCH_BUCKET_BASE = 500_000;

    /**
     * Threads for {@link #concurrentUpdateLeaderAndIsr}. Fixed rather than {@code Threads.MAX} so
     * the row is comparable across machines; 8 is enough to keep the Ratis log worker queue
     * non-empty, which is the precondition for fsync coalescing (§8.12.3).
     */
    private static final int CONCURRENT_WRITE_THREADS = 8;

    /** Bucket-id base for the concurrent write row; disjoint from every other range. */
    private static final int CONCURRENT_BUCKET_BASE = 700_000;

    /** Distinct buckets cycled through by the concurrent row (pre-registered in setup). */
    private static final int CONCURRENT_BUCKET_SLOTS = 512;

    /** {@code ZOOKEEPER} or {@code RAFT}. */
    @Param({"ZOOKEEPER", "RAFT"})
    public String backend;

    private MetadataJmhSupport.StoreSession session;
    private MetadataStoreClient store;
    private TableDescriptor descriptor;
    private TablePath readPath;
    private TablePath partTablePath;
    private TableBucket seedBucket;
    private Resource seedAclResource;
    private CoordinatorAddress seedCoordinator;
    private static final AtomicLong GLOBAL_SEQ = new AtomicLong();
    private final AtomicLong seq = new AtomicLong();
    /** Shared across the concurrent row's threads so they spread over distinct buckets. */
    private final AtomicLong concurrentSeq = new AtomicLong();

    private final AtomicInteger schemaIdSeq = new AtomicInteger(2);
    private final AtomicInteger snapshotIdSeq = new AtomicInteger(2);

    /** Starts an embedded ZK TestingServer or 1-voter Ratis metadata client and seeds catalog. */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        long blockSize = ConfigOptions.COORDINATOR_RAFT_ID_BLOCK_SIZE.defaultValue();
        session = MetadataJmhSupport.sharedStore(backend, blockSize);
        store = session.store;
        descriptor = MetadataJmhSupport.benchmarkTableDescriptor();
        readPath = TablePath.of(STORE_DB, "read_table");
        partTablePath = TablePath.of(STORE_DB, "part_table");
        seedBucket = new TableBucket(SEED_TABLE_ID, 0);
        seedAclResource = Resource.database(SEED_ACL_DB);
        seedCoordinator = MetadataJmhSupport.coordinatorAddress("jmh-coord");
        if (!session.seeded) {
            seedCatalog();
            session.seeded = true;
        }
    }

    private void seedCatalog() throws Exception {
        store.registerDatabase(STORE_DB, MetadataJmhSupport.databaseRegistration());
        store.registerTable(
                readPath, MetadataJmhSupport.tableRegistration(SEED_TABLE_ID, descriptor));
        store.registerFirstSchema(readPath, MetadataJmhSupport.benchmarkSchema());
        store.registerTableAssignment(SEED_TABLE_ID, MetadataJmhSupport.tableAssignment());
        store.registerTable(
                partTablePath,
                MetadataJmhSupport.tableRegistration(SEED_PART_TABLE_ID, descriptor));
        store.registerPartitionAssignmentAndMetadata(
                SEED_PARTITION_ID,
                SEED_PARTITION_NAME,
                MetadataJmhSupport.partitionAssignment(SEED_PART_TABLE_ID),
                MetadataJmhSupport.REMOTE_DIR,
                partTablePath,
                SEED_PART_TABLE_ID);
        store.registerLeaderAndIsr(
                seedBucket,
                MetadataJmhSupport.leaderAndIsr(),
                MetadataJmhSupport.MATCH_ANY_VERSION);
        store.registerTabletServer(1, MetadataJmhSupport.tabletServerRegistration(1));
        store.registerCoordinatorLeader(seedCoordinator);
        store.createResourceAcl(seedAclResource, MetadataJmhSupport.aclEntries());
        store.upsertServerEntityConfig(MetadataJmhSupport.entityConfig());
        store.registerServerTags(MetadataJmhSupport.serverTags());
        store.registerRebalanceTask(MetadataJmhSupport.rebalanceTask("jmh-rb-seed"));
        store.registerKvSnapshotLeaseMetadata(SEED_LEASE, MetadataJmhSupport.kvLeaseMetadata());
        store.tryRegisterProducerOffsets(SEED_PRODUCER, MetadataJmhSupport.producerOffsets());
        store.upsertRemoteLogManifestHandle(
                seedBucket, MetadataJmhSupport.remoteLogManifestHandle());
        store.registerTableBucketSnapshot(seedBucket, MetadataJmhSupport.bucketSnapshot(1L));
        store.upsertLakeTable(SEED_TABLE_ID, MetadataJmhSupport.lakeTable(), false);
        // Pre-create the buckets used by batchUpdateLeaderAndIsr: the ZooKeeper batch path issues
        // setData and needs existing znodes, which also makes the benchmark idempotent on both
        // backends (see that method's javadoc).
        for (int i = 0; i < ISR_BATCH_BUCKETS; i++) {
            store.registerLeaderAndIsr(
                    new TableBucket(SEED_TABLE_ID, ISR_BATCH_BUCKET_BASE + i),
                    MetadataJmhSupport.leaderAndIsr(),
                    MetadataJmhSupport.MATCH_ANY_VERSION);
        }
        // Same reason for the concurrent write row: updateLeaderAndIsr is setData on ZooKeeper.
        for (int i = 0; i < CONCURRENT_BUCKET_SLOTS; i++) {
            store.registerLeaderAndIsr(
                    new TableBucket(SEED_TABLE_ID, CONCURRENT_BUCKET_BASE + i),
                    MetadataJmhSupport.leaderAndIsr(),
                    MetadataJmhSupport.MATCH_ANY_VERSION);
        }
        store.getTableIdAndIncrement();
    }

    /** Shared {@code @Fork(0)} store is reused across methods; do not close per trial. */
    @TearDown(Level.Trial)
    public void teardown() {
        // Shared store lives for the in-process JVM (avoids Ratis port leaks between trials).
        // Print the Raft append attribution accumulated over this trial (analysis §8.9.1): how much
        // of a metadata write is the Ratis round-trip (send→reply: gRPC loopback, log append,
        // fsync,
        // majority ack) versus the commit-to-apply handoff. JMH only reports the end-to-end score,
        // so without this the 8.5ms band cannot be decomposed.
        MetadataJmhSupport.printAppendAttribution(session);
    }

    private long nextId() {
        return 1_000_000L + GLOBAL_SEQ.incrementAndGet();
    }

    private TablePath nextTablePath(String prefix) {
        return TablePath.of(STORE_DB, prefix + seq.incrementAndGet());
    }

    private TableRegistration newReg(long tableId) {
        return MetadataJmhSupport.tableRegistration(tableId, descriptor);
    }

    // ---- original rows (kept for §8.4 continuity) ----

    /** Durable metadata write ({@code registerTable} / Raft append + majority commit). */
    @Benchmark
    public void metadataWrite() throws Exception {
        store.registerTable(nextTablePath("t"), newReg(nextId()));
    }

    /** Leader-local metadata read ({@code getTable}). */
    @Benchmark
    public Optional<TableRegistration> metadataRead() throws Exception {
        return store.getTable(readPath);
    }

    /** Sequence-id allocation ({@code getTableIdAndIncrement}). */
    @Benchmark
    public long allocateId() throws Exception {
        return store.getTableIdAndIncrement();
    }

    // ---- schema DDL / database ----

    /** Register a unique database. */
    @Benchmark
    public void registerDatabase() throws Exception {
        store.registerDatabase(
                "db" + seq.incrementAndGet(), MetadataJmhSupport.databaseRegistration());
    }

    /** Overwrite the seeded database registration. */
    @Benchmark
    public void updateDatabase() throws Exception {
        store.updateDatabase(STORE_DB, MetadataJmhSupport.databaseRegistration());
    }

    /** Read the seeded database. */
    @Benchmark
    public Optional<DatabaseRegistration> getDatabase() throws Exception {
        return store.getDatabase(STORE_DB);
    }

    /** Create then delete a unique database. */
    @Benchmark
    public void deleteDatabase() throws Exception {
        String name = "deldb" + seq.incrementAndGet();
        store.registerDatabase(name, MetadataJmhSupport.databaseRegistration());
        store.deleteDatabase(name);
    }

    /** Existence check for the seeded database. */
    @Benchmark
    public boolean databaseExists() throws Exception {
        return store.databaseExists(STORE_DB);
    }

    /** List database names. */
    @Benchmark
    public List<String> listDatabases() throws Exception {
        return store.listDatabases();
    }

    /** Per-database summaries for the seeded name. */
    @Benchmark
    public List<DatabaseSummary> listDatabaseSummaries() throws Exception {
        return store.listDatabaseSummaries(Collections.singletonList(STORE_DB));
    }

    /** Register schema id 1 on a unique table path. */
    @Benchmark
    public int registerFirstSchema() throws Exception {
        return store.registerFirstSchema(
                nextTablePath("sch"), MetadataJmhSupport.benchmarkSchema());
    }

    /** Register a subsequent schema id on the seeded table. */
    @Benchmark
    public int registerSchema() throws Exception {
        return store.registerSchema(
                readPath, MetadataJmhSupport.benchmarkSchema(), schemaIdSeq.getAndIncrement());
    }

    /** Read schema id 1 of the seeded table. */
    @Benchmark
    public Optional<SchemaInfo> getSchemaById() throws Exception {
        return store.getSchemaById(readPath, 1);
    }

    /** Highest schema id of the seeded table. */
    @Benchmark
    public int getCurrentSchemaId() throws Exception {
        return store.getCurrentSchemaId(readPath);
    }

    /** Latest schema for the seeded table. */
    @Benchmark
    public Map<TablePath, SchemaInfo> getLatestSchemas() throws Exception {
        return store.getLatestSchemas(Collections.singletonList(readPath));
    }

    // ---- table / partition ----

    /** Overwrite the seeded table registration. */
    @Benchmark
    public void updateTable() throws Exception {
        store.updateTable(readPath, newReg(SEED_TABLE_ID));
    }

    /** Create then delete a unique table. */
    @Benchmark
    public void deleteTable() throws Exception {
        TablePath path = nextTablePath("del");
        store.registerTable(path, newReg(nextId()));
        store.deleteTable(path);
    }

    /** Existence check for the seeded table. */
    @Benchmark
    public boolean tableExist() throws Exception {
        return store.tableExist(readPath);
    }

    /** List tables in the seeded database. */
    @Benchmark
    public List<String> listTables() throws Exception {
        return store.listTables(STORE_DB);
    }

    /** Batch get of the seeded table. */
    @Benchmark
    public Map<TablePath, TableRegistration> getTables() throws Exception {
        return store.getTables(Collections.singletonList(readPath));
    }

    /** Register a unique partition (assignment + metadata). */
    @Benchmark
    public void registerPartition() throws Exception {
        long id = nextId();
        store.registerPartitionAssignmentAndMetadata(
                id,
                "p" + id,
                MetadataJmhSupport.partitionAssignment(SEED_PART_TABLE_ID),
                MetadataJmhSupport.REMOTE_DIR,
                partTablePath,
                SEED_PART_TABLE_ID);
    }

    /** Create then delete a unique partition. */
    @Benchmark
    public void deletePartition() throws Exception {
        long id = nextId();
        String name = "pdel" + id;
        store.registerPartitionAssignmentAndMetadata(
                id,
                name,
                MetadataJmhSupport.partitionAssignment(SEED_PART_TABLE_ID),
                MetadataJmhSupport.REMOTE_DIR,
                partTablePath,
                SEED_PART_TABLE_ID);
        store.deletePartition(partTablePath, name);
    }

    /** Read the seeded partition. */
    @Benchmark
    public Optional<PartitionRegistration> getPartition() throws Exception {
        return store.getPartition(partTablePath, SEED_PARTITION_NAME);
    }

    /** List partition names of the seeded partitioned table. */
    @Benchmark
    public Set<String> getPartitions() throws Exception {
        return store.getPartitions(partTablePath);
    }

    /** Partition count of the seeded partitioned table. */
    @Benchmark
    public int getPartitionNumber() throws Exception {
        return store.getPartitionNumber(partTablePath);
    }

    /** All partition registrations of the seeded partitioned table. */
    @Benchmark
    public Map<String, PartitionRegistration> getPartitionRegistrations() throws Exception {
        return store.getPartitionRegistrations(partTablePath);
    }

    /** Partition id → path for the seeded partitioned table. */
    @Benchmark
    public Map<Long, PhysicalTablePath> getPartitionIdAndPaths() throws Exception {
        return store.getPartitionIdAndPaths(Collections.singletonList(partTablePath));
    }

    /** Path → {@link TablePartition} for the seeded partition. */
    @Benchmark
    public Map<PhysicalTablePath, TablePartition> getPartitionIds() throws Exception {
        return store.getPartitionIds(
                Collections.singletonList(
                        PhysicalTablePath.of(partTablePath, SEED_PARTITION_NAME)));
    }

    /** Partition name → id for the seeded partitioned table. */
    @Benchmark
    public Map<TablePath, Map<String, Long>> getPartitionNameAndIdsForTables() throws Exception {
        return store.getPartitionNameAndIdsForTables(Collections.singletonList(partTablePath));
    }

    // ---- ISR / assignment ----

    /** Register a unique table assignment. */
    @Benchmark
    public void registerTableAssignment() throws Exception {
        store.registerTableAssignment(nextId(), MetadataJmhSupport.tableAssignment());
    }

    /** Overwrite the seeded table assignment. */
    @Benchmark
    public void updateTableAssignment() throws Exception {
        store.updateTableAssignment(
                SEED_TABLE_ID,
                MetadataJmhSupport.tableAssignment(),
                MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Create then delete a unique table assignment. */
    @Benchmark
    public void deleteTableAssignment() throws Exception {
        long id = nextId();
        store.registerTableAssignment(id, MetadataJmhSupport.tableAssignment());
        store.deleteTableAssignment(id);
    }

    /** Read the seeded table assignment. */
    @Benchmark
    public Optional<TableAssignment> getTableAssignment() throws Exception {
        return store.getTableAssignment(SEED_TABLE_ID);
    }

    /** Batch get of the seeded table assignment. */
    @Benchmark
    public Map<Long, TableAssignment> getTablesAssignments() throws Exception {
        return store.getTablesAssignments(Collections.singletonList(SEED_TABLE_ID));
    }

    /** List table ids that currently have an assignment. */
    @Benchmark
    public List<Long> listAssignedTableIds() throws Exception {
        return store.listAssignedTableIds();
    }

    /** Overwrite the seeded partition assignment. */
    @Benchmark
    public void updatePartitionAssignment() throws Exception {
        store.updatePartitionAssignment(
                SEED_PARTITION_ID,
                MetadataJmhSupport.partitionAssignment(SEED_PART_TABLE_ID),
                MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Create then delete a unique partition assignment. */
    @Benchmark
    public void deletePartitionAssignment() throws Exception {
        long id = nextId();
        store.registerPartitionAssignmentAndMetadata(
                id,
                "asndel" + id,
                MetadataJmhSupport.partitionAssignment(SEED_PART_TABLE_ID),
                MetadataJmhSupport.REMOTE_DIR,
                partTablePath,
                SEED_PART_TABLE_ID);
        store.deletePartitionAssignment(id);
    }

    /** Read the seeded partition assignment. */
    @Benchmark
    public Optional<PartitionAssignment> getPartitionAssignment() throws Exception {
        return store.getPartitionAssignment(SEED_PARTITION_ID);
    }

    /** Batch get of the seeded partition assignment. */
    @Benchmark
    public Map<Long, PartitionAssignment> getPartitionsAssignments() throws Exception {
        return store.getPartitionsAssignments(Collections.singletonList(SEED_PARTITION_ID));
    }

    /** List partition ids that currently have an assignment. */
    @Benchmark
    public List<Long> listAssignedPartitionIds() throws Exception {
        return store.listAssignedPartitionIds();
    }

    /** Register leader-and-isr for a unique bucket. */
    @Benchmark
    public void registerLeaderAndIsr() throws Exception {
        TableBucket bucket = new TableBucket(SEED_TABLE_ID, (int) seq.incrementAndGet() + 10);
        store.registerLeaderAndIsr(
                bucket, MetadataJmhSupport.leaderAndIsr(), MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Overwrite leader-and-isr for the seeded bucket. */
    @Benchmark
    public void updateLeaderAndIsr() throws Exception {
        store.updateLeaderAndIsr(
                seedBucket,
                MetadataJmhSupport.leaderAndIsr(),
                MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Read leader-and-isr for the seeded bucket. */
    @Benchmark
    public Optional<LeaderAndIsr> getLeaderAndIsr() throws Exception {
        return store.getLeaderAndIsr(seedBucket);
    }

    /**
     * Batch-update leader-and-isr for {@value #ISR_BATCH_BUCKETS} buckets — the shape the
     * coordinator uses on failover / ISR shrink ({@code TableBucketStateMachine}, {@code
     * ReplicaStateMachine}, {@code CoordinatorEventProcessor}).
     *
     * <p>ZooKeeper commits the batch in Curator transactions ({@code setData} per bucket). Raft
     * sends independent log entries and waits once for apply (analysis §8.9.2); before that
     * batching it was one commit-blocking append per bucket, i.e. N sequential round-trips. This
     * row exists so that regression is visible — the single-key {@code updateLeaderAndIsr} row
     * cannot show it.
     *
     * <p>Buckets are pre-registered in {@link #seedCatalog()} because the ZooKeeper path uses
     * {@code setData} and requires existing znodes. Both backends are therefore idempotent here,
     * which is what lets this run under {@code AverageTime}. {@code
     * batchRegisterLeaderAndIsrForTablePartition} is deliberately NOT benchmarked: the ZooKeeper
     * implementation uses non-idempotent {@code create} (including a parent-node create), so
     * repeated invocation would fail with {@code NodeExists} rather than measure anything.
     */
    @Benchmark
    public void batchUpdateLeaderAndIsr() throws Exception {
        Map<TableBucket, LeaderAndIsr> batch = new LinkedHashMap<>(ISR_BATCH_BUCKETS);
        LeaderAndIsr leaderAndIsr = MetadataJmhSupport.leaderAndIsr();
        for (int i = 0; i < ISR_BATCH_BUCKETS; i++) {
            batch.put(new TableBucket(SEED_TABLE_ID, ISR_BATCH_BUCKET_BASE + i), leaderAndIsr);
        }
        store.batchUpdateLeaderAndIsr(batch, MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /**
     * Concurrent single-key writes — the only shape in which {@code raft.server.log.async-flush}
     * can help (analysis §8.1.3 / §9.0 #8).
     *
     * <p>Every other write row here is {@code @Threads(1)} and serially awaits apply, so each
     * append pays a full fsync with nothing to batch it against. §8.12.3 showed the Ratis log
     * worker DOES coalesce fsyncs once entries queue up (a 100-bucket batch needed only 0.049
     * fsyncs per append versus 2.0 when serial), so concurrency is the other route to that
     * coalescing — and this row is what makes the effect measurable.
     *
     * <p>Each thread writes a disjoint bucket range so neither backend does cross-thread conflict
     * resolution; what is measured is purely the write pipeline under concurrency. Thread count is
     * fixed (not {@code Threads.MAX}) to keep results comparable across machines.
     */
    @Benchmark
    @Threads(CONCURRENT_WRITE_THREADS)
    public void concurrentUpdateLeaderAndIsr() throws Exception {
        int slot = (int) (concurrentSeq.getAndIncrement() % CONCURRENT_BUCKET_SLOTS);
        store.updateLeaderAndIsr(
                new TableBucket(SEED_TABLE_ID, CONCURRENT_BUCKET_BASE + slot),
                MetadataJmhSupport.leaderAndIsr(),
                MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Batch get of the seeded bucket's leader-and-isr. */
    @Benchmark
    public Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs() throws Exception {
        return store.getLeaderAndIsrs(Collections.singletonList(seedBucket));
    }

    /** Per-bucket metadata for the seeded table. */
    @Benchmark
    public Map<Long, List<BucketMetadata>> getBucketMetadataForTables() throws Exception {
        return store.getBucketMetadataForTables(Collections.singletonList(SEED_TABLE_ID));
    }

    /** Per-bucket metadata for the seeded partition. */
    @Benchmark
    public Map<Long, List<BucketMetadata>> getBucketMetadataForPartitions() throws Exception {
        return store.getBucketMetadataForPartitions(Collections.singletonList(SEED_PARTITION_ID));
    }

    // ---- tablet / coordinator registration ----

    /** Register a unique coordinator server id. Raft is a no-op (membership is the voter set). */
    @Benchmark
    public void registerCoordinatorServer() throws Exception {
        store.registerCoordinatorServer(
                MetadataJmhSupport.coordinatorAddress("c" + seq.incrementAndGet()));
    }

    /**
     * Register the coordinator-leader singleton. ZooKeeper is create-only ephemeral, so this
     * unregisters first; Raft is an overwrite append.
     */
    @Benchmark
    public void registerCoordinatorLeader() throws Exception {
        if (MetadataJmhSupport.ZK.equals(backend)) {
            store.unregisterCoordinatorLeader(seedCoordinator);
        }
        store.registerCoordinatorLeader(seedCoordinator);
    }

    /** Unregister the coordinator-leader singleton, then restore it for the next invocation. */
    @Benchmark
    public void unregisterCoordinatorLeader() throws Exception {
        store.unregisterCoordinatorLeader(seedCoordinator);
        store.registerCoordinatorLeader(seedCoordinator);
    }

    /** Read the current coordinator leader. */
    @Benchmark
    public Optional<CoordinatorAddress> getCoordinatorLeaderAddress() throws Exception {
        return store.getCoordinatorLeaderAddress();
    }

    /** List known coordinator server ids. */
    @Benchmark
    public List<String> getCoordinatorServerList() throws Exception {
        return store.getCoordinatorServerList();
    }

    /** Register a unique tablet server. */
    @Benchmark
    public void registerTabletServer() throws Exception {
        int id = (int) seq.incrementAndGet() + 10;
        store.registerTabletServer(id, MetadataJmhSupport.tabletServerRegistration(id));
    }

    /** Read tablet server 1. */
    @Benchmark
    public Optional<TabletServerRegistration> getTabletServer() throws Exception {
        return store.getTabletServer(1);
    }

    /** Batch get of tablet server 1. */
    @Benchmark
    public Map<Integer, TabletServerRegistration> getTabletServers() throws Exception {
        return store.getTabletServers(new int[] {1});
    }

    /** Sorted registered tablet-server ids. */
    @Benchmark
    public int[] getSortedTabletServerList() throws Exception {
        return store.getSortedTabletServerList();
    }

    // ---- ACL / config / tags / rebalance ----

    /** Create ACLs for a unique database resource. */
    @Benchmark
    public void createResourceAcl() throws Exception {
        store.createResourceAcl(
                Resource.database("acl" + seq.incrementAndGet()), MetadataJmhSupport.aclEntries());
    }

    /** Overwrite the seeded resource ACL ({@code MATCH_ANY} version). */
    @Benchmark
    public void updateResourceAcl() throws Exception {
        store.updateResourceAcl(
                seedAclResource,
                MetadataJmhSupport.aclEntries(),
                MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** Read versioned ACLs for the seeded resource. */
    @Benchmark
    public VersionedAcls getResourceAclWithVersion() throws Exception {
        return store.getResourceAclWithVersion(seedAclResource);
    }

    /** Create then conditionally delete a unique resource ACL. */
    @Benchmark
    public void conditionalDeleteResourceAcl() throws Exception {
        Resource resource = Resource.database("acldel" + seq.incrementAndGet());
        store.createResourceAcl(resource, MetadataJmhSupport.aclEntries());
        store.conditionalDeleteResourceAcl(resource, MetadataJmhSupport.MATCH_ANY_VERSION);
    }

    /** List database-type resources that have ACLs. */
    @Benchmark
    public List<String> listResourcesByType() throws Exception {
        return store.listResourcesByType(ResourceType.DATABASE);
    }

    /** ACL change notification (sequential ZK node; Raft no-op). */
    @Benchmark
    public void insertAclChangeNotification() throws Exception {
        store.insertAclChangeNotification(seedAclResource);
    }

    /** Read dynamic server entity config. */
    @Benchmark
    public Map<String, String> fetchEntityConfig() throws Exception {
        return store.fetchEntityConfig();
    }

    /** Overwrite dynamic server entity config. */
    @Benchmark
    public void upsertServerEntityConfig() throws Exception {
        store.upsertServerEntityConfig(MetadataJmhSupport.entityConfig());
    }

    /** Read the current rebalance task. */
    @Benchmark
    public Optional<RebalanceTask> getRebalanceTask() throws Exception {
        return store.getRebalanceTask();
    }

    /** Overwrite the rebalance-task singleton. */
    @Benchmark
    public void registerRebalanceTask() throws Exception {
        store.registerRebalanceTask(
                MetadataJmhSupport.rebalanceTask("jmh-rb-" + seq.incrementAndGet()));
    }

    /** Register then delete the rebalance-task singleton, then restore a seed. */
    @Benchmark
    public void deleteRebalanceTask() throws Exception {
        store.registerRebalanceTask(MetadataJmhSupport.rebalanceTask("jmh-rb-del"));
        store.deleteRebalanceTask();
    }

    /** Read cluster server tags. */
    @Benchmark
    public Optional<ServerTags> getServerTags() throws Exception {
        return store.getServerTags();
    }

    /**
     * Register cluster server tags. ZooKeeper is create-only, so this deletes first; Raft is an
     * overwrite append.
     */
    @Benchmark
    public void registerServerTags() throws Exception {
        if (MetadataJmhSupport.ZK.equals(backend)) {
            store.deleteServerTags();
        }
        store.registerServerTags(MetadataJmhSupport.serverTags());
    }

    /** Overwrite cluster server tags (update path). */
    @Benchmark
    public void updateServerTags() throws Exception {
        store.updateServerTags(MetadataJmhSupport.serverTags());
    }

    /** Delete then restore cluster server tags. */
    @Benchmark
    public void deleteServerTags() throws Exception {
        store.deleteServerTags();
        store.registerServerTags(MetadataJmhSupport.serverTags());
    }

    // ---- id allocation ----

    /** Sequence-id allocation ({@code getPartitionIdAndIncrement}). */
    @Benchmark
    public long getPartitionIdAndIncrement() throws Exception {
        return store.getPartitionIdAndIncrement();
    }

    /** Sequence-id allocation ({@code getWriterIdAndIncrement}). */
    @Benchmark
    public long getWriterIdAndIncrement() throws Exception {
        return store.getWriterIdAndIncrement();
    }

    // ---- snapshots / lake / producer / remote log ----

    /** Register a unique kv-snapshot lease. */
    @Benchmark
    public void registerKvSnapshotLeaseMetadata() throws Exception {
        store.registerKvSnapshotLeaseMetadata(
                "lease" + seq.incrementAndGet(), MetadataJmhSupport.kvLeaseMetadata());
    }

    /** Overwrite the seeded kv-snapshot lease. */
    @Benchmark
    public void updateKvSnapshotLeaseMetadata() throws Exception {
        store.updateKvSnapshotLeaseMetadata(SEED_LEASE, MetadataJmhSupport.kvLeaseMetadata());
    }

    /** Read the seeded kv-snapshot lease. */
    @Benchmark
    public Optional<KvSnapshotLeaseMetadata> getKvSnapshotLeaseMetadata() throws Exception {
        return store.getKvSnapshotLeaseMetadata(SEED_LEASE);
    }

    /** Create then delete a unique kv-snapshot lease. */
    @Benchmark
    public void deleteKvSnapshotLease() throws Exception {
        String id = "leasedel" + seq.incrementAndGet();
        store.registerKvSnapshotLeaseMetadata(id, MetadataJmhSupport.kvLeaseMetadata());
        store.deleteKvSnapshotLease(id);
    }

    /** List kv-snapshot lease ids. */
    @Benchmark
    public List<String> getKvSnapshotLeasesList() throws Exception {
        return store.getKvSnapshotLeasesList();
    }

    /** Atomically register producer offsets for a unique producer. */
    @Benchmark
    public boolean tryRegisterProducerOffsets() throws Exception {
        return store.tryRegisterProducerOffsets(
                "prod" + seq.incrementAndGet(), MetadataJmhSupport.producerOffsets());
    }

    /** Read the seeded producer offsets. */
    @Benchmark
    public Optional<ProducerOffsets> getProducerOffsets() throws Exception {
        return store.getProducerOffsets(SEED_PRODUCER);
    }

    /** Read the seeded producer offsets with CAS version. */
    @Benchmark
    public Optional<Tuple2<ProducerOffsets, Integer>> getProducerOffsetsWithVersion()
            throws Exception {
        return store.getProducerOffsetsWithVersion(SEED_PRODUCER);
    }

    /** Create then delete unique producer offsets. */
    @Benchmark
    public void deleteProducerOffsets() throws Exception {
        String id = "proddel" + seq.incrementAndGet();
        store.tryRegisterProducerOffsets(id, MetadataJmhSupport.producerOffsets());
        store.deleteProducerOffsets(id);
    }

    /** Create then version-CAS delete unique producer offsets. */
    @Benchmark
    public boolean deleteProducerSnapshotIfVersion() throws Exception {
        String id = "prodcas" + seq.incrementAndGet();
        store.tryRegisterProducerOffsets(id, MetadataJmhSupport.producerOffsets());
        return store.deleteProducerSnapshotIfVersion(id, 0);
    }

    /** List producer ids with a registered offset snapshot. */
    @Benchmark
    public List<String> listProducerIds() throws Exception {
        return store.listProducerIds();
    }

    /** Upsert the seeded bucket's remote-log-manifest handle. */
    @Benchmark
    public void upsertRemoteLogManifestHandle() throws Exception {
        store.upsertRemoteLogManifestHandle(
                seedBucket, MetadataJmhSupport.remoteLogManifestHandle());
    }

    /** Read the seeded bucket's remote-log-manifest handle. */
    @Benchmark
    public Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle() throws Exception {
        return store.getRemoteLogManifestHandle(seedBucket);
    }

    /** List remote-log-manifest handles of the seeded table. */
    @Benchmark
    public List<TableBucketAndManifest> listRemoteLogManifestHandles() throws Exception {
        return store.listRemoteLogManifestHandles(SEED_TABLE_ID, null);
    }

    /** Upsert lake-table metadata for the seeded table. */
    @Benchmark
    public void upsertLakeTable() throws Exception {
        store.upsertLakeTable(SEED_TABLE_ID, MetadataJmhSupport.lakeTable(), true);
    }

    /** Read lake-table metadata for the seeded table. */
    @Benchmark
    public Optional<LakeTable> getLakeTable() throws Exception {
        return store.getLakeTable(SEED_TABLE_ID);
    }

    /** Register a unique bucket snapshot on the seeded bucket. */
    @Benchmark
    public void registerTableBucketSnapshot() throws Exception {
        store.registerTableBucketSnapshot(
                seedBucket, MetadataJmhSupport.bucketSnapshot(snapshotIdSeq.getAndIncrement()));
    }

    /** Register then delete a unique bucket snapshot. */
    @Benchmark
    public void deleteTableBucketSnapshot() throws Exception {
        long snapshotId = snapshotIdSeq.getAndIncrement();
        store.registerTableBucketSnapshot(
                seedBucket, MetadataJmhSupport.bucketSnapshot(snapshotId));
        store.deleteTableBucketSnapshot(seedBucket, snapshotId);
    }

    /** Read snapshot id 1 of the seeded bucket. */
    @Benchmark
    public Optional<BucketSnapshot> getTableBucketSnapshot() throws Exception {
        return store.getTableBucketSnapshot(seedBucket, 1L);
    }

    /** Latest snapshot of the seeded bucket. */
    @Benchmark
    public Optional<BucketSnapshot> getTableBucketLatestSnapshot() throws Exception {
        return store.getTableBucketLatestSnapshot(seedBucket);
    }

    /** All snapshots of the seeded bucket. */
    @Benchmark
    public List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds() throws Exception {
        return store.getTableBucketAllSnapshotAndIds(seedBucket);
    }

    /** Snapshot ids of the seeded bucket. */
    @Benchmark
    public List<Long> listBucketSnapshotIds() throws Exception {
        return store.listBucketSnapshotIds(seedBucket);
    }

    /** Latest snapshot per bucket of the seeded table. */
    @Benchmark
    public Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot() throws Exception {
        return store.getTableLatestBucketSnapshot(SEED_TABLE_ID);
    }

    /** Latest snapshot per bucket of the seeded partition. */
    @Benchmark
    public Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot()
            throws Exception {
        return store.getPartitionLatestBucketSnapshot(SEED_PARTITION_ID);
    }

    /** Lake-table snapshot for a table with no lake metadata (empty optional). */
    @Benchmark
    public Optional<LakeTableSnapshot> getLakeTableSnapshot() throws Exception {
        return store.getLakeTableSnapshot(999_999L, null);
    }

    /** Latest readable lake-table snapshot of the seeded table. */
    @Benchmark
    public Optional<LakeTableSnapshot> getLatestReadableLakeTableSnapshot() throws Exception {
        return store.getLatestReadableLakeTableSnapshot(SEED_TABLE_ID);
    }

    /**
     * Runs this benchmark with the documented short JMH flags.
     *
     * @param args optional JMH include regex (default: this class)
     */
    public static void main(String[] args) throws RunnerException {
        String include =
                args.length > 0
                        ? args[0]
                        : ".*" + MetadataStoreBenchmark.class.getSimpleName() + ".*";
        try {
            new Runner(MetadataJmhSupport.shortRunOptions(include)).run();
        } finally {
            MetadataJmhSupport.closeShared();
        }
    }
}
