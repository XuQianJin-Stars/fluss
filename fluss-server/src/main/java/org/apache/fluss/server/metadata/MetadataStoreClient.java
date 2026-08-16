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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.coordinator.LeaderElection;
import org.apache.fluss.server.entity.RegisterTableBucketLeadAndIsrInfo;
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

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Backend-neutral metadata + coordination client.
 *
 * <p>Method signatures deliberately mirror the existing {@code ZooKeeperClient} public API so that
 * call sites can be made backend-agnostic without behavioral change. Two implementations are
 * planned:
 *
 * <ul>
 *   <li>{@code ZooKeeperClient} (legacy) — already exposes this surface; declared {@code
 *       implements} as an additive change (see {@code zookeeper-removal-design.md} §5.5.2).
 *   <li>{@code RaftMetadataClient} (new) — delegates to {@code RaftJournalWriter} + {@code
 *       RaftMetadataStore} (§5.5.3).
 * </ul>
 *
 * <p>This is the initial <em>skeleton</em> of the interface: it covers the coordinator/epoch,
 * tablet server, table/leaderAndIsr, sequence-id and leader-election surface. Remaining ZooKeeper
 * operations (dynamic config, ACL, kv-snapshot lease, producer snapshot) are added incrementally as
 * call sites migrate off the concrete {@code ZooKeeperClient}.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.5.1.
 */
@Internal
public interface MetadataStoreClient extends AutoCloseable {

    // ---- coordinator / epoch ----

    /** Registers this coordinator server (ephemeral in the ZooKeeper backend). */
    void registerCoordinatorServer(CoordinatorAddress coordinatorAddress) throws Exception;

    /** Registers the active coordinator leader node. */
    void registerCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception;

    /** Removes the active coordinator leader node (on graceful leadership release). */
    void unregisterCoordinatorLeader(CoordinatorAddress coordinatorAddress) throws Exception;

    /** Returns the current coordinator leader address, if any. */
    Optional<CoordinatorAddress> getCoordinatorLeaderAddress() throws Exception;

    /**
     * Returns the coordinator server ids known to the metadata store, used by the coordinator
     * leader-bootstrap to seed its live-coordinator set. On the ZooKeeper backend this is every
     * coordinator registered under {@code /coordinators/ids}; on the Raft backend it is the single
     * current leader's id (coordinator membership proper is the Raft voter set).
     */
    List<String> getCoordinatorServerList() throws Exception;

    // ---- database ----

    /** Registers a database. */
    void registerDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception;

    /** Overwrites a database's registration (property/comment change). */
    void updateDatabase(String database, DatabaseRegistration databaseRegistration)
            throws Exception;

    /** Returns the registration for a database, if present. */
    Optional<DatabaseRegistration> getDatabase(String database) throws Exception;

    /** Deletes a database. */
    void deleteDatabase(String database) throws Exception;

    /** Whether a database exists. */
    boolean databaseExists(String database) throws Exception;

    /** Lists all database names. */
    List<String> listDatabases() throws Exception;

    /** Returns per-database summaries (created time + table count) for the given names. */
    List<DatabaseSummary> listDatabaseSummaries(Collection<String> databaseNames) throws Exception;

    // ---- tablet server ----

    /** Registers a tablet server (ephemeral in the ZooKeeper backend). */
    void registerTabletServer(int tabletServerId, TabletServerRegistration tabletServerRegistration)
            throws Exception;

    /** Returns the registration for a single tablet server, if present. */
    Optional<TabletServerRegistration> getTabletServer(int tabletServerId) throws Exception;

    /** Returns the registrations for the given tablet server ids. */
    Map<Integer, TabletServerRegistration> getTabletServers(int[] tabletServerIds) throws Exception;

    /** Returns the sorted list of currently-registered tablet server ids. */
    int[] getSortedTabletServerList() throws Exception;

    // ---- table / leaderAndIsr ----

    /** Registers a table. */
    void registerTable(TablePath tablePath, TableRegistration tableRegistration) throws Exception;

    /** Registers a table, optionally creating the backing node. */
    void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception;

    /** Returns the registration for a table, if present. */
    Optional<TableRegistration> getTable(TablePath tablePath) throws Exception;

    /**
     * Updates the leader-and-ISR for a bucket, fenced by the expected version.
     *
     * <p>In the ZooKeeper backend {@code expectedZkVersion} is the znode CAS version; the Raft
     * backend maps this fencing to the leader term (see §5.1.6).
     */
    void updateLeaderAndIsr(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
            throws Exception;

    /**
     * Registers a bucket's leader-and-isr, fenced by the expected version (subsumed by the leader
     * term on the Raft backend, §5.1.6).
     */
    void registerLeaderAndIsr(
            TableBucket tableBucket, LeaderAndIsr leaderAndIsr, int expectedZkVersion)
            throws Exception;

    /** Batch-registers leader-and-isr for a partitioned table's buckets. */
    void batchRegisterLeaderAndIsrForTablePartition(
            List<RegisterTableBucketLeadAndIsrInfo> registerList, int expectedZkVersion)
            throws Exception;

    /** Batch-updates leader-and-isr for the given buckets. */
    void batchUpdateLeaderAndIsr(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrList, int expectedZkVersion)
            throws Exception;

    /** Returns a bucket's leader-and-isr, if present. */
    Optional<LeaderAndIsr> getLeaderAndIsr(TableBucket tableBucket) throws Exception;

    /**
     * Batch variant of {@link #getLeaderAndIsr}; only buckets with a leader-and-isr are present.
     */
    Map<TableBucket, LeaderAndIsr> getLeaderAndIsrs(Collection<TableBucket> tableBuckets)
            throws Exception;

    /** Batch variant of {@link #getTableAssignment}; only assigned tables appear in the result. */
    Map<Long, TableAssignment> getTablesAssignments(Collection<Long> tableIds) throws Exception;

    /**
     * Batch variant of {@link #getPartitionAssignment}; only assigned partitions appear in the
     * result.
     */
    Map<Long, PartitionAssignment> getPartitionsAssignments(Collection<Long> partitionIds)
            throws Exception;

    /** Batch variant of {@link #getTable}; only existing paths are included in the result. */
    Map<TablePath, TableRegistration> getTables(Collection<TablePath> tablePaths) throws Exception;

    /** Whether a table registration exists. */
    boolean tableExist(TablePath tablePath) throws Exception;

    /** Lists the table names registered under {@code databaseName}. */
    List<String> listTables(String databaseName) throws Exception;

    /** Overwrites a table's registration. */
    void updateTable(TablePath tablePath, TableRegistration tableRegistration) throws Exception;

    /** Deletes a table registration. */
    void deleteTable(TablePath tablePath) throws Exception;

    // ---- schema ----

    /** Registers a table's first schema; returns its schema id. */
    int registerFirstSchema(TablePath tablePath, Schema schema) throws Exception;

    /** Registers {@code schema} at {@code schemaId}; returns {@code schemaId}. */
    int registerSchema(TablePath tablePath, Schema schema, int schemaId) throws Exception;

    /** Returns the schema stored at {@code (tablePath, schemaId)}, if present. */
    Optional<SchemaInfo> getSchemaById(TablePath tablePath, int schemaId) throws Exception;

    /** Returns the highest registered schema id for {@code tablePath}, or 0 if none. */
    int getCurrentSchemaId(TablePath tablePath) throws Exception;

    /** Returns the latest schema per table; tables with no schema are omitted. */
    Map<TablePath, SchemaInfo> getLatestSchemas(Collection<TablePath> tablePaths) throws Exception;

    // ---- partition + assignments ----

    /** Returns the set of partition names registered for {@code tablePath}. */
    Set<String> getPartitions(TablePath tablePath) throws Exception;

    /** Returns the registration for a single partition, if present. */
    Optional<PartitionRegistration> getPartition(TablePath tablePath, String partitionName)
            throws Exception;

    /** Returns the number of partitions registered for {@code tablePath}. */
    int getPartitionNumber(TablePath tablePath) throws Exception;

    /** Returns every partition's registration for {@code tablePath}, keyed by partition name. */
    Map<String, PartitionRegistration> getPartitionRegistrations(TablePath tablePath)
            throws Exception;

    /** Returns the partitions of {@code tablePath} matching {@code partialPartitionSpec}. */
    Map<String, PartitionRegistration> getPartitionRegistrations(
            TablePath tablePath,
            List<String> partitionKeys,
            ResolvedPartitionSpec partialPartitionSpec)
            throws Exception;

    /** Registers a partition's assignment (by id) and metadata (by name) together. */
    void registerPartitionAssignmentAndMetadata(
            long partitionId,
            String partitionName,
            PartitionAssignment partitionAssignment,
            String remoteDataDir,
            TablePath tablePath,
            long tableId)
            throws Exception;

    /** Deletes a partition's metadata record. */
    void deletePartition(TablePath tablePath, String partitionName) throws Exception;

    /** Registers (or overwrites) a table's bucket assignment, keyed by table id. */
    void registerTableAssignment(long tableId, TableAssignment tableAssignment) throws Exception;

    /**
     * Overwrites a table's bucket assignment (rebalance path). {@code expectedZkVersion} is a
     * ZooKeeper CAS token, subsumed by the single-writer leader term on the Raft backend.
     */
    void updateTableAssignment(long tableId, TableAssignment tableAssignment, int expectedZkVersion)
            throws Exception;

    /**
     * Overwrites a partition's bucket assignment (rebalance path). {@code expectedZkVersion} is a
     * ZooKeeper CAS token, subsumed by the single-writer leader term on the Raft backend.
     */
    void updatePartitionAssignment(
            long partitionId, PartitionAssignment partitionAssignment, int expectedZkVersion)
            throws Exception;

    /** Deletes a table's bucket assignment. */
    void deleteTableAssignment(long tableId) throws Exception;

    /** Deletes a partition's bucket assignment. */
    void deletePartitionAssignment(long partitionId) throws Exception;

    // ---- kv snapshot lease ----

    /** Registers a new kv-snapshot lease, keyed by lease id. */
    void registerKvSnapshotLeaseMetadata(String leaseId, KvSnapshotLeaseMetadata leaseMetadata)
            throws Exception;

    /** Overwrites an existing kv-snapshot lease. */
    void updateKvSnapshotLeaseMetadata(String leaseId, KvSnapshotLeaseMetadata leaseMetadata)
            throws Exception;

    /** Returns a kv-snapshot lease by id, if present. */
    Optional<KvSnapshotLeaseMetadata> getKvSnapshotLeaseMetadata(String leaseId) throws Exception;

    /** Deletes a kv-snapshot lease by id. */
    void deleteKvSnapshotLease(String leaseId) throws Exception;

    /** Lists all kv-snapshot lease ids. */
    List<String> getKvSnapshotLeasesList() throws Exception;

    // ---- producer offsets (former /producers/[producerId]) ----

    /**
     * Atomically registers a producer's offset snapshot; returns {@code false} if one already
     * exists.
     */
    boolean tryRegisterProducerOffsets(String producerId, ProducerOffsets producerOffsets)
            throws Exception;

    /** Returns a producer's offset snapshot, if present. */
    Optional<ProducerOffsets> getProducerOffsets(String producerId) throws Exception;

    /** Returns a producer's offset snapshot with its version (0 on backends without CAS). */
    Optional<Tuple2<ProducerOffsets, Integer>> getProducerOffsetsWithVersion(String producerId)
            throws Exception;

    /** Deletes a producer's offset snapshot. */
    void deleteProducerOffsets(String producerId) throws Exception;

    /**
     * Deletes a producer's offset snapshot only if {@code expectedVersion} matches (version-CAS on
     * ZK; subsumed on single-writer backends). Returns {@code true} if a snapshot was deleted.
     */
    boolean deleteProducerSnapshotIfVersion(String producerId, int expectedVersion)
            throws Exception;

    /** Lists all producer ids with a registered offset snapshot. */
    List<String> listProducerIds() throws Exception;

    // ---- remote log manifest / lake table (reads used by the coordinator service) ----

    /** Upserts a bucket's current remote-log-manifest handle (keyed by table bucket). */
    void upsertRemoteLogManifestHandle(
            TableBucket tableBucket, RemoteLogManifestHandle remoteLogManifestHandle)
            throws Exception;

    /** Returns a bucket's current remote-log-manifest handle, if present. */
    Optional<RemoteLogManifestHandle> getRemoteLogManifestHandle(TableBucket tableBucket)
            throws Exception;

    /**
     * Lists the remote-log-manifest handles of every bucket of {@code tableId} (or of {@code
     * partitionId} when non-null).
     */
    List<TableBucketAndManifest> listRemoteLogManifestHandles(
            long tableId, @Nullable Long partitionId) throws Exception;

    /** Returns a table's lake-table metadata, if present. */
    Optional<LakeTable> getLakeTable(long tableId) throws Exception;

    // ---- kv bucket snapshot (reads used by the coordinator/tablet-server rpc base) ----

    /** Registers a bucket snapshot, keyed by {@code (tableBucket, snapshotId)}. */
    void registerTableBucketSnapshot(TableBucket tableBucket, BucketSnapshot snapshot)
            throws Exception;

    /** Deletes the bucket snapshot {@code (tableBucket, snapshotId)}. */
    void deleteTableBucketSnapshot(TableBucket tableBucket, long snapshotId) throws Exception;

    /** Returns the bucket snapshot {@code (tableBucket, snapshotId)}, if present. */
    Optional<BucketSnapshot> getTableBucketSnapshot(TableBucket tableBucket, long snapshotId)
            throws Exception;

    /** Returns the latest snapshot of {@code tableBucket}, if any. */
    Optional<BucketSnapshot> getTableBucketLatestSnapshot(TableBucket tableBucket) throws Exception;

    /** Returns every {@code (snapshot, snapshotId)} registered for {@code tableBucket}. */
    List<Tuple2<BucketSnapshot, Long>> getTableBucketAllSnapshotAndIds(TableBucket tableBucket)
            throws Exception;

    /** Lists the ids of all snapshots registered for {@code tableBucket}, ascending. */
    List<Long> listBucketSnapshotIds(TableBucket tableBucket) throws Exception;

    /** Returns the latest snapshot per bucket of {@code tableId} (empty per bucket with none). */
    Map<Integer, Optional<BucketSnapshot>> getTableLatestBucketSnapshot(long tableId)
            throws Exception;

    /** Partition variant of {@link #getTableLatestBucketSnapshot}. */
    Map<Integer, Optional<BucketSnapshot>> getPartitionLatestBucketSnapshot(long partitionId)
            throws Exception;

    // ---- lake-table snapshot / partition paths (reads used by the coordinator/tablet-server base)
    // ----

    /**
     * Returns a table's lake-table snapshot (the latest when {@code snapshotId} is null), if the
     * table has lake-table metadata.
     */
    Optional<LakeTableSnapshot> getLakeTableSnapshot(long tableId, @Nullable Long snapshotId)
            throws Exception;

    /** Returns a table's latest READABLE lake-table snapshot, if present. */
    Optional<LakeTableSnapshot> getLatestReadableLakeTableSnapshot(long tableId) throws Exception;

    /** Returns {@code partitionId -> PhysicalTablePath} for every partition of the given tables. */
    Map<Long, PhysicalTablePath> getPartitionIdAndPaths(Collection<TablePath> tablePaths)
            throws Exception;

    // ---- bucket metadata / partition ids / lake-table write (metadata providers + lake helper)
    // ----

    /**
     * Returns {@code tableId -> per-bucket metadata} for the given tables (empty list per table
     * with none).
     */
    Map<Long, List<BucketMetadata>> getBucketMetadataForTables(Collection<Long> tableIds)
            throws Exception;

    /**
     * Partition variant of {@link #getBucketMetadataForTables} (only assigned partitions present).
     */
    Map<Long, List<BucketMetadata>> getBucketMetadataForPartitions(Collection<Long> partitionIds)
            throws Exception;

    /**
     * Returns {@code partitionPath -> TablePartition} for the given partition paths (present only).
     */
    Map<PhysicalTablePath, TablePartition> getPartitionIds(
            Collection<PhysicalTablePath> partitionPaths) throws Exception;

    /** Upserts a table's lake-table metadata ({@code isUpdate} is a ZK lifecycle hint). */
    void upsertLakeTable(long tableId, LakeTable lakeTable, boolean isUpdate) throws Exception;

    /** Returns a table's bucket assignment, if present. */
    Optional<TableAssignment> getTableAssignment(long tableId) throws Exception;

    /** Returns a partition's bucket assignment, if present. */
    Optional<PartitionAssignment> getPartitionAssignment(long partitionId) throws Exception;

    /** Lists the ids of all tables that currently have a bucket assignment. */
    List<Long> listAssignedTableIds() throws Exception;

    /** Lists the ids of all partitions that currently have a bucket assignment. */
    List<Long> listAssignedPartitionIds() throws Exception;

    /**
     * Returns {@code partitionName -> partitionId} for every partition of each requested table
     * (empty map per table with none). Used by the coordinator leader-bootstrap load.
     */
    Map<TablePath, Map<String, Long>> getPartitionNameAndIdsForTables(List<TablePath> tablePaths)
            throws Exception;

    /**
     * Returns the cluster's server tags, if any (empty on backends that do not stream tags yet).
     */
    Optional<ServerTags> getServerTags() throws Exception;

    /** Registers the cluster's server tags (single global key). */
    void registerServerTags(ServerTags newServerTags) throws Exception;

    /** Overwrites the cluster's server tags (single global key). */
    void updateServerTags(ServerTags newServerTags) throws Exception;

    /** Deletes the cluster's server tags. */
    void deleteServerTags() throws Exception;

    // ---- dynamic entity config (former /config/server/global) ----

    /** Returns the dynamic server entity config (empty map if never set). */
    Map<String, String> fetchEntityConfig() throws Exception;

    /** Overwrites the dynamic server entity config. */
    void upsertServerEntityConfig(Map<String, String> configs) throws Exception;

    // ---- rebalance task (former /cluster/rebalance) ----

    /** Returns the current rebalance task, if any. */
    Optional<RebalanceTask> getRebalanceTask() throws Exception;

    /** Overwrites the current rebalance task. */
    void registerRebalanceTask(RebalanceTask rebalanceTask) throws Exception;

    /** Deletes the current rebalance task (no-op if none). Used by tests and cancel paths. */
    void deleteRebalanceTask() throws Exception;

    // ---- resource ACL (former /acls/[type]/[name], used by the authorizer) ----

    /** Creates a resource's ACL entries (create-or-overwrite by key). */
    void createResourceAcl(Resource resource, Set<AccessControlEntry> accessControlEntries)
            throws Exception;

    /**
     * Updates a resource's ACL entries fenced by {@code expectedVersion}; returns the new version.
     *
     * <p>In the ZooKeeper backend {@code expectedVersion} is the ACL znode CAS version; the Raft
     * backend subsumes the CAS on the single-writer leader and always returns {@code 0}.
     */
    int updateResourceAcl(
            Resource resource, Set<AccessControlEntry> accessControlEntries, int expectedVersion)
            throws Exception;

    /**
     * Returns a resource's versioned ACLs. When present the version is the ZK znode version (Raft:
     * always {@code 0}); when absent the version is {@link
     * org.apache.fluss.server.zk.ZooKeeperClient#UNKNOWN_VERSION} so {@link VersionedAcls#exists()}
     * reports {@code false}.
     */
    VersionedAcls getResourceAclWithVersion(Resource resource) throws Exception;

    /** Deletes a resource's ACLs fenced by {@code zkVersion} (subsumed on the Raft backend). */
    void conditionalDeleteResourceAcl(Resource resource, int zkVersion) throws Exception;

    /** Lists the names of resources of the given type that currently have ACLs. */
    List<String> listResourcesByType(ResourceType resourceType) throws Exception;

    /**
     * Publishes an ACL-change notification for {@code resource} so other nodes reload it.
     *
     * <p>On the ZooKeeper backend this creates a sequential change-notification node; on the Raft
     * backend the ACL record append is itself the notification carried by the metadata stream, so
     * this is a no-op.
     */
    void insertAclChangeNotification(Resource resource) throws Exception;

    // ---- sequence id counters ----

    /** Atomically returns the current table id and increments the counter. */
    long getTableIdAndIncrement() throws Exception;

    /** Atomically returns the current partition id and increments the counter. */
    long getPartitionIdAndIncrement() throws Exception;

    /** Atomically returns the current writer id and increments the counter. */
    long getWriterIdAndIncrement() throws Exception;

    // ---- leader election (backend-specific impl, common LeaderElection contract, §5.1.5) ----

    /**
     * Creates a coordinator {@link LeaderElection} bound to this backend. The ZooKeeper backend
     * returns a Curator-{@code LeaderLatch}-based {@code CoordinatorLeaderElection}; the Raft
     * backend returns a {@code RaftLeaderSelector}. {@code CoordinatorServer} holds the result as a
     * {@link LeaderElection} so the integration is backend-agnostic (see §7.3.1).
     */
    LeaderElection newLeaderSelector(String serverId);

    @Override
    void close();
}
