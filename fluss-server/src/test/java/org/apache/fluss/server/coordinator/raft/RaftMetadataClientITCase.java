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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.cluster.AlterConfig;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.exception.NotCoordinatorLeaderException;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseSummary;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
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
import org.apache.fluss.server.zk.data.TableBucketAndManifest;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.lake.LakeTable;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;
import org.apache.fluss.server.zk.data.lease.KvSnapshotLeaseMetadata;
import org.apache.fluss.server.zk.data.producer.ProducerOffsets;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-end test for {@link RaftMetadataClient} (T4-1) against a real single-node embedded Ratis
 * leader. Exercises the full {@code MetadataStoreClient} surface through the Raft log + local
 * store, plus the {@code requireLeader} fencing (§5.7) and the {@code getCoordinatorLeaderAddress}
 * redirect exemption.
 */
class RaftMetadataClientITCase {

    @TempDir private Path raftDir;

    @Test
    void testFullMetadataStoreClientSurfaceOnRealLeader() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, sm);

        // Wire leadership BEFORE start so the notifyLeaderChanged callback is not missed.
        RaftLeaderSelector leaderSelector = new RaftLeaderSelector("n1", server, sm, 20_000);
        sm.setLeaderSelector(leaderSelector);
        leaderSelector.startElectLeaderAsync(() -> {}, t -> {});

        RaftMetadataClient client = null;
        RaftLeaderSelector followerSelector = null;
        try {
            server.start();
            waitUntil(20_000, leaderSelector::isLeader, "node did not become raft leader");

            RaftProperties clientProps = server.buildRaftProperties();
            RaftClient raftClient =
                    RaftClient.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setRaftGroup(group)
                            .setProperties(clientProps)
                            .build();
            RaftJournalWriter writer = new RaftJournalWriter(raftClient, sm);
            // blockSize 1 => predictable ids 0, 1, 2, ... one Raft append per id.
            RaftSequenceIDCounter tableIds = new RaftSequenceIDCounter("table", writer, store, 1);
            RaftSequenceIDCounter partitionIds =
                    new RaftSequenceIDCounter("partition", writer, store, 1);
            RaftSequenceIDCounter writerIds = new RaftSequenceIDCounter("writer", writer, store, 1);
            client =
                    new RaftMetadataClient(
                            writer, store, leaderSelector, tableIds, partitionIds, writerIds);

            // ---- table register/get round-trip ----
            TablePath tablePath = TablePath.of("db", "orders");
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                            .distributedBy(3)
                            .build();
            TableRegistration reg =
                    TableRegistration.newTable(7L, "file:///tmp/remote", descriptor);
            client.registerTable(tablePath, reg);
            assertThat(client.getTable(tablePath)).contains(reg);
            assertThat(client.getTable(TablePath.of("db", "missing"))).isEmpty();

            // ---- database register/get/exists/list/summaries/update/delete round-trip ----
            DatabaseRegistration db1 =
                    DatabaseRegistration.of(DatabaseDescriptor.builder().comment("first").build());
            DatabaseRegistration db2 =
                    DatabaseRegistration.of(DatabaseDescriptor.builder().comment("second").build());
            client.registerDatabase("db1", db1);
            client.registerDatabase("db2", db2);
            assertThat(client.getDatabase("db1")).contains(db1);
            assertThat(client.getDatabase("nope")).isEmpty();
            assertThat(client.databaseExists("db1")).isTrue();
            assertThat(client.databaseExists("nope")).isFalse();
            assertThat(client.listDatabases()).containsExactlyInAnyOrder("db1", "db2");

            // one table under db1 (distinct from the earlier "db.orders"); summaries count it
            client.registerTable(
                    TablePath.of("db1", "t1"),
                    TableRegistration.newTable(8L, "file:///tmp/remote", descriptor));
            List<DatabaseSummary> summaries =
                    client.listDatabaseSummaries(Arrays.asList("db1", "db2", "gone"));
            assertThat(summaries).hasSize(2); // "gone" absent -> skipped
            assertThat(summaries)
                    .anySatisfy(
                            s -> {
                                assertThat(s.getDatabaseName()).isEqualTo("db1");
                                assertThat(s.getTableCount()).isEqualTo(1);
                                assertThat(s.getCreatedTime()).isEqualTo(db1.createdTime);
                            })
                    .anySatisfy(
                            s -> {
                                assertThat(s.getDatabaseName()).isEqualTo("db2");
                                assertThat(s.getTableCount()).isEqualTo(0);
                            });

            // update overwrites the registration (property/comment change)
            DatabaseRegistration db1Updated =
                    db1.newProperties(
                            DatabaseDescriptor.builder().comment("first-updated").build());
            client.updateDatabase("db1", db1Updated);
            assertThat(client.getDatabase("db1")).contains(db1Updated);

            // delete removes the record
            client.deleteDatabase("db2");
            assertThat(client.databaseExists("db2")).isFalse();
            assertThat(client.listDatabases()).containsExactly("db1");

            // ---- schema register/getById/currentId/latest round-trip ----
            TablePath schemaTable = TablePath.of("db1", "t1");
            Schema schemaV1 = Schema.newBuilder().column("id", DataTypes.INT()).build();
            Schema schemaV2 =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .build();
            assertThat(client.getCurrentSchemaId(schemaTable)).isEqualTo(0); // none yet
            assertThat(client.registerFirstSchema(schemaTable, schemaV1)).isEqualTo(1);
            assertThat(client.registerSchema(schemaTable, schemaV2, 2)).isEqualTo(2);
            assertThat(client.getCurrentSchemaId(schemaTable)).isEqualTo(2);
            assertThat(client.getSchemaById(schemaTable, 1)).contains(new SchemaInfo(schemaV1, 1));
            assertThat(client.getSchemaById(schemaTable, 2)).contains(new SchemaInfo(schemaV2, 2));
            assertThat(client.getSchemaById(schemaTable, 3)).isEmpty();
            // getLatestSchemas returns v2 for schemaTable; a schema-less table is skipped
            assertThat(
                            client.getLatestSchemas(
                                    Arrays.asList(schemaTable, TablePath.of("db1", "no"))))
                    .containsOnlyKeys(schemaTable)
                    .containsEntry(schemaTable, new SchemaInfo(schemaV2, 2));

            // ---- table getTables/tableExist/listTables/updateTable/deleteTable round-trip ----
            // "db1.t1" was registered above; register a second table under db1.
            TablePath t2 = TablePath.of("db1", "t2");
            TableRegistration t2Reg =
                    TableRegistration.newTable(9L, "file:///tmp/remote", descriptor);
            client.registerTable(t2, t2Reg);
            assertThat(client.tableExist(TablePath.of("db1", "t1"))).isTrue();
            assertThat(client.tableExist(TablePath.of("db1", "absent"))).isFalse();
            // getTables returns only the found paths (missing one skipped)
            assertThat(client.getTables(Arrays.asList(t2, TablePath.of("db1", "absent"))))
                    .containsOnlyKeys(t2)
                    .containsEntry(t2, t2Reg);
            // listTables returns the bare table names under db1 (t1 + t2); the "db.orders" table
            // under database "db" must NOT leak into db1's listing.
            assertThat(client.listTables("db1")).containsExactlyInAnyOrder("t1", "t2");
            assertThat(client.listTables("db")).containsExactly("orders");

            // updateTable overwrites
            TableRegistration t2Updated =
                    TableRegistration.newTable(9L, "file:///tmp/remote2", descriptor);
            client.updateTable(t2, t2Updated);
            assertThat(client.getTable(t2)).contains(t2Updated);

            // deleteTable removes it
            client.deleteTable(t2);
            assertThat(client.tableExist(t2)).isFalse();
            assertThat(client.listTables("db1")).containsExactly("t1");

            // deleteTable must cascade schemas (ZK deletingChildrenIfNeeded). Otherwise a
            // recreate of the same path still sees the old current schema id.
            assertThat(client.getCurrentSchemaId(schemaTable)).isEqualTo(2);
            client.deleteTable(schemaTable);
            assertThat(client.tableExist(schemaTable)).isFalse();
            assertThat(client.getCurrentSchemaId(schemaTable)).isEqualTo(0);
            assertThat(client.getSchemaById(schemaTable, 1)).isEmpty();
            assertThat(client.getSchemaById(schemaTable, 2)).isEmpty();
            assertThat(client.listTables("db1")).isEmpty();
            assertThat(client.registerFirstSchema(schemaTable, schemaV1)).isEqualTo(1);
            assertThat(client.getCurrentSchemaId(schemaTable)).isEqualTo(1);

            // ---- partition + assignment round-trip ----
            TablePath partTable = TablePath.of("db1", "pt");
            TableAssignment ta =
                    TableAssignment.builder().add(0, BucketAssignment.of(1, 2, 3)).build();
            client.registerTableAssignment(50L, ta);
            assertThat(client.getTableAssignment(50L)).contains(ta);
            // updateTableAssignment overwrites (expectedZkVersion subsumed by the leader term).
            TableAssignment ta2 =
                    TableAssignment.builder().add(0, BucketAssignment.of(4, 5, 6)).build();
            client.updateTableAssignment(50L, ta2, 0);
            assertThat(client.getTableAssignment(50L)).contains(ta2);
            client.updateTableAssignment(50L, ta, 999);
            assertThat(client.getTableAssignment(50L)).contains(ta);

            PartitionAssignment pa = new PartitionAssignment(50L, ta.getBucketAssignments());
            assertThat(client.getPartitions(partTable)).isEmpty();
            assertThat(client.getPartitionNumber(partTable)).isEqualTo(0);
            client.registerPartitionAssignmentAndMetadata(
                    101L, "p1", pa, "file:///tmp/remote", partTable, 50L);
            client.registerPartitionAssignmentAndMetadata(
                    102L, "p2", pa, "file:///tmp/remote", partTable, 50L);
            assertThat(client.getPartitions(partTable)).containsExactlyInAnyOrder("p1", "p2");
            assertThat(client.getPartitionNumber(partTable)).isEqualTo(2);
            assertThat(client.getPartition(partTable, "p1"))
                    .hasValueSatisfying(
                            p -> {
                                assertThat(p.getPartitionId()).isEqualTo(101L);
                                assertThat(p.getTableId()).isEqualTo(50L);
                            });
            assertThat(client.getPartitionRegistrations(partTable)).containsOnlyKeys("p1", "p2");
            assertThat(client.getPartitionAssignment(101L)).contains(pa);
            // updatePartitionAssignment overwrites (expectedZkVersion subsumed by the leader term).
            PartitionAssignment pa2 =
                    new PartitionAssignment(
                            50L,
                            TableAssignment.builder()
                                    .add(0, BucketAssignment.of(7, 8, 9))
                                    .build()
                                    .getBucketAssignments());
            client.updatePartitionAssignment(101L, pa2, 0);
            assertThat(client.getPartitionAssignment(101L)).contains(pa2);
            client.updatePartitionAssignment(101L, pa, 999);
            assertThat(client.getPartitionAssignment(101L)).contains(pa);
            // partitionId -> PhysicalTablePath across the given tables
            assertThat(client.getPartitionIdAndPaths(Collections.singletonList(partTable)))
                    .containsOnly(
                            entry(101L, PhysicalTablePath.of(partTable, "p1")),
                            entry(102L, PhysicalTablePath.of(partTable, "p2")));
            // filtered variant: a partition's own spec {region=p1} contains the {region=p1} query
            List<String> partitionKeys = Collections.singletonList("region");
            ResolvedPartitionSpec specP1 =
                    ResolvedPartitionSpec.fromPartitionName(partitionKeys, "p1");
            assertThat(client.getPartitionRegistrations(partTable, partitionKeys, specP1))
                    .containsOnlyKeys("p1");

            // ---- leader-bootstrap read families ----
            // all assigned table/partition ids (Raft equivalents of getChildren(TableIds/
            // PartitionIdsZNode))
            assertThat(client.listAssignedTableIds()).containsExactly(50L);
            assertThat(client.listAssignedPartitionIds()).containsExactlyInAnyOrder(101L, 102L);
            // partitionName -> partitionId per table
            assertThat(
                            client.getPartitionNameAndIdsForTables(
                                            Collections.singletonList(partTable))
                                    .get(partTable))
                    .containsOnly(entry("p1", 101L), entry("p2", 102L));
            // server tags are not streamed on the raft backend yet
            assertThat(client.getServerTags()).isEmpty();

            // delete partition metadata + assignments
            client.deletePartition(partTable, "p1");
            assertThat(client.getPartitions(partTable)).containsExactly("p2");
            client.deletePartitionAssignment(101L);
            assertThat(client.getPartitionAssignment(101L)).isEmpty();
            client.deleteTableAssignment(50L);
            assertThat(client.getTableAssignment(50L)).isEmpty();

            // ---- kv snapshot lease register/get/list/update/delete round-trip ----
            KvSnapshotLeaseMetadata leaseMeta =
                    new KvSnapshotLeaseMetadata(
                            123_456L,
                            Collections.singletonMap(7L, new FsPath("file:///tmp/lease/7")));
            assertThat(client.getKvSnapshotLeasesList()).isEmpty();
            client.registerKvSnapshotLeaseMetadata("lease-1", leaseMeta);
            assertThat(client.getKvSnapshotLeaseMetadata("lease-1")).contains(leaseMeta);
            assertThat(client.getKvSnapshotLeasesList()).containsExactly("lease-1");
            KvSnapshotLeaseMetadata leaseMeta2 =
                    new KvSnapshotLeaseMetadata(
                            999_999L,
                            Collections.singletonMap(7L, new FsPath("file:///tmp/lease/7b")));
            client.updateKvSnapshotLeaseMetadata("lease-1", leaseMeta2);
            assertThat(client.getKvSnapshotLeaseMetadata("lease-1")).contains(leaseMeta2);
            client.deleteKvSnapshotLease("lease-1");
            assertThat(client.getKvSnapshotLeaseMetadata("lease-1")).isEmpty();
            assertThat(client.getKvSnapshotLeasesList()).isEmpty();

            // ---- remote log manifest upsert/get/list/remove round-trip ----
            TableBucket rlmB0 = new TableBucket(70L, 0);
            TableBucket rlmB1 = new TableBucket(70L, 1);
            TableBucket rlmPart = new TableBucket(70L, 5L, 0); // partition 5, bucket 0
            RemoteLogManifestHandle h0 =
                    new RemoteLogManifestHandle(new FsPath("oss://b0/m.manifest"), 100L);
            RemoteLogManifestHandle h1 =
                    new RemoteLogManifestHandle(new FsPath("oss://b1/m.manifest"), 200L);
            RemoteLogManifestHandle hPart =
                    new RemoteLogManifestHandle(new FsPath("oss://p5b0/m.manifest"), 300L);
            assertThat(client.listRemoteLogManifestHandles(70L, null)).isEmpty();
            client.upsertRemoteLogManifestHandle(rlmB0, h0);
            client.upsertRemoteLogManifestHandle(rlmB1, h1);
            client.upsertRemoteLogManifestHandle(rlmPart, hPart);
            assertThat(client.getRemoteLogManifestHandle(rlmB0)).contains(h0);
            assertThat(client.getRemoteLogManifestHandle(new TableBucket(70L, 9))).isEmpty();
            // non-partition listing sees only the two table-level buckets, NOT the partition's
            assertThat(client.listRemoteLogManifestHandles(70L, null))
                    .extracting(TableBucketAndManifest::getTableBucket)
                    .containsExactlyInAnyOrder(rlmB0, rlmB1);
            // partition listing sees only that partition's bucket
            assertThat(client.listRemoteLogManifestHandles(70L, 5L))
                    .extracting(TableBucketAndManifest::getTableBucket)
                    .containsExactly(rlmPart);
            client.removeRemoteLogManifestHandle(rlmB0);
            assertThat(client.getRemoteLogManifestHandle(rlmB0)).isEmpty();
            assertThat(client.listRemoteLogManifestHandles(70L, null))
                    .extracting(TableBucketAndManifest::getTableBucket)
                    .containsExactly(rlmB1);

            // ---- lake table upsert/get round-trip ----
            assertThat(client.getLakeTable(70L)).isEmpty();
            LakeTable lakeTable =
                    new LakeTable(
                            Collections.singletonList(
                                    new LakeTable.LakeSnapshotMetadata(
                                            1L,
                                            new FsPath("/tiered/1"),
                                            new FsPath("/readable/1"))));
            client.upsertLakeTable(70L, lakeTable);
            assertThat(client.getLakeTable(70L)).contains(lakeTable);
            assertThat(client.getLakeTable(999L)).isEmpty();

            // ---- lake table snapshot derivations (v1 LakeTable => no file IO) ----
            Map<TableBucket, Long> lakeOffsets = new HashMap<>();
            lakeOffsets.put(new TableBucket(71L, 0), 100L);
            lakeOffsets.put(new TableBucket(71L, 1), 200L);
            LakeTableSnapshot lakeSnapshot = new LakeTableSnapshot(7L, lakeOffsets);
            client.upsertLakeTable(71L, new LakeTable(lakeSnapshot));
            assertThat(client.getLakeTableSnapshot(71L, null)).contains(lakeSnapshot);
            assertThat(client.getLakeTableSnapshot(71L, 7L)).contains(lakeSnapshot);
            // a v1 LakeTable only holds its one snapshot, so a non-matching id yields empty
            assertThat(client.getLakeTableSnapshot(71L, 9L)).isEmpty();
            assertThat(client.getLatestReadableLakeTableSnapshot(71L)).contains(lakeSnapshot);
            // a table with no lake-table metadata yields empty
            assertThat(client.getLakeTableSnapshot(998L, null)).isEmpty();
            assertThat(client.getLatestReadableLakeTableSnapshot(998L)).isEmpty();

            // ---- kv bucket snapshot register/get/latest/table-map round-trip ----
            TableAssignment snapTa =
                    TableAssignment.builder()
                            .add(0, BucketAssignment.of(1, 2, 3))
                            .add(1, BucketAssignment.of(1, 2, 3))
                            .build();
            client.registerTableAssignment(81L, snapTa);
            TableBucket snapB0 = new TableBucket(81L, 0);
            assertThat(client.getTableBucketLatestSnapshot(snapB0)).isEmpty();
            client.registerTableBucketSnapshot(snapB0, new BucketSnapshot(1L, 100L, "oss://s/1"));
            client.registerTableBucketSnapshot(snapB0, new BucketSnapshot(2L, 200L, "oss://s/2"));
            assertThat(client.getTableBucketSnapshot(snapB0, 1L))
                    .hasValueSatisfying(s -> assertThat(s.getSnapshotId()).isEqualTo(1L));
            assertThat(client.getTableBucketSnapshot(snapB0, 9L)).isEmpty();
            assertThat(client.getTableBucketLatestSnapshotId(snapB0)).hasValue(2L);
            assertThat(client.getTableBucketLatestSnapshot(snapB0))
                    .hasValueSatisfying(s -> assertThat(s.getSnapshotId()).isEqualTo(2L));
            // list all snapshot ids (ascending) + all (snapshot, id) pairs
            assertThat(client.listBucketSnapshotIds(snapB0)).containsExactly(1L, 2L);
            assertThat(client.getTableBucketAllSnapshotAndIds(snapB0))
                    .extracting(t -> t.f1)
                    .containsExactlyInAnyOrder(1L, 2L);
            // per-table map: bucket 0 -> latest id 2; bucket 1 -> no snapshot
            Map<Integer, Optional<BucketSnapshot>> latestByBucket =
                    client.getTableLatestBucketSnapshot(81L);
            assertThat(latestByBucket).containsOnlyKeys(0, 1);
            assertThat(latestByBucket.get(0))
                    .hasValueSatisfying(s -> assertThat(s.getSnapshotId()).isEqualTo(2L));
            assertThat(latestByBucket.get(1)).isEmpty();
            // an unassigned table yields an empty map (no buckets to seed)
            assertThat(client.getTableLatestBucketSnapshot(9999L)).isEmpty();
            client.deleteTableBucketSnapshot(snapB0, 2L);
            assertThat(client.getTableBucketSnapshot(snapB0, 2L)).isEmpty();
            assertThat(client.getTableBucketLatestSnapshotId(snapB0)).hasValue(1L);

            // ---- producer offsets register(atomic)/get/version/delete-if-version/list ----
            assertThat(client.listProducerIds()).isEmpty();
            ProducerOffsets po1 =
                    new ProducerOffsets(
                            1_000L,
                            Collections.singletonList(
                                    new ProducerOffsets.TableOffsetMetadata(
                                            90L, new FsPath("oss://p/90/o"))));
            assertThat(client.tryRegisterProducerOffsets("prod-1", po1)).isTrue();
            // a second register for the same producer is rejected (atomic create-if-absent)
            assertThat(client.tryRegisterProducerOffsets("prod-1", po1)).isFalse();
            assertThat(client.getProducerOffsets("prod-1")).contains(po1);
            assertThat(client.getProducerOffsets("prod-x")).isEmpty();
            // version is 0 on the Raft backend (single-writer subsumes ZK's CAS)
            assertThat(client.getProducerOffsetsWithVersion("prod-1"))
                    .hasValueSatisfying(
                            t -> {
                                assertThat(t.f0).isEqualTo(po1);
                                assertThat(t.f1).isEqualTo(0);
                            });
            assertThat(client.getProducerOffsetsWithVersion("prod-x")).isEmpty();
            assertThat(client.listProducerIds()).containsExactly("prod-1");
            // Absent producer: same success as ZK NoNode on conditional delete.
            assertThat(client.deleteProducerSnapshotIfVersion("prod-x", 0)).isTrue();
            // Version mismatch (Raft version is always 0) refuses the delete.
            assertThat(client.deleteProducerSnapshotIfVersion("prod-1", 7)).isFalse();
            assertThat(client.getProducerOffsets("prod-1")).contains(po1);
            assertThat(client.deleteProducerSnapshotIfVersion("prod-1", 0)).isTrue();
            assertThat(client.getProducerOffsets("prod-1")).isEmpty();
            assertThat(client.listProducerIds()).isEmpty();
            // plain delete is a no-op-safe path
            client.tryRegisterProducerOffsets("prod-2", po1);
            client.deleteProducerOffsets("prod-2");
            assertThat(client.getProducerOffsets("prod-2")).isEmpty();

            // ---- dynamic entity config (single global key) upsert/fetch ----
            assertThat(client.fetchEntityConfig()).isEmpty();
            Map<String, String> cfg1 = new HashMap<>();
            cfg1.put("a", "1");
            cfg1.put("b", "2");
            client.upsertServerEntityConfig(cfg1);
            assertThat(client.fetchEntityConfig()).containsExactlyInAnyOrderEntriesOf(cfg1);
            // overwrite replaces wholesale
            Map<String, String> cfg2 = Collections.singletonMap("a", "9");
            client.upsertServerEntityConfig(cfg2);
            assertThat(client.fetchEntityConfig()).containsExactlyInAnyOrderEntriesOf(cfg2);

            // ---- DynamicConfigManager over the Raft store (neutral ctor, NO ZK watcher) ----
            client.upsertServerEntityConfig(new HashMap<>()); // clean slate for startup
            Configuration dcmConf = new Configuration();
            dcmConf.setString(ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "100MB");
            DynamicConfigManager dcm = new DynamicConfigManager(client, dcmConf, true);
            dcm.startup(); // no watcher on the raft backend; reads the (empty) entity config
            dcm.alterConfigs(
                    Collections.singletonList(
                            new AlterConfig(
                                    ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(),
                                    "200MB",
                                    AlterConfigOpType.SET)));
            // change is persisted to the Raft store...
            assertThat(client.fetchEntityConfig())
                    .containsEntry(
                            ConfigOptions.KV_SHARED_RATE_LIMITER_BYTES_PER_SEC.key(), "200MB");
            // ...and a fresh manager picks it up on startup from the same store (no throw)
            DynamicConfigManager dcm2 = new DynamicConfigManager(client, dcmConf, true);
            dcm2.startup();
            dcm.close();
            dcm2.close();

            // ---- rebalance task register/get (single global key) ----
            assertThat(client.getRebalanceTask()).isEmpty();
            RebalanceTask rbTask =
                    new RebalanceTask("rb1", RebalanceStatus.COMPLETED, Collections.emptyMap());
            client.registerRebalanceTask(rbTask);
            assertThat(client.getRebalanceTask()).contains(rbTask);

            // ---- server tags register/get/update/delete (single global key) ----
            assertThat(client.getServerTags()).isEmpty();
            Map<Integer, ServerTag> tags = new HashMap<>();
            tags.put(1, ServerTag.PERMANENT_OFFLINE);
            client.registerServerTags(new ServerTags(tags));
            assertThat(client.getServerTags())
                    .hasValueSatisfying(
                            st ->
                                    assertThat(st.getServerTags())
                                            .containsEntry(1, ServerTag.PERMANENT_OFFLINE));
            Map<Integer, ServerTag> tags2 = new HashMap<>();
            tags2.put(2, ServerTag.TEMPORARY_OFFLINE);
            client.updateServerTags(new ServerTags(tags2));
            assertThat(client.getServerTags())
                    .hasValueSatisfying(
                            st -> {
                                assertThat(st.getServerTags())
                                        .containsEntry(2, ServerTag.TEMPORARY_OFFLINE);
                                assertThat(st.getServerTags()).doesNotContainKey(1);
                            });
            client.deleteServerTags();
            assertThat(client.getServerTags()).isEmpty();

            // ---- resource acl create/get/list/update/delete (version-CAS subsumed) ----
            Resource aclDb = Resource.database("acldb");
            assertThat(client.getResourceAclWithVersion(aclDb).exists()).isFalse();
            AccessControlEntry ace1 =
                    new AccessControlEntry(
                            new FlussPrincipal("u1", "USER"),
                            "*",
                            OperationType.READ,
                            PermissionType.ALLOW);
            client.createResourceAcl(aclDb, new HashSet<>(Collections.singletonList(ace1)));
            VersionedAcls got = client.getResourceAclWithVersion(aclDb);
            assertThat(got.exists()).isTrue();
            assertThat(got.acls).containsExactly(ace1);
            assertThat(client.listResourcesByType(ResourceType.DATABASE)).containsExactly("acldb");
            // other resource types are disjoint (flag byte keeps the scan bounded)
            assertThat(client.listResourcesByType(ResourceType.TABLE)).isEmpty();
            // update (version ignored on raft) replaces the entry set
            AccessControlEntry ace2 =
                    new AccessControlEntry(
                            new FlussPrincipal("u2", "USER"),
                            "h2",
                            OperationType.READ,
                            PermissionType.ALLOW);
            client.updateResourceAcl(aclDb, new HashSet<>(Collections.singletonList(ace2)), 999);
            assertThat(client.getResourceAclWithVersion(aclDb).acls).containsExactly(ace2);
            client.insertAclChangeNotification(aclDb); // no-op on raft, must not throw
            client.conditionalDeleteResourceAcl(aclDb, 999);
            assertThat(client.getResourceAclWithVersion(aclDb).exists()).isFalse();
            assertThat(client.listResourcesByType(ResourceType.DATABASE)).isEmpty();

            // ---- tablet server register/get/list (ids inserted out of order) ----
            client.registerTabletServer(3, tabletServer("rack3", 3));
            client.registerTabletServer(1, tabletServer("rack1", 1));
            client.registerTabletServer(2, tabletServer("rack2", 2));
            assertThat(client.getTabletServer(2)).contains(tabletServer("rack2", 2));
            assertThat(client.getTabletServer(9)).isEmpty();
            assertThat(client.getSortedTabletServerList()).containsExactly(1, 2, 3);
            assertThat(client.getTabletServers(new int[] {1, 3})).containsOnlyKeys(1, 3);

            // ---- leaderAndIsr write (leader-fenced; no CAS version needed) ----
            TableBucket bucket = new TableBucket(7L, 0);
            LeaderAndIsr leaderAndIsr =
                    new LeaderAndIsr(
                            1, 10, Arrays.asList(1, 2, 3), Collections.emptyList(), 100, 1000);
            client.updateLeaderAndIsr(bucket, leaderAndIsr, 0);
            // leaderAndIsr read-back (single + batch)
            assertThat(client.getLeaderAndIsr(bucket)).contains(leaderAndIsr);
            assertThat(client.getLeaderAndIsr(new TableBucket(7L, 9))).isEmpty();
            assertThat(client.getLeaderAndIsrs(Arrays.asList(bucket, new TableBucket(7L, 9))))
                    .containsOnlyKeys(bucket);

            // ---- leaderAndIsr register + batch writes (CAS version subsumed on the raft leader)
            // ----
            TableBucket regBucket = new TableBucket(7L, 1);
            LeaderAndIsr regLai =
                    new LeaderAndIsr(
                            2, 11, Arrays.asList(2, 3), Collections.emptyList(), 101, 1001);
            client.registerLeaderAndIsr(regBucket, regLai, 999);
            assertThat(client.getLeaderAndIsr(regBucket)).contains(regLai);
            TableBucket batchBucket = new TableBucket(7L, 2);
            LeaderAndIsr batchLai =
                    new LeaderAndIsr(
                            3, 12, Arrays.asList(3, 1), Collections.emptyList(), 102, 1002);
            client.batchUpdateLeaderAndIsr(Collections.singletonMap(batchBucket, batchLai), 999);
            assertThat(client.getLeaderAndIsr(batchBucket)).contains(batchLai);

            // ---- bucket metadata (assignment + leaderAndIsr fold) ----
            TableAssignment bmTa =
                    TableAssignment.builder()
                            .add(0, BucketAssignment.of(1, 2, 3))
                            .add(1, BucketAssignment.of(4, 5, 6))
                            .build();
            client.registerTableAssignment(60L, bmTa);
            // leaderAndIsr set only on bucket 0 -> bucket 1 has null leader/epoch
            client.updateLeaderAndIsr(
                    new TableBucket(60L, 0),
                    new LeaderAndIsr(4, 2, Arrays.asList(4, 5), Collections.emptyList(), 7, 70),
                    0);
            Map<Long, List<BucketMetadata>> bmForTables =
                    client.getBucketMetadataForTables(Arrays.asList(60L, 61L));
            assertThat(bmForTables).containsOnlyKeys(60L, 61L);
            assertThat(bmForTables.get(61L)).isEmpty(); // requested but unassigned
            assertThat(bmForTables.get(60L)).hasSize(2);
            assertThat(bmForTables.get(60L))
                    .anySatisfy(
                            bm -> {
                                assertThat(bm.getBucketId()).isEqualTo(0);
                                assertThat(bm.getLeaderId()).hasValue(4);
                                assertThat(bm.getReplicas()).containsExactly(1, 2, 3);
                            })
                    .anySatisfy(
                            bm -> {
                                assertThat(bm.getBucketId()).isEqualTo(1);
                                assertThat(bm.getLeaderId()).isEmpty();
                                assertThat(bm.getReplicas()).containsExactly(4, 5, 6);
                            });

            // partition variant + getPartitionIds (partition 102 was assigned above, bucket 0 only)
            PhysicalTablePath partPath = PhysicalTablePath.of(partTable, "p2");
            assertThat(client.getPartitionIds(Collections.singletonList(partPath)))
                    .hasEntrySatisfying(
                            partPath,
                            tp -> {
                                assertThat(tp.getTableId()).isEqualTo(50L);
                                assertThat(tp.getPartitionId()).isEqualTo(102L);
                            });
            Map<Long, List<BucketMetadata>> bmForParts =
                    client.getBucketMetadataForPartitions(Arrays.asList(102L, 999L));
            assertThat(bmForParts).containsOnlyKeys(102L); // 999 unassigned -> absent
            assertThat(bmForParts.get(102L)).hasSize(1);
            assertThat(bmForParts.get(102L).get(0).getReplicas()).containsExactly(1, 2, 3);

            // 3-arg upsertLakeTable (isUpdate ignored on raft) round-trips like the 2-arg
            Map<TableBucket, Long> lt3Offsets = new HashMap<>();
            lt3Offsets.put(new TableBucket(72L, 0), 42L);
            LakeTable lt3 = new LakeTable(new LakeTableSnapshot(3L, lt3Offsets));
            client.upsertLakeTable(72L, lt3, false);
            assertThat(client.getLakeTable(72L)).contains(lt3);
            client.upsertLakeTable(72L, lt3, true); // update path must not throw
            assertThat(client.getLakeTable(72L)).contains(lt3);

            // ---- coordinator leader register/get/unregister ----
            CoordinatorAddress leaderAddr =
                    new CoordinatorAddress(
                            "n1", Endpoint.fromListenersString("FLUSS://localhost:9124"));
            client.registerCoordinatorServer(leaderAddr); // no-op on raft, must not throw
            client.registerCoordinatorLeader(leaderAddr);
            assertThat(client.getCoordinatorLeaderAddress()).contains(leaderAddr);
            // getCoordinatorServerList derives from the leader record (same id space as the
            // change events), not the voter set.
            assertThat(client.getCoordinatorServerList()).containsExactly("n1");

            // ---- id counters: block-allocated, forward-only ----
            assertThat(client.getTableIdAndIncrement()).isEqualTo(0L);
            assertThat(client.getTableIdAndIncrement()).isEqualTo(1L);
            assertThat(client.getWriterIdAndIncrement()).isEqualTo(0L);
            assertThat(client.getPartitionIdAndIncrement()).isEqualTo(0L);

            // ---- requireLeader fencing: a non-leader client rejects reads/writes (§5.7) ----
            // Mock isLeader() rather than sharing the live RaftMetadataServer: the real
            // selector probes raftDivisionIsLeader(), so a second selector on the same server
            // would report leader and skip the fence.
            followerSelector = mock(RaftLeaderSelector.class);
            when(followerSelector.isLeader()).thenReturn(false);
            RaftMetadataClient follower =
                    new RaftMetadataClient(
                            writer, store, followerSelector, tableIds, partitionIds, writerIds);
            assertThatThrownBy(() -> follower.getTable(tablePath))
                    .isInstanceOf(NotCoordinatorLeaderException.class);
            assertThatThrownBy(() -> follower.registerTabletServer(4, tabletServer("rack4", 4)))
                    .isInstanceOf(NotCoordinatorLeaderException.class);
            // getCoordinatorLeaderAddress is exempt: a non-leader answers it for the redirect hint.
            assertThat(follower.getCoordinatorLeaderAddress()).contains(leaderAddr);

            // ---- newLeaderSelector returns the bound RaftLeaderSelector ----
            assertThat(client.newLeaderSelector("n1")).isSameAs(leaderSelector);

            // unregister clears the leader address
            client.unregisterCoordinatorLeader(leaderAddr);
            assertThat(client.getCoordinatorLeaderAddress()).isEmpty();
            assertThat(client.getCoordinatorServerList()).isEmpty();
        } finally {
            if (client != null) {
                client.close();
            }
            if (followerSelector != null) {
                followerSelector.close();
            }
            server.close();
        }
    }

    @Test
    void testFactoryBootstrapsWorkingSingleNodeClient() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        // seed.nodes present + zookeeper.address absent => MetadataBackend.RAFT
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        MetadataStoreClient metaClient = MetadataStoreClientFactory.create(conf, null);
        try {
            // the factory bootstrapped a real embedded Ratis server that elects itself leader
            waitUntil(
                    20_000,
                    () -> metaClient.newLeaderSelector("n1").isLeader(),
                    "factory-created raft client did not become leader");

            TablePath tablePath = TablePath.of("db", "orders");
            TableDescriptor descriptor =
                    TableDescriptor.builder()
                            .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                            .distributedBy(3)
                            .build();
            TableRegistration reg =
                    TableRegistration.newTable(7L, "file:///tmp/remote", descriptor);
            metaClient.registerTable(tablePath, reg);
            assertThat(metaClient.getTable(tablePath)).contains(reg);
        } finally {
            metaClient.close();
        }
    }

    @Test
    void testCreateTableAndPartitionBatchAreReadableAfterApply() throws Exception {
        int port = freePort();
        String address = "127.0.0.1:" + port;
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS.key(), address);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID.key(), address);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        try (RaftMetadataClient client = RaftMetadataClient.create(conf)) {
            waitUntil(20_000, client::isLeader, "node did not become raft leader");

            TablePath tablePath = TablePath.of("db", "orders");
            Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
            TableDescriptor descriptor =
                    TableDescriptor.builder().schema(schema).distributedBy(3).build();
            TableAssignment assignment =
                    TableAssignment.builder().add(0, BucketAssignment.of(1, 2, 3)).build();
            long tableId = client.getTableIdAndIncrement();
            TableRegistration reg =
                    TableRegistration.newTable(tableId, "file:///tmp/remote", descriptor);
            client.registerTableSchemaAssignmentAndMetadata(tablePath, schema, assignment, reg);

            assertThat(client.getTable(tablePath)).contains(reg);
            assertThat(client.getSchemaById(tablePath, 1)).contains(new SchemaInfo(schema, 1));
            assertThat(client.getTableAssignment(tableId)).contains(assignment);

            PartitionAssignment partitionAssignment =
                    new PartitionAssignment(tableId, assignment.getBucketAssignments());
            client.registerPartitionAssignmentAndMetadata(
                    101L, "p1", partitionAssignment, "file:///tmp/remote", tablePath, tableId);
            assertThat(client.getPartition(tablePath, "p1"))
                    .hasValueSatisfying(
                            p -> {
                                assertThat(p.getPartitionId()).isEqualTo(101L);
                                assertThat(p.getTableId()).isEqualTo(tableId);
                            });
            assertThat(client.getPartitionAssignment(101L)).contains(partitionAssignment);
        }
    }

    private static TabletServerRegistration tabletServer(String rack, int id) {
        return new TabletServerRegistration(
                rack,
                Endpoint.fromListenersString("FLUSS://localhost:" + (9000 + id)),
                10_000L + id);
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
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
