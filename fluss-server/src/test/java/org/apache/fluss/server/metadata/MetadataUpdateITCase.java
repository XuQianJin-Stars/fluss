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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.server.coordinator.LakeCatalogDynamicLoader;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.tablet.TabletServer;
import org.apache.fluss.server.testutils.DualBackendFlussClusters;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.TableAssignment;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.record.TestData.DATA2_TABLE_PATH;
import static org.apache.fluss.server.testutils.PartitionMetadataAssert.assertPartitionMetadata;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createPartition;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropPartitionRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newDropTableRequest;
import static org.apache.fluss.server.testutils.TableMetadataAssert.assertTableMetadata;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for metadata update. Runs on ZooKeeper and Raft. */
class MetadataUpdateITCase {

    @RegisterExtension
    public static final DualBackendFlussClusters CLUSTERS =
            DualBackendFlussClusters.builder().setNumOfTabletServers(3).build();

    private FlussClusterExtension flussClusterExtension;
    private CoordinatorGateway coordinatorGateway;
    private ServerNode coordinatorServerNode;
    private MetadataStoreClient metadataStore;
    private MetadataManager metadataManager;

    private void bind(MetadataBackend backend) {
        flussClusterExtension = CLUSTERS.get(backend);
        coordinatorGateway = flussClusterExtension.newCoordinatorClient();
        coordinatorServerNode = flussClusterExtension.getCoordinatorServerNode();
        refreshMetadataAccessors();
    }

    /** Raft coordinator restart replaces the metadata client; always rebind before asserting. */
    private void refreshMetadataAccessors() {
        metadataStore = flussClusterExtension.metadataStore();
        metadataManager =
                new MetadataManager(
                        metadataStore,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testMetadataUpdateForServerStartAndStop(MetadataBackend backend) throws Exception {
        bind(backend);
        // get metadata and check it
        flussClusterExtension.waitUntilAllGatewayHasSameMetadata();

        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        // create non-partitioned table
        long tableId1 =
                createTable(flussClusterExtension, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId1, new TableContext(false, false, DATA1_TABLE_PATH, tableId1, null));
        // create partitioned table
        long tableId2 =
                createTable(
                        flussClusterExtension,
                        DATA2_TABLE_PATH,
                        DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId2, new TableContext(false, true, DATA2_TABLE_PATH, tableId2, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // now, start one tablet server and check it
        flussClusterExtension.startTabletServer(3);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                4,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // now, kill one tablet server and check it
        flussClusterExtension.stopTabletServer(1);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // check when coordinator start, it should send update metadata request
        // to all tablet servers

        // let's stop the coordinator server
        flussClusterExtension.stopCoordinatorServer();
        // then kill one tablet server again
        flussClusterExtension.stopTabletServer(2);
        // let's start the coordinator server again;
        flussClusterExtension.startCoordinatorServer();
        coordinatorServerNode = flussClusterExtension.getCoordinatorServerNode();
        refreshMetadataAccessors();
        // Live tablets must re-register after the coordinator bounce before we assert membership.
        flussClusterExtension.assertHasTabletServerNumber(2);
        // check the metadata again (offline tablet 2 is lease-fenced after session-timeout)
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                2,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // add back tablet server2
        flussClusterExtension.startTabletServer(2);
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testMetadataUpdateForTableCreateAndDrop(MetadataBackend backend) throws Exception {
        bind(backend);
        flussClusterExtension.waitUntilAllGatewayHasSameMetadata();
        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        assertUpdateMetadataEquals(
                coordinatorServerNode, 3, expectedTablePathById, Collections.emptyMap());

        long tableId = createTable(flussClusterExtension, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);
        expectedTablePathById.put(
                tableId, new TableContext(false, false, DATA1_TABLE_PATH, tableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // create a partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long partitionedTableId =
                createTable(flussClusterExtension, partitionTablePath, partitionTableDescriptor);
        expectedTablePathById.put(
                partitionedTableId,
                new TableContext(false, true, partitionTablePath, partitionedTableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // test drop table.
        coordinatorGateway.dropTable(
                newDropTableRequest(
                        DATA1_TABLE_PATH.getDatabaseName(),
                        DATA1_TABLE_PATH.getTableName(),
                        false));
        expectedTablePathById.put(
                tableId, new TableContext(true, false, DATA1_TABLE_PATH, tableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // test drop table.
        // TODO, this is a bug, which will cause CoordinatorServer cache contains residual
        // tablePath, trace by: https://github.com/apache/fluss/issues/898
        //        coordinatorGateway.dropTable(
        //                newDropTableRequest(
        //                        partitionTablePath.getDatabaseName(),
        //                        partitionTablePath.getTableName(),
        //                        false));
        //        expectedTablePathById.put(partitionedTableId, new TablePathTuple(true, true,
        // partitionTablePath,
        //        partitionedTableId, null));
        //        retry(
        //                Duration.ofMinutes(1),
        //                () ->
        //                        assertUpdateMetadataEquals(
        //                                coordinatorServerNode,
        //                                3,
        //                                expectedTablePathById,
        //                                Collections.emptyMap()));
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testMetadataUpdateForPartitionCreateAndDrop(MetadataBackend backend) throws Exception {
        bind(backend);
        flussClusterExtension.waitUntilAllGatewayHasSameMetadata();
        Map<Long, TableContext> expectedTablePathById = new HashMap<>();
        Map<Long, TableContext> expectedPartitionNameById = new HashMap<>();
        assertUpdateMetadataEquals(
                coordinatorServerNode, 3, expectedTablePathById, Collections.emptyMap());

        // create a partitioned table.
        TablePath partitionTablePath = TablePath.of("test_db_1", "test_partition_table_1");
        TableDescriptor partitionTableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(3)
                        .partitionedBy("b")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, false)
                        .build();
        long partitionedTableId =
                createTable(flussClusterExtension, partitionTablePath, partitionTableDescriptor);
        expectedTablePathById.put(
                partitionedTableId,
                new TableContext(false, true, partitionTablePath, partitionedTableId, null));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                Collections.emptyMap()));

        // create two new partitions.
        String partitionName1 = "b1";
        String partitionName2 = "b2";
        long partitionId1 =
                createPartition(
                        flussClusterExtension,
                        partitionTablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                        false);
        long partitionId2 =
                createPartition(
                        flussClusterExtension,
                        partitionTablePath,
                        new PartitionSpec(Collections.singletonMap("b", partitionName2)),
                        false);
        expectedPartitionNameById.put(
                partitionId1,
                new TableContext(
                        false, true, partitionTablePath, partitionedTableId, partitionName1));
        expectedPartitionNameById.put(
                partitionId2,
                new TableContext(
                        false, true, partitionTablePath, partitionedTableId, partitionName2));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                expectedPartitionNameById));

        // drop one partition.
        coordinatorGateway
                .dropPartition(
                        newDropPartitionRequest(
                                partitionTablePath,
                                new PartitionSpec(Collections.singletonMap("b", partitionName1)),
                                false))
                .get();
        expectedPartitionNameById.put(
                partitionId1,
                new TableContext(
                        true, true, partitionTablePath, partitionedTableId, partitionName1));
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertUpdateMetadataEquals(
                                coordinatorServerNode,
                                3,
                                expectedTablePathById,
                                expectedPartitionNameById));
    }

    private void assertUpdateMetadataEquals(
            ServerNode expectedCoordinatorServer,
            int expectedTabletServerSize,
            Map<Long, TableContext> expectedTablePathById,
            Map<Long, TableContext> expectedPartitionNameById) {
        ServerMetadataCache csMetadataCache =
                flussClusterExtension.getCoordinatorServer().getMetadataCache();
        assertThat(csMetadataCache.getCoordinatorServer("FLUSS"))
                .isEqualTo(expectedCoordinatorServer);
        assertThat(csMetadataCache.getAliveTabletServerInfos().size())
                .isEqualTo(expectedTabletServerSize);

        List<TabletServerMetadataCache> metadataCacheList =
                flussClusterExtension.getTabletServers().stream()
                        .map(TabletServer::getMetadataCache)
                        .collect(Collectors.toList());
        metadataCacheList.forEach(
                serverMetadataCache -> {
                    assertThat(serverMetadataCache.getCoordinatorServer("FLUSS"))
                            .isEqualTo(expectedCoordinatorServer);

                    assertThat(serverMetadataCache.getAliveTabletServerInfos().size())
                            .isEqualTo(expectedTabletServerSize);

                    expectedTablePathById.forEach(
                            (tableId, tableContext) -> {
                                if (!tableContext.isDeleted) {
                                    TablePath tablePath = tableContext.tablePath;
                                    assertThat(serverMetadataCache.getTablePath(tableId))
                                            .hasValue(tablePath);

                                    // check table info and bucket location and leader only for
                                    // non-partitioned table.
                                    if (!tableContext.isPartitionedTable) {
                                        TableMetadata tableMetadataFromStore =
                                                getTableMetadataFromStore(tablePath, tableId);
                                        assertTableMetadata(
                                                        serverMetadataCache
                                                                .getTableMetadata(tablePath)
                                                                .get())
                                                .isEqualTo(tableMetadataFromStore);
                                    }
                                } else {
                                    assertThat(serverMetadataCache.getTablePath(tableId))
                                            .isNotPresent();
                                    assertThat(
                                                    serverMetadataCache.getTableMetadata(
                                                            tableContext.tablePath))
                                            .isEmpty();
                                }
                            });
                    expectedPartitionNameById.forEach(
                            (partitionId, tableContext) -> {
                                PhysicalTablePath physicalTablePath =
                                        PhysicalTablePath.of(
                                                tableContext.tablePath, tableContext.partitionName);
                                if (!tableContext.isDeleted) {
                                    assertThat(
                                                    serverMetadataCache.getPhysicalTablePath(
                                                            partitionId))
                                            .hasValue(physicalTablePath);

                                    PartitionMetadata partitionMetadataFromStore =
                                            getPartitionMetadataFromStore(
                                                    tableContext.tableId,
                                                    tableContext.partitionName,
                                                    partitionId);
                                    assertPartitionMetadata(
                                                    serverMetadataCache
                                                            .getPartitionMetadata(physicalTablePath)
                                                            .get())
                                            .isEqualTo(partitionMetadataFromStore);
                                } else {
                                    assertThat(
                                                    serverMetadataCache.getPhysicalTablePath(
                                                            partitionId))
                                            .isNotPresent();
                                    assertThat(
                                                    serverMetadataCache.getPartitionMetadata(
                                                            physicalTablePath))
                                            .isEmpty();
                                }
                            });
                });
    }

    /**
     * Build expected table metadata from the cluster metadata store. Uses AssertJ so that callers
     * wrapped in {@link org.apache.fluss.testutils.common.CommonTestUtils#retry} retry until
     * assignments / leader-and-isr are present (Raft apply is eventually consistent).
     */
    private TableMetadata getTableMetadataFromStore(TablePath tablePath, long tableId) {
        TableInfo tableInfo = metadataManager.getTable(tablePath);
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        try {
            assertThat(metadataStore.getTableAssignment(tableId)).isPresent();
            TableAssignment tableAssignment = metadataStore.getTableAssignment(tableId).get();
            for (Map.Entry<Integer, BucketAssignment> entry :
                    tableAssignment.getBucketAssignments().entrySet()) {
                int bucketId = entry.getKey();
                List<Integer> replicas = entry.getValue().getReplicas();
                TableBucket tableBucket = new TableBucket(tableId, bucketId);
                assertThat(metadataStore.getLeaderAndIsr(tableBucket)).isPresent();
                LeaderAndIsr leaderAndIsr = metadataStore.getLeaderAndIsr(tableBucket).get();
                bucketMetadataList.add(
                        new BucketMetadata(
                                bucketId,
                                leaderAndIsr.leader(),
                                leaderAndIsr.leaderEpoch(),
                                replicas));
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            // Leadership flaps / store not ready yet — let retry() keep polling.
            throw new AssertionError(e);
        }
        return new TableMetadata(tableInfo, bucketMetadataList);
    }

    private PartitionMetadata getPartitionMetadataFromStore(
            long tableId, String partitionName, long partitionId) {
        List<BucketMetadata> bucketMetadataList = new ArrayList<>();
        try {
            assertThat(metadataStore.getPartitionAssignment(partitionId)).isPresent();
            TableAssignment tableAssignment =
                    metadataStore.getPartitionAssignment(partitionId).get();
            for (Map.Entry<Integer, BucketAssignment> entry :
                    tableAssignment.getBucketAssignments().entrySet()) {
                int bucketId = entry.getKey();
                List<Integer> replicas = entry.getValue().getReplicas();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                assertThat(metadataStore.getLeaderAndIsr(tableBucket)).isPresent();
                LeaderAndIsr leaderAndIsr = metadataStore.getLeaderAndIsr(tableBucket).get();
                bucketMetadataList.add(
                        new BucketMetadata(
                                bucketId,
                                leaderAndIsr.leader(),
                                leaderAndIsr.leaderEpoch(),
                                replicas));
            }
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return new PartitionMetadata(tableId, partitionName, partitionId, bucketMetadataList);
    }

    private static class TableContext {
        final boolean isDeleted;
        final boolean isPartitionedTable;
        final TablePath tablePath;
        final long tableId;
        final @Nullable String partitionName;

        public TableContext(
                boolean isDeleted,
                boolean isPartitionedTable,
                TablePath tablePath,
                long tableId,
                @Nullable String partitionName) {
            this.isDeleted = isDeleted;
            this.isPartitionedTable = isPartitionedTable;
            this.tablePath = tablePath;
            this.tableId = tableId;
            this.partitionName = partitionName;
        }
    }
}
