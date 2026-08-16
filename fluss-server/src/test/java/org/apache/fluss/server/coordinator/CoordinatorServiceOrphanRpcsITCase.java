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

import org.apache.fluss.fs.FsPath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.ListKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.ListKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsRequest;
import org.apache.fluss.rpc.messages.ListRemoteLogManifestsResponse;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbRemoteLogManifestEntry;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.server.coordinator.raft.AllocateIdRpcServer;
import org.apache.fluss.server.coordinator.raft.MetadataStreamRpcs;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshot;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotHandleStore;
import org.apache.fluss.server.kv.snapshot.CompletedSnapshotStore;
import org.apache.fluss.server.kv.snapshot.KvSnapshotHandle;
import org.apache.fluss.server.kv.snapshot.SharedKvFileRegistry;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.testutils.DualBackendFlussClusters;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.data.RemoteLogManifestHandle;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for orphan-cleanup-related coordinator RPCs: ListRemoteLogManifests, ListKvSnapshots. */
class CoordinatorServiceOrphanRpcsITCase {

    @RegisterExtension
    public static final DualBackendFlussClusters CLUSTERS =
            DualBackendFlussClusters.builder().setNumOfTabletServers(1).build();

    private static final TablePath PK_TABLE_PATH = TablePath.of("orphan_rpc_test_db", "pk_table");
    private static final TableDescriptor PK_TABLE_DESCRIPTOR =
            TableDescriptor.builder()
                    .schema(
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .withComment("pk col")
                                    .column("val", DataTypes.STRING())
                                    .withComment("value col")
                                    .primaryKey("id")
                                    .build())
                    .distributedBy(3, "id")
                    .build();

    private static final int MAX_SNAPSHOTS_TO_RETAIN = 2;

    @TempDir static Path tempDir;

    private FlussClusterExtension flussClusterExtension;

    private void bind(MetadataBackend backend) {
        flussClusterExtension = CLUSTERS.get(backend);
    }

    private long ensurePkTable() throws Exception {
        return RpcMessageTestUtils.createTable(
                flussClusterExtension, PK_TABLE_PATH, PK_TABLE_DESCRIPTOR);
    }

    // -------------------------------------------------------------------------
    // ListRemoteLogManifests
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void listManifestsForUnpartitionedTable(MetadataBackend backend) throws Exception {
        bind(backend);
        long tableId = 999_111L;
        MetadataStoreClient store = flussClusterExtension.metadataStore();
        store.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 0),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-0/m.manifest"), 100L));
        store.upsertRemoteLogManifestHandle(
                new TableBucket(tableId, 1),
                new RemoteLogManifestHandle(new FsPath("oss://bucket-1/m.manifest"), 200L));

        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(tableId);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).hasSize(2);
        assertThat(response.getManifestsList())
                .extracting(PbRemoteLogManifestEntry::getRemoteLogEndOffset)
                .containsExactlyInAnyOrder(100L, 200L);
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void listManifestsForUnknownTableReturnsEmpty(MetadataBackend backend) throws Exception {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        ListRemoteLogManifestsRequest request = new ListRemoteLogManifestsRequest();
        request.setTableId(999_999L);
        ListRemoteLogManifestsResponse response = gateway.listRemoteLogManifests(request).get();

        assertThat(response.getManifestsList()).isEmpty();
    }

    // -------------------------------------------------------------------------
    // ListKvSnapshots
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void listSnapshotsReturnsRetainedSubset(MetadataBackend backend) throws Exception {
        bind(backend);
        long pkTableId = ensurePkTable();
        TableBucket tb = new TableBucket(pkTableId, 0);
        CompletedSnapshotStore store = createTestStore(snapshot -> false);
        for (long snapId = 1L; snapId <= 4L; snapId++) {
            store.add(createMinimalSnapshot(tb, snapId));
        }
        injectStore(tb, store);

        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(pkTableId);
        ListKvSnapshotsResponse response = gateway.listKvSnapshots(request).get();

        assertThat(response.getTableId()).isEqualTo(pkTableId);
        assertThat(response.hasPartitionId()).isFalse();
        List<PbKvSnapshot> bucket0 =
                response.getActiveSnapshotsList().stream()
                        .filter(s -> s.getBucketId() == 0)
                        .collect(Collectors.toList());
        assertThat(bucket0)
                .extracting(PbKvSnapshot::getSnapshotId)
                .containsExactlyInAnyOrder(3L, 4L);
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void listSnapshotsReturnsStillInUseBeyondRetention(MetadataBackend backend) throws Exception {
        bind(backend);
        long pkTableId = ensurePkTable();
        TableBucket tb = new TableBucket(pkTableId, 1);
        CompletedSnapshotStore store = createTestStore(snapshot -> snapshot.getSnapshotID() == 1L);
        for (long snapId = 1L; snapId <= 4L; snapId++) {
            store.add(createMinimalSnapshot(tb, snapId));
        }
        injectStore(tb, store);

        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(pkTableId);
        ListKvSnapshotsResponse response = gateway.listKvSnapshots(request).get();

        List<PbKvSnapshot> bucket1Snapshots =
                response.getActiveSnapshotsList().stream()
                        .filter(s -> s.getBucketId() == 1)
                        .collect(Collectors.toList());
        // RETAINED = {3, 4}, STILL_IN_USE = {1}
        assertThat(bucket1Snapshots)
                .extracting(PbKvSnapshot::getSnapshotId)
                .containsExactlyInAnyOrder(1L, 3L, 4L);
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void listSnapshotsForUnknownTableFails(MetadataBackend backend) {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        ListKvSnapshotsRequest request = new ListKvSnapshotsRequest();
        request.setTableId(999_888L);

        assertThatThrownBy(() -> gateway.listKvSnapshots(request).get())
                .isInstanceOf(ExecutionException.class);
    }

    // -------------------------------------------------------------------------
    // FetchMetadata / RegisterTabletServer / heartbeat / AllocateId — Raft-backed coordinators
    // serve these; ZooKeeper coordinators fail cleanly with UnsupportedOperationException.
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void fetchMetadataOnCoordinatorGateway(MetadataBackend backend) throws Exception {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        FetchMetadataRequest request = MetadataStreamRpcs.buildFetchRequest(1, 1L, 0L, 100, 0);
        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThatThrownBy(() -> gateway.fetchMetadata(request).get())
                    .isInstanceOf(ExecutionException.class);
        } else {
            FetchMetadataResponse response = gateway.fetchMetadata(request).get();
            assertThat(response.getLeaderEpoch()).isPositive();
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void registerTabletServerOnCoordinatorGateway(MetadataBackend backend) throws Exception {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        RegisterTabletServerRequest request = new RegisterTabletServerRequest();
        request.setServerId(99);
        request.setRegisterTimestamp(1L);
        request.setListeners("FLUSS://localhost:19099");
        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThatThrownBy(() -> gateway.registerTabletServer(request).get())
                    .isInstanceOf(ExecutionException.class);
        } else {
            RegisterTabletServerResponse response = gateway.registerTabletServer(request).get();
            assertThat(response.getLeaderEpoch()).isPositive();
            MetadataStoreClient store = flussClusterExtension.metadataStore();
            if (store instanceof RaftMetadataClient) {
                ((RaftMetadataClient) store).fenceTabletServer(99);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void tabletServerHeartbeatOnCoordinatorGateway(MetadataBackend backend) throws Exception {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        TabletServerHeartbeatRequest request = new TabletServerHeartbeatRequest();
        request.setServerId(0);
        request.setLeaderEpoch(0L);
        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThatThrownBy(() -> gateway.tabletServerHeartbeat(request).get())
                    .isInstanceOf(ExecutionException.class);
        } else {
            TabletServerHeartbeatResponse response = gateway.tabletServerHeartbeat(request).get();
            assertThat(response.getLeaderEpoch()).isPositive();
            assertThat(response.isRegistered()).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void allocateIdOnCoordinatorGateway(MetadataBackend backend) throws Exception {
        bind(backend);
        CoordinatorGateway gateway = flussClusterExtension.newCoordinatorClient();
        AllocateIdRequest request = new AllocateIdRequest();
        request.setIdKind(AllocateIdRpcServer.KIND_WRITER);
        request.setCount(1L);
        if (backend == MetadataBackend.ZOOKEEPER) {
            assertThatThrownBy(() -> gateway.allocateId(request).get())
                    .isInstanceOf(ExecutionException.class);
        } else {
            AllocateIdResponse response = gateway.allocateId(request).get();
            assertThat(response.getCount()).isEqualTo(1L);
            assertThat(response.getStartId()).isGreaterThanOrEqualTo(0L);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private CompletedSnapshotStore createTestStore(
            CompletedSnapshotStore.SnapshotInUseChecker inUseChecker) {
        return new CompletedSnapshotStore(
                MAX_SNAPSHOTS_TO_RETAIN,
                new SharedKvFileRegistry(),
                Collections.emptyList(),
                new NoOpSnapshotHandleStore(),
                directExecutor(),
                inUseChecker);
    }

    private void injectStore(TableBucket tb, CompletedSnapshotStore store) {
        CompletedSnapshotStoreManager storeManager =
                flussClusterExtension
                        .getCoordinatorServer()
                        .getCoordinatorEventProcessor()
                        .completedSnapshotStoreManager();
        storeManager.getBucketCompletedSnapshotStores().put(tb, store);
    }

    private CompletedSnapshot createMinimalSnapshot(TableBucket tb, long snapshotId) {
        Path snapDir = tempDir.resolve("snap-" + tb.getBucket() + "-" + snapshotId);
        snapDir.toFile().mkdirs();
        return new CompletedSnapshot(
                tb,
                snapshotId,
                new FsPath(snapDir.resolve("_metadata").toUri().toString()),
                KvSnapshotHandle.create(Collections.emptyList(), Collections.emptyList(), 0L),
                snapshotId,
                null,
                null);
    }

    /** No-op implementation that avoids ZK writes during test snapshot population. */
    private static class NoOpSnapshotHandleStore implements CompletedSnapshotHandleStore {

        @Override
        public void add(
                TableBucket tableBucket,
                long snapshotId,
                CompletedSnapshotHandle completedSnapshotHandle) {}

        @Override
        public void remove(TableBucket tableBucket, long snapshotId) {}

        @Override
        public Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId) {
            return Optional.empty();
        }

        @Override
        public List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(
                TableBucket tableBucket) {
            return Collections.emptyList();
        }

        @Override
        public Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(
                TableBucket tableBucket) {
            return Optional.empty();
        }
    }
}
