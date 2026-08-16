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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.TableRegistration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;

/**
 * Projects a {@link TabletServerMetadataCache} from a follower {@link RaftMetadataStore} kept in
 * sync by a {@link MetadataStreamSubscriber} (§5.8). On the Raft metadata backend this replaces the
 * coordinator's {@code UpdateMetadataRequest} push (over ZK the coordinator watched ZooKeeper and
 * pushed metadata to tablet servers); here the tablet server pulls the metadata stream and
 * re-derives its cache from the local replica store.
 *
 * <p><b>Scope:</b> the FULL cache view —
 *
 * <ul>
 *   <li>server topology (coordinator leader + alive tablet servers), via {@link
 *       RaftServerTopologyReader};
 *   <li>tables: every registered table's {@link TableInfo} (registration + latest schema) + its
 *       {@link BucketMetadata} (assignment + leaderAndIsr);
 *   <li>partitions: every partition's {@link PartitionMetadata} + buckets.
 * </ul>
 *
 * <p>Reads run through a {@link RaftMetadataClient#followerReadView} over the local store — the
 * exact, tested store-read logic the leader uses, minus the leader guard (the follower store is an
 * exact replica of the leader's committed state).
 *
 * <p><b>Deletions:</b> {@link TabletServerMetadataCache#updateClusterMetadata} merges tables/
 * partitions (it never removes on absence), so this applier is stateful — it remembers what it
 * projected last {@link #refresh()} and emits the cache's delete markers ({@code
 * DELETED_TABLE_PATH} + real table id / {@code DELETED_PARTITION_ID}) for tables/partitions that
 * have since vanished from the store.
 *
 * <p><b>Threading:</b> {@link #refresh()} is intended to run on the single subscriber thread after
 * each applied batch (its {@code last*} state is therefore not synchronized); {@code
 * updateClusterMetadata} swaps an immutable snapshot under its own lock, so concurrent cache reads
 * are safe.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class TabletServerMetadataStreamCacheApplier {

    private final RaftMetadataStore store;
    private final RaftMetadataClient readView;
    private final TabletServerMetadataCache cache;

    // What the previous refresh projected, so this one can emit delete markers for vanished ids.
    private Map<Long, TableInfo> lastTables = Collections.emptyMap();
    private Map<Long, PartitionRef> lastPartitions = Collections.emptyMap();

    public TabletServerMetadataStreamCacheApplier(
            RaftMetadataStore store, TabletServerMetadataCache cache) {
        this.store = store;
        this.readView = RaftMetadataClient.followerReadView(store);
        this.cache = cache;
    }

    /**
     * Re-derives the full cluster metadata from the current local store and installs it
     * (coordinator + alive servers replaced wholesale; tables/partitions merged with delete markers
     * for vanished ids). Idempotent.
     */
    public void refresh() {
        Set<ServerInfo> tabletServers = RaftServerTopologyReader.readTabletServers(store);
        ServerInfo coordinator = RaftServerTopologyReader.readCoordinator(store);

        // ---- tables: TableInfo (registration + latest schema) ----
        Map<Long, TableInfo> currentTables = new LinkedHashMap<>();
        for (String database : readView.listDatabases()) {
            for (String tableName : readView.listTables(database)) {
                TablePath tablePath = TablePath.of(database, tableName);
                Optional<TableRegistration> reg = readView.getTable(tablePath);
                if (!reg.isPresent()) {
                    continue;
                }
                SchemaInfo schemaInfo =
                        readView.getLatestSchemas(Collections.singletonList(tablePath))
                                .get(tablePath);
                if (schemaInfo == null) {
                    // Table record visible before its schema (mid-create); pick it up next refresh.
                    continue;
                }
                TableInfo info = reg.get().toTableInfo(tablePath, schemaInfo);
                currentTables.put(info.getTableId(), info);
            }
        }

        Map<Long, List<BucketMetadata>> tableBuckets =
                readView.getBucketMetadataForTables(currentTables.keySet());
        List<TableMetadata> tableMetadataList = new ArrayList<>();
        currentTables.forEach(
                (tableId, info) ->
                        tableMetadataList.add(
                                new TableMetadata(
                                        info,
                                        tableBuckets.getOrDefault(
                                                tableId, Collections.emptyList()))));
        // delete markers for tables projected last time but now gone
        lastTables.forEach(
                (tableId, oldInfo) -> {
                    if (!currentTables.containsKey(tableId)) {
                        tableMetadataList.add(
                                new TableMetadata(
                                        deletedTableMarker(oldInfo), Collections.emptyList()));
                    }
                });

        // ---- partitions: PartitionMetadata + buckets ----
        Map<Long, PartitionRef> currentPartitions = new LinkedHashMap<>();
        for (TableInfo info : currentTables.values()) {
            Map<String, PartitionRegistration> registrations =
                    readView.getPartitionRegistrations(info.getTablePath());
            registrations.forEach(
                    (name, reg) ->
                            currentPartitions.put(
                                    reg.getPartitionId(),
                                    new PartitionRef(info.getTableId(), name)));
        }

        Map<Long, List<BucketMetadata>> partitionBuckets =
                readView.getBucketMetadataForPartitions(currentPartitions.keySet());
        List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
        currentPartitions.forEach(
                (partitionId, ref) ->
                        partitionMetadataList.add(
                                new PartitionMetadata(
                                        ref.tableId,
                                        ref.partitionName,
                                        partitionId,
                                        partitionBuckets.getOrDefault(
                                                partitionId, Collections.emptyList()))));
        // delete markers for partitions projected last time but now gone
        lastPartitions.forEach(
                (partitionId, ref) -> {
                    if (!currentPartitions.containsKey(partitionId)) {
                        partitionMetadataList.add(
                                new PartitionMetadata(
                                        ref.tableId,
                                        ref.partitionName,
                                        DELETED_PARTITION_ID,
                                        Collections.emptyList()));
                    }
                });

        cache.updateClusterMetadata(
                new ClusterMetadata(
                        coordinator, tabletServers, tableMetadataList, partitionMetadataList));

        lastTables = currentTables;
        lastPartitions = currentPartitions;
    }

    /**
     * Builds a delete-marked {@link TableMetadata} for a table that disappeared from the stream
     * projection. Uses {@link TableMetadata#DELETED_TABLE_PATH} with the real table id so tablets
     * remove by id (safe if the same {@link org.apache.fluss.metadata.TablePath} is recreated
     * quickly); path + {@code DELETED_TABLE_ID} can wipe the new table when a late tombstone
     * arrives.
     */
    private static TableInfo deletedTableMarker(TableInfo old) {
        return TableInfo.of(
                DELETED_TABLE_PATH,
                old.getTableId(),
                old.getSchemaId(),
                old.toTableDescriptor(),
                old.getRemoteDataDir(),
                old.getCreatedTime(),
                old.getModifiedTime());
    }

    /** A partition's identity (owning table + name), retained to build its delete marker. */
    private static final class PartitionRef {
        private final long tableId;
        private final String partitionName;

        private PartitionRef(long tableId, String partitionName) {
            this.tableId = tableId;
            this.partitionName = partitionName;
        }
    }
}
