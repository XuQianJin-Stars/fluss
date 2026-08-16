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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.TableLifecycleThrottler;
import org.apache.fluss.server.coordinator.event.CreatePartitionEvent;
import org.apache.fluss.server.coordinator.event.CreateTableEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.SchemaChangeEvent;
import org.apache.fluss.server.coordinator.event.TableRegistrationChangeEvent;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.PartitionRegistration;
import org.apache.fluss.server.zk.data.PartitionRegistrationJsonSerde;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TableRegistrationJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Raft-backed replacement for the ZooKeeper {@code TableChangeWatcher} (§5.8, route-1 (c)):
 * translates table/schema/partition metadata changes into the SAME coordinator events the event
 * processor already handles ({@link CreateTableEvent} / {@link TableRegistrationChangeEvent} /
 * {@link SchemaChangeEvent} / {@link CreatePartitionEvent}, plus table/partition drops routed
 * through the {@link TableLifecycleThrottler}), but sourced from the in-process apply-watch rather
 * than a ZooKeeper {@code CuratorCache} over the {@code /databases} subtree.
 *
 * <p>Architecture shift (the same as {@link RaftTabletServerChangeWatcher}, but for the richest
 * watcher): on ZooKeeper the coordinator reacts to external {@code CuratorCache} notifications on
 * the databases subtree — including its OWN writes (a {@code CREATE TABLE} RPC writes ZK, its own
 * watch fires, and a {@link CreateTableEvent} is produced). On Raft the coordinator leader is the
 * single writer and reacts to its own committed writes as they are applied: {@link
 * RaftMetadataStore#apply} fires {@link MetadataWatchService} after each mutation, and this watcher
 * registers three key-prefix watches that mirror the three ZK znode kinds under the subtree:
 *
 * <ul>
 *   <li>{@code REGISTER_TABLE} (shared header with {@code DELETE_TABLE}) — a put with no prior
 *       value is a table creation ({@link CreateTableEvent}, reading the table's assignment +
 *       latest schema from the store); a put over an existing value is a properties change ({@link
 *       TableRegistrationChangeEvent}); a delete submits a table drop to the throttler.
 *   <li>{@code REGISTER_SCHEMA} — a put registers a schema version ({@link SchemaChangeEvent}).
 *   <li>{@code REGISTER_PARTITION} (shared header with {@code DELETE_PARTITION}) — a put creates a
 *       partition ({@link CreatePartitionEvent}, reading the partition's assignment); a delete
 *       submits a partition drop to the throttler.
 * </ul>
 *
 * <p>Ordering contract (mirrors ZooKeeper, where the table znode {@code setData} is the last write
 * of a create): the coordinator's create-table write path must append the {@code
 * REGISTER_TABLE}/{@code REGISTER_PARTITION} record LAST (after the assignment and schema records),
 * so that when this watcher reacts to it the assignment and schema are already committed and
 * readable. {@code RaftMetadataClient.registerPartitionAssignmentAndMetadata} already writes the
 * name-keyed registration last.
 *
 * <p>Leader-only by lifecycle: {@link #start() started} on becoming leader and {@link #stop()
 * stopped} on losing leadership, so followers (which also apply records) do not enqueue events —
 * mirroring the ZooKeeper watcher's start/stop-on-(de)election lifecycle. Store reads use the
 * leader-only {@link RaftMetadataClient}, valid because the watcher runs only on the leader.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4 / §5.8.
 */
@Internal
public final class RaftTableChangeWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(RaftTableChangeWatcher.class);

    private final MetadataWatchService watchService;
    private final RaftMetadataClient metadataClient;
    private final EventManager eventManager;
    private final TableLifecycleThrottler lifecycleThrottler;

    private final byte[] tablePrefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
    private final byte[] schemaPrefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_SCHEMA);
    private final byte[] partitionPrefix =
            MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_PARTITION);

    private final MetadataWatchService.WatchCallback tableCallback = this::onTableChange;
    private final MetadataWatchService.WatchCallback schemaCallback = this::onSchemaChange;
    private final MetadataWatchService.WatchCallback partitionCallback = this::onPartitionChange;

    private volatile boolean running;

    public RaftTableChangeWatcher(
            MetadataWatchService watchService,
            RaftMetadataClient metadataClient,
            EventManager eventManager,
            TableLifecycleThrottler lifecycleThrottler) {
        this.watchService = watchService;
        this.metadataClient = metadataClient;
        this.eventManager = eventManager;
        this.lifecycleThrottler = lifecycleThrottler;
    }

    public void start() {
        running = true;
        watchService.registerWatch(tablePrefix, tableCallback);
        watchService.registerWatch(schemaPrefix, schemaCallback);
        watchService.registerWatch(partitionPrefix, partitionCallback);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        LOG.info("Stopping RaftTableChangeWatcher");
        watchService.unregisterWatch(tablePrefix, tableCallback);
        watchService.unregisterWatch(schemaPrefix, schemaCallback);
        watchService.unregisterWatch(partitionPrefix, partitionCallback);
    }

    // ---- REGISTER_TABLE / DELETE_TABLE ----

    private void onTableChange(byte[] key, byte[] oldValue, byte[] newValue) {
        // Table record key body = tablePath ("db.table"), no delimiter.
        TablePath tablePath =
                parseTablePath(
                        new String(
                                key,
                                tablePrefix.length,
                                keyBodyLen(key, tablePrefix),
                                StandardCharsets.UTF_8));
        if (isPut(newValue)) {
            if (isPresent(oldValue)) {
                // put over an existing registration: a properties / schema-version change.
                TableRegistration newTable =
                        JsonSerdeUtils.readValue(newValue, TableRegistrationJsonSerde.INSTANCE);
                eventManager.put(new TableRegistrationChangeEvent(tablePath, newTable));
            } else {
                processCreateTable(tablePath, newValue);
            }
        } else {
            processTableDrop(oldValue);
        }
    }

    private void processCreateTable(TablePath tablePath, byte[] tableValue) {
        TableRegistration table =
                JsonSerdeUtils.readValue(tableValue, TableRegistrationJsonSerde.INSTANCE);
        TableAssignment assignment;
        if (table.isPartitioned()) {
            // partitioned tables have no table-level assignment (only per-partition assignments).
            assignment = TableAssignment.builder().build();
        } else {
            Optional<TableAssignment> optAssignment =
                    metadataClient.getTableAssignment(table.tableId);
            if (!optAssignment.isPresent()) {
                LOG.error("No assignments for table {} in the metadata store.", tablePath);
                return;
            }
            assignment = optAssignment.get();
        }
        int schemaId = metadataClient.getCurrentSchemaId(tablePath);
        Optional<SchemaInfo> optSchema = metadataClient.getSchemaById(tablePath, schemaId);
        if (!optSchema.isPresent()) {
            LOG.error("No schema for table {} in the metadata store.", tablePath);
            return;
        }
        TableInfo tableInfo = table.toTableInfo(tablePath, optSchema.get());
        eventManager.put(new CreateTableEvent(tableInfo, assignment));
    }

    private void processTableDrop(byte[] oldValue) {
        if (!isPresent(oldValue)) {
            return;
        }
        TableRegistration table =
                JsonSerdeUtils.readValue(oldValue, TableRegistrationJsonSerde.INSTANCE);
        TableConfig tableConfig = new TableConfig(Configuration.fromMap(table.properties));
        // Submit to the throttler (rather than a direct DropTableEvent) so a cascading drop cannot
        // starve unrelated coordinator work — identical to the ZK watcher's NODE_DELETED path.
        lifecycleThrottler.submitTableDrop(
                table.tableId,
                table.isPartitioned(),
                tableConfig.getAutoPartitionStrategy().isAutoPartitionEnabled(),
                tableConfig.isDataLakeEnabled());
    }

    // ---- REGISTER_SCHEMA ----

    private void onSchemaChange(byte[] key, byte[] oldValue, byte[] newValue) {
        // DELETE_SCHEMA fires this watch with a null newValue; drop-table cascade needs no event.
        if (!isPut(newValue)) {
            return;
        }
        // Schema record key body = tablePath + 0x00 + 4-byte BE schemaId.
        int delim = delimIndex(key, schemaPrefix.length);
        TablePath tablePath =
                parseTablePath(
                        new String(
                                key,
                                schemaPrefix.length,
                                delim - schemaPrefix.length,
                                StandardCharsets.UTF_8));
        int schemaId = ByteBuffer.wrap(key, delim + 1, 4).getInt();
        Optional<SchemaInfo> optSchema = metadataClient.getSchemaById(tablePath, schemaId);
        if (!optSchema.isPresent()) {
            LOG.error("No schema for table {} in the metadata store.", tablePath);
            return;
        }
        eventManager.put(new SchemaChangeEvent(tablePath, optSchema.get()));
    }

    // ---- REGISTER_PARTITION / DELETE_PARTITION ----

    private void onPartitionChange(byte[] key, byte[] oldValue, byte[] newValue) {
        // Partition record key body = tablePath + 0x00 + partitionName.
        int delim = delimIndex(key, partitionPrefix.length);
        TablePath tablePath =
                parseTablePath(
                        new String(
                                key,
                                partitionPrefix.length,
                                delim - partitionPrefix.length,
                                StandardCharsets.UTF_8));
        String partitionName =
                new String(key, delim + 1, key.length - (delim + 1), StandardCharsets.UTF_8);
        if (isPut(newValue)) {
            processCreatePartition(tablePath, partitionName, newValue);
        } else {
            processPartitionDrop(partitionName, oldValue);
        }
    }

    private void processCreatePartition(
            TablePath tablePath, String partitionName, byte[] partitionValue) {
        PartitionRegistration partition =
                JsonSerdeUtils.readValue(partitionValue, PartitionRegistrationJsonSerde.INSTANCE);
        Optional<PartitionAssignment> optAssignment =
                metadataClient.getPartitionAssignment(partition.getPartitionId());
        if (!optAssignment.isPresent()) {
            LOG.error(
                    "No assignments for partition {} of table {} in the metadata store.",
                    partitionName,
                    tablePath);
            return;
        }
        eventManager.put(
                new CreatePartitionEvent(
                        tablePath,
                        partition.getTableId(),
                        partition.getPartitionId(),
                        partitionName,
                        optAssignment.get()));
    }

    private void processPartitionDrop(String partitionName, byte[] oldValue) {
        if (!isPresent(oldValue)) {
            return;
        }
        PartitionRegistration partition =
                JsonSerdeUtils.readValue(oldValue, PartitionRegistrationJsonSerde.INSTANCE);
        // Submit to the throttler (rather than a direct DropPartitionEvent) so a cascading drop
        // cannot starve unrelated coordinator work — identical to the ZK watcher's NODE_DELETED
        // path.
        lifecycleThrottler.submitPartitionDrop(
                partition.getTableId(), partition.getPartitionId(), partitionName);
    }

    // ---- key helpers ----

    /** Parses a {@code "database.table"} logical key into a {@link TablePath}. */
    private static TablePath parseTablePath(String dbDotTable) {
        // Database and table names are ASCII alphanumerics, '_' and '-' only (no dots), so the
        // single '.' separator is unambiguous.
        int dot = dbDotTable.indexOf('.');
        return TablePath.of(dbDotTable.substring(0, dot), dbDotTable.substring(dot + 1));
    }

    /** Index of the first {@code 0x00} delimiter in {@code key} at or after {@code from}. */
    private static int delimIndex(byte[] key, int from) {
        for (int i = from; i < key.length; i++) {
            if (key[i] == 0x00) {
                return i;
            }
        }
        throw new IllegalStateException("No 0x00 delimiter found in metadata key");
    }

    private static int keyBodyLen(byte[] key, byte[] prefix) {
        return key.length - prefix.length;
    }

    private static boolean isPut(byte[] newValue) {
        return newValue != null && newValue.length > 0;
    }

    private static boolean isPresent(byte[] value) {
        return value != null && value.length > 0;
    }
}
