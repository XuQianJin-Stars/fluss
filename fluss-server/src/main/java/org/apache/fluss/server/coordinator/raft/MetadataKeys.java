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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.Map;

/**
 * Maps a {@link MetadataRecord} to a {@link MetadataKvStore} key using the uniform fixed 3-byte
 * header {@code [1B namespace][2B type]} followed by the record's logical key body.
 *
 * <p>The header keeps the logical layout aligned with the former ZK znode structure (namespace =
 * top-level category, type = entity/resource discriminator within the namespace), so the migration
 * tool (§6.3) is a mechanical path-rewrite. Because every record {@link MetadataRecord.Type} maps
 * to a distinct header, keys are collision-free across entity types; instances are disambiguated by
 * the length-prefixed body (the record's logical key).
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.1.
 */
@Internal
public final class MetadataKeys {

    // Namespaces (1 byte).
    private static final byte NS_METADATA = 0x01;
    private static final byte NS_ASSIGNMENT = 0x02;
    private static final byte NS_LEADER_ISR = 0x03;
    private static final byte NS_CONFIG = 0x04;
    private static final byte NS_ACL = 0x05;
    private static final byte NS_KV_LEASE = 0x06;
    private static final byte NS_PRODUCER = 0x07;
    private static final byte NS_ID_COUNTER = 0x08;
    private static final byte NS_SERVER = 0x09;
    private static final byte NS_COORDINATOR = 0x0A;
    private static final byte NS_REMOTE_LOG = 0x0B;
    private static final byte NS_LAKE_TABLE = 0x0C;
    private static final byte NS_BUCKET_SNAPSHOT = 0x0D;
    private static final byte NS_PRODUCER_OFFSETS = 0x0E;
    private static final byte NS_ENTITY_CONFIG = 0x0F;
    private static final byte NS_RESOURCE_ACL = 0x10;
    private static final byte NS_REBALANCE_TASK = 0x11;
    private static final byte NS_SERVER_TAGS = 0x12;
    private static final byte NS_METADATA_VERSION = 0x13;

    private static final int HEADER_LEN = 3;

    /** Per-type header {@code [1B namespace][2B type]}. */
    private static final Map<MetadataRecord.Type, byte[]> HEADERS =
            new EnumMap<>(MetadataRecord.Type.class);

    static {
        put(MetadataRecord.Type.REGISTER_DATABASE, NS_METADATA, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_DATABASE, NS_METADATA, (short) 0x0001);
        put(MetadataRecord.Type.REGISTER_TABLE, NS_METADATA, (short) 0x0002);
        put(MetadataRecord.Type.DELETE_TABLE, NS_METADATA, (short) 0x0002);
        put(MetadataRecord.Type.REGISTER_SCHEMA, NS_METADATA, (short) 0x0003);
        // Delete shares REGISTER_SCHEMA's discriminator so it removes the same per-(path,id) key.
        put(MetadataRecord.Type.DELETE_SCHEMA, NS_METADATA, (short) 0x0003);
        put(MetadataRecord.Type.REGISTER_PARTITION, NS_METADATA, (short) 0x0004);
        put(MetadataRecord.Type.DELETE_PARTITION, NS_METADATA, (short) 0x0004);
        put(MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT, NS_ASSIGNMENT, (short) 0x0001);
        put(MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT, NS_ASSIGNMENT, (short) 0x0002);
        // Delete shares its UPDATE counterpart's discriminator so it removes the same key.
        put(MetadataRecord.Type.DELETE_TABLE_ASSIGNMENT, NS_ASSIGNMENT, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_PARTITION_ASSIGNMENT, NS_ASSIGNMENT, (short) 0x0002);
        put(MetadataRecord.Type.UPDATE_LEADER_AND_ISR, NS_LEADER_ISR, (short) 0x0001);
        // Delete shares UPDATE's discriminator so it removes the same per-bucket key
        // (ZK deleteTableAssignment uses deletingChildrenIfNeeded for leader_isr children).
        put(MetadataRecord.Type.DELETE_LEADER_AND_ISR, NS_LEADER_ISR, (short) 0x0001);
        // Upsert + delete share a discriminator (same per-bucket remote-log-manifest key).
        put(MetadataRecord.Type.UPSERT_REMOTE_LOG_MANIFEST, NS_REMOTE_LOG, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_REMOTE_LOG_MANIFEST, NS_REMOTE_LOG, (short) 0x0001);
        put(MetadataRecord.Type.UPSERT_LAKE_TABLE, NS_LAKE_TABLE, (short) 0x0001);
        put(MetadataRecord.Type.UPSERT_ENTITY_CONFIG, NS_ENTITY_CONFIG, (short) 0x0001);
        put(MetadataRecord.Type.UPSERT_REBALANCE_TASK, NS_REBALANCE_TASK, (short) 0x0001);
        // Upsert + delete share a discriminator (same single global rebalance-task key).
        put(MetadataRecord.Type.DELETE_REBALANCE_TASK, NS_REBALANCE_TASK, (short) 0x0001);
        // Upsert + delete share a discriminator (same single global server-tags key).
        put(MetadataRecord.Type.UPSERT_SERVER_TAGS, NS_SERVER_TAGS, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_SERVER_TAGS, NS_SERVER_TAGS, (short) 0x0001);
        // Upsert + delete share a discriminator (same per-resource acl key).
        put(MetadataRecord.Type.UPSERT_RESOURCE_ACL, NS_RESOURCE_ACL, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_RESOURCE_ACL, NS_RESOURCE_ACL, (short) 0x0001);
        // Register + delete share a discriminator (same per-(bucket,snapshotId) key).
        put(MetadataRecord.Type.REGISTER_BUCKET_SNAPSHOT, NS_BUCKET_SNAPSHOT, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_BUCKET_SNAPSHOT, NS_BUCKET_SNAPSHOT, (short) 0x0001);
        // Register + delete share a discriminator (same per-producerId key).
        put(MetadataRecord.Type.REGISTER_PRODUCER_OFFSETS, NS_PRODUCER_OFFSETS, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_PRODUCER_OFFSETS, NS_PRODUCER_OFFSETS, (short) 0x0001);
        put(MetadataRecord.Type.REGISTER_TABLET_SERVER, NS_SERVER, (short) 0x0001);
        put(MetadataRecord.Type.FENCE_TABLET_SERVER, NS_SERVER, (short) 0x0001);
        put(MetadataRecord.Type.ALLOCATE_ID_RANGE, NS_ID_COUNTER, (short) 0x0001);
        put(MetadataRecord.Type.UPDATE_DYNAMIC_CONFIG, NS_CONFIG, (short) 0x0001);
        put(MetadataRecord.Type.UPDATE_ACL, NS_ACL, (short) 0x0001);
        put(MetadataRecord.Type.UPDATE_KV_SNAPSHOT_LEASE, NS_KV_LEASE, (short) 0x0001);
        // Delete shares the UPDATE discriminator so it removes the same lease key.
        put(MetadataRecord.Type.DELETE_KV_SNAPSHOT_LEASE, NS_KV_LEASE, (short) 0x0001);
        put(MetadataRecord.Type.UPDATE_PRODUCER_SNAPSHOT, NS_PRODUCER, (short) 0x0001);
        // Register + delete share a discriminator (singleton key): the current coordinator leader.
        put(MetadataRecord.Type.REGISTER_COORDINATOR_LEADER, NS_COORDINATOR, (short) 0x0001);
        put(MetadataRecord.Type.DELETE_COORDINATOR_LEADER, NS_COORDINATOR, (short) 0x0001);
        // Singleton cluster metadata.version (§5.11).
        put(MetadataRecord.Type.UPDATE_METADATA_VERSION, NS_METADATA_VERSION, (short) 0x0001);
    }

    private MetadataKeys() {}

    private static void put(MetadataRecord.Type type, byte namespace, short discriminator) {
        byte[] header = new byte[HEADER_LEN];
        header[0] = namespace;
        header[1] = (byte) (discriminator >> 8);
        header[2] = (byte) discriminator;
        HEADERS.put(type, header);
    }

    /** Returns the KvStore key for {@code record}: {@code header(type) + logicalKey}. */
    public static byte[] keyOf(MetadataRecord record) {
        byte[] header = HEADERS.get(record.type());
        if (header == null) {
            throw new IllegalArgumentException("No key mapping for record type: " + record.type());
        }
        byte[] logicalKey = record.key() == null ? new byte[0] : record.key();
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LEN + logicalKey.length);
        buffer.put(header);
        buffer.put(logicalKey);
        return buffer.array();
    }

    /** Returns the header-only prefix for a record type, for prefix scans over an entity kind. */
    public static byte[] prefixOf(MetadataRecord.Type type) {
        byte[] header = HEADERS.get(type);
        if (header == null) {
            throw new IllegalArgumentException("No key mapping for record type: " + type);
        }
        return header.clone();
    }

    /**
     * Returns the KvStore key holding the persisted high-water mark for a sequence-id counter
     * ({@code "table"}/{@code "partition"}/{@code "writer"}). Identical to {@link #keyOf} for an
     * {@link MetadataRecord.Type#ALLOCATE_ID_RANGE} record whose logical key is the counter name,
     * so a value written by {@code RaftMetadataStore.apply} is read back under the same key.
     */
    public static byte[] idCounterKey(String counterName) {
        byte[] header = HEADERS.get(MetadataRecord.Type.ALLOCATE_ID_RANGE);
        byte[] body = counterName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_LEN + body.length);
        buffer.put(header);
        buffer.put(body);
        return buffer.array();
    }
}
