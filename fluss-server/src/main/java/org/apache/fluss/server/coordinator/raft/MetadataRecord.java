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

import javax.annotation.Nullable;

/**
 * A single replicated metadata mutation. Serialized into the Ratis log via {@link #toBytes()} and
 * re-applied deterministically on every Coordinator by {@code
 * FlussMetadataStateMachine#applyEntry}.
 *
 * <p>Analogous to a KRaft {@code ApiMessageAndVersion} record. Immutable; carried inside a Ratis
 * {@code Message}.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.3.
 */
@Internal
public final class MetadataRecord {

    /** Record type. One enum value per former ZK write path. */
    public enum Type {
        // --- metadata CRUD (former /metadata/** znodes) ---
        REGISTER_DATABASE,
        DELETE_DATABASE,
        REGISTER_TABLE,
        DELETE_TABLE,
        REGISTER_SCHEMA,
        REGISTER_PARTITION,
        DELETE_PARTITION,
        // --- assignments (former /tabletservers/{tables,partitions}/**) ---
        UPDATE_TABLE_ASSIGNMENT,
        UPDATE_PARTITION_ASSIGNMENT,
        DELETE_TABLE_ASSIGNMENT,
        DELETE_PARTITION_ASSIGNMENT,
        // --- table state (former .../buckets/[id]/leader_isr) ---
        UPDATE_LEADER_AND_ISR,
        // --- remote log manifest (former .../buckets/[id]/remote_logs) ---
        UPSERT_REMOTE_LOG_MANIFEST,
        DELETE_REMOTE_LOG_MANIFEST,
        // --- kv bucket snapshot (former .../buckets/[id]/snapshots/[snapshotId]) ---
        REGISTER_BUCKET_SNAPSHOT,
        DELETE_BUCKET_SNAPSHOT,
        // --- producer offsets (former /producers/[producerId]) ---
        REGISTER_PRODUCER_OFFSETS,
        DELETE_PRODUCER_OFFSETS,
        // --- lake table (former /metadata/.../lake_table/[tableId]) ---
        UPSERT_LAKE_TABLE,
        // --- dynamic entity config (former /config/server/global), single global key ---
        UPSERT_ENTITY_CONFIG,
        // --- rebalance task (former /cluster/rebalance), single global key ---
        UPSERT_REBALANCE_TASK,
        // --- resource acl (former /acl/[resourceType]/[resourceName]) ---
        UPSERT_RESOURCE_ACL,
        DELETE_RESOURCE_ACL,
        // --- server membership (former ephemeral /tabletservers/ids/**) ---
        REGISTER_TABLET_SERVER,
        FENCE_TABLET_SERVER,
        // --- id generation (former DistributedAtomicLong counters) ---
        ALLOCATE_ID_RANGE,
        // --- dynamic config / acl / lease / producer ---
        UPDATE_DYNAMIC_CONFIG,
        UPDATE_ACL,
        UPDATE_KV_SNAPSHOT_LEASE,
        DELETE_KV_SNAPSHOT_LEASE,
        UPDATE_PRODUCER_SNAPSHOT,
        // --- coordinator leader (former /coordinators/leader) ---
        REGISTER_COORDINATOR_LEADER,
        DELETE_COORDINATOR_LEADER,
        // --- server tags (former /metadata/server_tags), single global key ---
        UPSERT_SERVER_TAGS,
        DELETE_SERVER_TAGS,
        // Append-only: new types MUST be added at the end (codec uses enum ordinal).
        DELETE_LEADER_AND_ISR,
        /** Cluster {@code metadata.version} feature gate (§5.11). Singleton key. */
        UPDATE_METADATA_VERSION,
        /** Delete the singleton rebalance task (shares UPSERT_REBALANCE_TASK key). */
        DELETE_REBALANCE_TASK,
        /**
         * Delete a schema record (shares {@link #REGISTER_SCHEMA}'s key). Required so {@code
         * deleteTable} can cascade schemas the way ZooKeeper's {@code deletingChildrenIfNeeded}
         * removes {@code table_path/schemas/*}.
         */
        DELETE_SCHEMA
    }

    private final Type type;
    private final byte[] payload; // reuse existing *JsonSerde (e.g. TableRegistrationJsonSerde)
    @Nullable private final byte[] key; // logical key, e.g. tablePath / tableBucket bytes

    public MetadataRecord(Type type, @Nullable byte[] key, byte[] payload) {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        this.type = type;
        this.key = key;
        this.payload = payload;
    }

    public Type type() {
        return type;
    }

    @Nullable
    public byte[] key() {
        return key;
    }

    public byte[] payload() {
        return payload;
    }

    /** {@code [1B version][2B type ordinal][4B keyLen][key][4B payloadLen][payload]}. */
    public byte[] toBytes() {
        return MetadataRecordCodec.encode(this);
    }

    public static MetadataRecord fromBytes(byte[] bytes) {
        return MetadataRecordCodec.decode(bytes);
    }
}
