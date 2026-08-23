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

import java.util.List;

/**
 * {@link MetadataKvStore}-backed metadata store. {@link #apply(MetadataRecord)} is the ONLY write
 * path and is called from the single-threaded state-machine apply loop, so no external locking is
 * needed for writes (Ratis serializes {@code applyTransaction}). Reads are lock-free.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.2.
 */
@Internal
public class RaftMetadataStore {

    private final MetadataKvStore kvStore;
    private final MetadataWatchService watchService;

    public RaftMetadataStore(MetadataKvStore kvStore, MetadataWatchService watchService) {
        this.kvStore = kvStore;
        this.watchService = watchService;
    }

    /** The in-process watch service fired from the apply loop after each committed mutation. */
    public MetadataWatchService watchService() {
        return watchService;
    }

    /**
     * Deterministic mutation. Dispatch is a plain switch over record type.
     *
     * <p>The {@code oldValue} pre-read exists only to feed {@link MetadataWatchService} callbacks,
     * so it is skipped when no watch matches this key (analysis §8.9.4 C1). Only three watchers
     * register today (table/schema/partition, tablet-server, coordinator), so most record types
     * avoid an extra KvStore lookup per applied entry — and apply latency is charged to every
     * writer through {@code awaitApplied} (§4.3).
     */
    public void apply(MetadataRecord record) {
        byte[] key = MetadataKeys.keyOf(record);
        boolean watched = watchService.hasWatch(key);
        byte[] oldValue = watched ? kvStore.get(key) : null;
        switch (record.type()) {
                // Removal types: delete the key. (FENCE_TABLET_SERVER removes a membership key.)
            case DELETE_DATABASE:
            case DELETE_TABLE:
            case DELETE_SCHEMA:
            case DELETE_PARTITION:
            case DELETE_TABLE_ASSIGNMENT:
            case DELETE_PARTITION_ASSIGNMENT:
            case DELETE_LEADER_AND_ISR:
            case DELETE_KV_SNAPSHOT_LEASE:
            case DELETE_REMOTE_LOG_MANIFEST:
            case DELETE_BUCKET_SNAPSHOT:
            case DELETE_PRODUCER_OFFSETS:
            case DELETE_RESOURCE_ACL:
            case FENCE_TABLET_SERVER:
            case DELETE_COORDINATOR_LEADER:
            case DELETE_SERVER_TAGS:
            case DELETE_REBALANCE_TASK:
                kvStore.delete(key);
                if (watched) {
                    watchService.fireWatch(key, oldValue, null);
                }
                break;
                // All other types (REGISTER_*, UPDATE_*, ALLOCATE_ID_RANGE) are plain key puts.
            default:
                kvStore.put(key, record.payload());
                if (watched) {
                    watchService.fireWatch(key, oldValue, record.payload());
                }
                break;
        }
    }

    // ---- read view (used by MetadataManager / ServerMetadataCache) ----

    @Nullable
    public byte[] get(byte[] key) {
        return kvStore.get(key);
    }

    public List<byte[]> scanPrefix(byte[] prefix) {
        return kvStore.scanPrefix(prefix);
    }

    /**
     * Returns every {@code [key, value]} pair whose key starts with {@code prefix}. Unlike {@link
     * #scanPrefix} (values only) this also exposes the keys, so callers can recover the logical key
     * body encoded after the {@link MetadataKeys} header — e.g. enumerating registered
     * tablet-server ids for {@code getSortedTabletServerList} (§5.5.3). Delegates to the KvStore
     * prefix iterator (seek + stop); does not scan the full store.
     */
    public List<byte[][]> scanPrefixEntries(byte[] prefix) {
        return kvStore.scanPrefixEntries(prefix);
    }

    // ---- snapshot support (§5.2.5) ----

    /** Returns every stored entry as a {@code [key, value]} pair, for snapshot serialization. */
    public List<byte[][]> snapshotEntries() {
        return kvStore.entries();
    }

    /**
     * Replaces the entire store contents with {@code entries} (clears first). Used by {@code
     * FlussMetadataStateMachine.load} when restoring from a snapshot. Does NOT fire watches:
     * snapshot bootstrap is observed through the metadata stream (§5.8), not in-process watches.
     */
    public void restoreFromSnapshot(List<byte[][]> entries) {
        kvStore.clear();
        for (byte[][] entry : entries) {
            kvStore.put(entry[0], entry[1]);
        }
    }

    /** Releases the backing {@link MetadataKvStore} (RocksDB or in-memory). */
    public void close() {
        kvStore.close();
    }
}
