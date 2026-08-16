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
import org.apache.fluss.server.zk.data.ConfigJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Propagates dynamic server-config changes to a {@code DynamicConfigManager} from a follower {@link
 * RaftMetadataStore} kept in sync by a {@link MetadataStreamSubscriber} (§5.8). This is the
 * cross-process replacement for the ZooKeeper {@code ZkNodeChangeNotificationWatcher} that used to
 * notify every node of config changes: instead of reacting to ZK sequential change-notification
 * nodes, a subscriber replays the metadata stream into the local store (an {@code
 * UPSERT_ENTITY_CONFIG} record IS the notification) and this notifier re-reads the full entity
 * config from that store and hands it to the manager to apply.
 *
 * <p>It reads the store directly (no leader guard): the follower store is an exact replica of the
 * leader's committed state, so the config a follower reads equals the leader's. Note it
 * deliberately does NOT go through {@code RaftMetadataClient.fetchEntityConfig()}, which would
 * {@code requireLeader()}-throw on a follower/tablet-server — the whole point is to serve
 * subscribers.
 *
 * <p>A full re-read per batch (rather than per-record deltas) mirrors {@link
 * AclChangeStreamNotifier#refresh()} — it is idempotent and robust, and config is a single
 * low-volume global record. The entity config is stored under one global key ({@code
 * UPSERT_ENTITY_CONFIG} with an empty logical key, see {@code RaftMetadataClient}); an absent key
 * projects to an empty map (parity with the ZooKeeper backend's empty config).
 *
 * <p>On the ZooKeeper backend the {@code isCoordinator} branch short-circuited the notification
 * (the coordinator was the writer and watched its own znode). On Raft this notifier is wired only
 * onto subscriber threads (tablet servers / standby coordinators), never the writing leader, so no
 * such guard is needed.
 *
 * <p>Thread-safety: {@link #refresh()} may be called from the single subscriber thread after each
 * applied batch; the manager applies the update under its own config write-lock.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class ConfigChangeStreamNotifier {

    /** Callback the {@code DynamicConfigManager} implements to apply a full config snapshot. */
    public interface ConfigReloadListener {

        /**
         * Refreshes the dynamic server config to exactly {@code configs} (the streamed entity
         * config). Implementations should not throw — errors are the manager's concern to log so a
         * single bad update does not break the subscriber loop.
         */
        void onConfigSnapshot(Map<String, String> configs);
    }

    private final RaftMetadataStore store;
    private final ConfigReloadListener listener;

    public ConfigChangeStreamNotifier(RaftMetadataStore store, ConfigReloadListener listener) {
        this.store = store;
        this.listener = listener;
    }

    /**
     * Re-reads the entity config from the current local store and hands it to the manager to apply.
     * Cheap and idempotent; intended to run after each {@link MetadataStreamSubscriber#pollOnce()}
     * batch.
     */
    public void refresh() {
        listener.onConfigSnapshot(readEntityConfig());
    }

    /**
     * Reads the singleton {@code UPSERT_ENTITY_CONFIG} record into a {@code key -> value} map
     * (empty when never set). Its logical key is empty, so the full store key equals the
     * record-type header prefix.
     */
    private Map<String, String> readEntityConfig() {
        byte[] key = MetadataKeys.prefixOf(MetadataRecord.Type.UPSERT_ENTITY_CONFIG);
        byte[] value = store.get(key);
        if (value == null) {
            return new HashMap<>();
        }
        return JsonSerdeUtils.readValue(value, ConfigJsonSerde.INSTANCE);
    }
}
