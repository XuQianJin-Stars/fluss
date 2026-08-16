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
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.zk.data.ResourceAcl;
import org.apache.fluss.server.zk.data.ResourceAclJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Propagates resource-ACL changes to a {@code DefaultAuthorizer} from a follower {@link
 * RaftMetadataStore} kept in sync by a {@link MetadataStreamSubscriber} (§5.8). This is the
 * cross-process replacement for the ZooKeeper {@code aclChangeNotificationWatcher} that used to
 * reload ACLs on every node: instead of reacting to ZK sequential change-notification nodes, a
 * subscriber replays the metadata stream into the local store (an {@code UPSERT_RESOURCE_ACL} /
 * {@code DELETE_RESOURCE_ACL} record IS the notification) and this notifier re-derives the full ACL
 * snapshot from that store and hands it to the authorizer to reconcile.
 *
 * <p>It reads the store directly (no leader guard): the follower store is an exact replica of the
 * leader's committed state, so the snapshot a follower produces equals the leader's.
 *
 * <p>A full re-derive per batch (rather than per-record deltas) mirrors {@link
 * MetadataStreamCacheApplier#refresh()} — it is idempotent and robust, and ACLs are low-volume, so
 * a full scan after each applied batch is cheap. Deletions are reflected naturally: {@code
 * DELETE_RESOURCE_ACL} removes the key from the store, so the resource is simply absent from the
 * re-derived snapshot.
 *
 * <p>Thread-safety: {@link #refresh()} may be called from the single subscriber thread after each
 * applied batch; the authorizer reconciles under its own lock.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class AclChangeStreamNotifier {

    /** Callback the authorizer implements to reconcile its ACL cache to a full stream snapshot. */
    public interface AclReloadListener {

        /**
         * Reconciles the authorizer's ACL cache to exactly {@code snapshot}: resources present are
         * upserted, resources absent (deleted upstream) are dropped.
         */
        void onAclSnapshot(Map<Resource, VersionedAcls> snapshot);
    }

    private final RaftMetadataStore store;
    private final AclReloadListener listener;

    public AclChangeStreamNotifier(RaftMetadataStore store, AclReloadListener listener) {
        this.store = store;
        this.listener = listener;
    }

    /**
     * Re-derives the full resource-ACL snapshot from the current local store and hands it to the
     * authorizer to reconcile. Cheap and idempotent; intended to run after each {@link
     * MetadataStreamSubscriber#pollOnce()} batch.
     */
    public void refresh() {
        listener.onAclSnapshot(readAllAcls());
    }

    /**
     * Scans every {@code UPSERT_RESOURCE_ACL} record into a {@code resource -> VersionedAcls} map.
     */
    private Map<Resource, VersionedAcls> readAllAcls() {
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.UPSERT_RESOURCE_ACL);
        Map<Resource, VersionedAcls> snapshot = new HashMap<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            byte[] key = entry[0];
            // key body = 1-byte resource-type code + UTF-8 resource name (see RaftMetadataClient).
            ResourceType resourceType = ResourceType.fromCode(key[prefix.length]);
            String name =
                    new String(
                            key,
                            prefix.length + 1,
                            key.length - prefix.length - 1,
                            java.nio.charset.StandardCharsets.UTF_8);
            Resource resource = new Resource(resourceType, name);
            ResourceAcl acl = JsonSerdeUtils.readValue(entry[1], ResourceAclJsonSerde.INSTANCE);
            // Version is always 0 on the Raft backend when present (per-record CAS subsumed).
            snapshot.put(resource, new VersionedAcls(0, acl.getEntries()));
        }
        return snapshot;
    }
}
