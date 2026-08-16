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
import org.apache.fluss.cluster.rebalance.ServerTag;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.ServerInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Projects the server-topology view of a {@link CoordinatorMetadataCache} from a follower {@link
 * RaftMetadataStore} kept in sync by a {@link MetadataStreamSubscriber} (§5.8). This is the
 * cross-process replacement for the ZooKeeper-watch path that used to feed the cache: instead of
 * reacting to ZK child-watches, a subscriber replays the metadata stream into the local store and
 * this applier re-derives the alive-server set from that store.
 *
 * <p>It reads the store directly (no leader guard) via {@link RaftServerTopologyReader}: the
 * follower store is an exact replica of the leader's committed state, so the projection a follower
 * produces equals what the leader would.
 *
 * <p>Scope (first T6-2 slice): it projects the server topology consumed by {@link
 * CoordinatorMetadataCache} — alive tablet servers (from {@code REGISTER_TABLET_SERVER}, which
 * {@code FENCE_TABLET_SERVER} deletes) and the coordinator leader (from {@code
 * REGISTER_COORDINATOR_LEADER}). Server tags are not yet streamed, so they project as empty (a
 * rebalance-only concern; a dedicated record type is a follow-up). The richer {@code
 * TabletServerMetadataCache} (tables/buckets) projection is handled by {@link
 * TabletServerMetadataStreamCacheApplier}.
 *
 * <p>Thread-safety: {@link #refresh()} may be called from the single subscriber thread after each
 * applied batch; the underlying {@link CoordinatorMetadataCache#updateMetadata} swaps an immutable
 * snapshot under its own lock, so concurrent cache reads are safe.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class MetadataStreamCacheApplier {

    private final RaftMetadataStore store;
    private final CoordinatorMetadataCache cache;

    public MetadataStreamCacheApplier(RaftMetadataStore store, CoordinatorMetadataCache cache) {
        this.store = store;
        this.cache = cache;
    }

    /**
     * Re-derives the cache's server topology from the current local store and installs it. Cheap
     * and idempotent; intended to run after each {@link MetadataStreamSubscriber#pollOnce()} batch.
     */
    public void refresh() {
        Set<ServerInfo> tabletServers = RaftServerTopologyReader.readTabletServers(store);
        ServerInfo coordinator = RaftServerTopologyReader.readCoordinator(store);
        Map<Integer, ServerTag> serverTags = Collections.emptyMap();
        cache.updateMetadata(coordinator, tabletServers, serverTags);
    }
}
