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
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.CoordinatorAddressJsonSerde;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistrationJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Stateless reads that derive the cluster's SERVER TOPOLOGY (alive tablet servers + the current
 * coordinator leader) directly from a follower {@link RaftMetadataStore} kept in sync by a {@link
 * MetadataStreamSubscriber} (§5.8). No leader guard: the follower store is an exact replica of the
 * leader's committed state, so a follower derives the same topology the leader would.
 *
 * <p>Shared by every stream-fed cache applier ({@link MetadataStreamCacheApplier} for the
 * coordinator cache, {@link TabletServerMetadataStreamCacheApplier} for the tablet-server cache) so
 * the record key/value decoding lives in one place.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
final class RaftServerTopologyReader {

    /**
     * The metadata-cache node id for the (singleton) coordinator server. The coordinator's own
     * {@code serverId} is a UUID (leader-election / fencing identity), not an int; the server
     * metadata cache — on both backends — represents the coordinator node with this fixed sentinel
     * id (see {@code CoordinatorEventProcessor.getCoordinatorServerInfo}, which uses {@code 0} on
     * the ZooKeeper path). There is exactly one coordinator node, so a constant id is sufficient
     * and keeps the Raft-derived topology identical to the ZooKeeper one.
     */
    private static final int COORDINATOR_SERVER_ID = 0;

    private RaftServerTopologyReader() {}

    /** Reads every {@code REGISTER_TABLET_SERVER} record into a {@link ServerInfo} set. */
    static Set<ServerInfo> readTabletServers(RaftMetadataStore store) {
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLET_SERVER);
        Set<ServerInfo> servers = new LinkedHashSet<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            int serverId = ByteBuffer.wrap(entry[0], prefix.length, 4).getInt();
            TabletServerRegistration reg =
                    JsonSerdeUtils.readValue(entry[1], TabletServerRegistrationJsonSerde.INSTANCE);
            servers.add(
                    new ServerInfo(
                            serverId, reg.getRack(), reg.getEndpoints(), ServerType.TABLET_SERVER));
        }
        return servers;
    }

    /**
     * Reads the singleton {@code REGISTER_COORDINATOR_LEADER} record into a {@link ServerInfo}
     * keyed by the {@link #COORDINATOR_SERVER_ID} sentinel, or {@code null} when absent. The stored
     * {@link CoordinatorAddress#getId()} is the coordinator's UUID and is intentionally NOT used as
     * the cache node id (mirroring the ZooKeeper path).
     */
    @Nullable
    static ServerInfo readCoordinator(RaftMetadataStore store) {
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_COORDINATOR_LEADER);
        List<byte[][]> entries = store.scanPrefixEntries(prefix);
        if (entries.isEmpty()) {
            return null;
        }
        CoordinatorAddress address =
                JsonSerdeUtils.readValue(entries.get(0)[1], CoordinatorAddressJsonSerde.INSTANCE);
        return new ServerInfo(
                COORDINATOR_SERVER_ID, null, address.getEndpoints(), ServerType.COORDINATOR);
    }
}
