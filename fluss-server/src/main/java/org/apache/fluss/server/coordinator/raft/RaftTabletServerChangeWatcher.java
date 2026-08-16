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
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.metadata.ServerInfo;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistrationJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import java.nio.ByteBuffer;

/**
 * Raft-backed replacement for the ZooKeeper {@code TabletServerChangeWatcher} (§5.8, route-1 (c)):
 * translates tablet-server membership changes into the SAME coordinator events ({@link
 * NewTabletServerEvent} / {@link DeadTabletServerEvent}) the event processor already handles, but
 * sourced from the in-process apply-watch instead of a ZooKeeper {@code CuratorCache}.
 *
 * <p>Architecture shift: on ZooKeeper the coordinator reacts to external watch notifications on
 * {@code /tabletservers/ids}; on Raft the coordinator leader is the single writer and reacts to its
 * OWN committed writes as they are applied. {@link RaftMetadataStore#apply} fires {@link
 * MetadataWatchService} after each mutation, and this watcher — registered for the {@code
 * REGISTER_TABLET_SERVER} key prefix (which {@code FENCE_TABLET_SERVER} shares) — maps the change
 * to an event: a put (register) -> {@link NewTabletServerEvent}, a delete (fence) -> {@link
 * DeadTabletServerEvent}.
 *
 * <p>Re-REGISTER overwrite (both {@code oldValue} and {@code newValue} non-null) is treated as a
 * <em>bounce</em>: the tablet process died and re-registered before the heartbeat lease expired, so
 * no {@code FENCE_TABLET_SERVER} ran. ZooKeeper gets delete+create from the ephemeral node; Raft
 * only sees a put-overwrite. Emitting {@link DeadTabletServerEvent} then {@link
 * NewTabletServerEvent} mirrors that so {@code processNewTabletServer} re-runs OnlineReplica /
 * NotifyLeaderAndIsr / LogTiering on the new process instead of early-returning on an already-live
 * id.
 *
 * <p>Leader-only by lifecycle: it is {@link #start() started} when the coordinator becomes leader
 * and {@link #stop() stopped} when leadership is lost, so followers (which also apply records) do
 * not enqueue events. Mirrors the ZooKeeper watcher's start/stop-on-(de)election lifecycle.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4 / §5.8.
 */
@Internal
public final class RaftTabletServerChangeWatcher {

    private final MetadataWatchService watchService;
    private final EventManager eventManager;
    private final byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLET_SERVER);
    private final MetadataWatchService.WatchCallback callback = this::onChange;

    private volatile boolean running;

    public RaftTabletServerChangeWatcher(
            MetadataWatchService watchService, EventManager eventManager) {
        this.watchService = watchService;
        this.eventManager = eventManager;
    }

    public void start() {
        running = true;
        watchService.registerWatch(prefix, callback);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        watchService.unregisterWatch(prefix, callback);
    }

    private void onChange(byte[] key, byte[] oldValue, byte[] newValue) {
        int serverId = ByteBuffer.wrap(key, prefix.length, 4).getInt();
        if (newValue != null && newValue.length > 0) {
            // Bounce: re-REGISTER overwrote membership without a preceding FENCE.
            if (oldValue != null && oldValue.length > 0) {
                eventManager.put(new DeadTabletServerEvent(serverId));
            }
            TabletServerRegistration registration =
                    JsonSerdeUtils.readValue(newValue, TabletServerRegistrationJsonSerde.INSTANCE);
            eventManager.put(
                    new NewTabletServerEvent(
                            new ServerInfo(
                                    serverId,
                                    registration.getRack(),
                                    registration.getEndpoints(),
                                    ServerType.TABLET_SERVER)));
        } else {
            eventManager.put(new DeadTabletServerEvent(serverId));
        }
    }
}
