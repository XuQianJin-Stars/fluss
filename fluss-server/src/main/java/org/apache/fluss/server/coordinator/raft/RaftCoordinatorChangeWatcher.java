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
import org.apache.fluss.server.coordinator.event.DeadCoordinatorEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NewCoordinatorEvent;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.CoordinatorAddressJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

/**
 * Raft-backed replacement for the ZooKeeper {@code CoordinatorChangeWatcher} (§5.8, route-1 (c)):
 * translates coordinator-membership changes into the SAME coordinator events the event processor
 * already handles ({@link NewCoordinatorEvent} / {@link DeadCoordinatorEvent}), but sourced from
 * the in-process apply-watch instead of a ZooKeeper {@code CuratorCache} over {@code
 * /coordinators/ids}.
 *
 * <p>Architecture shift (same as {@link RaftTabletServerChangeWatcher}): on ZooKeeper the
 * coordinator reacts to external {@code CuratorCache} notifications on {@code /coordinators/ids};
 * on Raft the coordinator leader is the single writer and reacts to its own committed writes as
 * they are applied. {@link RaftMetadataStore#apply} fires {@link MetadataWatchService} after each
 * mutation, and this watcher — registered for the {@code REGISTER_COORDINATOR_LEADER} singleton key
 * (which {@code DELETE_COORDINATOR_LEADER} shares) — maps the change to an event: a put (register
 * leader) -&gt; {@link NewCoordinatorEvent}, a delete (unregister leader) -&gt; {@link
 * DeadCoordinatorEvent}. The server id carried by both events is the {@link
 * CoordinatorAddress#getId() id} decoded from the record value (on delete, from the old value).
 *
 * <p>Membership-model note: ZooKeeper's {@code /coordinators/ids} held an ephemeral child per
 * coordinator, so its watcher observed ALL coordinators; the Raft {@code
 * REGISTER_COORDINATOR_LEADER} record is the single CURRENT leader (coordinator membership proper
 * is the Raft voter set, Appendix 11.2). The event contract (a {@code serverId} to add/remove) is
 * preserved; enumerating the full coordinator set is a separate concern ({@code
 * getCoordinatorServerList} voter-set wiring), not this watcher's.
 *
 * <p>Leader-only by lifecycle: {@link #start() started} on becoming leader and {@link #stop()
 * stopped} on losing leadership, so followers (which also apply records) do not enqueue events —
 * mirroring the ZooKeeper watcher's start/stop-on-(de)election lifecycle.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4 / §5.8.
 */
@Internal
public final class RaftCoordinatorChangeWatcher {

    private final MetadataWatchService watchService;
    private final EventManager eventManager;
    private final byte[] prefix =
            MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_COORDINATOR_LEADER);
    private final MetadataWatchService.WatchCallback callback = this::onChange;

    private volatile boolean running;

    public RaftCoordinatorChangeWatcher(
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
        if (newValue != null && newValue.length > 0) {
            CoordinatorAddress address =
                    JsonSerdeUtils.readValue(newValue, CoordinatorAddressJsonSerde.INSTANCE);
            eventManager.put(new NewCoordinatorEvent(address.getId()));
        } else if (oldValue != null && oldValue.length > 0) {
            CoordinatorAddress address =
                    JsonSerdeUtils.readValue(oldValue, CoordinatorAddressJsonSerde.INSTANCE);
            eventManager.put(new DeadCoordinatorEvent(address.getId()));
        }
    }
}
