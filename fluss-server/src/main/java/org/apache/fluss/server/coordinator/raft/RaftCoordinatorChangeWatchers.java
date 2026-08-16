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
import org.apache.fluss.server.coordinator.TableLifecycleThrottler;
import org.apache.fluss.server.coordinator.event.EventManager;

/**
 * The Raft-backend replacement for the three ZooKeeper {@code CuratorCache} watchers the {@code
 * CoordinatorEventProcessor} runs on the leader (§5.8, route-1 (c)). Assembles the three
 * apply-watch watchers on the owning {@link RaftMetadataClient}'s {@link MetadataWatchService} and
 * exposes a single {@link #start()}/{@link #stop()} lifecycle so the event processor can toggle
 * them on become-leader / lose-leadership exactly as it toggles the ZooKeeper watchers.
 *
 * <p>Encapsulating the assembly here (rather than in the {@code
 * org.apache.fluss.server.coordinator} package) keeps the {@link RaftMetadataClient#watchService()}
 * accessor package-private — the coordinator only needs this façade, mirroring the {@code
 * RaftCoordinatorMetadataServices} seam.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4 / §5.8.
 */
@Internal
public final class RaftCoordinatorChangeWatchers {

    private final RaftCoordinatorChangeWatcher coordinatorWatcher;
    private final RaftTabletServerChangeWatcher tabletServerWatcher;
    private final RaftTableChangeWatcher tableWatcher;

    public RaftCoordinatorChangeWatchers(
            RaftMetadataClient metadataClient,
            EventManager eventManager,
            TableLifecycleThrottler lifecycleThrottler) {
        MetadataWatchService watchService = metadataClient.watchService();
        this.coordinatorWatcher = new RaftCoordinatorChangeWatcher(watchService, eventManager);
        this.tabletServerWatcher = new RaftTabletServerChangeWatcher(watchService, eventManager);
        this.tableWatcher =
                new RaftTableChangeWatcher(
                        watchService, metadataClient, eventManager, lifecycleThrottler);
    }

    /**
     * Registers all three apply-watches. Called when the coordinator becomes leader (mirrors the
     * ZooKeeper watchers' start order: coordinator, tablet-server, then table).
     */
    public void start() {
        coordinatorWatcher.start();
        tabletServerWatcher.start();
        tableWatcher.start();
    }

    /** Unregisters all three apply-watches. Called when the coordinator loses leadership. */
    public void stop() {
        coordinatorWatcher.stop();
        tableWatcher.stop();
        tabletServerWatcher.stop();
    }
}
