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

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.DeadTabletServerEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NewTabletServerEvent;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistrationJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RaftTabletServerChangeWatcher}, including bounce (re-REGISTER overwrite)
 * detection.
 */
class RaftTabletServerChangeWatcherTest {

    @Test
    void testFirstRegisterEmitsNewOnly() {
        RecordingEventManager events = new RecordingEventManager();
        MetadataWatchService watchService = new MetadataWatchService();
        RaftTabletServerChangeWatcher watcher =
                new RaftTabletServerChangeWatcher(watchService, events);
        watcher.start();

        byte[] key =
                MetadataKeys.keyOf(
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLET_SERVER,
                                intToBytes(7),
                                new byte[0]));
        byte[] payload = registrationPayload(1_000L);
        watchService.fireWatch(key, null, payload);

        assertThat(events.events).hasSize(1);
        assertThat(events.events.get(0)).isInstanceOf(NewTabletServerEvent.class);
        assertThat(((NewTabletServerEvent) events.events.get(0)).getServerInfo().id()).isEqualTo(7);
    }

    @Test
    void testFenceEmitsDead() {
        RecordingEventManager events = new RecordingEventManager();
        MetadataWatchService watchService = new MetadataWatchService();
        RaftTabletServerChangeWatcher watcher =
                new RaftTabletServerChangeWatcher(watchService, events);
        watcher.start();

        byte[] key =
                MetadataKeys.keyOf(
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLET_SERVER,
                                intToBytes(3),
                                new byte[0]));
        watchService.fireWatch(key, registrationPayload(1L), null);

        assertThat(events.events).hasSize(1);
        assertThat(events.events.get(0)).isInstanceOf(DeadTabletServerEvent.class);
        assertThat(((DeadTabletServerEvent) events.events.get(0)).getServerId()).isEqualTo(3);
    }

    @Test
    void testReRegisterOverwriteEmitsDeadThenNew() {
        RecordingEventManager events = new RecordingEventManager();
        MetadataWatchService watchService = new MetadataWatchService();
        RaftTabletServerChangeWatcher watcher =
                new RaftTabletServerChangeWatcher(watchService, events);
        watcher.start();

        byte[] key =
                MetadataKeys.keyOf(
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLET_SERVER,
                                intToBytes(2),
                                new byte[0]));
        byte[] oldPayload = registrationPayload(1_000L);
        byte[] newPayload = registrationPayload(2_000L);
        watchService.fireWatch(key, oldPayload, newPayload);

        assertThat(events.events).hasSize(2);
        assertThat(events.events.get(0)).isInstanceOf(DeadTabletServerEvent.class);
        assertThat(((DeadTabletServerEvent) events.events.get(0)).getServerId()).isEqualTo(2);
        assertThat(events.events.get(1)).isInstanceOf(NewTabletServerEvent.class);
        assertThat(((NewTabletServerEvent) events.events.get(1)).getServerInfo().id()).isEqualTo(2);
    }

    private static byte[] registrationPayload(long registerTimestamp) {
        TabletServerRegistration registration =
                new TabletServerRegistration(
                        "rack-0",
                        Collections.singletonList(new Endpoint("127.0.0.1", 9123, "FLUSS")),
                        registerTimestamp);
        return JsonSerdeUtils.writeValueAsBytes(
                registration, TabletServerRegistrationJsonSerde.INSTANCE);
    }

    private static byte[] intToBytes(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }

    private static final class RecordingEventManager implements EventManager {
        private final List<CoordinatorEvent> events = new ArrayList<>();

        @Override
        public void put(CoordinatorEvent event) {
            events.add(event);
        }
    }
}
