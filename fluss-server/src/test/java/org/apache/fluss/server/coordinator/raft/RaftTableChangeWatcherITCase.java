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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.TableLifecycleThrottler;
import org.apache.fluss.server.coordinator.event.CoordinatorEvent;
import org.apache.fluss.server.coordinator.event.CreatePartitionEvent;
import org.apache.fluss.server.coordinator.event.CreateTableEvent;
import org.apache.fluss.server.coordinator.event.DropPartitionEvent;
import org.apache.fluss.server.coordinator.event.DropTableEvent;
import org.apache.fluss.server.coordinator.event.SchemaChangeEvent;
import org.apache.fluss.server.coordinator.event.TableRegistrationChangeEvent;
import org.apache.fluss.server.coordinator.event.TestingEventManager;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link RaftTableChangeWatcher} (§5.8, route-1 (c)): the coordinator leader's
 * table / schema / partition metadata events are produced from the in-process apply-watch of a real
 * single-node embedded Ratis leader — the cross-backend replacement for the ZooKeeper
 * TableChangeWatcher. Verifies every mapping: table create/registration-change, schema change,
 * partition create, and table/partition drops routed through a real {@link
 * TableLifecycleThrottler}.
 */
class RaftTableChangeWatcherITCase {

    @TempDir private Path raftDir;

    @Test
    void testTableEventsProducedFromApplyWatch() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        MetadataWatchService watchService = new MetadataWatchService();
        RaftMetadataStore leaderStore =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), watchService);
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(leaderStore);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, sm);
        RaftLeaderSelector leaderSelector = new RaftLeaderSelector("n1", server, sm, 20_000);
        sm.setLeaderSelector(leaderSelector);
        leaderSelector.startElectLeaderAsync(() -> {}, t -> {});

        RaftMetadataClient client = null;
        RaftTableChangeWatcher watcher = null;
        TableLifecycleThrottler throttler = null;
        try {
            server.start();
            waitUntilLeaderReady(server, 20_000);

            RaftProperties clientProps = server.buildRaftProperties();
            RaftClient raftClient =
                    RaftClient.newBuilder()
                            .setClientId(ClientId.randomId())
                            .setRaftGroup(group)
                            .setProperties(clientProps)
                            .build();
            RaftJournalWriter writer = new RaftJournalWriter(raftClient, sm);
            client =
                    new RaftMetadataClient(
                            writer,
                            leaderStore,
                            leaderSelector,
                            new RaftSequenceIDCounter("table", writer, leaderStore, 1),
                            new RaftSequenceIDCounter("partition", writer, leaderStore, 1),
                            new RaftSequenceIDCounter("writer", writer, leaderStore, 1));

            TestingEventManager eventManager = new TestingEventManager();
            throttler = new TableLifecycleThrottler(eventManager, SystemClock.getInstance(), conf);
            watcher = new RaftTableChangeWatcher(watchService, client, eventManager, throttler);
            watcher.start();

            TablePath t1 = TablePath.of("db", "t1");
            Schema schemaV1 = Schema.newBuilder().column("id", DataTypes.INT()).build();
            TableDescriptor descriptor =
                    TableDescriptor.builder().schema(schemaV1).distributedBy(3).build();
            TableAssignment ta =
                    TableAssignment.builder().add(0, BucketAssignment.of(1, 2, 3)).build();

            // ---- create non-partitioned table: assignment + schema first, table LAST ----
            client.registerTableAssignment(100L, ta);
            client.registerFirstSchema(t1, schemaV1);
            client.registerTable(
                    t1, TableRegistration.newTable(100L, "file:///tmp/remote", descriptor));
            CreateTableEvent createEvent =
                    (CreateTableEvent) awaitEvent(eventManager, CreateTableEvent.class);
            assertThat(createEvent.getTableInfo().getTablePath()).isEqualTo(t1);
            assertThat(createEvent.getTableInfo().getTableId()).isEqualTo(100L);
            assertThat(createEvent.getTableAssignment()).isEqualTo(ta);

            // ---- registration change: a put over an existing registration ----
            client.updateTable(
                    t1, TableRegistration.newTable(100L, "file:///tmp/remote2", descriptor));
            TableRegistrationChangeEvent regChange =
                    (TableRegistrationChangeEvent)
                            awaitEvent(eventManager, TableRegistrationChangeEvent.class);
            assertThat(regChange.getTablePath()).isEqualTo(t1);
            assertThat(regChange.getNewTableRegistration().tableId).isEqualTo(100L);

            // ---- schema change: a new schema version ----
            Schema schemaV2 =
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("name", DataTypes.STRING())
                            .build();
            client.registerSchema(t1, schemaV2, 2);
            SchemaChangeEvent schemaChange =
                    (SchemaChangeEvent) awaitEvent(eventManager, SchemaChangeEvent.class);
            assertThat(schemaChange.getTablePath()).isEqualTo(t1);
            assertThat(schemaChange.getSchemaInfo()).isEqualTo(new SchemaInfo(schemaV2, 2));

            // ---- create partition (assignment written first, then the name-keyed record) ----
            TablePath t2 = TablePath.of("db", "t2");
            PartitionAssignment pa = new PartitionAssignment(200L, ta.getBucketAssignments());
            client.registerPartitionAssignmentAndMetadata(
                    201L, "p1", pa, "file:///tmp/remote", t2, 200L);
            CreatePartitionEvent createPartition =
                    (CreatePartitionEvent) awaitEvent(eventManager, CreatePartitionEvent.class);
            assertThat(createPartition.getTablePath()).isEqualTo(t2);
            assertThat(createPartition.getTableId()).isEqualTo(200L);
            assertThat(createPartition.getPartitionId()).isEqualTo(201L);
            assertThat(createPartition.getPartitionName()).isEqualTo("p1");

            // ---- delete partition -> throttler admits a DropPartitionEvent ----
            client.deletePartition(t2, "p1");
            DropPartitionEvent dropPartition =
                    (DropPartitionEvent) awaitEvent(eventManager, DropPartitionEvent.class);
            assertThat(dropPartition.getTableId()).isEqualTo(200L);
            assertThat(dropPartition.getPartitionId()).isEqualTo(201L);
            assertThat(dropPartition.getPartitionName()).isEqualTo("p1");
            // release the in-flight slot (partition drops are not fire-and-forget) so the next drop
            // can be admitted.
            throttler.onPartitionDropCompleted(new TablePartition(200L, 201L));

            // ---- delete table -> throttler admits a DropTableEvent ----
            client.deleteTable(t1);
            DropTableEvent dropTable =
                    (DropTableEvent) awaitEvent(eventManager, DropTableEvent.class);
            assertThat(dropTable.getTableId()).isEqualTo(100L);

            // ---- after stop, no further events are produced ----
            watcher.stop();
            eventManager.clearEvents();
            client.registerTableAssignment(300L, ta);
            client.registerFirstSchema(TablePath.of("db", "t3"), schemaV1);
            client.registerTable(
                    TablePath.of("db", "t3"),
                    TableRegistration.newTable(300L, "file:///tmp/remote", descriptor));
            Thread.sleep(200);
            assertThat(eventManager.getEvents()).isEmpty();
        } finally {
            if (watcher != null) {
                watcher.stop();
            }
            if (throttler != null) {
                throttler.close();
            }
            if (client != null) {
                client.close();
            }
            leaderSelector.close();
            server.close();
        }
    }

    private static CoordinatorEvent awaitEvent(
            TestingEventManager eventManager, Class<? extends CoordinatorEvent> type)
            throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            List<CoordinatorEvent> events = eventManager.getEvents();
            for (CoordinatorEvent event : events) {
                if (type.isInstance(event)) {
                    eventManager.clearEvents();
                    return event;
                }
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("No " + type.getSimpleName() + " event was produced");
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitUntilLeaderReady(RaftMetadataServer server, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean ready =
                    server.getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .isLeaderReady();
            if (ready) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("Raft leader was not ready within " + timeoutMs + "ms");
    }
}
