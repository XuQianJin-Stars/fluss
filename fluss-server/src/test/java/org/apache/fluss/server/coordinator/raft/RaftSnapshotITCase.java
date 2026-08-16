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
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FlussMetadataStateMachine} snapshotting (§5.2.5): (1) the snapshot file
 * serialization round-trips exactly, and (2) on a real embedded Ratis node, {@code takeSnapshot()}
 * writes a snapshot tagged with the last applied index, and a fresh state machine started against
 * the same storage directory recovers the full metadata store and applied index.
 */
class RaftSnapshotITCase {

    @TempDir private Path raftDir;

    @Test
    void testSnapshotSerializationRoundTrip(@TempDir Path tmp) throws Exception {
        List<byte[][]> entries = new ArrayList<>();
        entries.add(new byte[][] {bytes("k1"), bytes("v1")});
        entries.add(new byte[][] {bytes("key-two"), bytes("")}); // empty value
        entries.add(new byte[][] {new byte[] {0, 1, 2, (byte) 0xFF}, new byte[] {(byte) 0x80, 7}});

        File file = tmp.resolve("snapshot.test").toFile();
        FlussMetadataStateMachine.writeEntries(file, entries);
        List<byte[][]> restored = FlussMetadataStateMachine.readEntries(file);

        assertThat(restored).hasSize(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            assertThat(restored.get(i)[0]).containsExactly(entries.get(i)[0]);
            assertThat(restored.get(i)[1]).containsExactly(entries.get(i)[1]);
        }
    }

    @Test
    void testTakeSnapshotAndRecoverOnRestart() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        byte[][] keys = {bytes("db.orders"), bytes("db.items"), bytes("db.users")};
        byte[][] payloads = {bytes("{\"id\":1}"), bytes("{\"id\":2}"), bytes("{\"id\":3}")};
        long snapshotIndex;

        // ---- phase 1: append records, take a snapshot ----
        RaftMetadataStore store1 =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm1 = new FlussMetadataStateMachine(store1);
        RaftMetadataServer server1 = new RaftMetadataServer(conf, peerId, group, sm1);
        try {
            server1.start();
            waitUntilLeaderReady(server1, 20_000);
            RaftProperties clientProps = server1.buildRaftProperties();
            try (RaftClient client = newClient(group, clientProps);
                    RaftJournalWriter writer = new RaftJournalWriter(client, sm1)) {
                for (int i = 0; i < keys.length; i++) {
                    writer.append(
                                    new MetadataRecord(
                                            MetadataRecord.Type.REGISTER_TABLE,
                                            keys[i],
                                            payloads[i]))
                            .get(15, TimeUnit.SECONDS);
                }
            }
            snapshotIndex = sm1.takeSnapshot();
            // Snapshot is tagged with the SM applied index (user metadata), not Ratis
            // getLastAppliedTermIndex(), which can include configuration log entries that
            // never went through applyEntry.
            assertThat(snapshotIndex).isPositive();
            assertThat(snapshotIndex).isEqualTo(sm1.lastAppliedIndex());
        } finally {
            server1.close();
        }

        // ---- phase 2: fresh state machine + store, same storage dir -> recovers via snapshot ----
        RaftMetadataStore store2 =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm2 = new FlussMetadataStateMachine(store2);
        RaftMetadataServer server2 = new RaftMetadataServer(conf, peerId, group, sm2);
        try {
            server2.start();
            waitUntilLeaderReady(server2, 20_000);

            // the recovered store contains every previously-committed record
            for (int i = 0; i < keys.length; i++) {
                MetadataRecord probe =
                        new MetadataRecord(
                                MetadataRecord.Type.REGISTER_TABLE, keys[i], new byte[0]);
                assertThat(store2.get(MetadataKeys.keyOf(probe)))
                        .as("recovered value for key %d", i)
                        .containsExactly(payloads[i]);
            }
            // recovery advanced the applied index to at least the snapshot offset
            assertThat(sm2.lastAppliedIndex()).isGreaterThanOrEqualTo(snapshotIndex);
        } finally {
            server2.close();
        }
    }

    private static RaftClient newClient(RaftGroup group, RaftProperties props) {
        return RaftClient.newBuilder()
                .setClientId(ClientId.randomId())
                .setRaftGroup(group)
                .setProperties(props)
                .build();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
