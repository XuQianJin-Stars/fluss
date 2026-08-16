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

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test for a single-node embedded Ratis quorum: it must self-elect, accept
 * an appended {@link MetadataRecord}, commit + apply it, and make it readable from the store. This
 * is the definitive proof that the relocated Ratis engine (fluss-shaded-ratis) works inside
 * fluss-server.
 */
class RaftSingleNodeITCase {

    @TempDir private Path raftDir;

    @Test
    void testSingleNodeElectAppendCommitApplyAndRead() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine stateMachine = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, stateMachine);

        try {
            server.start();

            // 1. self-elect as leader of the single-voter group
            waitUntilLeaderReady(server, 20_000);

            // 2. append a metadata record via the journal writer
            RaftProperties clientProps = server.buildRaftProperties();
            try (RaftClient client =
                            RaftClient.newBuilder()
                                    .setClientId(ClientId.randomId())
                                    .setRaftGroup(group)
                                    .setProperties(clientProps)
                                    .build();
                    RaftJournalWriter writer = new RaftJournalWriter(client, stateMachine)) {

                byte[] key = "db.orders".getBytes(StandardCharsets.UTF_8);
                byte[] payload = "{\"tableId\":7}".getBytes(StandardCharsets.UTF_8);
                MetadataRecord record =
                        new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, payload);

                long appliedIndex = writer.append(record).get(15, TimeUnit.SECONDS);

                // 3. committed + applied
                assertThat(appliedIndex).isPositive();
                assertThat(stateMachine.lastAppliedIndex()).isGreaterThanOrEqualTo(appliedIndex);

                // 4. readable from the store (leader-local linearizable read, §5.7)
                MetadataRecord probe =
                        new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, new byte[0]);
                assertThat(store.get(MetadataKeys.keyOf(probe))).containsExactly(payload);
            }
        } finally {
            server.close();
        }
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
