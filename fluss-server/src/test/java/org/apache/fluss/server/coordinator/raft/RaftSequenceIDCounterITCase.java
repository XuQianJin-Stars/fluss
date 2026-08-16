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
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test for {@link RaftSequenceIDCounter} against a real single-node embedded
 * Ratis quorum. Verifies block pre-allocation (ids handed out locally between Raft appends),
 * monotonic uniqueness, {@code getAndAdd} range reservation, and the failover no-reuse guarantee
 * (§5.3.2/§5.3.3): a fresh counter over the same persisted store resumes past every id already
 * handed out, never reusing one.
 */
class RaftSequenceIDCounterITCase {

    @TempDir private Path raftDir;

    @Test
    void testBlockAllocationMonotonicUniqueAndNoReuseOnFailover() throws Exception {
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
            waitUntilLeaderReady(server, 20_000);

            RaftProperties clientProps = server.buildRaftProperties();
            try (RaftClient client =
                            RaftClient.newBuilder()
                                    .setClientId(ClientId.randomId())
                                    .setRaftGroup(group)
                                    .setProperties(clientProps)
                                    .build();
                    RaftJournalWriter writer = new RaftJournalWriter(client, stateMachine)) {

                long blockSize = 10;
                SequenceIDCounter counter =
                        new RaftSequenceIDCounter("table", writer, store, blockSize);

                // fresh cluster starts at 0
                assertThat(counter.getCurrent()).isEqualTo(0L);

                // hand out 25 ids across multiple blocks; must be 0..24, strictly monotonic
                long previous = -1L;
                for (int i = 0; i < 25; i++) {
                    long id = counter.getAndIncrement();
                    assertThat(id).isEqualTo((long) i);
                    assertThat(id).isGreaterThan(previous);
                    previous = id;
                }
                assertThat(counter.getCurrent()).isEqualTo(25L);

                // the persisted high-water mark is a block-rounded value >= 25 (blockSize=10 -> 30)
                long persisted =
                        RaftSequenceIDCounter.bytesToLong(
                                store.get(MetadataKeys.idCounterKey("table")));
                assertThat(persisted).isGreaterThanOrEqualTo(25L);

                // getAndAdd reserves a range and returns the start
                long rangeStart = counter.getAndAdd(5L);
                assertThat(rangeStart).isEqualTo(25L);
                assertThat(counter.getCurrent()).isEqualTo(30L);

                long maxHandedOut = counter.getCurrent(); // 30 (next to hand out)

                // simulate failover: a brand-new counter over the SAME persisted store must resume
                // at the persisted high-water mark, never reusing an already-handed-out id.
                SequenceIDCounter resumed =
                        new RaftSequenceIDCounter("table", writer, store, blockSize);
                long resumedFirst = resumed.getAndIncrement();
                assertThat(resumedFirst).isGreaterThanOrEqualTo(maxHandedOut);

                // a different counter name is independent
                SequenceIDCounter writerCounter =
                        new RaftSequenceIDCounter("writer", writer, store, blockSize);
                assertThat(writerCounter.getAndIncrement()).isEqualTo(0L);

                // negative delta is rejected
                assertThatThrownBy(() -> counter.getAndAdd(-1L))
                        .isInstanceOf(IllegalArgumentException.class);
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
