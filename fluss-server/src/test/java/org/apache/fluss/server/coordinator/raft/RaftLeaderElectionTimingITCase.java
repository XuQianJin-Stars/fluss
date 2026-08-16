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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lightweight §8.4 timing sample: measures failover election + a metadata write on loopback (3
 * voters, election.timeout=1s). Not a full JMH baseline — logs measured ms for the design-doc notes
 * table.
 */
class RaftLeaderElectionTimingITCase {

    private static final Logger LOG = LoggerFactory.getLogger(RaftLeaderElectionTimingITCase.class);
    private static final int NUM_NODES = 3;
    /** Soft ceiling under CI load; §8.4 aspirational target is &lt;1s on dedicated NVMe. */
    private static final long ELECTION_SOFT_CEILING_MS = 5_000L;

    @TempDir private Path baseDir;

    private final List<Node> nodes = new ArrayList<>();
    private RaftGroup group;

    @AfterEach
    void tearDown() {
        for (Node node : nodes) {
            try {
                node.server.close();
            } catch (Exception ignored) {
                // best-effort shutdown
            }
        }
        nodes.clear();
    }

    @Test
    void testFailoverElectionAndWriteTiming() throws Exception {
        startCluster();
        Node firstLeader = waitForLeader(30_000);

        long writeStart = System.nanoTime();
        appendToLeader(
                new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        "t".getBytes(StandardCharsets.UTF_8),
                        "1".getBytes(StandardCharsets.UTF_8)));
        long writeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writeStart);
        LOG.info(
                "Raft metadata write latency (loopback 3-voter, warm): {} ms — see design §8.4",
                writeMs);

        long electionStart = System.nanoTime();
        firstLeader.server.close();
        nodes.remove(firstLeader);
        Node newLeader = waitForLeader(30_000);
        long electionMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - electionStart);
        LOG.info(
                "Raft leader election after crash (election.timeout=1s, loopback): {} ms — see design §8.4",
                electionMs);

        assertThat(newLeader.peerId).isNotEqualTo(firstLeader.peerId);
        assertThat(electionMs)
                .as("failover election should complete under soft CI ceiling")
                .isLessThan(ELECTION_SOFT_CEILING_MS);
        assertThat(writeMs).isGreaterThanOrEqualTo(0L);
    }

    private void startCluster() throws Exception {
        int[] ports = freePorts(NUM_NODES);
        List<RaftPeer> peers = new ArrayList<>();
        for (int i = 0; i < NUM_NODES; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            peers.add(RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + ports[i]).build());
        }
        group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peers);
        for (int i = 0; i < NUM_NODES; i++) {
            RaftPeerId id = RaftPeerId.valueOf("n" + (i + 1));
            Configuration conf = new Configuration();
            conf.setString(
                    ConfigOptions.COORDINATOR_RAFT_DIR, baseDir.resolve("n" + (i + 1)).toString());
            conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
            conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
            RaftMetadataStore store =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
            RaftMetadataServer server = new RaftMetadataServer(conf, id, group, sm);
            server.start();
            nodes.add(new Node(id, server, store, sm));
        }
    }

    private void appendToLeader(MetadataRecord record) throws Exception {
        Node leader = waitForLeader(30_000);
        RaftProperties props = leader.server.buildRaftProperties();
        try (RaftClient client =
                        RaftClient.newBuilder()
                                .setClientId(ClientId.randomId())
                                .setRaftGroup(group)
                                .setProperties(props)
                                .build();
                RaftJournalWriter writer = new RaftJournalWriter(client, leader.sm)) {
            writer.append(record).get(20, TimeUnit.SECONDS);
        }
    }

    private Node waitForLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (Node node : nodes) {
                try {
                    if (node.server
                            .getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .isLeaderReady()) {
                        return node;
                    }
                } catch (Exception ignored) {
                    // division may not be ready yet
                }
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("No leader elected within " + timeoutMs + "ms");
    }

    private static int[] freePorts(int n) throws Exception {
        ServerSocket[] sockets = new ServerSocket[n];
        int[] ports = new int[n];
        try {
            for (int i = 0; i < n; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } finally {
            for (ServerSocket s : sockets) {
                if (s != null) {
                    s.close();
                }
            }
        }
        return ports;
    }

    private static final class Node {
        final RaftPeerId peerId;
        final RaftMetadataServer server;
        final RaftMetadataStore store;
        final FlussMetadataStateMachine sm;

        Node(
                RaftPeerId peerId,
                RaftMetadataServer server,
                RaftMetadataStore store,
                FlussMetadataStateMachine sm) {
            this.peerId = peerId;
            this.server = server;
            this.store = store;
            this.sm = sm;
        }
    }
}
