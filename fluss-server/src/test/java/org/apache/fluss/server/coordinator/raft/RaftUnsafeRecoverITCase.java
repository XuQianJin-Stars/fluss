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
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.messages.PbRaftVoterState;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Smoke coverage for §9.4 {@code raft unsafe-recover}: rewrite {@code raft-meta.conf} to a
 * singleton survivor group (idempotent on an already-single-node dir) and recover after a 3-voter
 * quorum loses two peers.
 */
class RaftUnsafeRecoverITCase {

    @TempDir private Path baseDir;

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (int i = toClose.size() - 1; i >= 0; i--) {
            try {
                toClose.get(i).close();
            } catch (Exception ignored) {
                // best-effort
            }
        }
        toClose.clear();
    }

    @Test
    void testResolveSurvivorRequiresIdentity() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, baseDir.toString());
        assertThatThrownBy(() -> RaftUnsafeRecover.resolveSurvivorPeer(conf))
                .isInstanceOf(IllegalConfigurationException.class);

        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:19001");
        RaftPeer peer = RaftUnsafeRecover.resolveSurvivorPeer(conf);
        assertThat(peer.getAddress()).isEqualTo("127.0.0.1:19001");
        assertThat(peer.getId().toString()).isEqualTo("127.0.0.1_19001");
    }

    @Test
    void testUnsafeRecoverIdempotentOnSingleNode() throws Exception {
        int[] ports = freePorts(1);
        String addr = "127.0.0.1:" + ports[0];
        Path raftDir = baseDir.resolve("solo");

        Configuration conf = confFor(raftDir, addr);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr);
        RaftMetadataClient client = RaftMetadataClient.create(conf);
        toClose.add(client);
        waitUntilLeader(client, 30_000);
        assertThat(client.listVoters()).hasSize(1);
        client.close();
        toClose.remove(client);

        String summary1 = RaftUnsafeRecover.recover(conf);
        assertThat(summary1).contains(addr).contains("conf index");

        Path metaConf =
                raftDir.resolve(RaftMetadataServer.DEFAULT_GROUP_UUID.toString())
                        .resolve("current")
                        .resolve(RaftUnsafeRecover.RAFT_META_CONF);
        assertThat(metaConf).exists();
        long index1 = readIndex(metaConf);

        String summary2 = RaftUnsafeRecover.recover(conf);
        assertThat(summary2).contains(addr);
        long index2 = readIndex(metaConf);
        assertThat(index2).isEqualTo(index1 + 1);

        // Restart as singleton — must elect.
        Configuration restart = confFor(raftDir, addr);
        restart.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr);
        RaftMetadataClient again = RaftMetadataClient.create(restart);
        toClose.add(again);
        waitUntilLeader(again, 30_000);
        assertThat(again.listVoters()).hasSize(1);
        assertThat(again.listVoters().get(0).getAddress()).isEqualTo(addr);
    }

    @Test
    void testUnsafeRecoverAfterLosingQuorumMajority() throws Exception {
        int[] ports = freePorts(3);
        String addr1 = "127.0.0.1:" + ports[0];
        String addr2 = "127.0.0.1:" + ports[1];
        String addr3 = "127.0.0.1:" + ports[2];

        Path dir1 = baseDir.resolve("n1");
        Configuration conf1 = confFor(dir1, addr1);
        conf1.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr1);
        RaftMetadataClient leaderClient = RaftMetadataClient.create(conf1);
        toClose.add(leaderClient);
        waitUntilLeader(leaderClient, 30_000);

        RaftPeer p1 = peer(addr1);
        RaftPeer p2 = peer(addr2);
        RaftPeer p3 = peer(addr3);

        Node n2 =
                startJoiningNode(
                        baseDir.resolve("n2"),
                        p2,
                        RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, Arrays.asList(p1, p2)));
        toClose.add(n2.server);
        waitUntilAlive(n2.server, 20_000);
        assertThat(leaderClient.addVoter(addr2)).hasSize(2);

        Node n3 =
                startJoiningNode(
                        baseDir.resolve("n3"),
                        p3,
                        RaftGroup.valueOf(
                                RaftMetadataServer.RAFT_GROUP_ID, Arrays.asList(p1, p2, p3)));
        toClose.add(n3.server);
        waitUntilAlive(n3.server, 20_000);
        assertThat(leaderClient.addVoter(addr3)).hasSize(3);
        waitUntilClusterHasLeader(
                Arrays.asList(leaderClient.ownedServer(), n2.server, n3.server), 60_000);

        // Quorum loss: stop all, discard n2/n3, recover n1 alone.
        leaderClient.close();
        toClose.remove(leaderClient);
        n2.server.close();
        toClose.remove(n2.server);
        n3.server.close();
        toClose.remove(n3.server);

        Configuration survivorConf = confFor(dir1, addr1);
        survivorConf.setString(
                ConfigOptions.COORDINATOR_RAFT_VOTERS, addr1 + "," + addr2 + "," + addr3);
        survivorConf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, addr1);
        String summary = RaftUnsafeRecover.recover(survivorConf);
        assertThat(summary).contains(addr1).contains("single voter");

        // Start only the survivor with a singleton voter set.
        Configuration solo = confFor(dir1, addr1);
        solo.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr1);
        RaftMetadataClient recovered = RaftMetadataClient.create(solo);
        toClose.add(recovered);
        waitUntilLeader(recovered, 60_000);
        List<PbRaftVoterState> voters = recovered.listVoters();
        assertThat(voters).hasSize(1);
        assertThat(voters.get(0).getAddress()).isEqualTo(addr1);
    }

    private static long readIndex(Path metaConf) throws Exception {
        try (InputStream in = Files.newInputStream(metaConf)) {
            return LogEntryProto.newBuilder().mergeFrom(in).build().getIndex();
        }
    }

    private static Node startJoiningNode(Path dir, RaftPeer self, RaftGroup group)
            throws Exception {
        Configuration conf = confFor(dir, self.getAddress());
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(store);
        RaftMetadataServer server = new RaftMetadataServer(conf, self.getId(), group, sm);
        server.start();
        return new Node(self.getId(), server, sm);
    }

    private static Configuration confFor(Path dir, String nodeAddress) {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, dir.toString());
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID, nodeAddress);
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(1000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(300));
        return conf;
    }

    private static RaftPeer peer(String address) {
        return RaftPeer.newBuilder()
                .setId(RaftMetadataClient.peerIdForAddress(address))
                .setAddress(address)
                .build();
    }

    private static void waitUntilLeader(RaftMetadataClient client, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (client.isLeader()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("No leader within " + timeoutMs + "ms");
    }

    private static void waitUntilClusterHasLeader(List<RaftMetadataServer> servers, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (RaftMetadataServer server : servers) {
                try {
                    if (server.getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .isLeaderReady()) {
                        return;
                    }
                } catch (Exception ignored) {
                    // division may not be ready
                }
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("No leader among survivors within " + timeoutMs + "ms");
    }

    private static void waitUntilAlive(RaftMetadataServer server, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (server.getRaftServer()
                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                        .getInfo()
                        .isAlive()) {
                    return;
                }
            } catch (Exception ignored) {
                // division may not be ready
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Joining peer not alive within " + timeoutMs + "ms");
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
        final FlussMetadataStateMachine sm;

        Node(RaftPeerId peerId, RaftMetadataServer server, FlussMetadataStateMachine sm) {
            this.peerId = peerId;
            this.server = server;
            this.sm = sm;
        }
    }
}
