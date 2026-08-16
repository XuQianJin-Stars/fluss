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
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.rpc.messages.PbRaftVoterState;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for §5.10 single-server Raft membership change: bootstrap a 1-voter quorum, add
 * two voters, remove one, and verify survivors still elect a leader. Also covers the quorum-safety
 * refuse path for removing the last voter.
 */
class RaftVoterReconfigurationITCase {

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
    void testAddTwoVotersThenRemoveOne() throws Exception {
        int[] ports = freePorts(3);
        String addr1 = "127.0.0.1:" + ports[0];
        String addr2 = "127.0.0.1:" + ports[1];
        String addr3 = "127.0.0.1:" + ports[2];

        Configuration conf1 = confFor(baseDir.resolve("n1"), addr1);
        conf1.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr1);
        RaftMetadataClient leaderClient = RaftMetadataClient.create(conf1);
        toClose.add(leaderClient);
        waitUntilLeader(leaderClient, 30_000);

        assertThat(leaderClient.listVoters()).hasSize(1);

        // Start each joiner with the group it is about to enter. Pre-creating all three peers in
        // the target conf makes the 2→3 setConfiguration wait forever for a peer whose on-disk
        // conf never matches the leader.
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

        List<PbRaftVoterState> afterAdd2 = leaderClient.addVoter(addr2);
        assertThat(afterAdd2).hasSize(2);
        assertThat(addressesOf(afterAdd2)).containsExactlyInAnyOrder(addr1, addr2);

        Node n3 =
                startJoiningNode(
                        baseDir.resolve("n3"),
                        p3,
                        RaftGroup.valueOf(
                                RaftMetadataServer.RAFT_GROUP_ID, Arrays.asList(p1, p2, p3)));
        toClose.add(n3.server);
        waitUntilAlive(n3.server, 20_000);

        List<PbRaftVoterState> afterAdd3 = leaderClient.addVoter(addr3);
        assertThat(afterAdd3).hasSize(3);
        assertThat(addressesOf(afterAdd3)).containsExactlyInAnyOrder(addr1, addr2, addr3);

        // Wait until all three divisions see a ready leader (catch-up after membership change).
        waitUntilClusterHasLeader(
                Arrays.asList(leaderClient.ownedServer(), n2.server, n3.server), 60_000);

        List<PbRaftVoterState> afterRemove = leaderClient.removeVoter(addr2);
        assertThat(afterRemove).hasSize(2);
        assertThat(addressesOf(afterRemove)).containsExactlyInAnyOrder(addr1, addr3);

        // Survivors must still elect; stop the removed peer to avoid confusion.
        n2.server.close();
        toClose.remove(n2.server);
        waitUntilClusterHasLeader(Arrays.asList(leaderClient.ownedServer(), n3.server), 60_000);
    }

    @Test
    void testRefuseRemoveLastVoter() throws Exception {
        int[] ports = freePorts(1);
        String addr = "127.0.0.1:" + ports[0];
        Configuration conf = confFor(baseDir.resolve("solo"), addr);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, addr);
        RaftMetadataClient client = RaftMetadataClient.create(conf);
        toClose.add(client);
        waitUntilLeader(client, 30_000);

        assertThatThrownBy(() -> client.removeVoter(addr))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("last Raft voter");
    }

    @Test
    void testValidateRemoveQuorumSafeUnit() {
        RaftPeer a = peer("127.0.0.1:1");
        RaftPeer b = peer("127.0.0.1:2");
        RaftPeer c = peer("127.0.0.1:3");
        List<RaftPeer> three = Arrays.asList(a, b, c);

        // Removing last voter of a singleton is refused.
        assertThatThrownBy(
                        () ->
                                RaftMetadataClient.validateRemoveQuorumSafe(
                                        Arrays.asList(a),
                                        a.getId(),
                                        new HashSet<>(Arrays.asList(a.getId()))))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("last Raft voter");

        // 3 voters, only the leader live, remove a dead follower → remaining live 1 < majority(2).
        Set<RaftPeerId> onlyLeaderLive = new HashSet<>();
        onlyLeaderLive.add(a.getId());
        assertThatThrownBy(
                        () ->
                                RaftMetadataClient.validateRemoveQuorumSafe(
                                        three, b.getId(), onlyLeaderLive))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("below majority");

        // 3 voters, all live, remove one → remaining live 2 >= majority(2).
        Set<RaftPeerId> allLive = new HashSet<>();
        allLive.add(a.getId());
        allLive.add(b.getId());
        allLive.add(c.getId());
        RaftMetadataClient.validateRemoveQuorumSafe(three, b.getId(), allLive);
    }

    // ---- helpers ----

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

    private static List<String> addressesOf(List<PbRaftVoterState> voters) {
        List<String> out = new ArrayList<>(voters.size());
        for (PbRaftVoterState v : voters) {
            out.add(v.getAddress());
        }
        return out;
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
