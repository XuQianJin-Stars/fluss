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
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.AuthorizationPlugin;
import org.apache.fluss.server.authorizer.DefaultAuthorizer;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.ZooKeeperClient;
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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link AclChangeStreamNotifier} (§5.8, T6-2 ACL slice): a Raft-backed {@link
 * DefaultAuthorizer}'s ACL cache is kept in sync purely by consuming the metadata stream from a
 * real single-node embedded Ratis leader — the cross-process replacement for the ZooKeeper ACL
 * change-notification watcher. Verifies (1) resource ACLs written on the leader project into the
 * authorizer's access decisions via the follower store + notifier, and (2) a {@code
 * DELETE_RESOURCE_ACL} on the leader drops the ACL from the authorizer.
 */
class AclChangeStreamNotifierITCase {

    @TempDir private Path raftDir;

    @Test
    void testAuthorizerAclsProjectedFromStream() throws Exception {
        int port = freePort();
        RaftPeerId peerId = RaftPeerId.valueOf("n1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("127.0.0.1:" + port).build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);

        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataStore leaderStore =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        FlussMetadataStateMachine sm = new FlussMetadataStateMachine(leaderStore);
        RaftMetadataServer server = new RaftMetadataServer(conf, peerId, group, sm);
        RaftLeaderSelector leaderSelector = new RaftLeaderSelector("n1", server, sm, 20_000);
        sm.setLeaderSelector(leaderSelector);
        leaderSelector.startElectLeaderAsync(() -> {}, t -> {});

        RaftMetadataClient client = null;
        try {
            server.start();
            waitUntilLeaderReady(server, 20_000);

            MetadataStreamService stream =
                    new MetadataStreamService(
                            server.getRaftServer()
                                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                                    .getRaftLog(),
                            () -> currentTerm(server),
                            sm::captureSnapshot);
            sm.setApplyListener(stream::onApplied);

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

            // A Raft-backed DefaultAuthorizer: no ZK watcher (client is not a ZooKeeperClient),
            // cache fed purely by the stream notifier (startup()/loadCache intentionally skipped).
            final MetadataStoreClient leaderClient = client;
            DefaultAuthorizer authorizer =
                    new DefaultAuthorizer(
                            new AuthorizationPlugin.Context() {
                                @Override
                                public Configuration getConfiguration() {
                                    return new Configuration();
                                }

                                @Override
                                public Optional<ZooKeeperClient> getZooKeeperClient() {
                                    return Optional.empty();
                                }

                                @Override
                                public Optional<MetadataStoreClient> getMetadataStoreClient() {
                                    return Optional.of(leaderClient);
                                }
                            });

            // Subscriber-fed follower store + the notifier projecting it into the authorizer.
            RaftMetadataStore subStore =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            MetadataStreamSubscriber subscriber =
                    new MetadataStreamSubscriber(stream, subStore, 100, 0L, 20L, null);
            AtomicReference<Map<Resource, VersionedAcls>> lastSnapshot = new AtomicReference<>();
            AclChangeStreamNotifier notifier =
                    new AclChangeStreamNotifier(
                            subStore,
                            snapshot -> {
                                lastSnapshot.set(snapshot);
                                authorizer.reconcileAclCache(snapshot);
                            });

            FlussPrincipal user1 = new FlussPrincipal("user1", "User");
            FlussPrincipal user2 = new FlussPrincipal("user2", "User");
            Resource db1 = Resource.database("db1");
            Resource db2 = Resource.database("db2");
            AccessControlEntry ace1 =
                    new AccessControlEntry(
                            user1,
                            AccessControlEntry.WILD_CARD_HOST,
                            OperationType.READ,
                            PermissionType.ALLOW);
            AccessControlEntry ace2 =
                    new AccessControlEntry(
                            user2,
                            AccessControlEntry.WILD_CARD_HOST,
                            OperationType.READ,
                            PermissionType.ALLOW);

            // ---- 1. leader writes ACLs -> notifier projects them into the authorizer ----
            client.createResourceAcl(db1, Collections.singleton(ace1));
            client.createResourceAcl(db2, Collections.singleton(ace2));
            drainUntilCaughtUp(subscriber, sm);
            notifier.refresh();

            assertThat(lastSnapshot.get()).containsOnlyKeys(db1, db2);
            assertThat(lastSnapshot.get().get(db1).acls).containsExactly(ace1);
            assertThat(authorizer.aclsAllowAccess(db1, user1, OperationType.READ, "1.2.3.4"))
                    .isTrue();
            assertThat(authorizer.aclsAllowAccess(db2, user2, OperationType.READ, "1.2.3.4"))
                    .isTrue();
            // A principal without an ALLOW entry is denied.
            assertThat(authorizer.aclsAllowAccess(db1, user2, OperationType.READ, "1.2.3.4"))
                    .isFalse();

            // ---- 2. leader deletes db1's ACL -> notifier drops it from the authorizer ----
            client.conditionalDeleteResourceAcl(db1, 0);
            drainUntilCaughtUp(subscriber, sm);
            notifier.refresh();

            assertThat(lastSnapshot.get()).containsOnlyKeys(db2);
            assertThat(authorizer.aclsAllowAccess(db1, user1, OperationType.READ, "1.2.3.4"))
                    .isFalse();
            assertThat(authorizer.aclsAllowAccess(db2, user2, OperationType.READ, "1.2.3.4"))
                    .isTrue();

            authorizer.close();
        } finally {
            if (client != null) {
                client.close();
            }
            leaderSelector.close();
            server.close();
        }
    }

    /** Polls the subscriber until it has applied everything the leader has committed. */
    private static void drainUntilCaughtUp(
            MetadataStreamSubscriber subscriber, FlussMetadataStateMachine sm) throws Exception {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            subscriber.pollOnce();
            if (subscriber.appliedOffset() >= sm.lastAppliedIndex()) {
                return;
            }
        }
        throw new IllegalStateException(
                "Subscriber did not catch up: applied="
                        + subscriber.appliedOffset()
                        + " leader="
                        + sm.lastAppliedIndex());
    }

    private static long currentTerm(RaftMetadataServer server) {
        try {
            return server.getRaftServer()
                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                    .getInfo()
                    .getCurrentTerm();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
