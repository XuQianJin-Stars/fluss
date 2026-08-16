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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.authorizer.VersionedAcls;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.coordinator.raft.RaftCoordinatorMetadataServices;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link TabletServerRaftMetadataFeed} (§5.4.2 / §5.8, T6-3) against a real
 * single-node embedded Ratis leader (via {@link RaftCoordinatorMetadataServices}). It exercises the
 * exact production object graph a Raft-backend TabletServer boots: the registration/heartbeat loop
 * joins the leader's membership, and the background metadata-stream feed converges the {@link
 * TabletServerMetadataCache} — with NO ZooKeeper and NO manual polling — purely by pulling the
 * stream over a {@code CoordinatorGateway}. Also verifies the optional config-notifier and
 * ACL-notifier branches run.
 */
class TabletServerRaftMetadataFeedITCase {

    private static final String LISTENER = "FLUSS";

    @TempDir private Path raftDir;

    /**
     * A gateway that routes the register/heartbeat + metadata-stream RPCs to the leader handlers.
     */
    private static final class DelegatingGateway extends TestCoordinatorGateway {
        private final RaftCoordinatorMetadataServices services;

        DelegatingGateway(RaftCoordinatorMetadataServices services) {
            this.services = services;
        }

        @Override
        public CompletableFuture<RegisterTabletServerResponse> registerTabletServer(
                RegisterTabletServerRequest request) {
            return services.tabletServerRegistrationRpcServer().registerTabletServer(request);
        }

        @Override
        public CompletableFuture<TabletServerHeartbeatResponse> tabletServerHeartbeat(
                TabletServerHeartbeatRequest request) {
            return services.tabletServerRegistrationRpcServer().tabletServerHeartbeat(request);
        }

        @Override
        public CompletableFuture<FetchMetadataResponse> fetchMetadata(
                FetchMetadataRequest request) {
            return services.metadataStreamRpcServer().fetchMetadata(request);
        }

        @Override
        public CompletableFuture<FetchMetadataSnapshotResponse> fetchMetadataSnapshot(
                FetchMetadataSnapshotRequest request) {
            return services.metadataStreamRpcServer().fetchMetadataSnapshot(request);
        }
    }

    @Test
    void testFeedRegistersAndConvergesCache() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.set(ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT, Duration.ofMillis(3_000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(150));
        conf.set(ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT, Duration.ofMillis(100));

        RaftMetadataClient client =
                (RaftMetadataClient) MetadataStoreClientFactory.create(conf, null);
        RaftCoordinatorMetadataServices services =
                RaftCoordinatorMetadataServices.create(client, conf);

        AtomicInteger configReloads = new AtomicInteger();
        AtomicReference<Map<Resource, VersionedAcls>> lastAclSnapshot = new AtomicReference<>();
        TabletServerRaftMetadataFeed feed = null;
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");
            services.start();

            TabletServerMetadataCache cache =
                    new TabletServerMetadataCache(new MetadataManager(client, conf, null));

            feed =
                    TabletServerRaftMetadataFeed.create(
                            conf,
                            /* serverId */ 1,
                            new DelegatingGateway(services),
                            cache,
                            () ->
                                    new RegisterTabletServerRequest()
                                            .setServerId(1)
                                            .setRegisterTimestamp(System.currentTimeMillis())
                                            .setRack("rack1")
                                            .setListeners(LISTENER + "://localhost:9001"),
                            configs -> configReloads.incrementAndGet(),
                            lastAclSnapshot::set);
            feed.start();

            // ---- the registration loop joined the leader's membership ----
            assertThat(feed.registrationLoop().isRegistered()).isTrue();
            waitUntil(
                    5_000,
                    () -> meta.getSortedTabletServerList().length == 1,
                    "server was not registered with the leader");
            assertThat(meta.getSortedTabletServerList()).containsExactly(1);

            // ---- leader writes cluster metadata AFTER the feed is already running ----
            client.registerTabletServer(2, tabletServer(2));
            client.registerCoordinatorLeader(
                    new CoordinatorAddress(
                            "100", Endpoint.fromListenersString(LISTENER + "://localhost:9124")));

            Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
            TableDescriptor descriptor =
                    TableDescriptor.builder().schema(schema).distributedBy(3).build();
            TableAssignment ta =
                    TableAssignment.builder().add(0, BucketAssignment.of(1, 2)).build();
            TablePath t1 = TablePath.of("db", "t1");
            client.registerDatabase(
                    "db", DatabaseRegistration.of(DatabaseDescriptor.builder().build()));
            client.registerTableAssignment(100L, ta);
            client.registerFirstSchema(t1, schema);
            client.registerTable(
                    t1, TableRegistration.newTable(100L, "file:///tmp/remote", descriptor));

            Resource db1 = Resource.database("db1");
            AccessControlEntry ace =
                    new AccessControlEntry(
                            new FlussPrincipal("user1", "User"),
                            AccessControlEntry.WILD_CARD_HOST,
                            OperationType.READ,
                            PermissionType.ALLOW);
            client.createResourceAcl(db1, Collections.singleton(ace));

            // ---- the background feed converges the cache with no manual polling ----
            waitUntil(
                    15_000,
                    () ->
                            cache.isAliveTabletServer(1)
                                    && cache.isAliveTabletServer(2)
                                    && cache.getCoordinatorServer(LISTENER) != null
                                    && cache.getTablePath(100L).isPresent()
                                    && cache.contains(new TableBucket(100L, 0)),
                    "feed did not converge the cache");

            assertThat(cache.getAllAliveTabletServers(LISTENER)).containsOnlyKeys(1, 2);
            assertThat(cache.getTablePath(100L)).contains(t1);

            // ---- the optional config-notifier branch ran at least once off the stream ----
            assertThat(configReloads.get()).isPositive();

            // ---- ACL notifier projects leader ACL writes into the feed callback ----
            waitUntil(
                    15_000,
                    () -> lastAclSnapshot.get() != null && lastAclSnapshot.get().containsKey(db1),
                    "feed did not converge ACLs");
            assertThat(lastAclSnapshot.get().get(db1).acls).containsExactly(ace);

            // ---- a later change also propagates through the running feed ----
            client.deleteTable(t1);
            waitUntil(
                    15_000,
                    () -> !cache.getTablePath(100L).isPresent(),
                    "feed did not propagate the table drop");
        } finally {
            if (feed != null) {
                feed.close();
            }
            services.close();
            client.close();
        }
    }

    private static org.apache.fluss.server.zk.data.TabletServerRegistration tabletServer(int id) {
        return new org.apache.fluss.server.zk.data.TabletServerRegistration(
                "rack" + id,
                Endpoint.fromListenersString(LISTENER + "://localhost:" + (9000 + id)),
                10_000L + id);
    }

    private interface Condition {
        boolean test() throws Exception;
    }

    private static void waitUntil(long timeoutMs, Condition condition, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
