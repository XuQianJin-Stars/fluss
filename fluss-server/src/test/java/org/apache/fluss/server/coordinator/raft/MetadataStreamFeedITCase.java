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
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.DatabaseRegistration;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.ClientId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link MetadataStreamFeed} (§5.8, T6-3 slice 2): a {@link
 * TabletServerMetadataCache} is kept in sync by the feed's OWN subscriber thread (no manual
 * polling), pulling the metadata stream from a real single-node embedded Ratis leader through the
 * production {@link CoordinatorGatewayMetadataStreamTransport} over a {@code CoordinatorGateway}
 * (slice 1). Verifies the whole tablet-server read path: register servers + create a table on the
 * leader, then the cache reflects them purely because the background feed replayed the stream and
 * ran the applier's {@code refresh()} after each advancing batch.
 */
class MetadataStreamFeedITCase {

    private static final String LISTENER = "FLUSS";

    @TempDir private Path raftDir;

    @Test
    void testCacheKeptInSyncByBackgroundFeed() throws Exception {
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
        MetadataStreamFeed feed = null;
        try {
            server.start();
            waitUntilLeaderReady(server, 20_000);

            RaftLog raftLog =
                    server.getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getRaftLog();
            MetadataStreamService stream =
                    new MetadataStreamService(
                            raftLog, () -> currentTerm(server), sm::captureSnapshot);
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

            // Subscriber fed via the PRODUCTION transport (slice 1) over a CoordinatorGateway that
            // forwards the two stream RPCs to the leader's in-process handler.
            MetadataStreamRpcServer rpcServer = new MetadataStreamRpcServer(stream);
            CoordinatorGatewayMetadataStreamTransport transport =
                    new CoordinatorGatewayMetadataStreamTransport(new StreamOnlyGateway(rpcServer));
            RpcMetadataStreamGateway streamGateway = new RpcMetadataStreamGateway(transport, 1);

            RaftMetadataStore subStore =
                    new RaftMetadataStore(
                            new InMemoryMetadataKvStore(), new MetadataWatchService());
            MetadataStreamSubscriber subscriber =
                    new MetadataStreamSubscriber(streamGateway, subStore, 100, 50L, 20L, null);
            TabletServerMetadataCache cache =
                    new TabletServerMetadataCache(new MetadataManager(client, conf, null));
            TabletServerMetadataStreamCacheApplier applier =
                    new TabletServerMetadataStreamCacheApplier(subStore, cache);

            // The feed drives the subscriber on its OWN thread and refreshes the cache per batch.
            feed =
                    new MetadataStreamFeed(
                            subscriber,
                            Collections.singletonList((Runnable) applier::refresh),
                            20L,
                            "test-metadata-feed");
            feed.start();

            // ---- leader writes membership + a table AFTER the feed is already running ----
            client.registerTabletServer(1, tabletServer(1));
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
            if (client != null) {
                client.close();
            }
            leaderSelector.close();
            server.close();
        }
    }

    /** A {@link CoordinatorGateway} that only serves the two metadata-stream RPCs (in-process). */
    private static final class StreamOnlyGateway extends TestCoordinatorGateway {
        private final MetadataStreamRpcServer rpcServer;

        StreamOnlyGateway(MetadataStreamRpcServer rpcServer) {
            this.rpcServer = rpcServer;
        }

        @Override
        public CompletableFuture<FetchMetadataResponse> fetchMetadata(
                FetchMetadataRequest request) {
            return rpcServer.fetchMetadata(request);
        }

        @Override
        public CompletableFuture<FetchMetadataSnapshotResponse> fetchMetadataSnapshot(
                FetchMetadataSnapshotRequest request) {
            return rpcServer.fetchMetadataSnapshot(request);
        }
    }

    private static TabletServerRegistration tabletServer(int id) {
        return new TabletServerRegistration(
                "rack" + id,
                Endpoint.fromListenersString(LISTENER + "://localhost:" + (9000 + id)),
                10_000L + id);
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
