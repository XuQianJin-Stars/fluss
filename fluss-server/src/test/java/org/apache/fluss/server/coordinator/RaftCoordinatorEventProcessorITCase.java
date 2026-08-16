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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.lease.KvSnapshotLeaseManager;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.coordinator.remote.RemoteDirDynamicLoader;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;
import org.apache.fluss.server.metrics.group.TestingMetricGroups;
import org.apache.fluss.server.zk.data.CoordinatorAddress;
import org.apache.fluss.server.zk.data.TabletServerRegistration;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;
import org.apache.fluss.utils.concurrent.FlussScheduler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.fluss.config.ConfigOptions.DEFAULT_LISTENER_NAME;
import static org.apache.fluss.server.coordinator.CoordinatorTestUtils.makeSendLeaderAndStopRequestAlwaysSuccess;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for a Raft-backed {@link CoordinatorEventProcessor} on a real single-node
 * embedded Ratis leader (route-1 final integration). It exercises the two pieces that only exist on
 * the Raft backend:
 *
 * <ul>
 *   <li><b>Leader-bootstrap</b>: {@code startup()} runs {@code initCoordinatorContext} reading the
 *       coordinator + tablet-server membership from the {@code RaftMetadataStore} (not ZooKeeper),
 *       so the {@link CoordinatorContext} is seeded from the replicated log.
 *   <li><b>Watcher assembly</b>: the constructor's Raft branch assembles the three apply-watch
 *       watchers ({@code RaftCoordinatorChangeWatchers}) on the leader's {@code
 *       MetadataWatchService} in place of the ZooKeeper CuratorCache watchers, and {@code
 *       startup()} starts them — so a committed membership write propagates through the watcher →
 *       event manager → event handler → context pipeline.
 * </ul>
 *
 * <p>The ZooKeeper path of the same (now backend-neutral) constructor is covered by {@code
 * CoordinatorEventProcessorTest}. See {@code zookeeper-removal-design.md} §5.8.
 */
class RaftCoordinatorEventProcessorITCase {

    @TempDir private Path raftDir;
    @TempDir private Path remoteDir;

    @Test
    void testRaftLeaderBootstrapAndWatcherAssembly() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        // seed.nodes present + zookeeper.address absent => MetadataBackend.RAFT
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, remoteDir.toString());

        MetadataStoreClient metaClient = MetadataStoreClientFactory.create(conf, null);
        RaftMetadataClient raftClient = (RaftMetadataClient) metaClient;
        ExecutorService ioExecutor =
                Executors.newFixedThreadPool(1, new ExecutorThreadFactory("test-coordinator-io"));
        KvSnapshotLeaseManager kvSnapshotLeaseManager = null;
        CoordinatorEventProcessor eventProcessor = null;
        try {
            waitUntil(
                    20_000,
                    () -> metaClient.newLeaderSelector("n1").isLeader(),
                    "raft client did not become leader");

            // Seed the replicated store with the coordinator leader + three tablet servers, exactly
            // what the leader-bootstrap will read back (no ZooKeeper involved).
            raftClient.registerCoordinatorLeader(
                    new CoordinatorAddress(
                            "0", Endpoint.fromListenersString("CLIENT://localhost:10012")));
            for (int id = 1; id <= 3; id++) {
                raftClient.registerTabletServer(id, tabletServer(id));
            }

            CoordinatorMetadataCache serverMetadataCache = new CoordinatorMetadataCache();
            TestCoordinatorChannelManager channelManager = new TestCoordinatorChannelManager();
            MetadataManager metadataManager =
                    new MetadataManager(
                            raftClient,
                            conf,
                            new LakeCatalogDynamicLoader(new Configuration(), null, true));
            ReplicaCapacityController replicaCapacityController =
                    new ReplicaCapacityController(conf, serverMetadataCache);
            AutoPartitionManager autoPartitionManager =
                    new AutoPartitionManager(
                            serverMetadataCache,
                            metadataManager,
                            new RemoteDirDynamicLoader(conf),
                            conf,
                            replicaCapacityController);
            LakeTableTieringManager lakeTableTieringManager =
                    new LakeTableTieringManager(TestingMetricGroups.LAKE_TIERING_METRICS);
            kvSnapshotLeaseManager =
                    new KvSnapshotLeaseManager(
                            Duration.ofMinutes(10).toMillis(),
                            raftClient,
                            remoteDir.toString(),
                            SystemClock.getInstance(),
                            TestingMetricGroups.COORDINATOR_METRICS);
            kvSnapshotLeaseManager.start();

            // The coordinator epoch is the Raft leader term (RaftEpoch, §5.1.6).
            CoordinatorContext coordinatorContext =
                    new CoordinatorContext(raftClient.newLeaderSelector("n1").currentEpoch());

            // Prime the channel for every server the startup + watcher paths will push metadata to
            // (the initial three plus the one registered later to exercise the watcher).
            Set<Integer> serverIds = new HashSet<>(Arrays.asList(1, 2, 3, 4));
            makeSendLeaderAndStopRequestAlwaysSuccess(
                    channelManager, serverIds, Collections.emptySet());

            FlussScheduler scheduler = new FlussScheduler(1);
            scheduler.startup();
            eventProcessor =
                    new CoordinatorEventProcessor(
                            raftClient,
                            serverMetadataCache,
                            channelManager,
                            coordinatorContext,
                            replicaCapacityController,
                            autoPartitionManager,
                            lakeTableTieringManager,
                            TestingMetricGroups.COORDINATOR_METRICS,
                            conf,
                            ioExecutor,
                            metadataManager,
                            kvSnapshotLeaseManager,
                            scheduler,
                            SystemClock.getInstance());
            eventProcessor.startup();

            // (1) Leader-bootstrap: the context was seeded from the Raft store, not ZooKeeper.
            assertThat(coordinatorContext.getLiveTabletServers().keySet())
                    .containsExactlyInAnyOrder(1, 2, 3);

            // (2) Watcher assembly is live: registering a fourth tablet server is a committed Raft
            // write; the apply-watch RaftTabletServerChangeWatcher (started by the Raft branch of
            // the constructor) enqueues a NewTabletServerEvent that the processor applies to the
            // context.
            raftClient.registerTabletServer(4, tabletServer(4));
            waitUntil(
                    20_000,
                    () -> coordinatorContext.getLiveTabletServers().containsKey(4),
                    "new tablet server was not observed through the Raft apply-watch");
            assertThat(coordinatorContext.getLiveTabletServers().keySet())
                    .containsExactlyInAnyOrder(1, 2, 3, 4);
        } finally {
            if (eventProcessor != null) {
                eventProcessor.shutdown();
            }
            if (kvSnapshotLeaseManager != null) {
                kvSnapshotLeaseManager.close();
            }
            ioExecutor.shutdownNow();
            metaClient.close();
        }
    }

    private static TabletServerRegistration tabletServer(int id) {
        return new TabletServerRegistration(
                "rack" + id,
                Collections.singletonList(
                        new Endpoint("host" + id, 1000 + id, DEFAULT_LISTENER_NAME)),
                System.currentTimeMillis());
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
            Thread.sleep(100);
        }
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
