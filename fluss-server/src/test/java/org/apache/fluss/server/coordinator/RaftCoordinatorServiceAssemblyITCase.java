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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.coordinator.raft.AllocateIdRpcServer;
import org.apache.fluss.server.coordinator.raft.RaftCoordinatorMetadataServices;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.metadata.CoordinatorMetadataCache;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Narrow integration test for flipping the {@code CoordinatorServer} RAFT gate: it builds a
 * Raft-backed {@link CoordinatorService} (over a real single-node embedded Ratis {@link
 * RaftMetadataClient}, with the heavy leader-event collaborators left as an unused lazy supplier)
 * and assembles the three leader-side handlers by invoking the EXACT production wiring {@link
 * CoordinatorServer#wireRaftLeaderHandlers} that {@code initCoordinatorStandby} performs on the
 * Raft backend. It proves that on the Raft backend the coordinator's RPC surface — {@code
 * AllocateId} (§5.3), {@code RegisterTabletServer} (§5.4.2) and {@code FetchMetadata} (§5.8) — is
 * SERVED (not the ZooKeeper-backend {@code raftBackendOnly} error). This exercises the actual gate
 * wiring without booting the full coordinator leader (which drags in the whole
 * CoordinatorEventProcessor — a separate milestone).
 */
class RaftCoordinatorServiceAssemblyITCase {

    @TempDir private Path raftDir;

    @Test
    void testRaftBackedCoordinatorServiceServesTheThreeHandlers() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));
        conf.setString(ConfigOptions.REMOTE_DATA_DIR, raftDir.resolve("remote").toString());

        // sanity: this configuration resolves to the Raft backend.
        assertThat(MetadataBackend.from(conf)).isEqualTo(MetadataBackend.RAFT);

        RaftMetadataClient client =
                (RaftMetadataClient) MetadataStoreClientFactory.create(conf, null);
        RaftCoordinatorMetadataServices services =
                RaftCoordinatorMetadataServices.create(client, conf);
        ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
        CoordinatorService coordinatorService = null;
        try {
            // Build a Raft-backed CoordinatorService with the minimal neutral collaborators; the
            // leader-event processor is a lazy supplier that is never invoked by the three handler
            // RPCs (they only delegate to the handlers set below).
            CoordinatorMetadataCache metadataCache = new CoordinatorMetadataCache();
            coordinatorService =
                    new CoordinatorService(
                            conf,
                            /* remoteFileSystem */ null,
                            /* zkClient */ client,
                            /* coordinatorEventProcessorSupplier */ () -> {
                                throw new UnsupportedOperationException(
                                        "event processor not needed for this assembly test");
                            },
                            metadataCache,
                            new MetadataManager(client, conf, null),
                            /* authorizer */ null,
                            /* lakeCatalogDynamicLoader */ null,
                            /* lakeTableTieringManager */ null,
                            /* remoteDirDynamicLoader */ null,
                            new DynamicConfigManager(client, conf, true),
                            ioExecutor,
                            /* kvSnapshotLeaseManager */ null,
                            client.newLeaderSelector("n1"),
                            new ReplicaCapacityController(conf, metadataCache));

            // ---- BEFORE wiring: the handlers are null, so the RPC reports unsupported ----
            final CoordinatorService svc = coordinatorService;
            assertThatThrownBy(
                            () ->
                                    svc.allocateId(
                                                    new AllocateIdRequest()
                                                            .setIdKind(
                                                                    AllocateIdRpcServer.KIND_WRITER)
                                                            .setCount(1L))
                                            .get())
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class);

            // ---- assemble the three leader-side handlers via the EXACT production wiring that
            // ---- CoordinatorServer.initCoordinatorStandby performs on the Raft backend ----
            CoordinatorServer.wireRaftLeaderHandlers(coordinatorService, services);
            services.start();
            waitUntil(20_000, () -> client.newLeaderSelector("n1").isLeader(), "no raft leader");

            // ---- AllocateId is now SERVED by the Raft-backed coordinator ----
            AllocateIdResponse alloc0 =
                    coordinatorService
                            .allocateId(
                                    new AllocateIdRequest()
                                            .setIdKind(AllocateIdRpcServer.KIND_WRITER)
                                            .setCount(1L))
                            .get();
            assertThat(alloc0.getStartId()).isEqualTo(0L);
            AllocateIdResponse alloc1 =
                    coordinatorService
                            .allocateId(
                                    new AllocateIdRequest()
                                            .setIdKind(AllocateIdRpcServer.KIND_WRITER)
                                            .setCount(1L))
                            .get();
            assertThat(alloc1.getStartId()).isEqualTo(1L);

            // ---- RegisterTabletServer is served: the membership record is appended ----
            coordinatorService
                    .registerTabletServer(
                            new RegisterTabletServerRequest()
                                    .setServerId(1)
                                    .setRegisterTimestamp(1L)
                                    .setListeners("FLUSS://localhost:9001"))
                    .get();
            assertThat(client.getTabletServer(1)).isPresent();

            // ---- FetchMetadata is served: the stream returns the committed records ----
            FetchMetadataResponse fetch =
                    coordinatorService
                            .fetchMetadata(
                                    new FetchMetadataRequest()
                                            .setServerId(1)
                                            .setFromOffset(1L)
                                            .setLeaderEpoch(0L)
                                            .setMaxRecords(100)
                                            .setMaxWaitMs(1_000))
                            .get();
            assertThat(fetch.getLeaderEpoch()).isGreaterThan(0L);
            assertThat(fetch.getRecordsCount()).isGreaterThan(0);
        } finally {
            if (coordinatorService != null) {
                coordinatorService.shutdown();
            }
            services.close();
            client.close();
            ioExecutor.shutdownNow();
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
}
