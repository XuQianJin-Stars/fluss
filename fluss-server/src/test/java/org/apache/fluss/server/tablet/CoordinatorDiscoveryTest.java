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

import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbServerNode;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link CoordinatorDiscovery} (seed parsing, discovery, seed rotation). */
class CoordinatorDiscoveryTest {

    private static MetadataResponse responseWithCoordinator(int nodeId, String host, int port) {
        return new MetadataResponse()
                .setCoordinatorServer(
                        new PbServerNode().setNodeId(nodeId).setHost(host).setPort(port));
    }

    @Test
    void testParseSeed() {
        ServerNode node = CoordinatorDiscovery.parseSeed("127.0.0.1:9123");
        assertThat(node.host()).isEqualTo("127.0.0.1");
        assertThat(node.port()).isEqualTo(9123);
        assertThat(node.serverType()).isEqualTo(ServerType.UNKNOWN);

        assertThatThrownBy(() -> CoordinatorDiscovery.parseSeed("no-port"))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testDiscoversCoordinatorFromSingleSeed() throws Exception {
        MetadataResponse response = responseWithCoordinator(0, "coord-host", 9123);
        Function<ServerNode, AdminReadOnlyGateway> factory =
                seed ->
                        new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                return CompletableFuture.completedFuture(response);
                            }
                        };
        CoordinatorDiscovery discovery =
                new CoordinatorDiscovery(Collections.singletonList("127.0.0.1:9123"), factory);

        ServerNode coordinator = discovery.discoverCoordinatorLeader(5_000, 200);
        assertThat(coordinator.host()).isEqualTo("coord-host");
        assertThat(coordinator.port()).isEqualTo(9123);
        assertThat(coordinator.serverType()).isEqualTo(ServerType.COORDINATOR);
    }

    @Test
    void testRotatesToNextSeedWhenFirstHasNoCoordinator() throws Exception {
        // seed on port 1 fails; seed on port 2 returns the coordinator.
        Function<ServerNode, AdminReadOnlyGateway> factory =
                seed ->
                        new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                if (seed.port() == 1) {
                                    CompletableFuture<MetadataResponse> failed =
                                            new CompletableFuture<>();
                                    failed.completeExceptionally(
                                            new IllegalStateException("seed 1 down"));
                                    return failed;
                                }
                                return CompletableFuture.completedFuture(
                                        responseWithCoordinator(0, "coord-host", 2));
                            }
                        };
        CoordinatorDiscovery discovery =
                new CoordinatorDiscovery(Arrays.asList("127.0.0.1:1", "127.0.0.1:2"), factory);

        ServerNode coordinator = discovery.discoverCoordinatorLeader(5_000, 50);
        assertThat(coordinator.host()).isEqualTo("coord-host");
        assertThat(coordinator.port()).isEqualTo(2);
    }

    @Test
    void testThrowsWhenNoSeedYieldsCoordinator() {
        Function<ServerNode, AdminReadOnlyGateway> factory =
                seed ->
                        new TestCoordinatorGateway() {
                            @Override
                            public CompletableFuture<MetadataResponse> metadata(
                                    MetadataRequest request) {
                                // no coordinator_server set
                                return CompletableFuture.completedFuture(new MetadataResponse());
                            }
                        };
        CoordinatorDiscovery discovery =
                new CoordinatorDiscovery(Collections.singletonList("127.0.0.1:9123"), factory);

        assertThatThrownBy(() -> discovery.discoverCoordinatorLeader(300, 50))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to discover the coordinator leader");
    }
}
