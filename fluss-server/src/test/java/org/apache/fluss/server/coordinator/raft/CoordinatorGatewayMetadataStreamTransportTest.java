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

import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;
import org.apache.fluss.server.coordinator.TestCoordinatorGateway;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link CoordinatorGatewayMetadataStreamTransport}: it forwards the two
 * metadata-stream RPCs to the injected {@link org.apache.fluss.rpc.gateway.CoordinatorGateway}
 * verbatim and returns the gateway's own future, and — when driven through {@link
 * RpcMetadataStreamGateway} — a transport failure surfaces as an {@link IOException} so the
 * subscriber reconnects.
 */
class CoordinatorGatewayMetadataStreamTransportTest {

    @Test
    void forwardsFetchToGatewayAndReturnsItsFuture() {
        RecordingGateway gateway = new RecordingGateway();
        gateway.fetchReply = CompletableFuture.completedFuture(new FetchMetadataResponse());
        CoordinatorGatewayMetadataStreamTransport transport =
                new CoordinatorGatewayMetadataStreamTransport(gateway);

        FetchMetadataRequest request =
                new FetchMetadataRequest()
                        .setServerId(7)
                        .setFromOffset(3L)
                        .setLeaderEpoch(1L)
                        .setMaxRecords(50)
                        .setMaxWaitMs(100);

        CompletableFuture<FetchMetadataResponse> result = transport.sendFetch(request);

        assertThat(result).isSameAs(gateway.fetchReply);
        assertThat(gateway.fetchSeen.get()).isSameAs(request);
    }

    @Test
    void forwardsFetchSnapshotToGatewayAndReturnsItsFuture() {
        RecordingGateway gateway = new RecordingGateway();
        gateway.snapshotReply =
                CompletableFuture.completedFuture(new FetchMetadataSnapshotResponse());
        CoordinatorGatewayMetadataStreamTransport transport =
                new CoordinatorGatewayMetadataStreamTransport(gateway);

        FetchMetadataSnapshotRequest request =
                new FetchMetadataSnapshotRequest().setServerId(7).setLeaderEpoch(1L);

        CompletableFuture<FetchMetadataSnapshotResponse> result =
                transport.sendFetchSnapshot(request);

        assertThat(result).isSameAs(gateway.snapshotReply);
        assertThat(gateway.snapshotSeen.get()).isSameAs(request);
    }

    @Test
    void transportFailureSurfacesAsIoExceptionThroughGateway() {
        RecordingGateway gateway = new RecordingGateway();
        gateway.fetchReply = new CompletableFuture<>();
        gateway.fetchReply.completeExceptionally(new RuntimeException("leader unreachable"));
        CoordinatorGatewayMetadataStreamTransport transport =
                new CoordinatorGatewayMetadataStreamTransport(gateway);
        RpcMetadataStreamGateway streamGateway = new RpcMetadataStreamGateway(transport, 7);

        assertThatThrownBy(() -> streamGateway.fetch(0L, 100, 10L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Metadata stream RPC failed");
    }

    /** A {@link TestCoordinatorGateway} that records the request and returns a preset future. */
    private static final class RecordingGateway extends TestCoordinatorGateway {
        final AtomicReference<FetchMetadataRequest> fetchSeen = new AtomicReference<>();
        final AtomicReference<FetchMetadataSnapshotRequest> snapshotSeen = new AtomicReference<>();
        CompletableFuture<FetchMetadataResponse> fetchReply =
                CompletableFuture.completedFuture(new FetchMetadataResponse());
        CompletableFuture<FetchMetadataSnapshotResponse> snapshotReply =
                CompletableFuture.completedFuture(new FetchMetadataSnapshotResponse());

        @Override
        public CompletableFuture<FetchMetadataResponse> fetchMetadata(
                FetchMetadataRequest request) {
            fetchSeen.set(request);
            return fetchReply;
        }

        @Override
        public CompletableFuture<FetchMetadataSnapshotResponse> fetchMetadataSnapshot(
                FetchMetadataSnapshotRequest request) {
            snapshotSeen.set(request);
            return snapshotReply;
        }
    }
}
