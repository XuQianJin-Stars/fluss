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

import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the metadata-stream proto2 wire mapping ({@link MetadataStreamRpcs}) and the
 * subscriber-side {@link RpcMetadataStreamGateway} (§5.8.1). Proves the domain↔proto translation is
 * symmetric and that transport failures map to {@link IOException} so the subscriber reconnects.
 */
class MetadataStreamRpcsTest {

    @Test
    void testFetchResponseRoundTripPreservesRecordsAndOffsets() {
        List<MetadataStreamService.StreamRecord> records = new ArrayList<>();
        records.add(
                new MetadataStreamService.StreamRecord(5L, "r5".getBytes(StandardCharsets.UTF_8)));
        records.add(
                new MetadataStreamService.StreamRecord(6L, "r6".getBytes(StandardCharsets.UTF_8)));
        MetadataStreamService.FetchResult original =
                new MetadataStreamService.FetchResult(3L, 6L, records, 7L, false);

        MetadataStreamService.FetchResult roundTripped =
                MetadataStreamRpcs.fromResponse(MetadataStreamRpcs.toResponse(original));

        assertThat(roundTripped.leaderEpoch()).isEqualTo(3L);
        assertThat(roundTripped.highMetadataOffset()).isEqualTo(6L);
        assertThat(roundTripped.nextOffset()).isEqualTo(7L);
        assertThat(roundTripped.snapshotRequired()).isFalse();
        assertThat(roundTripped.records()).hasSize(2);
        assertThat(roundTripped.records().get(0).offset()).isEqualTo(5L);
        assertThat(roundTripped.records().get(1).bytes())
                .isEqualTo("r6".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testFetchResponseRoundTripPreservesSnapshotRequired() {
        MetadataStreamService.FetchResult original =
                new MetadataStreamService.FetchResult(9L, 100L, new ArrayList<>(), 1L, true);

        MetadataStreamService.FetchResult roundTripped =
                MetadataStreamRpcs.fromResponse(MetadataStreamRpcs.toResponse(original));

        assertThat(roundTripped.snapshotRequired()).isTrue();
        assertThat(roundTripped.records()).isEmpty();
        assertThat(roundTripped.leaderEpoch()).isEqualTo(9L);
    }

    @Test
    void testSnapshotResponseRoundTripPreservesImageAndOffset() {
        byte[] image = new byte[] {1, 2, 3, 4, 5};
        MetadataStreamService.FetchSnapshotResult original =
                new MetadataStreamService.FetchSnapshotResult(2L, 42L, image);

        MetadataStreamService.FetchSnapshotResult roundTripped =
                MetadataStreamRpcs.fromSnapshotResponse(
                        MetadataStreamRpcs.toSnapshotResponse(original));

        assertThat(roundTripped.leaderEpoch()).isEqualTo(2L);
        assertThat(roundTripped.snapshotMetadataOffset()).isEqualTo(42L);
        assertThat(roundTripped.image()).isEqualTo(image);
    }

    @Test
    void testGatewayTranslatesFetchThroughTransport() throws Exception {
        FetchMetadataResponse canned =
                MetadataStreamRpcs.toResponse(
                        new MetadataStreamService.FetchResult(
                                1L,
                                3L,
                                Arrays.asList(
                                        new MetadataStreamService.StreamRecord(
                                                3L, "x".getBytes(StandardCharsets.UTF_8))),
                                4L,
                                false));
        RpcMetadataStreamGateway.Transport transport =
                new RpcMetadataStreamGateway.Transport() {
                    @Override
                    public CompletableFuture<FetchMetadataResponse> sendFetch(
                            org.apache.fluss.rpc.messages.FetchMetadataRequest request) {
                        // echo the request cursor back so we can assert it was carried.
                        assertThat(request.getFromOffset()).isEqualTo(3L);
                        assertThat(request.getServerId()).isEqualTo(7);
                        return CompletableFuture.completedFuture(canned);
                    }

                    @Override
                    public CompletableFuture<FetchMetadataSnapshotResponse> sendFetchSnapshot(
                            org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };

        RpcMetadataStreamGateway gateway = new RpcMetadataStreamGateway(transport, 7);
        MetadataStreamService.FetchResult result = gateway.fetch(3L, 100, 0L);

        assertThat(result.records()).hasSize(1);
        assertThat(result.nextOffset()).isEqualTo(4L);
    }

    @Test
    void testGatewayMapsTransportFailureToIOException() {
        RpcMetadataStreamGateway.Transport failing =
                new RpcMetadataStreamGateway.Transport() {
                    @Override
                    public CompletableFuture<FetchMetadataResponse> sendFetch(
                            org.apache.fluss.rpc.messages.FetchMetadataRequest request) {
                        CompletableFuture<FetchMetadataResponse> f = new CompletableFuture<>();
                        f.completeExceptionally(new IOException("leader unreachable"));
                        return f;
                    }

                    @Override
                    public CompletableFuture<FetchMetadataSnapshotResponse> sendFetchSnapshot(
                            org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };

        RpcMetadataStreamGateway gateway = new RpcMetadataStreamGateway(failing, 1);
        assertThatThrownBy(() -> gateway.fetch(1L, 100, 0L))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("leader unreachable");
    }
}
