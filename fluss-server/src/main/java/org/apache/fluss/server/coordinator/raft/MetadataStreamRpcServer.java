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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Leader-side RPC handler for the cross-process metadata stream (§5.8): adapts the {@code
 * FetchMetadata}/{@code FetchMetadataSnapshot} proto2 requests onto the local {@link
 * MetadataStreamService} and builds the proto responses. This is exactly the logic the Raft-backed
 * {@code CoordinatorService} will delegate to once the coordinator RPC gateway is wired to serve
 * these API keys (deferred until the coordinator runs on the Raft backend — T4); keeping it as a
 * standalone, testable unit is the build-component-wire-later pattern used throughout this branch.
 *
 * <p>Only the leader serves these calls. A non-leader / stale leader is surfaced to the subscriber
 * via the response {@code leaderEpoch} (fencing) — the caller re-targets the current leader.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.2.
 */
@Internal
public class MetadataStreamRpcServer {

    private final MetadataStreamService streamService;

    public MetadataStreamRpcServer(MetadataStreamService streamService) {
        this.streamService = streamService;
    }

    /** Serves {@link org.apache.fluss.rpc.protocol.ApiKeys#FETCH_METADATA}. */
    public CompletableFuture<FetchMetadataResponse> fetchMetadata(FetchMetadataRequest request) {
        CompletableFuture<FetchMetadataResponse> future = new CompletableFuture<>();
        try {
            int maxRecords = request.hasMaxRecords() ? request.getMaxRecords() : 0;
            long maxWaitMs = request.hasMaxWaitMs() ? request.getMaxWaitMs() : 0L;
            MetadataStreamService.FetchResult result =
                    streamService.fetch(request.getFromOffset(), maxRecords, maxWaitMs);
            future.complete(MetadataStreamRpcs.toResponse(result));
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /** Serves {@link org.apache.fluss.rpc.protocol.ApiKeys#FETCH_METADATA_SNAPSHOT}. */
    public CompletableFuture<FetchMetadataSnapshotResponse> fetchMetadataSnapshot(
            FetchMetadataSnapshotRequest request) {
        CompletableFuture<FetchMetadataSnapshotResponse> future = new CompletableFuture<>();
        try {
            MetadataStreamService.FetchSnapshotResult result = streamService.fetchSnapshot();
            future.complete(MetadataStreamRpcs.toSnapshotResponse(result));
        } catch (RuntimeException e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
