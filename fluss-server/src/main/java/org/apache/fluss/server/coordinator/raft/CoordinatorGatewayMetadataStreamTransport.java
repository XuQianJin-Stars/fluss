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
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Production {@link RpcMetadataStreamGateway.Transport} for a tablet server (or standby
 * coordinator) on the Raft backend: forwards the two metadata-stream RPCs ({@code
 * FetchMetadata}/{@code FetchMetadataSnapshot}, §5.8.1) to the coordinator leader through a {@link
 * CoordinatorGateway} proxy.
 *
 * <p>The supplied gateway is expected to be a {@code GatewayClientProxy} that re-resolves the
 * current coordinator server node on every call, so leader failover is handled by the proxy's node
 * supplier (the tablet server points that supplier at the leader it discovers via {@code
 * CoordinatorDiscovery} at boot, then at the coordinator node the metadata stream itself projects
 * into the cache once flowing). This class deliberately does no error mapping: a transport failure
 * completes the returned future exceptionally, and {@link RpcMetadataStreamGateway} converts that
 * to an {@link java.io.IOException} so the subscriber's {@code runLoop()} reconnects from the same
 * cursor.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public class CoordinatorGatewayMetadataStreamTransport
        implements RpcMetadataStreamGateway.Transport {

    private final CoordinatorGateway coordinatorGateway;

    public CoordinatorGatewayMetadataStreamTransport(CoordinatorGateway coordinatorGateway) {
        this.coordinatorGateway = coordinatorGateway;
    }

    @Override
    public CompletableFuture<FetchMetadataResponse> sendFetch(FetchMetadataRequest request) {
        return coordinatorGateway.fetchMetadata(request);
    }

    @Override
    public CompletableFuture<FetchMetadataSnapshotResponse> sendFetchSnapshot(
            FetchMetadataSnapshotRequest request) {
        return coordinatorGateway.fetchMetadataSnapshot(request);
    }
}
