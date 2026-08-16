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
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Subscriber-side {@link MetadataStreamGateway} that speaks the {@code FetchMetadata}/{@code
 * FetchMetadataSnapshot} proto2 RPC (§5.8.1). It translates the synchronous gateway calls onto an
 * async {@link Transport} (a request→response-future send, backed by a Raft-leader RPC client in
 * production or an in-process handler in tests), then bridges the future back to the synchronous
 * model {@link MetadataStreamSubscriber} expects.
 *
 * <p>Any transport failure (leader unreachable / changed) surfaces as an {@link IOException} so the
 * subscriber's {@link MetadataStreamSubscriber#runLoop()} reconnects and retries from the same
 * cursor.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public class RpcMetadataStreamGateway implements MetadataStreamGateway {

    /** Async transport for the two metadata-stream RPCs; leader-targeted. */
    @Internal
    public interface Transport {
        CompletableFuture<FetchMetadataResponse> sendFetch(
                org.apache.fluss.rpc.messages.FetchMetadataRequest request);

        CompletableFuture<FetchMetadataSnapshotResponse> sendFetchSnapshot(
                org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest request);
    }

    private final Transport transport;
    private final int serverId;
    private volatile long lastKnownEpoch = -1L;

    public RpcMetadataStreamGateway(Transport transport, int serverId) {
        this.transport = transport;
        this.serverId = serverId;
    }

    @Override
    public MetadataStreamService.FetchResult fetch(long fromOffset, int maxRecords, long maxWaitMs)
            throws IOException {
        int waitMs = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, maxWaitMs));
        FetchMetadataResponse response =
                await(
                        transport.sendFetch(
                                MetadataStreamRpcs.buildFetchRequest(
                                        serverId, fromOffset, lastKnownEpoch, maxRecords, waitMs)));
        lastKnownEpoch = response.getLeaderEpoch();
        return MetadataStreamRpcs.fromResponse(response);
    }

    @Override
    public MetadataStreamService.FetchSnapshotResult fetchSnapshot() throws IOException {
        FetchMetadataSnapshotResponse response =
                await(
                        transport.sendFetchSnapshot(
                                MetadataStreamRpcs.buildSnapshotRequest(serverId, lastKnownEpoch)));
        lastKnownEpoch = response.getLeaderEpoch();
        return MetadataStreamRpcs.fromSnapshotResponse(response);
    }

    /** Bridges an async response future to the synchronous gateway model, mapping errors to IOE. */
    private static <T> T await(CompletableFuture<T> future) throws IOException {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Metadata stream RPC failed", cause);
        } catch (java.util.concurrent.CancellationException e) {
            throw new IOException("Metadata stream RPC was cancelled", e);
        }
    }
}
