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
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.Message;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftClientReply;
import org.apache.fluss.shaded.ratis.org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Leader-side writer that appends metadata records to the Raft log and completes when the entry is
 * committed AND applied to the local store.
 *
 * <p><b>Thread rule (§5.6):</b> {@link #append} and {@link #appendAll} are ASYNC — they return a
 * {@link CompletableFuture} and never block the calling thread. They MUST NOT be called from the
 * single-threaded apply loop (doing so would self-deadlock, because the future only completes once
 * apply advances). RPC handlers, timers, and id allocation all call this from their own threads and
 * compose on the returned future.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.3.
 */
@Internal
public class RaftJournalWriter implements AutoCloseable {

    /**
     * Default max number of entries sent before waiting for that group to be applied.
     *
     * <p>Ratis caps in-flight async requests per client at {@code
     * RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX} (default <b>100</b> in Ratis 2.4.1);
     * exceeding it makes {@code async().send} fail with {@code ResourceUnavailableException} or
     * block the sending thread. Callers batching per-bucket work (cascade deletes, {@code
     * batchUpdateLeaderAndIsr}) can far exceed that, so {@link #appendAll} sends in chunks of this
     * size and waits for each chunk before starting the next. 64 leaves headroom under the default
     * cap while still amortizing the apply wait across the chunk (analysis §8.9.2). Override with
     * {@code coordinator.raft.append-chunk-size}, which also raises the Ratis cap in lockstep.
     */
    @VisibleForTesting static final int DEFAULT_APPEND_CHUNK_SIZE = 64;

    private final RaftClient raftClient; // built against the local group (RaftMetadataServer group)
    private final FlussMetadataStateMachine stateMachine;
    private final int appendChunkSize;
    private final AppendLatencyStats latencyStats = new AppendLatencyStats();

    public RaftJournalWriter(RaftClient raftClient, FlussMetadataStateMachine stateMachine) {
        this(raftClient, stateMachine, DEFAULT_APPEND_CHUNK_SIZE);
    }

    public RaftJournalWriter(
            RaftClient raftClient, FlussMetadataStateMachine stateMachine, int appendChunkSize) {
        checkArgument(appendChunkSize > 0, "appendChunkSize must be positive");
        this.raftClient = raftClient;
        this.stateMachine = stateMachine;
        this.appendChunkSize = appendChunkSize;
    }

    /** Effective chunk size used by {@link #appendAll}. */
    @VisibleForTesting
    int appendChunkSize() {
        return appendChunkSize;
    }

    /**
     * Asynchronously append and wait for majority commit. The returned future completes with the
     * committed log index once the local state machine has applied it (so a subsequent local read
     * on the leader observes the write — see §5.7). Fails with a Raft exception (e.g. NotLeader) if
     * this node is no longer leader (fencing).
     *
     * <p>Timestamps the send / reply / applied boundaries into {@link #latencyStats()} so the
     * append cost can be split into "Ratis round-trip" versus "commit-to-apply" (§8.9.1).
     */
    public CompletableFuture<Long> append(MetadataRecord record) {
        Message msg = Message.valueOf(ByteString.copyFrom(record.toBytes()));
        long sendNanos = System.nanoTime();
        return raftClient
                .async()
                .send(msg)
                .thenCompose(
                        (RaftClientReply reply) -> {
                            long replyNanos = System.nanoTime();
                            if (!reply.isSuccess()) {
                                latencyStats.recordFailure();
                                CompletableFuture<Long> f = new CompletableFuture<>();
                                f.completeExceptionally(reply.getException());
                                return f;
                            }
                            // wait until our own committed index has been applied locally
                            return stateMachine
                                    .awaitApplied(reply.getLogIndex())
                                    .whenComplete(
                                            (index, error) -> {
                                                if (error != null) {
                                                    latencyStats.recordFailure();
                                                } else {
                                                    latencyStats.record(
                                                            sendNanos,
                                                            replyNanos,
                                                            System.nanoTime());
                                                }
                                            });
                        });
    }

    /**
     * Per-segment append latency attribution (§8.9.1). Read by benchmarks and diagnostics; never
     * used for control flow.
     */
    public AppendLatencyStats latencyStats() {
        return latencyStats;
    }

    /**
     * Sends each record as an <em>independent</em> Raft log entry (FIFO {@link #append}), then
     * completes when <em>all</em> have been committed and applied locally.
     *
     * <p>This is the analysis §8.2 pipeline: one wait for apply instead of serial {@code
     * append().get()} per key. There is no {@code MULTI_OP} record type — each entry is still one
     * {@link MetadataRecord}.
     *
     * <p><b>Chunking:</b> records are sent in groups of {@link #appendChunkSize()}, waiting for
     * each group before sending the next, so a large batch cannot exceed the Ratis client's
     * in-flight request cap (§8.9.2). Batches at or below the chunk size are a single group and
     * behave exactly as before.
     *
     * <p><b>Partial failure:</b> Ratis may already have committed a prefix of the batch when a
     * later send or apply fails. The returned future completes exceptionally with a message that
     * includes how many entries were already applied locally; callers must not treat a failure as
     * "nothing was written". With chunking, earlier chunks are already applied when a later chunk
     * fails, and the reported count spans the whole batch. {@code ALLOCATE_ID_RANGE} must not be
     * mixed into a batch with records that embed the new id — wait for that append to apply first
     * (no-reuse, §5.3.3).
     *
     * @param records non-empty list of records, sent in iteration order
     * @return a future completed with the applied log index of the <em>last</em> record
     */
    public CompletableFuture<Long> appendAll(List<MetadataRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }
        CompletableFuture<Long> result = new CompletableFuture<>();
        sendChunk(records, 0, new AtomicInteger(), result);
        return result;
    }

    /**
     * Sends {@code records[from, from + appendChunkSize)}, then recurses on the next chunk once
     * this one is applied. Recursion happens on the thread completing the chunk, so it never blocks
     * a caller; depth is bounded by {@code ceil(size / appendChunkSize)}.
     */
    private void sendChunk(
            List<MetadataRecord> records,
            int from,
            AtomicInteger appliedSoFar,
            CompletableFuture<Long> result) {
        int to = Math.min(records.size(), from + appendChunkSize);
        List<CompletableFuture<Long>> futures = new ArrayList<CompletableFuture<Long>>(to - from);
        for (int i = from; i < to; i++) {
            futures.add(append(records.get(i)));
        }
        CompletableFuture<?>[] all = new CompletableFuture<?>[futures.size()];
        for (int i = 0; i < futures.size(); i++) {
            all[i] = futures.get(i);
        }
        final boolean last = to == records.size();
        CompletableFuture.allOf(all)
                .whenComplete(
                        (ignored, error) -> {
                            for (int i = 0; i < futures.size(); i++) {
                                CompletableFuture<Long> f = futures.get(i);
                                if (f.isDone() && !f.isCompletedExceptionally()) {
                                    appliedSoFar.incrementAndGet();
                                }
                            }
                            if (error != null) {
                                result.completeExceptionally(
                                        new Exception(
                                                "appendAll failed after sending "
                                                        + records.size()
                                                        + " independent log entries ("
                                                        + appliedSoFar.get()
                                                        + " already applied locally); Ratis may have committed a prefix of the batch.",
                                                error));
                                return;
                            }
                            if (last) {
                                result.complete(futures.get(futures.size() - 1).join());
                                return;
                            }
                            sendChunk(records, to, appliedSoFar, result);
                        });
    }

    /** The underlying Ratis client (admin / membership APIs). */
    public RaftClient raftClient() {
        return raftClient;
    }

    @Override
    public void close() throws Exception {
        raftClient.close();
    }
}
