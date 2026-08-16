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
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.Message;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftClientReply;
import org.apache.fluss.shaded.ratis.org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.concurrent.CompletableFuture;

/**
 * Leader-side writer that appends metadata records to the Raft log and completes when the entry is
 * committed AND applied to the local store.
 *
 * <p><b>Thread rule (§5.6):</b> {@link #append} is ASYNC — it returns a {@link CompletableFuture}
 * and never blocks the calling thread. It MUST NOT be called from the single-threaded apply loop
 * (doing so would self-deadlock, because the future only completes once apply advances). RPC
 * handlers, timers, and id allocation all call this from their own threads and compose on the
 * returned future.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.3.
 */
@Internal
public class RaftJournalWriter implements AutoCloseable {

    private final RaftClient raftClient; // built against the local group (RaftMetadataServer group)
    private final FlussMetadataStateMachine stateMachine;

    public RaftJournalWriter(RaftClient raftClient, FlussMetadataStateMachine stateMachine) {
        this.raftClient = raftClient;
        this.stateMachine = stateMachine;
    }

    /**
     * Asynchronously append and wait for majority commit. The returned future completes with the
     * committed log index once the local state machine has applied it (so a subsequent local read
     * on the leader observes the write — see §5.7). Fails with a Raft exception (e.g. NotLeader) if
     * this node is no longer leader (fencing).
     */
    public CompletableFuture<Long> append(MetadataRecord record) {
        Message msg = Message.valueOf(ByteString.copyFrom(record.toBytes()));
        return raftClient
                .async()
                .send(msg)
                .thenCompose(
                        (RaftClientReply reply) -> {
                            if (!reply.isSuccess()) {
                                CompletableFuture<Long> f = new CompletableFuture<>();
                                f.completeExceptionally(reply.getException());
                                return f;
                            }
                            // wait until our own committed index has been applied locally
                            return stateMachine.awaitApplied(reply.getLogIndex());
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
