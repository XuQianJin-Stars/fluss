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

import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit test for {@link RaftRemoteSequenceIDCounter} against a fake {@code AllocateId} transport
 * that simulates the leader-side per-key counters (contiguous ranges), so the client's contract can
 * be verified without a real Ratis cluster.
 */
class RaftRemoteSequenceIDCounterTest {

    /** In-memory stand-in for the leader's per-key {@code RaftSequenceIDCounter}s. */
    private static final class FakeAllocator
            implements Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> {
        private final Map<String, Long> next = new HashMap<>();
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public CompletableFuture<AllocateIdResponse> apply(AllocateIdRequest request) {
            calls.incrementAndGet();
            byte[] key = request.hasCounterKey() ? request.getCounterKey() : new byte[0];
            String name = request.getIdKind() + ":" + java.util.Arrays.toString(key);
            long start = next.getOrDefault(name, 0L);
            next.put(name, start + request.getCount());
            return CompletableFuture.completedFuture(
                    new AllocateIdResponse().setStartId(start).setCount(request.getCount()));
        }
    }

    @Test
    void testAllocateForwardOnlyAndCursor() throws Exception {
        FakeAllocator allocator = new FakeAllocator();
        RaftRemoteSequenceIDCounter counter =
                new RaftRemoteSequenceIDCounter(
                        allocator, AllocateIdRpcServer.KIND_WRITER, new byte[0]);

        assertThat(counter.getAndIncrement()).isEqualTo(0L);
        assertThat(counter.getAndIncrement()).isEqualTo(1L);
        assertThat(counter.getCurrent()).isEqualTo(2L);

        // block reservation returns the start and advances the cursor by the delta
        assertThat(counter.getAndAdd(100L)).isEqualTo(2L);
        assertThat(counter.getCurrent()).isEqualTo(102L);
    }

    @Test
    void testPerKeyIsolation() throws Exception {
        FakeAllocator allocator = new FakeAllocator();
        RaftRemoteSequenceIDCounter columnA =
                new RaftRemoteSequenceIDCounter(
                        allocator,
                        AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN,
                        new byte[] {1, 2, 3});
        RaftRemoteSequenceIDCounter columnB =
                new RaftRemoteSequenceIDCounter(
                        allocator, AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN, new byte[] {9});

        assertThat(columnA.getAndAdd(10L)).isEqualTo(0L);
        assertThat(columnA.getAndIncrement()).isEqualTo(10L);
        // a different key is an independent counter
        assertThat(columnB.getAndIncrement()).isEqualTo(0L);
    }

    @Test
    void testGetAndAddZeroDoesNotCallServerAfterFirstAllocation() throws Exception {
        FakeAllocator allocator = new FakeAllocator();
        RaftRemoteSequenceIDCounter counter =
                new RaftRemoteSequenceIDCounter(
                        allocator, AllocateIdRpcServer.KIND_BUCKET_SNAPSHOT, new byte[] {7});

        assertThat(counter.getAndIncrement()).isEqualTo(0L);
        int callsAfterFirst = allocator.calls.get();
        assertThat(counter.getAndAdd(0L)).isEqualTo(1L); // returns cursor, no RPC
        assertThat(allocator.calls.get()).isEqualTo(callsAfterFirst);
    }

    @Test
    void testRejectsNegativeDeltaAndUninitializedCursorIsZero() throws Exception {
        FakeAllocator allocator = new FakeAllocator();
        RaftRemoteSequenceIDCounter counter =
                new RaftRemoteSequenceIDCounter(
                        allocator, AllocateIdRpcServer.KIND_WRITER, new byte[0]);

        assertThatThrownBy(() -> counter.getAndAdd(-1L))
                .isInstanceOf(IllegalArgumentException.class);
        // getCurrent before any allocation matches ZK uninitialized semantics (0)
        assertThat(counter.getCurrent()).isEqualTo(0L);
    }
}
