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
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.server.SequenceIDCounter;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Client-side {@link SequenceIDCounter} for a tablet server on the Raft metadata backend. Each
 * mutating call reserves a contiguous id range from the coordinator leader via the {@code
 * AllocateId} RPC (§5.3), replacing the tablet server's ZooKeeper-backed {@code
 * ZkSequenceIDCounter} without changing the {@code SequenceIDCounter} contract its callers rely on.
 * The durable, no-reuse reservation happens on the leader ({@link AllocateIdRpcServer} over a
 * {@link RaftSequenceIDCounter}), so this client keeps no persistent state — it only remembers the
 * last boundary it has been handed so {@link #getCurrent()} can report the next id within its own
 * lifecycle.
 *
 * <p>The three id kinds and their counter keys (see {@link AllocateIdRpcServer}):
 *
 * <ul>
 *   <li>{@code KIND_WRITER} — empty key (single global writer id).
 *   <li>{@code KIND_AUTO_INCREMENT_COLUMN} — {@code (tablePath, columnId)} bytes.
 *   <li>{@code KIND_BUCKET_SNAPSHOT} — {@code TableBucket} bytes.
 * </ul>
 *
 * <p>The RPC transport is injected as a narrow function ({@code request ->
 * gateway.allocateId(request)} over the tablet server's {@code CoordinatorGateway} proxy, which
 * already routes to the current leader) so this class is unit-testable without Netty and does not
 * couple to the full gateway.
 *
 * <p><b>getCurrent note:</b> before the first allocation this returns {@code 0}, matching {@code
 * ZkSequenceIDCounter}'s uninitialized-znode semantics so test helpers (and any caller that samples
 * the cursor before {@link #getAndIncrement()}) behave consistently across backends. After the
 * first allocation, it returns this instance's local allocation cursor (the next id it would hand
 * out). The authoritative cross-process "latest committed" value for kinds that survive failover is
 * the metadata store / stream cache.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.3.
 */
@ThreadSafe
@Internal
public class RaftRemoteSequenceIDCounter implements SequenceIDCounter {

    private static final byte[] EMPTY_KEY = new byte[0];

    private final Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> transport;
    private final int idKind;
    private final byte[] counterKey;

    // Local allocation cursor: the next id this instance would hand out. Set from the leader's
    // committed reservation on each allocate; -1 until the first allocation.
    private long nextId = -1L;

    public RaftRemoteSequenceIDCounter(
            Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> transport,
            int idKind,
            byte[] counterKey) {
        this.transport = transport;
        this.idKind = idKind;
        this.counterKey = counterKey == null ? EMPTY_KEY : counterKey;
    }

    @Override
    public synchronized long getCurrent() throws Exception {
        if (nextId < 0) {
            // Match ZkSequenceIDCounter's uninitialized znode semantics (starts at 0) so callers
            // like FlussClusterExtension.triggerSnapshot can sample the cursor before the first
            // allocation. After the first getAndIncrement/getAndAdd, nextId is the next id to
            // hand out.
            return 0L;
        }
        return nextId;
    }

    @Override
    public long getAndIncrement() throws Exception {
        return getAndAdd(1L);
    }

    @Override
    public synchronized long getAndAdd(Long delta) throws Exception {
        if (delta == null || delta < 0) {
            throw new IllegalArgumentException("delta must be non-negative, was " + delta);
        }
        if (delta == 0) {
            // Preserve getAndAdd(0) semantics (return the current start without advancing) without
            // an RPC when we already know the cursor; otherwise fall through to a count>=1 path is
            // not possible, so require at least one prior allocation.
            return getCurrent();
        }
        AllocateIdRequest request =
                new AllocateIdRequest().setIdKind(idKind).setCount(delta).setCounterKey(counterKey);
        AllocateIdResponse response = transport.apply(request).get();
        long start = response.getStartId();
        nextId = start + delta;
        return start;
    }
}
