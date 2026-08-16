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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Leader-side RPC handler for TabletServer id/sequence allocation (§5.3): adapts the {@code
 * AllocateId} proto2 request onto {@link RaftSequenceIDCounter}s. On the Raft metadata backend this
 * replaces the three ZooKeeper-backed sequence generators a tablet server used:
 *
 * <ul>
 *   <li>{@link #KIND_WRITER} — the single global writer id (producer fencing/dedup). Routed to the
 *       {@code RaftMetadataClient}'s owned "writer" counter so the persisted high-water mark has a
 *       single writer (a duplicate same-name counter would collide on the shared mark).
 *   <li>{@link #KIND_AUTO_INCREMENT_COLUMN} — one counter per {@code (tablePath, columnId)} (kv
 *       auto-increment column value).
 *   <li>{@link #KIND_BUCKET_SNAPSHOT} — one counter per {@code TableBucket} (kv snapshot id).
 * </ul>
 *
 * <p>The keyed counters (column / snapshot) are created lazily and cached under a namespaced
 * counter name ({@code "aic:"+hex(key)} / {@code "bsnap:"+hex(key)}) that never equals the reserved
 * {@code table}/{@code partition}/{@code writer} names, so each distinct key has exactly one
 * counter instance on a given leader — required for correctness, since {@link
 * RaftSequenceIDCounter} rounds reservations off its own in-memory block end (two instances of the
 * same name would overlap).
 *
 * <p>The caller receives a contiguous range {@code [start_id, start_id + count)} it owns
 * exclusively. No-reuse across leader failover is inherited from {@link RaftSequenceIDCounter}: the
 * high-water mark is committed before any id is handed out, and a new leader resumes from it
 * (discarding the unused tail of a block = a gap, never a reuse). Only the leader serves these
 * calls — the underlying {@code writer.append(...)} does {@code requireLeader()} and rejects a
 * stale leader.
 *
 * <p>Build-component-wire-later (mirrors {@link TabletServerRegistrationRpcServer}); the
 * Raft-backed {@code CoordinatorService} delegates to it.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.3.
 */
@Internal
public class AllocateIdRpcServer {

    /** Single global writer id (producer fencing/dedup). Empty counter key. */
    public static final int KIND_WRITER = 0;
    /** Per {@code (tablePath, columnId)} kv auto-increment column value. */
    public static final int KIND_AUTO_INCREMENT_COLUMN = 1;
    /** Per {@code TableBucket} kv snapshot id. */
    public static final int KIND_BUCKET_SNAPSHOT = 2;

    private static final byte[] EMPTY_KEY = new byte[0];

    private final RaftMetadataClient client;
    private final RaftJournalWriter writer;
    private final RaftMetadataStore store;
    private final long blockSize;

    // Lazily-created keyed counters (column / snapshot). One instance per namespaced name.
    private final ConcurrentHashMap<String, RaftSequenceIDCounter> keyedCounters =
            new ConcurrentHashMap<>();

    public AllocateIdRpcServer(RaftMetadataClient client, long blockSize) {
        this.client = client;
        this.writer = client.journalWriter();
        this.store = client.store();
        this.blockSize = blockSize;
    }

    /** Serves {@link org.apache.fluss.rpc.protocol.ApiKeys#ALLOCATE_ID}. */
    public CompletableFuture<AllocateIdResponse> allocateId(AllocateIdRequest request) {
        CompletableFuture<AllocateIdResponse> future = new CompletableFuture<>();
        try {
            long count = request.getCount();
            if (count < 1) {
                throw new IllegalArgumentException("AllocateId count must be >= 1, was " + count);
            }
            int kind = request.getIdKind();
            byte[] key = request.hasCounterKey() ? request.getCounterKey() : EMPTY_KEY;
            long start;
            switch (kind) {
                case KIND_WRITER:
                    start = client.allocateWriterIdBlock(count);
                    break;
                case KIND_AUTO_INCREMENT_COLUMN:
                    start = keyedCounter("aic:" + hex(key)).getAndAdd(count);
                    break;
                case KIND_BUCKET_SNAPSHOT:
                    start = keyedCounter("bsnap:" + hex(key)).getAndAdd(count);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown AllocateId id_kind: " + kind);
            }
            future.complete(new AllocateIdResponse().setStartId(start).setCount(count));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private RaftSequenceIDCounter keyedCounter(String name) {
        return keyedCounters.computeIfAbsent(
                name, n -> new RaftSequenceIDCounter(n, writer, store, blockSize));
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
