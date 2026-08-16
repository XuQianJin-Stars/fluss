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
import org.apache.fluss.server.SequenceIDCounter;

import javax.annotation.concurrent.ThreadSafe;

import java.nio.charset.StandardCharsets;

/**
 * Raft-backed {@link SequenceIDCounter}. Keeps the exact {@code SequenceIDCounter} contract so
 * callers (currently satisfied by {@code ZkSequenceIDCounter}) are unchanged.
 *
 * <p>Ids are allocated in <b>pre-reserved blocks</b>: a single {@link
 * MetadataRecord.Type#ALLOCATE_ID_RANGE} record advances the persisted high-water mark by {@code
 * blockSize}, and ids inside the block are then handed out locally without any Raft round-trip (the
 * KRaft "producer id block" pattern). On leader failover the new leader resumes from the persisted
 * high-water mark, so ids remain unique — a failover discards the unused tail of the current block,
 * producing a gap but never a reuse.
 *
 * <p><b>No-reuse guarantee (§5.3.3):</b> {@link #reserveThrough} persists the new high-water mark
 * via {@code writer.append(...).get()}, which resolves only after the entry is committed on a
 * majority AND applied locally. Hence a crashed leader can never hand out an id that a new leader
 * also hands out — critical for {@code writerId} (producer fencing/dedup), where reuse would be
 * catastrophic; for {@code tableId}/{@code partitionId} gaps are merely cosmetic.
 *
 * <p><b>Thread rule (§5.6):</b> all mutating methods are {@code synchronized} and call {@code
 * writer.append(...).get()} from the caller's (id-allocation) thread — NEVER from the Ratis apply
 * loop, which would self-deadlock.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.3.2.
 */
@ThreadSafe
@Internal
public class RaftSequenceIDCounter implements SequenceIDCounter {

    private final String counterName; // "table" | "partition" | "writer"
    private final RaftJournalWriter writer;
    private final RaftMetadataStore store;
    private final long blockSize;

    private long nextId; // next id to hand out
    private long blockEnd; // exclusive upper bound of the currently reserved block

    public RaftSequenceIDCounter(
            String counterName, RaftJournalWriter writer, RaftMetadataStore store, long blockSize) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive, was " + blockSize);
        }
        this.counterName = counterName;
        this.writer = writer;
        this.store = store;
        this.blockSize = blockSize;
        long persisted = readPersistedHighWaterMark(); // 0 on fresh cluster
        this.nextId = persisted;
        this.blockEnd = persisted; // force a reservation on first use
    }

    @Override
    public synchronized long getCurrent() throws Exception {
        return nextId;
    }

    @Override
    public synchronized long getAndIncrement() throws Exception {
        return getAndAdd(1L);
    }

    @Override
    public synchronized long getAndAdd(Long delta) throws Exception {
        if (delta == null || delta < 0) {
            throw new IllegalArgumentException("delta must be non-negative, was " + delta);
        }
        long start = nextId;
        long target = nextId + delta;
        if (target > blockEnd) {
            reserveThrough(target); // one Raft append covers many ids
        }
        nextId = target;
        return start;
    }

    /**
     * Persist a new high-water mark {@code >= target} via Raft, rounding up by {@code blockSize}.
     */
    private void reserveThrough(long target) throws Exception {
        long newEnd = Math.max(target, blockEnd + blockSize);
        MetadataRecord record =
                new MetadataRecord(
                        MetadataRecord.Type.ALLOCATE_ID_RANGE,
                        counterName.getBytes(StandardCharsets.UTF_8),
                        longToBytes(newEnd));
        // Resolves after commit+local-apply (§5.2.3), so the new high-water mark is durably
        // persisted before any id from the block is handed out. No separate store.apply() — the
        // state machine is the single writer (§5.1.4).
        writer.append(record).get();
        blockEnd = newEnd;
    }

    private long readPersistedHighWaterMark() {
        byte[] value = store.get(MetadataKeys.idCounterKey(counterName));
        return value == null ? 0L : bytesToLong(value);
    }

    static byte[] longToBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (v & 0xFF);
            v >>= 8;
        }
        return b;
    }

    static long bytesToLong(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[i] & 0xFF);
        }
        return v;
    }
}
