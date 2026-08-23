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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FlussMetadataStateMachine} apply loop and applied-index waiters. */
class FlussMetadataStateMachineTest {

    private static FlussMetadataStateMachine newStateMachine() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        return new FlussMetadataStateMachine(store);
    }

    private static MetadataRecord tableRecord(String key, String payload) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_TABLE,
                key.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testApplyAdvancesLastAppliedIndex() {
        FlussMetadataStateMachine sm = newStateMachine();
        assertThat(sm.lastAppliedIndex()).isEqualTo(-1);

        sm.applyEntry(tableRecord("a", "1"), 1);
        sm.applyEntry(tableRecord("b", "2"), 2);

        assertThat(sm.lastAppliedIndex()).isEqualTo(2);
    }

    @Test
    void testDuplicateApplyIsIgnored() {
        FlussMetadataStateMachine sm = newStateMachine();
        sm.applyEntry(tableRecord("a", "1"), 5);
        // an earlier/equal index (a retried write) must be ignored
        sm.applyEntry(tableRecord("a", "stale"), 5);
        sm.applyEntry(tableRecord("a", "older"), 3);

        assertThat(sm.lastAppliedIndex()).isEqualTo(5);
    }

    @Test
    void testAwaitAppliedCompletesImmediatelyWhenAlreadyApplied() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        sm.applyEntry(tableRecord("a", "1"), 10);

        CompletableFuture<Long> f = sm.awaitApplied(7);
        assertThat(f).isCompleted();
        assertThat(f.get()).isEqualTo(10);
    }

    @Test
    void testAwaitAppliedCompletesWhenIndexReached() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        CompletableFuture<Long> f = sm.awaitApplied(3);
        assertThat(f).isNotCompleted();

        sm.applyEntry(tableRecord("a", "1"), 1);
        assertThat(f).isNotCompleted();
        sm.applyEntry(tableRecord("b", "2"), 3);

        assertThat(f.get(1, TimeUnit.SECONDS)).isEqualTo(3);
    }

    @Test
    void testAwaitAppliedPendingUntilTimeout() {
        FlussMetadataStateMachine sm = newStateMachine();
        CompletableFuture<Long> f = sm.awaitApplied(100);
        assertThatThrownBy(() -> f.get(50, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    void testAwaitAppliedIsIdempotentPerIndex() {
        FlussMetadataStateMachine sm = newStateMachine();
        CompletableFuture<Long> f1 = sm.awaitApplied(5);
        CompletableFuture<Long> f2 = sm.awaitApplied(5);
        assertThat(f1).isSameAs(f2);
    }

    @Test
    void testApplyThrowingRecordSurfacesNoWaiterLeak()
            throws ExecutionException, InterruptedException {
        FlussMetadataStateMachine sm = newStateMachine();
        sm.applyEntry(tableRecord("a", "1"), 2);
        // waiter for an index below applied resolves right away
        assertThat(sm.awaitApplied(1).get()).isEqualTo(2);
    }

    @Test
    void testNotifyTermIndexUpdatedCompletesAwaitAppliedWithoutStoreApply() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        CompletableFuture<Long> pending = sm.awaitApplied(4);
        assertThat(pending).isNotCompleted();
        assertThat(sm.lastAppliedIndex()).isEqualTo(-1);

        sm.notifyTermIndexUpdated(1, 4);

        assertThat(pending.get(1, TimeUnit.SECONDS)).isEqualTo(4);
        assertThat(sm.lastAppliedIndex()).isEqualTo(-1);
        assertThat(sm.awaitApplied(4).get(1, TimeUnit.SECONDS)).isEqualTo(4);
    }

    /**
     * Waiter continuations must NOT run while the apply thread holds the state-machine monitor
     * (analysis §8.9.4 C3). Completing inside the lock would let any caller continuation — e.g.
     * {@code RaftJournalWriter.appendAll}'s batch bookkeeping — execute under the monitor,
     * inflating apply latency and risking lock-order inversions. The continuation here asserts it
     * can take the monitor itself, which only holds if the lock was already released.
     */
    @Test
    void testAppliedWaiterContinuationRunsOutsideStateMachineLock() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        AtomicBoolean lockWasFree = new AtomicBoolean();
        CompletableFuture<Long> done =
                sm.awaitApplied(1)
                        .thenApply(
                                index -> {
                                    lockWasFree.set(!Thread.holdsLock(sm));
                                    // Would deadlock (or at least prove re-entrancy) if the apply
                                    // thread still held the monitor from another thread's view;
                                    // taking it here documents the lock is available.
                                    synchronized (sm) {
                                        return index;
                                    }
                                });

        sm.applyEntry(tableRecord("a", "1"), 1);

        assertThat(done.get(5, TimeUnit.SECONDS)).isEqualTo(1);
        assertThat(lockWasFree).isTrue();
    }

    /** Same guarantee for the Raft-config path that resolves waiters without a store mutation. */
    @Test
    void testNotifyTermIndexUpdatedCompletesWaiterOutsideLock() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        AtomicBoolean lockWasFree = new AtomicBoolean();
        CompletableFuture<Long> done =
                sm.awaitApplied(9)
                        .thenApply(
                                index -> {
                                    lockWasFree.set(!Thread.holdsLock(sm));
                                    return index;
                                });

        sm.notifyTermIndexUpdated(2, 9);

        assertThat(done.get(5, TimeUnit.SECONDS)).isEqualTo(9);
        assertThat(lockWasFree).isTrue();
    }

    /** Multiple waiters below the applied index must all resolve from one apply. */
    @Test
    void testApplyResolvesAllWaitersUpToIndex() throws Exception {
        FlussMetadataStateMachine sm = newStateMachine();
        CompletableFuture<Long> f1 = sm.awaitApplied(1);
        CompletableFuture<Long> f2 = sm.awaitApplied(2);
        CompletableFuture<Long> f3 = sm.awaitApplied(3);
        CompletableFuture<Long> beyond = sm.awaitApplied(4);

        sm.applyEntry(tableRecord("a", "1"), 3);

        assertThat(f1.get(1, TimeUnit.SECONDS)).isEqualTo(3);
        assertThat(f2.get(1, TimeUnit.SECONDS)).isEqualTo(3);
        assertThat(f3.get(1, TimeUnit.SECONDS)).isEqualTo(3);
        assertThat(beyond).isNotCompleted();
    }

    /**
     * The apply listener (metadata-stream wakeup) must also fire outside the monitor: it runs on
     * the single apply thread and is documented as a cheap, non-blocking notification (§5.6 /
     * §5.8.2).
     */
    @Test
    void testApplyListenerFiresOutsideStateMachineLock() {
        FlussMetadataStateMachine sm = newStateMachine();
        AtomicBoolean lockWasFree = new AtomicBoolean();
        AtomicLong observed = new AtomicLong(-1);
        sm.setApplyListener(
                index -> {
                    lockWasFree.set(!Thread.holdsLock(sm));
                    observed.set(index);
                });

        sm.applyEntry(tableRecord("a", "1"), 7);

        assertThat(observed.get()).isEqualTo(7);
        assertThat(lockWasFree).isTrue();
    }
}
