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
}
