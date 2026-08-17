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

import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link RaftJournalWriter#appendAll}: the batch future completes only when every
 * entry is applied, sends stay in list order, and a later failure still reports that a prefix may
 * already be applied.
 */
class RaftJournalWriterTest {

    @Test
    void testAppendAllCompletesWhenAllApplied() throws Exception {
        FakeWriter writer = new FakeWriter();
        MetadataRecord a = record((byte) 1);
        MetadataRecord b = record((byte) 2);
        MetadataRecord c = record((byte) 3);

        CompletableFuture<Long> all = writer.appendAll(Arrays.asList(a, b, c));
        assertThat(all).isNotDone();
        assertThat(writer.sent).containsExactly(a, b, c);

        writer.complete(0, 10L);
        assertThat(all).isNotDone();
        writer.complete(1, 11L);
        assertThat(all).isNotDone();
        writer.complete(2, 12L);

        assertThat(all.get(5, TimeUnit.SECONDS)).isEqualTo(12L);
    }

    @Test
    void testAppendAllWaitsForOutOfOrderApply() throws Exception {
        FakeWriter writer = new FakeWriter();
        MetadataRecord a = record((byte) 1);
        MetadataRecord b = record((byte) 2);
        MetadataRecord c = record((byte) 3);

        CompletableFuture<Long> all = writer.appendAll(Arrays.asList(a, b, c));
        // Apply the last entry first; the batch must still wait for the earlier ones.
        writer.complete(2, 30L);
        assertThat(all).isNotDone();
        writer.complete(0, 10L);
        assertThat(all).isNotDone();
        writer.complete(1, 20L);

        assertThat(all.get(5, TimeUnit.SECONDS)).isEqualTo(30L);
        assertThat(writer.sent).containsExactly(a, b, c);
    }

    @Test
    void testAppendAllFailsWithPrefixAlreadyApplied() {
        FakeWriter writer = new FakeWriter();
        CompletableFuture<Long> all =
                writer.appendAll(
                        Arrays.asList(record((byte) 1), record((byte) 2), record((byte) 3)));

        writer.complete(0, 1L);
        writer.fail(1, new RuntimeException("send failed"));
        writer.complete(2, 3L);

        assertThatThrownBy(() -> all.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(Exception.class)
                .cause()
                .hasMessageContaining("already applied locally")
                .hasMessageContaining("prefix of the batch");
    }

    @Test
    void testAppendAllRejectsEmpty() {
        FakeWriter writer = new FakeWriter();
        assertThatThrownBy(() -> writer.appendAll(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
        assertThatThrownBy(() -> writer.appendAll(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    private static MetadataRecord record(byte key) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_DATABASE, new byte[] {key}, new byte[] {key});
    }

    /**
     * Overrides {@link RaftJournalWriter#append} so tests can complete apply independently of
     * Ratis.
     */
    static final class FakeWriter extends RaftJournalWriter {
        final List<MetadataRecord> sent = new CopyOnWriteArrayList<>();
        private final List<CompletableFuture<Long>> pending = new ArrayList<>();

        FakeWriter() {
            super(mock(RaftClient.class), mock(FlussMetadataStateMachine.class));
        }

        @Override
        public CompletableFuture<Long> append(MetadataRecord record) {
            sent.add(record);
            CompletableFuture<Long> future = new CompletableFuture<>();
            pending.add(future);
            return future;
        }

        void complete(int i, long index) {
            pending.get(i).complete(index);
        }

        void fail(int i, Throwable error) {
            pending.get(i).completeExceptionally(error);
        }
    }
}
