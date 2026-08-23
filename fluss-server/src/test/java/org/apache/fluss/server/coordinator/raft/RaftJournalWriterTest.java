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

import java.nio.charset.StandardCharsets;
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

    /**
     * A batch at the chunk boundary must still be sent as one group, so small batches keep the
     * single-apply-wait behaviour of §8.8.
     */
    @Test
    void testAppendAllSendsChunkSizeAtOnce() throws Exception {
        FakeWriter writer = new FakeWriter();
        int chunk = writer.appendChunkSize();
        assertThat(chunk).isEqualTo(RaftJournalWriter.DEFAULT_APPEND_CHUNK_SIZE);
        List<MetadataRecord> records = records(chunk);

        CompletableFuture<Long> all = writer.appendAll(records);
        assertThat(writer.sent).hasSize(chunk);

        for (int i = 0; i < records.size(); i++) {
            writer.complete(i, 100L + i);
        }
        assertThat(all.get(5, TimeUnit.SECONDS)).isEqualTo(100L + chunk - 1);
    }

    /**
     * Batches larger than the chunk size must NOT be sent all at once: Ratis caps in-flight async
     * requests per client (default 100), so a per-bucket batch has to be throttled (§8.9.2).
     */
    @Test
    void testAppendAllChunksLargeBatch() throws Exception {
        FakeWriter writer = new FakeWriter();
        int chunk = writer.appendChunkSize();
        int total = chunk * 2 + 5;
        List<MetadataRecord> records = records(total);

        CompletableFuture<Long> all = writer.appendAll(records);

        // Only the first chunk is in flight.
        assertThat(writer.sent).hasSize(chunk);
        assertThat(all).isNotDone();

        // Completing the first chunk releases the second, and so on.
        completeRange(writer, 0, chunk);
        assertThat(writer.sent).hasSize(chunk * 2);
        assertThat(all).isNotDone();

        completeRange(writer, chunk, chunk * 2);
        assertThat(writer.sent).hasSize(total);
        assertThat(all).isNotDone();

        completeRange(writer, chunk * 2, total);
        assertThat(all.get(5, TimeUnit.SECONDS)).isEqualTo(1000L + total - 1);
        assertThat(writer.sent).containsExactlyElementsOf(records);
    }

    /** A custom chunk size (coordinator.raft.append-chunk-size) must drive the grouping. */
    @Test
    void testAppendAllHonoursConfiguredChunkSize() throws Exception {
        FakeWriter writer = new FakeWriter(3);
        assertThat(writer.appendChunkSize()).isEqualTo(3);
        List<MetadataRecord> records = records(7);

        CompletableFuture<Long> all = writer.appendAll(records);
        assertThat(writer.sent).hasSize(3);

        completeRange(writer, 0, 3);
        assertThat(writer.sent).hasSize(6);

        completeRange(writer, 3, 6);
        assertThat(writer.sent).hasSize(7);

        completeRange(writer, 6, 7);
        assertThat(all.get(5, TimeUnit.SECONDS)).isEqualTo(1000L + 6);
    }

    @Test
    void testRejectsNonPositiveChunkSize() {
        assertThatThrownBy(() -> new FakeWriter(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("appendChunkSize must be positive");
    }

    /** A failure in a later chunk must report the entries already applied by earlier chunks. */
    @Test
    void testAppendAllChunkedFailureReportsAppliedPrefix() {
        FakeWriter writer = new FakeWriter();
        int chunk = writer.appendChunkSize();
        CompletableFuture<Long> all = writer.appendAll(records(chunk + 3));

        completeRange(writer, 0, chunk); // whole first chunk applied
        writer.fail(chunk, new RuntimeException("send failed"));
        writer.complete(chunk + 1, 2L);
        writer.complete(chunk + 2, 3L);

        assertThatThrownBy(() -> all.get(5, TimeUnit.SECONDS))
                .cause()
                // chunk entries from the first group + 2 successful ones in the failing group
                .hasMessageContaining((chunk + 2) + " already applied locally")
                .hasMessageContaining("prefix of the batch");
    }

    private static void completeRange(FakeWriter writer, int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            writer.complete(i, 1000L + i);
        }
    }

    private static List<MetadataRecord> records(int count) {
        List<MetadataRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(
                    new MetadataRecord(
                            MetadataRecord.Type.REGISTER_DATABASE,
                            ("k" + i).getBytes(StandardCharsets.UTF_8),
                            ("v" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return records;
    }

    private static MetadataRecord record(byte key) {
        return new MetadataRecord(
                MetadataRecord.Type.REGISTER_DATABASE, new byte[] {key}, new byte[] {key});
    }

    /**
     * Overrides {@link RaftJournalWriter#append} so tests can complete apply independently of
     * Ratis. {@code pending} is copy-on-write because chunked batches send the next group from the
     * thread that completes the previous one.
     */
    static final class FakeWriter extends RaftJournalWriter {
        final List<MetadataRecord> sent = new CopyOnWriteArrayList<>();
        private final List<CompletableFuture<Long>> pending = new CopyOnWriteArrayList<>();

        FakeWriter() {
            super(mock(RaftClient.class), mock(FlussMetadataStateMachine.class));
        }

        FakeWriter(int appendChunkSize) {
            super(mock(RaftClient.class), mock(FlussMetadataStateMachine.class), appendChunkSize);
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
