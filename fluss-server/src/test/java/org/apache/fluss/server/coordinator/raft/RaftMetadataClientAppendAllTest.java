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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.PartitionAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClient;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests that {@link RaftMetadataClient} pipelines multi-key DDL after id allocation has
 * applied, and does not wait per key inside the batch (analysis §8.2).
 */
class RaftMetadataClientAppendAllTest {

    @Test
    void testIdAllocationAppliesBeforeDependentCreateTableRecords() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        RaftMetadataClient client = newClient(writer);
        TablePath path = TablePath.of("db", "orders");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(1, 2, 3)).build();

        CompletableFuture<Void> call =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                long tableId = client.getTableIdAndIncrement();
                                client.registerTableSchemaAssignmentAndMetadata(
                                        path,
                                        schema,
                                        assignment,
                                        TableRegistration.newTable(
                                                tableId, "file:///tmp/remote", descriptor));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        waitUntil(
                () -> writer.sentTypes().size() >= 4,
                "schema/assignment/table were not sent after id allocation");

        List<String> events = new ArrayList<>(writer.events);
        int allocApplied = events.indexOf("applied:ALLOCATE_ID_RANGE");
        int schemaSent = events.indexOf("send:REGISTER_SCHEMA");
        int assignSent = events.indexOf("send:UPDATE_TABLE_ASSIGNMENT");
        int tableSent = events.indexOf("send:REGISTER_TABLE");
        assertThat(allocApplied).isGreaterThanOrEqualTo(0);
        assertThat(schemaSent).isGreaterThan(allocApplied);
        assertThat(assignSent).isGreaterThan(schemaSent);
        assertThat(tableSent).isGreaterThan(assignSent);
        // Pipeline: all three DDL records are sent before any of them is applied.
        assertThat(events.indexOf("applied:REGISTER_SCHEMA")).isEqualTo(-1);
        assertThat(events.indexOf("applied:UPDATE_TABLE_ASSIGNMENT")).isEqualTo(-1);
        assertThat(events.indexOf("applied:REGISTER_TABLE")).isEqualTo(-1);
        assertThat(call).isNotDone();

        writer.completePending();
        call.get(5, TimeUnit.SECONDS);
        assertThat(eventsAfter(writer, "applied:REGISTER_SCHEMA")).isNotEqualTo(-1);
        assertThat(writer.sentTypes())
                .containsExactly(
                        MetadataRecord.Type.ALLOCATE_ID_RANGE,
                        MetadataRecord.Type.REGISTER_SCHEMA,
                        MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT,
                        MetadataRecord.Type.REGISTER_TABLE);
    }

    @Test
    void testRegisterPartitionSendsBothKeysBeforeApply() throws Exception {
        RecordingWriter writer = new RecordingWriter();
        RaftMetadataClient client = newClient(writer);
        TablePath path = TablePath.of("db", "parted");
        PartitionAssignment assignment =
                new PartitionAssignment(
                        7L,
                        TableAssignment.builder()
                                .add(0, BucketAssignment.of(1, 2, 3))
                                .build()
                                .getBucketAssignments());

        CompletableFuture<Void> call =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                client.registerPartitionAssignmentAndMetadata(
                                        101L, "p1", assignment, "file:///tmp/remote", path, 7L);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        waitUntil(
                () -> writer.sentTypes().size() >= 2,
                "partition assignment+metadata were not sent");

        List<String> events = new ArrayList<>(writer.events);
        int assignSent = events.indexOf("send:UPDATE_PARTITION_ASSIGNMENT");
        int partSent = events.indexOf("send:REGISTER_PARTITION");
        assertThat(assignSent).isGreaterThanOrEqualTo(0);
        assertThat(partSent).isGreaterThan(assignSent);
        assertThat(events.indexOf("applied:UPDATE_PARTITION_ASSIGNMENT")).isEqualTo(-1);
        assertThat(events.indexOf("applied:REGISTER_PARTITION")).isEqualTo(-1);
        assertThat(call).isNotDone();

        writer.completePending();
        call.get(5, TimeUnit.SECONDS);
        assertThat(writer.sentTypes())
                .containsExactly(
                        MetadataRecord.Type.UPDATE_PARTITION_ASSIGNMENT,
                        MetadataRecord.Type.REGISTER_PARTITION);
    }

    private static RaftMetadataClient newClient(RecordingWriter writer) {
        RaftLeaderSelector leaderSelector = mock(RaftLeaderSelector.class);
        when(leaderSelector.isLeader()).thenReturn(true);
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        // blockSize 1 forces ALLOCATE_ID_RANGE on the first getTableIdAndIncrement.
        RaftSequenceIDCounter tableIds = new RaftSequenceIDCounter("table", writer, store, 1);
        RaftSequenceIDCounter partitionIds =
                new RaftSequenceIDCounter("partition", writer, store, 1);
        RaftSequenceIDCounter writerIds = new RaftSequenceIDCounter("writer", writer, store, 1);
        return new RaftMetadataClient(
                writer, store, leaderSelector, tableIds, partitionIds, writerIds);
    }

    private static int eventsAfter(RecordingWriter writer, String event) {
        return writer.events.indexOf(event);
    }

    private static void waitUntil(Check check, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError(message);
    }

    @FunctionalInterface
    private interface Check {
        boolean ok();
    }

    /**
     * Completes {@code ALLOCATE_ID_RANGE} immediately (so id allocation can {@code .get()}) and
     * holds other appends until {@link #completePending()}, exposing the pipeline send window.
     */
    static final class RecordingWriter extends RaftJournalWriter {
        final List<String> events = new CopyOnWriteArrayList<>();
        private final List<MetadataRecord> sent = new CopyOnWriteArrayList<>();
        private final List<Pending> pending = new CopyOnWriteArrayList<>();
        private final AtomicLong nextIndex = new AtomicLong(1L);

        RecordingWriter() {
            super(mock(RaftClient.class), mock(FlussMetadataStateMachine.class));
        }

        @Override
        public CompletableFuture<Long> append(MetadataRecord record) {
            sent.add(record);
            events.add("send:" + record.type());
            CompletableFuture<Long> future = new CompletableFuture<>();
            if (record.type() == MetadataRecord.Type.ALLOCATE_ID_RANGE) {
                future.complete(nextIndex.getAndIncrement());
                events.add("applied:" + record.type());
            } else {
                pending.add(new Pending(record, future));
            }
            return future;
        }

        void completePending() {
            for (Pending p : pending) {
                p.future.complete(nextIndex.getAndIncrement());
                events.add("applied:" + p.record.type());
            }
            pending.clear();
        }

        List<MetadataRecord.Type> sentTypes() {
            List<MetadataRecord.Type> types = new ArrayList<>();
            for (MetadataRecord record : sent) {
                types.add(record.type());
            }
            return types;
        }

        private static final class Pending {
            final MetadataRecord record;
            final CompletableFuture<Long> future;

            Pending(MetadataRecord record, CompletableFuture<Long> future) {
                this.record = record;
                this.future = future;
            }
        }
    }
}
