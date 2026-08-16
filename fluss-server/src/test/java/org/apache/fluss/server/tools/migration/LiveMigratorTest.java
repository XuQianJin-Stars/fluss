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

package org.apache.fluss.server.tools.migration;

import org.apache.fluss.server.coordinator.raft.MetadataRecord;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the §6.3 online path: re-scan diffs catch mutations while ZK is still serving, then a
 * write-fence + final scan picks up the last writes.
 */
class LiveMigratorTest {

    @Test
    void testConvergeCatchesMutationDuringLivePhase() throws Exception {
        InMemoryZk zk = new InMemoryZk();
        zk.put("/metadata/databases/db", "{\"comment\":\"v1\"}");
        RecordingSink sink = new RecordingSink();

        LiveMigrator.Result result =
                LiveMigrator.run(
                        zk,
                        sink,
                        new LiveMigrator.Options()
                                .setMaxConvergeRounds(8)
                                .setBeforeRound(
                                        round -> {
                                            if (round == 1) {
                                                zk.put(
                                                        "/metadata/databases/db",
                                                        "{\"comment\":\"v2\"}");
                                                zk.put(
                                                        "/metadata/databases/other",
                                                        "{\"comment\":\"new\"}");
                                            }
                                        })
                                .setFenceWrites(zk::fence));

        assertThat(result.baselineImported).isEqualTo(1);
        assertThat(result.convergeReplayed).isGreaterThanOrEqualTo(1);
        assertThat(result.convergedBeforeFence).isTrue();
        assertThat(result.finalReplayed).isEqualTo(0);
        assertThat(sink.types).contains(MetadataRecord.Type.REGISTER_DATABASE);
        assertThat(payloads(sink))
                .anyMatch(p -> new String(p, StandardCharsets.UTF_8).contains("v2"));
        assertThat(payloads(sink))
                .anyMatch(p -> new String(p, StandardCharsets.UTF_8).contains("new"));
        assertThat(zk.fenced.get()).isTrue();
    }

    @Test
    void testFenceCatchesLastWriteAfterConverge() throws Exception {
        InMemoryZk zk = new InMemoryZk();
        zk.put("/metadata/databases/db", "{\"comment\":\"v1\"}");
        RecordingSink sink = new RecordingSink();
        AtomicBoolean extraWrite = new AtomicBoolean(true);

        LiveMigrator.Result result =
                LiveMigrator.run(
                        zk,
                        sink,
                        new LiveMigrator.Options()
                                .setMaxConvergeRounds(4)
                                .setFenceWrites(
                                        () -> {
                                            if (extraWrite.compareAndSet(true, false)) {
                                                zk.put(
                                                        "/metadata/databases/db",
                                                        "{\"comment\":\"last\"}");
                                            }
                                            zk.fence();
                                        }));

        assertThat(result.convergedBeforeFence).isTrue();
        assertThat(result.finalReplayed).isEqualTo(1);
        assertThat(payloads(sink))
                .anyMatch(p -> new String(p, StandardCharsets.UTF_8).contains("last"));
    }

    @Test
    void testDeleteDuringConvergeIsReplayed() throws Exception {
        InMemoryZk zk = new InMemoryZk();
        zk.put("/metadata/databases/db", "{\"comment\":\"gone\"}");
        RecordingSink sink = new RecordingSink();

        LiveMigrator.Result result =
                LiveMigrator.run(
                        zk,
                        sink,
                        new LiveMigrator.Options()
                                .setMaxConvergeRounds(4)
                                .setBeforeRound(
                                        round -> {
                                            if (round == 1) {
                                                zk.delete("/metadata/databases/db");
                                            }
                                        })
                                .setFenceWrites(zk::fence));

        assertThat(result.baselineImported).isEqualTo(1);
        assertThat(sink.types).contains(MetadataRecord.Type.DELETE_DATABASE);
        assertThat(result.convergedBeforeFence).isTrue();
    }

    private static List<byte[]> payloads(RecordingSink sink) {
        return sink.payloads;
    }

    private static final class InMemoryZk implements LiveMigrator.MetadataSource {
        private final ConcurrentHashMap<String, LiveMigrator.ZnodeSnapshot> nodes =
                new ConcurrentHashMap<>();
        private final AtomicLong zxid = new AtomicLong(0L);
        private final AtomicBoolean fenced = new AtomicBoolean(false);

        void put(String path, String payload) {
            if (fenced.get()) {
                throw new IllegalStateException("ZK writes are fenced");
            }
            long z = zxid.incrementAndGet();
            nodes.put(
                    path,
                    new LiveMigrator.ZnodeSnapshot(
                            path, payload.getBytes(StandardCharsets.UTF_8), z));
        }

        void delete(String path) {
            if (fenced.get()) {
                throw new IllegalStateException("ZK writes are fenced");
            }
            nodes.remove(path);
        }

        void fence() {
            fenced.set(true);
        }

        @Override
        public List<LiveMigrator.ZnodeSnapshot> scan() {
            return new ArrayList<>(nodes.values());
        }
    }

    private static final class RecordingSink implements LiveMigrator.RecordSink {
        private final List<MetadataRecord.Type> types = new ArrayList<>();
        private final List<byte[]> payloads = new ArrayList<>();

        @Override
        public void append(MetadataRecord record) {
            types.add(record.type());
            payloads.add(record.payload());
        }
    }
}
