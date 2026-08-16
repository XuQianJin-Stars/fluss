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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RaftMetadataStore}, {@link MetadataKeys} and {@link InMemoryMetadataKvStore}. */
class RaftMetadataStoreTest {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void testApplyRegisterThenGet() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        byte[] key = utf8("db.t1");
        byte[] payload = utf8("{\"tableId\":1}");

        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, payload));

        MetadataRecord probe = new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, payload);
        assertThat(store.get(MetadataKeys.keyOf(probe))).containsExactly(payload);
    }

    @Test
    void testDeleteRemovesKey() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        byte[] key = utf8("db1");

        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, key, utf8("v")));
        MetadataRecord del =
                new MetadataRecord(MetadataRecord.Type.DELETE_DATABASE, key, new byte[0]);
        store.apply(del);

        assertThat(store.get(MetadataKeys.keyOf(del))).isNull();
    }

    @Test
    void testDeleteSchemaRemovesRegisterSchemaKey() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        byte[] key = utf8("db.t");
        byte[] payload = utf8("{\"schemaId\":1}");

        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_SCHEMA, key, payload));
        MetadataRecord registerProbe =
                new MetadataRecord(MetadataRecord.Type.REGISTER_SCHEMA, key, new byte[0]);
        assertThat(store.get(MetadataKeys.keyOf(registerProbe))).containsExactly(payload);

        store.apply(new MetadataRecord(MetadataRecord.Type.DELETE_SCHEMA, key, new byte[0]));
        assertThat(store.get(MetadataKeys.keyOf(registerProbe))).isNull();
    }

    @Test
    void testWatchFiresOnPutAndDelete() {
        MetadataWatchService watch = new MetadataWatchService();
        RaftMetadataStore store = new RaftMetadataStore(new InMemoryMetadataKvStore(), watch);
        AtomicInteger puts = new AtomicInteger();
        AtomicInteger deletes = new AtomicInteger();
        // watch the whole database namespace via its header prefix
        watch.registerWatch(
                MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_DATABASE),
                (k, oldV, newV) -> {
                    if (newV == null) {
                        deletes.incrementAndGet();
                    } else {
                        puts.incrementAndGet();
                    }
                });

        byte[] key = utf8("db1");
        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, key, utf8("v")));
        store.apply(new MetadataRecord(MetadataRecord.Type.DELETE_DATABASE, key, new byte[0]));

        assertThat(puts.get()).isEqualTo(1);
        assertThat(deletes.get()).isEqualTo(1);
    }

    @Test
    void testScanPrefixReturnsOnlyMatchingEntityType() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, utf8("a"), utf8("ta")));
        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, utf8("b"), utf8("tb")));
        store.apply(
                new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, utf8("a"), utf8("da")));

        List<byte[]> tables =
                store.scanPrefix(MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE));

        assertThat(tables).hasSize(2);
        assertThat(tables).anySatisfy(v -> assertThat(v).containsExactly(utf8("ta")));
        assertThat(tables).anySatisfy(v -> assertThat(v).containsExactly(utf8("tb")));
    }

    @Test
    void testDistinctTypesDoNotCollideOnSameLogicalKey() {
        RaftMetadataStore store =
                new RaftMetadataStore(new InMemoryMetadataKvStore(), new MetadataWatchService());
        byte[] key = utf8("same");
        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, utf8("table")));
        store.apply(new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, key, utf8("db")));

        MetadataRecord tableProbe =
                new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, new byte[0]);
        MetadataRecord dbProbe =
                new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, key, new byte[0]);
        assertThat(store.get(MetadataKeys.keyOf(tableProbe))).containsExactly(utf8("table"));
        assertThat(store.get(MetadataKeys.keyOf(dbProbe))).containsExactly(utf8("db"));
    }

    @Test
    void testEveryRecordTypeHasKeyMapping() {
        for (MetadataRecord.Type type : MetadataRecord.Type.values()) {
            assertThat(MetadataKeys.prefixOf(type)).isNotNull().hasSize(3);
        }
    }
}
