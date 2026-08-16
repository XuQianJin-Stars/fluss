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
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link RocksDBMetadataKvStore}: basic put/get/scan and restart reload from
 * disk under {@code coordinator.raft.dir/metadata-kv/}.
 */
class RocksDBMetadataKvStoreITCase {

    @TempDir private Path tempDir;

    @Test
    void testPutGetScanAndRestartReload() throws Exception {
        Path raftDir = tempDir.resolve("raft");
        byte[] keyA = bytes("a/key");
        byte[] keyB = bytes("a/key2");
        byte[] keyC = bytes("b/other");
        byte[] valA = bytes("va");
        byte[] valB = bytes("vb");
        byte[] valC = bytes("vc");

        try (RocksDBMetadataKvStore store = RocksDBMetadataKvStore.open(raftDir.toFile())) {
            assertThat(store.dbPath().getName()).isEqualTo(RocksDBMetadataKvStore.SUBDIR);

            store.put(keyA, valA);
            store.put(keyB, valB);
            store.put(keyC, valC);

            assertThat(store.get(keyA)).isEqualTo(valA);
            assertThat(store.get(keyB)).isEqualTo(valB);
            assertThat(store.get(bytes("missing"))).isNull();

            List<byte[]> prefixA = store.scanPrefix(bytes("a/"));
            assertThat(prefixA).containsExactly(valA, valB);

            List<byte[][]> prefixAEntries = store.scanPrefixEntries(bytes("a/"));
            assertThat(prefixAEntries).hasSize(2);
            assertThat(prefixAEntries.get(0)[0]).isEqualTo(keyA);
            assertThat(prefixAEntries.get(0)[1]).isEqualTo(valA);
            assertThat(prefixAEntries.get(1)[0]).isEqualTo(keyB);
            assertThat(prefixAEntries.get(1)[1]).isEqualTo(valB);
            assertThat(store.scanPrefixEntries(bytes("b/"))).hasSize(1);
            assertThat(store.scanPrefixEntries(bytes("c/"))).isEmpty();
            assertThat(store.scanPrefixEntries(bytes("a/z"))).isEmpty();

            assertThat(store.entries()).hasSize(3);

            store.delete(keyB);
            assertThat(store.get(keyB)).isNull();
            assertThat(store.scanPrefix(bytes("a/"))).containsExactly(valA);
        }

        // Reopen the same directory — data must survive close/reopen.
        try (RocksDBMetadataKvStore reopened = RocksDBMetadataKvStore.open(raftDir.toFile())) {
            assertThat(reopened.get(keyA)).isEqualTo(valA);
            assertThat(reopened.get(keyB)).isNull();
            assertThat(reopened.get(keyC)).isEqualTo(valC);
            assertThat(reopened.scanPrefix(bytes("a/"))).containsExactly(valA);
            assertThat(reopened.entries()).hasSize(2);
        }
    }

    @Test
    void testScanPrefixEntriesExcludesNonMatchingKeys() throws Exception {
        Path raftDir = tempDir.resolve("raft-prefix-entries");
        try (RocksDBMetadataKvStore store = RocksDBMetadataKvStore.open(raftDir.toFile())) {
            store.put(bytes("db/alpha"), bytes("1"));
            store.put(bytes("db/beta"), bytes("2"));
            store.put(bytes("tbl/alpha"), bytes("3"));
            store.put(bytes("dc/other"), bytes("4"));

            List<byte[][]> dbEntries = store.scanPrefixEntries(bytes("db/"));
            assertThat(dbEntries).hasSize(2);
            assertThat(dbEntries.get(0)[0]).isEqualTo(bytes("db/alpha"));
            assertThat(dbEntries.get(0)[1]).isEqualTo(bytes("1"));
            assertThat(dbEntries.get(1)[0]).isEqualTo(bytes("db/beta"));
            assertThat(dbEntries.get(1)[1]).isEqualTo(bytes("2"));

            List<byte[][]> tblEntries = store.scanPrefixEntries(bytes("tbl/"));
            assertThat(tblEntries).hasSize(1);
            assertThat(tblEntries.get(0)[0]).isEqualTo(bytes("tbl/alpha"));
            assertThat(tblEntries.get(0)[1]).isEqualTo(bytes("3"));

            assertThat(store.scanPrefixEntries(bytes("missing/"))).isEmpty();
            assertThat(store.scanPrefix(bytes("db/"))).containsExactly(bytes("1"), bytes("2"));
        }
    }

    @Test
    void testClearRemovesAllEntries() throws Exception {
        Path raftDir = tempDir.resolve("raft-clear");
        try (RocksDBMetadataKvStore store = RocksDBMetadataKvStore.open(raftDir.toFile())) {
            store.put(bytes("k1"), bytes("v1"));
            store.put(bytes("k2"), bytes("v2"));
            store.clear();
            assertThat(store.entries()).isEmpty();
            assertThat(store.get(bytes("k1"))).isNull();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
