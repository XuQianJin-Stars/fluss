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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MetadataWatchService} after the byte-prefix rewrite (analysis §8.9.4 C2):
 * dispatch must match on raw bytes, {@link MetadataWatchService#hasWatch} must agree with {@link
 * MetadataWatchService#fireWatch}, and registrations must be independent.
 */
class MetadataWatchServiceTest {

    @Test
    void testFireWatchMatchesOnBytePrefix() {
        MetadataWatchService service = new MetadataWatchService();
        List<byte[]> seen = new ArrayList<>();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
        service.registerWatch(prefix, (k, oldV, newV) -> seen.add(k));

        byte[] matching = key(MetadataRecord.Type.REGISTER_TABLE, "db.t");
        byte[] other = key(MetadataRecord.Type.REGISTER_DATABASE, "db");

        service.fireWatch(matching, null, new byte[] {1});
        service.fireWatch(other, null, new byte[] {1});

        assertThat(seen).hasSize(1);
        assertThat(seen.get(0)).isEqualTo(matching);
    }

    /** A key shorter than the prefix must not match (guards the length check). */
    @Test
    void testKeyShorterThanPrefixDoesNotMatch() {
        MetadataWatchService service = new MetadataWatchService();
        List<byte[]> seen = new ArrayList<>();
        service.registerWatch(
                MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE),
                (k, oldV, newV) -> seen.add(k));

        service.fireWatch(new byte[] {0x01}, null, new byte[] {1});

        assertThat(seen).isEmpty();
        assertThat(service.hasWatch(new byte[] {0x01})).isFalse();
    }

    /**
     * {@code hasWatch} gates the {@code oldValue} pre-read in {@code RaftMetadataStore.apply}
     * (§8.9.4 C1), so it must agree with {@code fireWatch} on exactly the same keys.
     */
    @Test
    void testHasWatchAgreesWithFireWatch() {
        MetadataWatchService service = new MetadataWatchService();
        byte[] watched = key(MetadataRecord.Type.REGISTER_PARTITION, "db.t$p1");
        byte[] unwatched = key(MetadataRecord.Type.UPDATE_LEADER_AND_ISR, "b1");

        assertThat(service.hasWatch(watched)).isFalse();

        List<byte[]> seen = new ArrayList<>();
        service.registerWatch(
                MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_PARTITION),
                (k, oldV, newV) -> seen.add(k));

        assertThat(service.hasWatch(watched)).isTrue();
        assertThat(service.hasWatch(unwatched)).isFalse();

        service.fireWatch(watched, null, new byte[] {1});
        service.fireWatch(unwatched, null, new byte[] {1});
        assertThat(seen).hasSize(1);
    }

    @Test
    void testUnregisterRemovesOnlyThatCallback() {
        MetadataWatchService service = new MetadataWatchService();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
        List<byte[]> first = new ArrayList<>();
        List<byte[]> second = new ArrayList<>();
        MetadataWatchService.WatchCallback cb1 = (k, oldV, newV) -> first.add(k);
        MetadataWatchService.WatchCallback cb2 = (k, oldV, newV) -> second.add(k);
        service.registerWatch(prefix, cb1);
        service.registerWatch(prefix, cb2);

        byte[] k = key(MetadataRecord.Type.REGISTER_TABLE, "db.t");
        service.fireWatch(k, null, new byte[] {1});
        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);

        service.unregisterWatch(prefix, cb1);
        service.fireWatch(k, null, new byte[] {1});

        assertThat(first).hasSize(1); // unchanged
        assertThat(second).hasSize(2);
        assertThat(service.hasWatch(k)).isTrue();

        service.unregisterWatch(prefix, cb2);
        assertThat(service.hasWatch(k)).isFalse();
        assertThat(service.registeredPrefixes()).isEmpty();
    }

    /** Unregistering something that was never registered must be a no-op, not an error. */
    @Test
    void testUnregisterUnknownIsNoOp() {
        MetadataWatchService service = new MetadataWatchService();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
        service.unregisterWatch(prefix, (k, oldV, newV) -> {});
        assertThat(service.registeredPrefixes()).isEmpty();
    }

    /** The service must not alias the caller's prefix array. */
    @Test
    void testRegisteredPrefixIsDefensivelyCopied() {
        MetadataWatchService service = new MetadataWatchService();
        byte[] prefix = MetadataKeys.prefixOf(MetadataRecord.Type.REGISTER_TABLE);
        byte[] mutable = prefix.clone();
        List<byte[]> seen = new ArrayList<>();
        service.registerWatch(mutable, (k, oldV, newV) -> seen.add(k));

        mutable[0] = (byte) 0x7F; // caller mutates its array afterwards

        service.fireWatch(key(MetadataRecord.Type.REGISTER_TABLE, "db.t"), null, new byte[] {1});
        assertThat(seen).hasSize(1);
    }

    private static byte[] key(MetadataRecord.Type type, String body) {
        return MetadataKeys.keyOf(new MetadataRecord(type, utf8(body), new byte[0]));
    }

    private static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
