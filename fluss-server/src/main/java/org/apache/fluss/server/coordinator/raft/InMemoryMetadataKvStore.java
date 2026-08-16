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

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory {@link MetadataKvStore} backed by a {@link ConcurrentSkipListMap} keyed by unsigned
 * lexicographic byte order. Used for unit tests and follower read-view / stream-subscriber
 * harnesses; production Raft coordinators use {@link RocksDBMetadataKvStore} (§5.2.2 / §5.2.5).
 */
@Internal
public class InMemoryMetadataKvStore implements MetadataKvStore {

    private final ConcurrentSkipListMap<byte[], byte[]> map =
            new ConcurrentSkipListMap<>(InMemoryMetadataKvStore::compareUnsigned);

    @Nullable
    @Override
    public byte[] get(byte[] key) {
        byte[] v = map.get(key);
        return v == null ? null : v.clone();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        map.put(key.clone(), value.clone());
    }

    @Override
    public void delete(byte[] key) {
        map.remove(key);
    }

    @Override
    public List<byte[]> scanPrefix(byte[] prefix) {
        List<byte[][]> entries = scanPrefixEntries(prefix);
        List<byte[]> result = new ArrayList<>(entries.size());
        for (byte[][] entry : entries) {
            result.add(entry[1]);
        }
        return result;
    }

    @Override
    public List<byte[][]> scanPrefixEntries(byte[] prefix) {
        List<byte[][]> result = new ArrayList<>();
        byte[] end = prefixUpperBound(prefix);
        Map<byte[], byte[]> sub =
                end == null ? map.tailMap(prefix, true) : map.subMap(prefix, true, end, false);
        for (Map.Entry<byte[], byte[]> e : sub.entrySet()) {
            result.add(new byte[][] {e.getKey().clone(), e.getValue().clone()});
        }
        return result;
    }

    /** Number of entries currently stored (test/observability helper). */
    public int size() {
        return map.size();
    }

    @Override
    public List<byte[][]> entries() {
        List<byte[][]> result = new ArrayList<>(map.size());
        for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
            result.add(new byte[][] {e.getKey().clone(), e.getValue().clone()});
        }
        return result;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void close() {
        // nothing to release
    }

    /**
     * Smallest key that is strictly greater than every key with {@code prefix}, or {@code null} if
     * the prefix is all {@code 0xFF} bytes (unbounded upper end).
     */
    @Nullable
    private static byte[] prefixUpperBound(byte[] prefix) {
        byte[] end = prefix.clone();
        for (int i = end.length - 1; i >= 0; i--) {
            if ((end[i] & 0xFF) != 0xFF) {
                end[i] = (byte) ((end[i] & 0xFF) + 1);
                byte[] trimmed = new byte[i + 1];
                System.arraycopy(end, 0, trimmed, 0, i + 1);
                return trimmed;
            }
        }
        return null;
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        int min = Math.min(a.length, b.length);
        for (int i = 0; i < min; i++) {
            int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (cmp != 0) {
                return cmp;
            }
        }
        return a.length - b.length;
    }
}
