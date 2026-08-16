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

import java.util.List;

/**
 * A minimal ordered key-value abstraction backing {@link RaftMetadataStore}.
 *
 * <p>The design (§5.2.2) refers to a {@code KvStore}; Fluss does not yet ship a generic KvStore
 * interface (the {@code kv/} package is table-oriented: {@code KvManager}/{@code
 * KvTablet}/RocksDB). This narrow interface isolates the metadata store from its backing engine so
 * implementations can evolve independently:
 *
 * <ul>
 *   <li>{@link RocksDBMetadataKvStore} — production Raft path under {@code
 *       coordinator.raft.dir/metadata-kv/}.
 *   <li>{@link InMemoryMetadataKvStore} — unit tests and follower read-view / stream-subscriber
 *       harnesses that do not need disk durability.
 * </ul>
 *
 * <p>Keys are compared lexicographically by unsigned byte order so {@link #scanPrefix} returns a
 * contiguous range. Writes come exclusively from the single-threaded state-machine apply loop.
 */
@Internal
public interface MetadataKvStore extends AutoCloseable {

    /** Returns the value for {@code key}, or {@code null} if absent. */
    @Nullable
    byte[] get(byte[] key);

    /** Associates {@code value} with {@code key}, overwriting any previous value. */
    void put(byte[] key, byte[] value);

    /** Removes {@code key} if present. */
    void delete(byte[] key);

    /** Returns all values whose key starts with {@code prefix}, in ascending key order. */
    List<byte[]> scanPrefix(byte[] prefix);

    /**
     * Returns every entry as a {@code [key, value]} pair, in ascending key order. Used to serialize
     * the full store into a Raft snapshot (§5.2.5).
     */
    List<byte[][]> entries();

    /** Removes all entries. Used to reset the store before restoring a snapshot (§5.2.5). */
    void clear();

    /** Releases backing resources (no-op for the in-memory impl). */
    @Override
    void close();
}
