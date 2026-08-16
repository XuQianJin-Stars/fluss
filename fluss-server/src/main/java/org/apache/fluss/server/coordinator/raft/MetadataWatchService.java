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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * In-process watch dispatch, fired from the (single-threaded) apply loop after each mutation.
 *
 * <p>This covers ONLY the in-process (same-JVM) watch scope — e.g. the coordinator event processor
 * reacting to its own metadata writes. The cross-process scope (TabletServers/standby coordinators)
 * is handled separately by the metadata stream (§5.8), whose enqueue hook is also {@link
 * #fireWatch}.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4.
 */
@Internal
public class MetadataWatchService {

    /** Callback invoked on a watched key change. */
    @FunctionalInterface
    public interface WatchCallback {
        void onChange(byte[] key, byte[] oldValue, byte[] newValue);
    }

    // prefix (as hex string) -> callbacks
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<WatchCallback>> watches =
            new ConcurrentHashMap<>();

    public void registerWatch(byte[] keyPrefix, WatchCallback cb) {
        watches.computeIfAbsent(hex(keyPrefix), k -> new CopyOnWriteArrayList<>()).add(cb);
    }

    public void unregisterWatch(byte[] keyPrefix, WatchCallback cb) {
        CopyOnWriteArrayList<WatchCallback> list = watches.get(hex(keyPrefix));
        if (list != null) {
            list.remove(cb);
        }
    }

    /** Called by {@link RaftMetadataStore#apply} after the KvStore mutation is applied. */
    public void fireWatch(byte[] key, byte[] oldValue, byte[] newValue) {
        String h = hex(key);
        for (Map.Entry<String, CopyOnWriteArrayList<WatchCallback>> e : watches.entrySet()) {
            if (h.startsWith(e.getKey())) {
                for (WatchCallback cb : e.getValue()) {
                    cb.onChange(key, oldValue, newValue);
                }
            }
        }
    }

    private static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xF, 16));
            sb.append(Character.forDigit(x & 0xF, 16));
        }
        return sb.toString();
    }
}
