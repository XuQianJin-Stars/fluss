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

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * In-process watch dispatch, fired from the (single-threaded) apply loop after each mutation.
 *
 * <p>This covers ONLY the in-process (same-JVM) watch scope — e.g. the coordinator event processor
 * reacting to its own metadata writes. The cross-process scope (TabletServers/standby coordinators)
 * is handled separately by the metadata stream (§5.8), whose enqueue hook is also {@link
 * #fireWatch}.
 *
 * <p><b>Apply-loop cost (analysis §8.9.4 C2):</b> dispatch compares raw bytes against a copy-on-
 * write snapshot of the registered prefixes. It allocates nothing on the hot path. An earlier
 * version hex-encoded every key into a {@link String} and ran {@code String.startsWith} against
 * every registration, costing a {@code StringBuilder} plus {@code 2 * keyLen} char writes per
 * applied record — and apply latency is charged to every writer through {@code awaitApplied}
 * (§4.3). Registrations are few (three watchers today: table/schema/partition, tablet-server,
 * coordinator), so a linear byte-compare over the snapshot is cheaper than any hashing scheme.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.2.4.
 */
@Internal
@ThreadSafe
public class MetadataWatchService {

    /** Callback invoked on a watched key change. */
    @FunctionalInterface
    public interface WatchCallback {
        void onChange(byte[] key, byte[] oldValue, byte[] newValue);
    }

    /** A registered prefix and the callback listening on it. */
    private static final class Registration {
        private final byte[] prefix;
        private final WatchCallback callback;

        Registration(byte[] prefix, WatchCallback callback) {
            this.prefix = prefix;
            this.callback = callback;
        }
    }

    // Copy-on-write snapshot: mutations replace the array under `lock`, the apply loop reads it
    // without synchronization. Empty array (not null) when nothing is registered.
    private volatile Registration[] registrations = new Registration[0];

    private final Object lock = new Object();

    public void registerWatch(byte[] keyPrefix, WatchCallback cb) {
        byte[] prefix = keyPrefix.clone();
        synchronized (lock) {
            Registration[] current = registrations;
            Registration[] next = Arrays.copyOf(current, current.length + 1);
            next[current.length] = new Registration(prefix, cb);
            registrations = next;
        }
    }

    public void unregisterWatch(byte[] keyPrefix, WatchCallback cb) {
        synchronized (lock) {
            Registration[] current = registrations;
            int index = -1;
            for (int i = 0; i < current.length; i++) {
                if (current[i].callback == cb && Arrays.equals(current[i].prefix, keyPrefix)) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return;
            }
            Registration[] next = new Registration[current.length - 1];
            System.arraycopy(current, 0, next, 0, index);
            System.arraycopy(current, index + 1, next, index, current.length - index - 1);
            registrations = next;
        }
    }

    /**
     * Whether any registered prefix matches {@code key}. Lets {@code RaftMetadataStore.apply} skip
     * the {@code oldValue} pre-read (an extra KvStore lookup on every mutation) for keys nobody
     * watches — which is the overwhelming majority of record types (analysis §8.9.4 C1).
     */
    public boolean hasWatch(byte[] key) {
        Registration[] snapshot = registrations;
        for (int i = 0; i < snapshot.length; i++) {
            if (startsWith(key, snapshot[i].prefix)) {
                return true;
            }
        }
        return false;
    }

    /** Called by {@link RaftMetadataStore#apply} after the KvStore mutation is applied. */
    public void fireWatch(byte[] key, byte[] oldValue, byte[] newValue) {
        Registration[] snapshot = registrations;
        for (int i = 0; i < snapshot.length; i++) {
            Registration registration = snapshot[i];
            if (startsWith(key, registration.prefix)) {
                registration.callback.onChange(key, oldValue, newValue);
            }
        }
    }

    /** Registered prefixes, for tests/observability. */
    List<byte[]> registeredPrefixes() {
        Registration[] snapshot = registrations;
        List<byte[]> prefixes = new ArrayList<>(snapshot.length);
        for (int i = 0; i < snapshot.length; i++) {
            prefixes.add(snapshot[i].prefix.clone());
        }
        return prefixes;
    }

    private static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
