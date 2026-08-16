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

package org.apache.fluss.server.coordinator.gossip;

import org.apache.fluss.annotation.Internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe membership table. Merge is SWIM-style: a higher incarnation always wins; otherwise
 * the higher heartbeat wins. {@code lastSeenMs} is local wall-clock metadata and is not exchanged.
 */
@Internal
public final class GossipView {

    private final ConcurrentHashMap<String, Entry> members = new ConcurrentHashMap<>();

    /** Insert or replace the local node (always wins). */
    public void putLocal(GossipMember self, long nowMs) {
        members.put(self.nodeId(), new Entry(self, nowMs));
    }

    /**
     * Merge {@code incoming} into this view. Returns the number of members that were inserted or
     * updated.
     */
    public int merge(GossipView incoming, long nowMs) {
        int updates = 0;
        for (GossipMember member : incoming.members()) {
            if (merge(member, nowMs)) {
                updates++;
            }
        }
        return updates;
    }

    /** Merge a single member. Returns {@code true} if the table changed. */
    public boolean merge(GossipMember incoming, long nowMs) {
        Entry[] changed = new Entry[1];
        members.compute(
                incoming.nodeId(),
                (id, existing) -> {
                    if (existing == null || existing.member.isStaleComparedTo(incoming)) {
                        changed[0] = new Entry(incoming, nowMs);
                        return changed[0];
                    }
                    return existing;
                });
        return changed[0] != null;
    }

    /** Snapshot of currently known members (no last-seen metadata). */
    public List<GossipMember> members() {
        List<GossipMember> out = new ArrayList<>();
        for (Entry e : members.values()) {
            out.add(e.member);
        }
        return out;
    }

    /** RPC {@code host:port} list for TabletServer seed expansion. */
    public List<String> rpcAddresses() {
        List<String> out = new ArrayList<>();
        for (Entry e : members.values()) {
            if (e.member.rpcPort() > 0) {
                out.add(e.member.rpcAddress());
            }
        }
        return out;
    }

    /** Member by node id, or {@code null}. */
    public GossipMember get(String nodeId) {
        Entry e = members.get(nodeId);
        return e == null ? null : e.member;
    }

    /**
     * Remove members (other than {@code selfId}) whose last-seen time is older than {@code
     * failureTimeoutMs}.
     */
    public int expire(String selfId, long nowMs, long failureTimeoutMs) {
        int removed = 0;
        for (Entry e : members.values()) {
            if (e.member.nodeId().equals(selfId)) {
                continue;
            }
            if (nowMs - e.lastSeenMs > failureTimeoutMs) {
                if (members.remove(e.member.nodeId(), e)) {
                    removed++;
                }
            }
        }
        return removed;
    }

    /** Number of members currently in the view. */
    public int size() {
        return members.size();
    }

    /** Deep copy for push-pull (members only; last-seen is not transferred). */
    public GossipView snapshot() {
        GossipView copy = new GossipView();
        for (Entry e : members.values()) {
            copy.members.put(e.member.nodeId(), new Entry(e.member, e.lastSeenMs));
        }
        return copy;
    }

    static GossipView of(Collection<GossipMember> members, long nowMs) {
        GossipView view = new GossipView();
        for (GossipMember m : members) {
            view.merge(m, nowMs);
        }
        return view;
    }

    private static final class Entry {
        private final GossipMember member;
        private final long lastSeenMs;

        private Entry(GossipMember member, long lastSeenMs) {
            this.member = member;
            this.lastSeenMs = lastSeenMs;
        }
    }
}
