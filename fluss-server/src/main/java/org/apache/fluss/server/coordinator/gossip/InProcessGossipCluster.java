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

import org.apache.fluss.annotation.VisibleForTesting;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process push-pull cluster for unit tests. Nodes register by gossip address; {@link
 * #transport()} exchanges views without TCP.
 */
@VisibleForTesting
final class InProcessGossipCluster {

    private final ConcurrentHashMap<String, GossipView> views = new ConcurrentHashMap<>();

    void register(GossipMember member, GossipView view) {
        views.put(member.gossipAddress(), view);
    }

    void unregister(GossipMember member) {
        views.remove(member.gossipAddress());
    }

    GossipTransport transport() {
        return new GossipTransport() {
            @Override
            public GossipView exchange(GossipMember peer, GossipView localView) throws IOException {
                GossipView remote = views.get(peer.gossipAddress());
                if (remote == null) {
                    throw new IOException("No in-process gossip peer at " + peer.gossipAddress());
                }
                long now = System.currentTimeMillis();
                remote.merge(localView, now);
                return remote.snapshot();
            }

            @Override
            public void close() {
                // nothing to close
            }
        };
    }
}
