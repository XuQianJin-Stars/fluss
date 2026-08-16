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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Helpers to turn {@code coordinator.seed.nodes} into gossip contact points and to expand a partial
 * RPC seed list by pulling membership from a seed's gossip port (design §5.4.4).
 */
@Internal
public final class GossipDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(GossipDiscovery.class);

    private GossipDiscovery() {}

    /**
     * Parse {@code coordinator.seed.nodes} as gossip contacts at {@code coordinator.gossip.port} on
     * each seed host.
     *
     * @param conf coordinator / tablet configuration
     * @return contact members (may be empty)
     */
    public static List<GossipMember> seedsFromConfig(Configuration conf) {
        String seeds = conf.getString(ConfigOptions.COORDINATOR_SEED_NODES);
        int gossipPort = conf.get(ConfigOptions.COORDINATOR_GOSSIP_PORT);
        List<GossipMember> out = new ArrayList<>();
        if (seeds == null || seeds.trim().isEmpty()) {
            return out;
        }
        for (String part : seeds.split(",")) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String host = hostOf(trimmed);
            if (host != null) {
                out.add(GossipMember.contact(host, gossipPort));
            }
        }
        return out;
    }

    /**
     * Query each RPC seed's gossip port and union the advertised RPC addresses onto {@code
     * rpcSeeds}. Failures are ignored so a down gossip socket never blocks bootstrap.
     *
     * @param rpcSeeds configured {@code coordinator.seed.nodes}
     * @param conf tablet / coordinator configuration
     * @return expanded RPC seed list (original seeds first)
     */
    public static List<String> expandRpcSeeds(List<String> rpcSeeds, Configuration conf) {
        if (rpcSeeds == null || rpcSeeds.isEmpty()) {
            return rpcSeeds;
        }
        if (!conf.getBoolean(ConfigOptions.COORDINATOR_GOSSIP_ENABLED)) {
            return rpcSeeds;
        }
        int gossipPort = conf.get(ConfigOptions.COORDINATOR_GOSSIP_PORT);
        LinkedHashSet<String> expanded = new LinkedHashSet<>(rpcSeeds);
        TcpGossipTransport client = TcpGossipTransport.clientOnly();
        try {
            for (String seed : rpcSeeds) {
                String host = hostOf(seed);
                if (host == null) {
                    continue;
                }
                GossipMember peer = GossipMember.contact(host, gossipPort);
                try {
                    GossipView remote = client.exchange(peer, new GossipView());
                    for (String rpc : remote.rpcAddresses()) {
                        expanded.add(rpc);
                    }
                } catch (IOException e) {
                    LOG.debug(
                            "Gossip seed expansion via {} failed; keeping static seeds.", seed, e);
                }
            }
        } finally {
            try {
                client.close();
            } catch (IOException ignored) {
                // client-only close is a no-op besides the flag
            }
        }
        return new ArrayList<>(expanded);
    }

    static String hostOf(String hostPort) {
        String trimmed = hostPort.trim();
        if (trimmed.startsWith("[")) {
            int end = trimmed.indexOf(']');
            if (end > 1) {
                return trimmed.substring(1, end);
            }
            return null;
        }
        int idx = trimmed.lastIndexOf(':');
        if (idx <= 0) {
            return null;
        }
        return trimmed.substring(0, idx);
    }
}
