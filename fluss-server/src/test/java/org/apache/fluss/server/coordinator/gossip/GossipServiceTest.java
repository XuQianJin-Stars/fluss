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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.utils.clock.SystemClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Gossip merge, in-process discovery, and TCP push-pull. */
class GossipServiceTest {

    @Test
    void testViewMergePrefersHigherHeartbeat() {
        GossipView view = new GossipView();
        GossipMember old = new GossipMember("n1", "h", 1, "h", 2, 1L, 1);
        GossipMember newer = new GossipMember("n1", "h", 1, "h", 2, 5L, 1);
        assertThat(view.merge(old, 10L)).isTrue();
        assertThat(view.merge(newer, 20L)).isTrue();
        assertThat(view.get("n1").heartbeat()).isEqualTo(5L);
        assertThat(view.merge(old, 30L)).isFalse();
        assertThat(view.get("n1").heartbeat()).isEqualTo(5L);
    }

    @Test
    void testInProcessThreeNodesDiscoverEachOther() throws Exception {
        InProcessGossipCluster cluster = new InProcessGossipCluster();
        GossipTransport transport = cluster.transport();

        GossipMember a = member("a", 1);
        GossipMember b = member("b", 2);
        GossipMember c = member("c", 3);
        GossipView viewA = new GossipView();
        GossipView viewB = new GossipView();
        GossipView viewC = new GossipView();
        cluster.register(a, viewA);
        cluster.register(b, viewB);
        cluster.register(c, viewC);

        GossipService svcA =
                GossipService.createForTest(
                        a,
                        Collections.emptyList(),
                        viewA,
                        transport,
                        50L,
                        10_000L,
                        SystemClock.getInstance());
        GossipService svcB =
                GossipService.createForTest(
                        b,
                        Collections.singletonList(a),
                        viewB,
                        transport,
                        50L,
                        10_000L,
                        SystemClock.getInstance());
        GossipService svcC =
                GossipService.createForTest(
                        c,
                        Collections.singletonList(a),
                        viewC,
                        transport,
                        50L,
                        10_000L,
                        SystemClock.getInstance());

        for (int i = 0; i < 8; i++) {
            svcB.gossipOnce();
            svcC.gossipOnce();
            svcA.gossipOnce();
        }

        assertThat(rpcIds(viewA)).contains("a", "b", "c");
        assertThat(rpcIds(viewB)).contains("a", "b", "c");
        assertThat(rpcIds(viewC)).contains("a", "b", "c");
        svcA.close();
        svcB.close();
        svcC.close();
    }

    @Test
    void testTcpTwoNodesDiscoverWithoutFullSeedList() throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_GOSSIP_INTERVAL, Duration.ofMillis(40));
        conf.set(ConfigOptions.COORDINATOR_GOSSIP_FAILURE_TIMEOUT, Duration.ofSeconds(5));
        conf.set(ConfigOptions.COORDINATOR_GOSSIP_PORT, 0);

        GossipMember aSelf = new GossipMember("a", "127.0.0.1", 19123, "127.0.0.1", 0, 0L, 1);
        GossipService a = GossipService.start(aSelf, Collections.emptyList(), conf);
        try {
            GossipMember bSelf = new GossipMember("b", "127.0.0.1", 19124, "127.0.0.1", 0, 0L, 1);
            GossipMember seed = GossipMember.contact("127.0.0.1", a.gossipPort());
            GossipService b = GossipService.start(bSelf, Collections.singletonList(seed), conf);
            try {
                long deadline = System.currentTimeMillis() + 8_000L;
                while (System.currentTimeMillis() < deadline) {
                    if (rpcIds(a.view()).containsAll(Arrays.asList("a", "b"))
                            && rpcIds(b.view()).containsAll(Arrays.asList("a", "b"))) {
                        break;
                    }
                    Thread.sleep(50L);
                }
                assertThat(rpcIds(a.view())).contains("a", "b");
                assertThat(rpcIds(b.view())).contains("a", "b");

                List<String> expanded =
                        GossipDiscovery.expandRpcSeeds(
                                Collections.singletonList("127.0.0.1:19123"),
                                gossipClientConf(a.gossipPort()));
                assertThat(expanded).contains("127.0.0.1:19123", "127.0.0.1:19124");
            } finally {
                b.close();
            }
        } finally {
            a.close();
        }
    }

    @Test
    void testExpireSilentMember() {
        GossipMember a = member("a", 1);
        GossipMember b = member("b", 2);
        GossipView viewA = new GossipView();
        viewA.putLocal(a, 0L);
        viewA.merge(b, 0L);
        viewA.expire("a", 1_000L, 100L);
        assertThat(viewA.get("b")).isNull();
        assertThat(viewA.get("a")).isNotNull();
    }

    private static Configuration gossipClientConf(int gossipPort) {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.COORDINATOR_GOSSIP_ENABLED, true);
        conf.set(ConfigOptions.COORDINATOR_GOSSIP_PORT, gossipPort);
        return conf;
    }

    private static GossipMember member(String id, int rpcPort) {
        return new GossipMember(id, "127.0.0.1", rpcPort, "127.0.0.1", rpcPort + 1000, 0L, 1);
    }

    private static Set<String> rpcIds(GossipView view) {
        Set<String> ids = new HashSet<>();
        for (GossipMember m : view.members()) {
            ids.add(m.nodeId());
        }
        return ids;
    }
}
