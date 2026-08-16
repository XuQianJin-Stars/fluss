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

package org.apache.fluss.server.tools;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code fluss raft status --watch} parse + polling helper. */
class FlussCliRaftStatusWatchTest {

    @Test
    void testParseWatchAndInterval() {
        FlussCli.CoordinatorEndpoint endpoint =
                FlussCli.parseCoordinatorEndpoint(
                        new String[] {
                            "--coordinator",
                            "10.0.0.1:9123",
                            "--json",
                            "--watch",
                            "--interval",
                            "500"
                        });
        assertThat(endpoint.node.host()).isEqualTo("10.0.0.1");
        assertThat(endpoint.node.port()).isEqualTo(9123);
        assertThat(endpoint.json).isTrue();
        assertThat(endpoint.watch).isTrue();
        assertThat(endpoint.intervalMs).isEqualTo(500L);
    }

    @Test
    void testParseDefaultsWithoutWatch() {
        FlussCli.CoordinatorEndpoint endpoint = FlussCli.parseCoordinatorEndpoint(new String[0]);
        assertThat(endpoint.watch).isFalse();
        assertThat(endpoint.intervalMs).isEqualTo(RaftStatusWatchLoop.DEFAULT_INTERVAL_MS);
        assertThat(endpoint.json).isFalse();
    }

    @Test
    void testWatchLoopStopsAfterRequestedTicks() throws Exception {
        AtomicInteger ticks = new AtomicInteger();
        int rc =
                RaftStatusWatchLoop.run(
                        0L, () -> ticks.get() >= 3, millis -> {}, ticks::incrementAndGet);
        assertThat(ticks.get()).isEqualTo(3);
        assertThat(rc).isEqualTo(3);
    }

    @Test
    void testUsageMentionsWatch() {
        PrintStream original = System.err;
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buf));
        try {
            int rc = FlussCli.run(new String[0]);
            assertThat(rc).isEqualTo(1);
        } finally {
            System.setErr(original);
        }
        String usage = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        assertThat(usage).contains("--watch");
        assertThat(usage).contains("zk-to-raft-live-migrate");
    }

    @Test
    void testLiveMigrateCliRequiresZkAddress() throws Exception {
        int rc = FlussCli.run(new String[] {"zk-to-raft-live-migrate"});
        assertThat(rc).isEqualTo(1);
    }
}
