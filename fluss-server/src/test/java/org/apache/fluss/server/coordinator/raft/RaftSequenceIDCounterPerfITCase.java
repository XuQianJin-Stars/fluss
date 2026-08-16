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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.SequenceIDCounter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lightweight timed comparison of block-allocated {@link RaftSequenceIDCounter} vs one Raft append
 * per id (T3-3 / T8-3).
 *
 * <p><b>Conditions:</b> single-voter embedded Ratis on loopback; warm JVM; allocates {@value #IDS}
 * consecutive ids; block size {@value #BLOCK_SIZE}. Not a full JMH microbenchmark — documents
 * relative cost of block allocation for CI. The §8.4 JMH comparison lives in {@code fluss-jmh}
 * ({@code SequenceIdAllocationBenchmark}).
 */
class RaftSequenceIDCounterPerfITCase {

    private static final int IDS = 200;
    private static final long BLOCK_SIZE = 64L;

    @TempDir Path tempDir;

    @Test
    void testBlockAllocationFasterThanOneAppendPerId() throws Exception {
        long blockNs = timedAllocate(BLOCK_SIZE, IDS);
        long onePerIdNs = timedAllocate(1L, IDS);

        // Block allocation should need far fewer Raft appends (~IDS/BLOCK_SIZE vs IDS).
        assertThat(blockNs)
                .as(
                        "blockSize=%d took %d ms; one-append-per-id took %d ms",
                        BLOCK_SIZE,
                        TimeUnit.NANOSECONDS.toMillis(blockNs),
                        TimeUnit.NANOSECONDS.toMillis(onePerIdNs))
                .isLessThan(onePerIdNs);
    }

    private long timedAllocate(long blockSize, int count) throws Exception {
        int port = freePort();
        String address = "127.0.0.1:" + port;
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS.key(), address);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_NODE_ID.key(), address);
        conf.setString(
                ConfigOptions.COORDINATOR_RAFT_DIR.key(),
                tempDir.resolve("raft-" + blockSize).toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ID_BLOCK_SIZE, blockSize);
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(100));
        conf.set(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL, Duration.ofMillis(100));

        try (RaftMetadataClient client = RaftMetadataClient.create(conf)) {
            long deadline = System.currentTimeMillis() + 30_000L;
            while (!client.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            assertThat(client.isLeader()).isTrue();

            SequenceIDCounter counter =
                    new RaftSequenceIDCounter(
                            "perf-" + blockSize, client.journalWriter(), client.store(), blockSize);
            // Warm one id so leadership/apply path is hot.
            counter.getAndIncrement();

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                counter.getAndIncrement();
            }
            return System.nanoTime() - start;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
