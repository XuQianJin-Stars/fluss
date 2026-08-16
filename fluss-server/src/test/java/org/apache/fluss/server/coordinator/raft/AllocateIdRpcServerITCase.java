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
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.metadata.MetadataStoreClientFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for {@link AllocateIdRpcServer} (T6/B1) against a real single-node embedded Ratis
 * leader. Verifies that the {@code AllocateId} RPC reserves durably-committed, contiguous id ranges
 * for each id kind, that per-key counters are isolated, and that a fresh handler over the same
 * store never re-hands an already-allocated id (no-reuse across leader failover).
 */
class AllocateIdRpcServerITCase {

    private static final long BLOCK_SIZE = 1_000L;
    private static final byte[] COLUMN_KEY_A = {1, 2, 3};
    private static final byte[] COLUMN_KEY_B = {9};
    private static final byte[] BUCKET_KEY = {0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 1};

    @TempDir private Path raftDir;

    @Test
    void testAllocateAllKindsIsolatedAndNoReuse() throws Exception {
        int port = freePort();
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_VOTERS, "127.0.0.1:" + port);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, raftDir.toString());
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(500));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(150));

        RaftMetadataClient client =
                (RaftMetadataClient) MetadataStoreClientFactory.create(conf, null);
        AllocateIdRpcServer rpc = new AllocateIdRpcServer(client, BLOCK_SIZE);
        try {
            MetadataStoreClient meta = client;
            waitUntil(20_000, () -> meta.newLeaderSelector("n1").isLeader(), "no raft leader");

            // ---- WRITER: single global counter, contiguous forward-only ----
            assertThat(allocate(rpc, AllocateIdRpcServer.KIND_WRITER, null, 1)).isEqualTo(0L);
            assertThat(allocate(rpc, AllocateIdRpcServer.KIND_WRITER, null, 1)).isEqualTo(1L);

            // ---- AUTO_INCREMENT_COLUMN: per-(tablePath,columnId) counter, block reservation ----
            assertThat(
                            allocate(
                                    rpc,
                                    AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN,
                                    COLUMN_KEY_A,
                                    100))
                    .isEqualTo(0L); // owns [0, 100)
            long afterA =
                    allocate(rpc, AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN, COLUMN_KEY_A, 1);
            assertThat(afterA).isEqualTo(100L); // next contiguous id

            // a DIFFERENT key is an isolated counter, starts at 0 independently
            assertThat(
                            allocate(
                                    rpc,
                                    AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN,
                                    COLUMN_KEY_B,
                                    1))
                    .isEqualTo(0L);

            // ---- BUCKET_SNAPSHOT: per-bucket counter ----
            assertThat(allocate(rpc, AllocateIdRpcServer.KIND_BUCKET_SNAPSHOT, BUCKET_KEY, 1))
                    .isEqualTo(0L);
            assertThat(allocate(rpc, AllocateIdRpcServer.KIND_BUCKET_SNAPSHOT, BUCKET_KEY, 1))
                    .isEqualTo(1L);

            // ---- NO-REUSE: a fresh handler over the SAME store resumes column-A at a value that
            // is strictly greater than every id already handed out (101 was the last used id of A);
            // the unused tail of the previous block is discarded (a gap, never a reuse). ----
            AllocateIdRpcServer freshRpc = new AllocateIdRpcServer(client, BLOCK_SIZE);
            long resumed =
                    allocate(
                            freshRpc,
                            AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN,
                            COLUMN_KEY_A,
                            1);
            assertThat(resumed).isGreaterThanOrEqualTo(101L);
        } finally {
            client.close();
        }
    }

    private static long allocate(AllocateIdRpcServer rpc, int kind, byte[] key, long count)
            throws Exception {
        AllocateIdRequest request = new AllocateIdRequest().setIdKind(kind).setCount(count);
        if (key != null) {
            request.setCounterKey(key);
        }
        AllocateIdResponse response = rpc.allocateId(request).get();
        assertThat(response.getCount()).isEqualTo(count);
        return response.getStartId();
    }

    private interface Condition {
        boolean test() throws Exception;
    }

    private static void waitUntil(long timeoutMs, Condition condition, String message)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Timed out: " + message);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
