/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.server.kv.autoinc;

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.server.coordinator.raft.AllocateIdRpcServer;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link RaftSequenceGeneratorFactory}. */
class RaftSequenceGeneratorFactoryTest {

    private static final TablePath TABLE_PATH = new TablePath("db", "t");

    /**
     * A transport that emulates the leader-side per-key counter: each distinct counter key keeps a
     * forward-only high-water mark and returns the reserved range start, advancing by {@code
     * count}. Records the last request so the test can assert the RPC shape (kind + key) the
     * factory sends.
     */
    private static final class FakeAllocateTransport
            implements Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> {
        private final Map<String, Long> highWater = new HashMap<>();
        private final AtomicReference<AllocateIdRequest> lastRequest = new AtomicReference<>();

        @Override
        public CompletableFuture<AllocateIdResponse> apply(AllocateIdRequest request) {
            lastRequest.set(request);
            String key = hex(request.getCounterKey());
            long start = highWater.getOrDefault(key, 0L);
            highWater.put(key, start + request.getCount());
            return CompletableFuture.completedFuture(
                    new AllocateIdResponse().setStartId(start).setCount(request.getCount()));
        }

        private static String hex(byte[] bytes) {
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        }
    }

    @Test
    void testGeneratesContinuousIdsThroughAllocateIdRpc() {
        FakeAllocateTransport transport = new FakeAllocateTransport();
        RaftSequenceGeneratorFactory factory = new RaftSequenceGeneratorFactory(transport);

        Schema.Column column = new Schema.Column("id", DataTypes.INT(), null, 5);
        SequenceGenerator generator = factory.createSequenceGenerator(TABLE_PATH, column, 10);

        // ids are continuous 1..N across segment (cache) boundaries, just like the ZK generator.
        for (long i = 1; i <= 25; i++) {
            assertThat(generator.nextVal()).isEqualTo(i);
        }

        // the factory sent the auto-increment-column kind with the (tablePath, columnId) key.
        AllocateIdRequest last = transport.lastRequest.get();
        assertThat(last.getIdKind()).isEqualTo(AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN);
        assertThat(last.getCounterKey())
                .isEqualTo(RaftSequenceGeneratorFactory.autoIncrementColumnKey(TABLE_PATH, 5));
    }

    @Test
    void testDistinctColumnsGetIndependentSequences() {
        FakeAllocateTransport transport = new FakeAllocateTransport();
        RaftSequenceGeneratorFactory factory = new RaftSequenceGeneratorFactory(transport);

        SequenceGenerator gen5 =
                factory.createSequenceGenerator(
                        TABLE_PATH, new Schema.Column("a", DataTypes.INT(), null, 5), 10);
        SequenceGenerator gen6 =
                factory.createSequenceGenerator(
                        TABLE_PATH, new Schema.Column("b", DataTypes.INT(), null, 6), 10);

        // each column has its own leader counter, so both start at 1 independently.
        assertThat(gen5.nextVal()).isEqualTo(1);
        assertThat(gen6.nextVal()).isEqualTo(1);
        assertThat(gen5.nextVal()).isEqualTo(2);
        assertThat(gen6.nextVal()).isEqualTo(2);
    }

    @Test
    void testCounterKeyIsDeterministicAndDistinct() {
        byte[] k5a = RaftSequenceGeneratorFactory.autoIncrementColumnKey(TABLE_PATH, 5);
        byte[] k5b = RaftSequenceGeneratorFactory.autoIncrementColumnKey(TABLE_PATH, 5);
        byte[] k6 = RaftSequenceGeneratorFactory.autoIncrementColumnKey(TABLE_PATH, 6);
        byte[] otherTable =
                RaftSequenceGeneratorFactory.autoIncrementColumnKey(new TablePath("db", "t2"), 5);

        assertThat(k5a).isEqualTo(k5b);
        assertThat(k5a).isNotEqualTo(k6);
        assertThat(k5a).isNotEqualTo(otherTable);
    }

    @Test
    void testBigintColumnAllowedAndUnsupportedTypeRejected() {
        RaftSequenceGeneratorFactory factory =
                new RaftSequenceGeneratorFactory(new FakeAllocateTransport());

        // BIGINT is a valid auto-increment type.
        assertThat(
                        factory.createSequenceGenerator(
                                TABLE_PATH,
                                new Schema.Column("id", DataTypes.BIGINT(), null, 1),
                                10))
                .isNotNull();

        // a non-integer type is rejected.
        assertThatThrownBy(
                        () ->
                                factory.createSequenceGenerator(
                                        TABLE_PATH,
                                        new Schema.Column("id", DataTypes.STRING(), null, 1),
                                        10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be of type INTEGER or BIGINT");
    }
}
