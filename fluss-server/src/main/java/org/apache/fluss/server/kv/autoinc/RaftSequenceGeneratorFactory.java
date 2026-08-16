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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AllocateIdRequest;
import org.apache.fluss.rpc.messages.AllocateIdResponse;
import org.apache.fluss.server.SequenceIDCounter;
import org.apache.fluss.server.coordinator.raft.AllocateIdRpcServer;
import org.apache.fluss.server.coordinator.raft.RaftRemoteSequenceIDCounter;
import org.apache.fluss.types.DataTypeRoot;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft-backend implementation of {@link SequenceGeneratorFactory} (§5.3, T6 B1 slice 3): the
 * replacement for {@link ZkSequenceGeneratorFactory} on a tablet server that has no ZooKeeper. Each
 * generated {@link BoundedSegmentSequenceGenerator} is backed by a {@link
 * RaftRemoteSequenceIDCounter} that reserves id ranges from the coordinator leader through the
 * {@code AllocateId} RPC (kind {@link AllocateIdRpcServer#KIND_AUTO_INCREMENT_COLUMN}) instead of a
 * per-column ZooKeeper {@code ZkSequenceIDCounter} znode.
 *
 * <p>The counter key namespaces the leader-side counter to a single {@code (tablePath, columnId)}
 * pair (see {@link #autoIncrementColumnKey}); the leader hex-encodes it under {@code "aic:"+hex} so
 * every distinct column has exactly one leader counter (required for the no-reuse guarantee).
 *
 * <p>The RPC transport is injected as a narrow function ({@code request ->
 * coordinatorGateway.allocateId(request)}) so this factory is unit-testable without Netty and does
 * not couple to the full gateway. Wiring it into {@code KvManager.createSequenceGeneratorFactory}
 * on the Raft backend is done as part of the tablet-server boot milestone (B1 slice 4).
 *
 * <p>See {@code zookeeper-removal-design.md} §5.3.
 */
@Internal
public class RaftSequenceGeneratorFactory implements SequenceGeneratorFactory {

    private final Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> transport;

    public RaftSequenceGeneratorFactory(
            Function<AllocateIdRequest, CompletableFuture<AllocateIdResponse>> transport) {
        this.transport = transport;
    }

    @Override
    public SequenceGenerator createSequenceGenerator(
            TablePath tablePath, Schema.Column autoIncrementColumn, long idCacheSize) {
        return new BoundedSegmentSequenceGenerator(
                tablePath,
                autoIncrementColumn.getColumnId(),
                autoIncrementColumn.getName(),
                createSequenceIDCounter(tablePath, autoIncrementColumn.getColumnId()),
                idCacheSize,
                checkMaxAllowedValue(autoIncrementColumn));
    }

    private SequenceIDCounter createSequenceIDCounter(TablePath tablePath, int columnId) {
        return new RaftRemoteSequenceIDCounter(
                transport,
                AllocateIdRpcServer.KIND_AUTO_INCREMENT_COLUMN,
                autoIncrementColumnKey(tablePath, columnId));
    }

    /**
     * Builds the stable, collision-free counter key for a {@code (tablePath, columnId)} pair: the
     * UTF-8 table path, a {@code 0x00} separator, then the 4-byte big-endian column id.
     * Deterministic across restarts so the same column always maps to the same leader-side counter.
     */
    @VisibleForTesting
    static byte[] autoIncrementColumnKey(TablePath tablePath, int columnId) {
        byte[] pathBytes = tablePath.toString().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(pathBytes.length + 1 + Integer.BYTES)
                .put(pathBytes)
                .put((byte) 0)
                .putInt(columnId)
                .array();
    }

    private static long checkMaxAllowedValue(Schema.Column column) {
        DataTypeRoot typeRoot = column.getDataType().getTypeRoot();
        if (typeRoot == DataTypeRoot.INTEGER) {
            return Integer.MAX_VALUE;
        } else if (typeRoot == DataTypeRoot.BIGINT) {
            return Long.MAX_VALUE;
        } else {
            throw new IllegalArgumentException(
                    "Auto-increment column must be of type INTEGER or BIGINT");
        }
    }
}
