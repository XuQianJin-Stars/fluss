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
import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotRequest;
import org.apache.fluss.rpc.messages.FetchMetadataSnapshotResponse;
import org.apache.fluss.rpc.messages.PbMetadataRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates between the {@code FetchMetadata}/{@code FetchMetadataSnapshot} proto2 messages
 * (§5.8.1, {@code fluss-rpc}) and the {@link MetadataStreamService} value types. Keeping the
 * mapping in one place means the leader-side handler ({@link MetadataStreamRpcServer}) and the
 * subscriber-side client ({@link RpcMetadataStreamGateway}) can never disagree on the wire shape.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class MetadataStreamRpcs {

    private MetadataStreamRpcs() {}

    // ---- request builders (subscriber side) ----

    public static FetchMetadataRequest buildFetchRequest(
            int serverId, long fromOffset, long leaderEpoch, int maxRecords, int maxWaitMs) {
        return new FetchMetadataRequest()
                .setServerId(serverId)
                .setFromOffset(fromOffset)
                .setLeaderEpoch(leaderEpoch)
                .setMaxRecords(maxRecords)
                .setMaxWaitMs(maxWaitMs);
    }

    public static FetchMetadataSnapshotRequest buildSnapshotRequest(
            int serverId, long leaderEpoch) {
        return new FetchMetadataSnapshotRequest().setServerId(serverId).setLeaderEpoch(leaderEpoch);
    }

    // ---- response builders (leader side) ----

    public static FetchMetadataResponse toResponse(MetadataStreamService.FetchResult result) {
        FetchMetadataResponse response =
                new FetchMetadataResponse()
                        .setLeaderEpoch(result.leaderEpoch())
                        .setHighMetadataOffset(result.highMetadataOffset())
                        .setNextOffset(result.nextOffset())
                        .setSnapshotRequired(result.snapshotRequired());
        for (MetadataStreamService.StreamRecord record : result.records()) {
            PbMetadataRecord pb = response.addRecord();
            pb.setOffset(record.offset());
            pb.setRecordBytes(record.bytes());
        }
        return response;
    }

    public static FetchMetadataSnapshotResponse toSnapshotResponse(
            MetadataStreamService.FetchSnapshotResult result) {
        return new FetchMetadataSnapshotResponse()
                .setLeaderEpoch(result.leaderEpoch())
                .setSnapshotMetadataOffset(result.snapshotMetadataOffset())
                .setSnapshotImage(result.image());
    }

    // ---- response parsers (subscriber side) ----

    public static MetadataStreamService.FetchResult fromResponse(FetchMetadataResponse response) {
        List<MetadataStreamService.StreamRecord> records =
                new ArrayList<>(response.getRecordsCount());
        for (PbMetadataRecord pb : response.getRecordsList()) {
            records.add(
                    new MetadataStreamService.StreamRecord(pb.getOffset(), pb.getRecordBytes()));
        }
        boolean snapshotRequired = response.hasSnapshotRequired() && response.isSnapshotRequired();
        return new MetadataStreamService.FetchResult(
                response.getLeaderEpoch(),
                response.getHighMetadataOffset(),
                records,
                response.getNextOffset(),
                snapshotRequired);
    }

    public static MetadataStreamService.FetchSnapshotResult fromSnapshotResponse(
            FetchMetadataSnapshotResponse response) {
        return new MetadataStreamService.FetchSnapshotResult(
                response.getLeaderEpoch(),
                response.getSnapshotMetadataOffset(),
                response.getSnapshotImage());
    }
}
