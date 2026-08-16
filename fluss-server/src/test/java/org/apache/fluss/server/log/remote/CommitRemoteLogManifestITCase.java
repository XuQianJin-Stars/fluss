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

package org.apache.fluss.server.log.remote;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_TABLE_DESCRIPTOR;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.assertProduceLogResponse;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT case for commit remote log manifest: out-of-sync followers still drop local segments after the
 * leader commits a remote-log manifest and the coordinator notifies remote-log offsets (RPC, not ZK
 * watches — works on both ZooKeeper and Raft backends).
 */
class CommitRemoteLogManifestITCase {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    @Test
    void testDeleteOutOfSyncReplicaLogAfterCommit() throws Exception {
        long tableId =
                createTable(FLUSS_CLUSTER_EXTENSION, DATA1_TABLE_PATH, DATA1_TABLE_DESCRIPTOR);

        TableBucket tb = new TableBucket(tableId, 0);
        FLUSS_CLUSTER_EXTENSION.waitUntilAllReplicaReady(tb);
        int leader =
                Objects.requireNonNull(
                        FLUSS_CLUSTER_EXTENSION.waitAndGetLeaderReplica(tb).getLeaderId());
        TabletServerGateway leaderGateWay =
                FLUSS_CLUSTER_EXTENSION.newTabletServerClientForNode(leader);

        // Produce enough for multiple local segments (LOG_SEGMENT_FILE_SIZE=1b).
        for (int i = 0; i < 3; i++) {
            assertProduceLogResponse(
                    leaderGateWay
                            .produceLog(
                                    newProduceLogRequest(
                                            tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                            .get(),
                    0,
                    i * 10L);
        }

        // Stop followers before remote-log tiering notifies them, so they stay out of ISR with
        // all local segments still present.
        List<Integer> stopFollowers =
                Stream.of(0, 1, 2).filter(i -> i != leader).collect(Collectors.toList());
        for (int stopFollower : stopFollowers) {
            FLUSS_CLUSTER_EXTENSION.stopReplica(stopFollower, tb, 1);
        }
        leaderGateWay
                .produceLog(
                        newProduceLogRequest(tableId, 0, -1, genMemoryLogRecordsByObject(DATA1)))
                .get();
        for (int stopFollower : stopFollowers) {
            FLUSS_CLUSTER_EXTENSION.waitUntilReplicaShrinkFromIsr(tb, stopFollower);
            LogTablet stopfollowerLogTablet =
                    FLUSS_CLUSTER_EXTENSION
                            .waitAndGetFollowerReplica(tb, stopFollower)
                            .getLogTablet();
            assertThat(stopfollowerLogTablet.logSegments().size()).isGreaterThanOrEqualTo(2);
        }

        // Leader (interval=1s) commits remote-log; coordinator notifies followers via RPC.
        FLUSS_CLUSTER_EXTENSION.waitUntilSomeLogSegmentsCopyToRemote(tb);

        for (int stopFollower : stopFollowers) {
            LogTablet stopfollowerLogTablet =
                    FLUSS_CLUSTER_EXTENSION
                            .waitAndGetFollowerReplica(tb, stopFollower)
                            .getLogTablet();
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(stopfollowerLogTablet.logSegments()).hasSize(2));
        }
    }

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);
        // Short enough for the IT, long enough that stopReplica can win the race before the first
        // tiering tick after produce (avoids tablet restart + bounce, which is covered separately).
        conf.set(ConfigOptions.REMOTE_LOG_TASK_INTERVAL_DURATION, Duration.ofSeconds(1));
        conf.set(ConfigOptions.LOG_SEGMENT_FILE_SIZE, MemorySize.parse("1b"));
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofSeconds(2));
        return conf;
    }
}
