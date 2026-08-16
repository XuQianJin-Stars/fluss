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

package org.apache.fluss.server.replica;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.PbProduceLogRespForBucket;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.testutils.DualBackendFlussClusters;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.server.zk.data.LeaderAndIsr;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA1_TABLE_PATH;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.apache.fluss.testutils.common.CommonTestUtils.waitValue;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for remove offline replica from ISR. Runs on ZooKeeper and Raft. */
class RemoveOfflineReplicaFromIsrITCase {

    @RegisterExtension
    public static final DualBackendFlussClusters CLUSTERS =
            DualBackendFlussClusters.builder()
                    .setNumOfTabletServers(3)
                    .setClusterConf(initConfig())
                    .build();

    private static Configuration initConfig() {
        Configuration conf = new Configuration();
        conf.setInt(ConfigOptions.DEFAULT_REPLICATION_FACTOR, 3);

        // set a long max lag time, so that the log replica can be removed from isr due to
        // fetch lag by leader to make sure it's removed by coordinator server due to offline
        // replica
        conf.set(ConfigOptions.LOG_REPLICA_MAX_LAG_TIME, Duration.ofDays(1));

        // set log replica min in sync replicas number to 2, if the isr set size less than 2,
        // the produce log request will be failed, and the leader HW will not increase.
        conf.setInt(ConfigOptions.LOG_REPLICA_MIN_IN_SYNC_REPLICAS_NUMBER, 2);
        return conf;
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testRemoveOfflineReplicaFromIsr(MetadataBackend backend) throws Exception {
        FlussClusterExtension cluster = CLUSTERS.get(backend);
        MetadataStoreClient zkClient = cluster.metadataStore();
        long tableId = createLogTable(cluster);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr currentLeaderAndIsr =
                waitValue(
                        () -> zkClient.getLeaderAndIsr(tb),
                        Duration.ofSeconds(20),
                        "Leader and isr not found");
        List<Integer> isr = currentLeaderAndIsr.isr();
        assertThat(isr).containsExactlyInAnyOrder(0, 1, 2);

        int leader = cluster.waitAndGetLeader(tb);
        Integer follower = isr.stream().filter(i -> i != leader).findFirst().get();

        // stop the follower
        cluster.stopTabletServer(follower);

        // the follower should be removed from isr
        isr.remove(follower);

        // send one batch data to check the stop follower will become out of sync replica.
        TabletServerGateway leaderGateWay = cluster.newTabletServerClientForNode(leader);

        // Wait the stop follower to be removed from ISR since the follower is offline
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));

        // Produce with acks=-1; retry until the leader has applied NotifyLeaderAndIsr with the
        // shrunk ISR (metadata-store ISR shrink can race ahead of the leader's in-memory view).
        // Do not assert a fixed base offset — failed retries may have already appended.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    PbProduceLogRespForBucket resp =
                            leaderGateWay
                                    .produceLog(
                                            newProduceLogRequest(
                                                    tableId,
                                                    tb.getBucket(),
                                                    -1,
                                                    genMemoryLogRecordsByObject(DATA1)))
                                    .get()
                                    .getBucketsRespsList()
                                    .get(0);
                    assertThat(resp.hasErrorMessage()).isFalse();
                    assertThat(resp.hasErrorCode()).isFalse();
                    assertThat(resp.hasBaseOffset()).isTrue();
                });

        // make this tablet server re-start.
        cluster.startTabletServer(follower);
        isr.add(follower);

        // make sure the stopped follower can add back to isr after restart
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .isPresent()
                                .hasValueSatisfying(
                                        leaderAndIsr ->
                                                assertThat(leaderAndIsr.isr())
                                                        .containsExactlyInAnyOrderElementsOf(isr)));
    }

    private long createLogTable(FlussClusterExtension cluster) throws Exception {
        // Set bucket to 1 to easy for debug.
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(DATA1_SCHEMA).distributedBy(1, "a").build();
        return RpcMessageTestUtils.createTable(cluster, DATA1_TABLE_PATH, tableDescriptor);
    }
}
