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

package org.apache.fluss.server.tablet;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.RetriableException;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ErrorMessage;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.server.log.LogSegment;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.testutils.DualBackendFlussClusters;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.record.TestData.DATA1;
import static org.apache.fluss.record.TestData.DATA_1_WITH_KEY_AND_VALUE;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.createTable;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newProduceLogRequest;
import static org.apache.fluss.server.testutils.RpcMessageTestUtils.newPutKvRequest;
import static org.apache.fluss.testutils.DataTestUtils.genKvRecordBatch;
import static org.apache.fluss.testutils.DataTestUtils.genMemoryLogRecordsByObject;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;

/** The ITCase for tabletServer shutdown (controlled shutdown). Runs on ZooKeeper and Raft. */
public class TabletServerShutdownITCase {
    @RegisterExtension
    public static final DualBackendFlussClusters CLUSTERS =
            DualBackendFlussClusters.builder().setNumOfTabletServers(3).build();

    private FlussClusterExtension flussClusterExtension;

    private void bind(MetadataBackend backend) {
        flussClusterExtension = CLUSTERS.get(backend);
    }

    @ParameterizedTest
    @MethodSource("org.apache.fluss.server.testutils.DualBackendFlussClusters#backendsAndBooleans")
    void testIOExceptionShouldStopTabletServer(MetadataBackend backend, boolean isLogTable)
            throws Exception {
        bind(backend);
        flussClusterExtension.assertHasTabletServerNumber(3);
        Schema schema =
                isLogTable
                        ? Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .build()
                        : Schema.newBuilder()
                                .column("a", DataTypes.INT())
                                .column("b", DataTypes.STRING())
                                .primaryKey("a")
                                .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();

        TablePath tablePath =
                TablePath.of(
                        "test_failover", "test_ioexception_table_" + (isLogTable ? "log" : "pk"));
        long tableId = createTable(flussClusterExtension, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        flussClusterExtension.waitUntilAllReplicaReady(tb);

        int leader = flussClusterExtension.waitAndGetLeader(tb);
        TabletServerGateway leaderGateWay =
                flussClusterExtension.newTabletServerClientForNode(leader);

        // delete the active segment, which will cause IOException when append log/changelog
        LogSegment logSegment =
                flussClusterExtension.waitAndGetLeaderReplica(tb).getLogTablet().activeLogSegment();
        logSegment.deleteIfExists();

        ErrorMessage errorResponse = null;
        ExecutionException requestFailure = null;
        try {
            errorResponse = writeData(leaderGateWay, tableId, isLogTable);
        } catch (ExecutionException e) {
            requestFailure = e;
        }

        TabletServer leaderServer = flussClusterExtension.getTabletServerById(leader);
        try {
            // The storage IOException should trigger the fatal error handler and stop the leader
            // server. The failed request may either receive the storage error response or fail when
            // the RPC connection is closed, depending on which completes first.
            flussClusterExtension.assertHasTabletServerNumber(2);
        } finally {
            // Wait for the fatal shutdown to finish before reusing the same server ID and data dir.
            leaderServer.closeAsync().get();
            flussClusterExtension.startTabletServer(leader, true);
        }

        if (requestFailure != null) {
            assertThat(requestFailure).cause().isInstanceOf(RetriableException.class);
        } else {
            assertThat(errorResponse).isNotNull();
            assertThat(errorResponse.hasErrorCode()).isTrue();
            assertThat(errorResponse.getErrorCode())
                    .isEqualTo(
                            isLogTable
                                    ? Errors.LOG_STORAGE_EXCEPTION.code()
                                    : Errors.KV_STORAGE_EXCEPTION.code());
        }
    }

    @Test
    void testControlledShutdownConfiguration() throws Exception {
        // Test that the controlled shutdown configuration options are properly loaded
        Configuration conf = new Configuration();

        // Verify default values are loaded correctly
        assertThat(conf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(3);
        assertThat(
                        conf.get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(1000L);

        // Test custom configuration values
        Configuration customConf = new Configuration();
        customConf.set(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES, 5);
        customConf.set(
                ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL,
                Duration.ofMillis(2000));

        assertThat(customConf.getInt(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_MAX_RETRIES))
                .isEqualTo(5);
        assertThat(
                        customConf
                                .get(ConfigOptions.TABLET_SERVER_CONTROLLED_SHUTDOWN_RETRY_INTERVAL)
                                .toMillis())
                .isEqualTo(2000L);
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testControlledShutdown(MetadataBackend backend) throws Exception {
        bind(backend);
        flussClusterExtension.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .build();
        TablePath tablePath = TablePath.of("test_shutdown", "test_controlled_shutdown");
        long tableId = createTable(flussClusterExtension, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = flussClusterExtension.waitLeaderAndIsrReady(tb);
        int leader = leaderAndIsr.leader();

        // test kill the tabletServers with leader on.
        flussClusterExtension.stopTabletServer(leader);
        MetadataStoreClient zkClient = flussClusterExtension.metadataStore();

        // the leader should be removed from isr, and new leader should be elected.
        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(zkClient.getLeaderAndIsr(tb))
                                .map(LeaderAndIsr::leader)
                                .isNotEqualTo(leader));

        // restart the shutdown server
        flussClusterExtension.startTabletServer(leader, true);
    }

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testControlledShutdownRetriesFailover(MetadataBackend backend) throws Exception {
        bind(backend);
        // This case is to test the scenario that the controlled shutdown request is retried and
        // failed by cannot elect any new leader. In this case the controlled shutdown will finally
        // go uncontrolled shutdown.
        flussClusterExtension.assertHasTabletServerNumber(3);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(Schema.newBuilder().column("a", DataTypes.INT()).build())
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 2)
                        .build();
        TablePath tablePath = TablePath.of("test_failover", "test_controlled_shutdown_failed");
        long tableId = createTable(flussClusterExtension, tablePath, tableDescriptor);
        TableBucket tb = new TableBucket(tableId, 0);

        LeaderAndIsr leaderAndIsr = flussClusterExtension.waitLeaderAndIsrReady(tb);
        List<Integer> isr = new ArrayList<>(leaderAndIsr.isr());
        int leader = leaderAndIsr.leader();
        isr.remove(Integer.valueOf(leader));
        int follower = isr.get(0);

        // Let's kil follower. Will go controlled shutdown.
        flussClusterExtension.stopTabletServer(follower);
        MetadataStoreClient zkClient = flussClusterExtension.metadataStore();

        // Follower leaves ISR. Do not pin bucketEpoch: Raft may record extra LeaderAndIsr
        // writes (epoch 3 vs 1) for the same ISR shrink.
        retry(
                Duration.ofMinutes(1),
                () -> {
                    LeaderAndIsr actual = zkClient.getLeaderAndIsr(tb).get();
                    assertThat(actual.leader()).isEqualTo(leader);
                    assertThat(actual.isr()).containsExactly(leader);
                });

        // kill the leader. As we only have 1 replica, no leader can be elected as we send the
        // controlled shutdown request to the leader. So the controlled shutdown will finally go
        // uncontrolled shutdown.
        flussClusterExtension.stopTabletServer(leader);

        retry(
                Duration.ofMinutes(1),
                () -> {
                    LeaderAndIsr actual = zkClient.getLeaderAndIsr(tb).get();
                    assertThat(actual.leader()).isEqualTo(LeaderAndIsr.NO_LEADER);
                    assertThat(actual.isr()).containsExactly(leader);
                });

        // start the follower
        // should still be no leader since the follower is out of isr, should be elected as leader
        flussClusterExtension.startTabletServer(follower);

        // start the leader server, the leader should be the previous leader server
        flussClusterExtension.startTabletServer(leader);
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(zkClient.getLeaderAndIsr(tb).get().leader()).isEqualTo(leader));
    }

    private ErrorMessage writeData(
            TabletServerGateway tabletServerGateway, long tableId, boolean isLogTable)
            throws Exception {
        if (isLogTable) {
            return tabletServerGateway
                    .produceLog(
                            newProduceLogRequest(tableId, 0, 1, genMemoryLogRecordsByObject(DATA1)))
                    .get()
                    .getBucketsRespAt(0);
        } else {
            return tabletServerGateway
                    .putKv(
                            newPutKvRequest(
                                    tableId, 0, 1, genKvRecordBatch(DATA_1_WITH_KEY_AND_VALUE)))
                    .get()
                    .getBucketsRespAt(0);
        }
    }
}
