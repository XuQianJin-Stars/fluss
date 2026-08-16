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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;

import static org.apache.fluss.testutils.common.CommonTestUtils.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extension-level tablet rediscovery after coordinator bounce (T8-2).
 *
 * <p>Stops the Raft coordinator (reusing the same Raft dir/port), starts it again, restarts the
 * tablet so registration + metadata stream resubscribe, then asserts DDL works again. Complements
 * {@code MetadataStreamChaosITCase} / {@code RaftClusterITCase} which cover in-process Ratis
 * failover without recycling the coordinator JVM process.
 *
 * <p>Thread-model note (design §5.6): Raft appends and metadata-stream apply stay on their
 * dedicated threads; this test only exercises discovery + registration after a clean coordinator
 * restart.
 */
class RaftCoordinatorFailoverReconnectITCase {

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setMetadataBackend(MetadataBackend.RAFT)
                    .build();

    @Test
    void testCoordinatorRestartThenTabletReconnect() throws Exception {
        TablePath before = TablePath.of("fluss", "raft_failover_before");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        RpcMessageTestUtils.createTable(CLUSTER, before, descriptor);

        CLUSTER.stopCoordinatorServer();
        CLUSTER.startCoordinatorServer();
        assertThat(CLUSTER.getCoordinatorServer().isLeader()).isTrue();

        // Tablet keeps the old seed FLUSS address; restart so registration + stream subscribe
        // against the restarted coordinator.
        CLUSTER.restartTabletServer(0, null);

        waitUntil(
                () -> CLUSTER.getCoordinatorServer().isLeader(),
                Duration.ofSeconds(30),
                "coordinator lost leadership after tablet restart");

        CoordinatorGateway gateway = CLUSTER.newCoordinatorClient();
        waitUntil(
                () -> {
                    try {
                        List<String> tables =
                                gateway.listTables(new ListTablesRequest().setDatabaseName("fluss"))
                                        .get()
                                        .getTableNamesList();
                        return tables.contains("raft_failover_before");
                    } catch (Exception e) {
                        return false;
                    }
                },
                Duration.ofSeconds(60),
                "pre-failover table not visible after reconnect");

        // listTables only needs Raft metadata; createTable also needs the tablet re-registered.
        CLUSTER.assertHasTabletServerNumber(1);

        TablePath after = TablePath.of("fluss", "raft_failover_after");
        long tableId = RpcMessageTestUtils.createTable(CLUSTER, after, descriptor);
        assertThat(tableId).isGreaterThanOrEqualTo(0L);
        assertThat(CLUSTER.getCoordinatorServer().isLeader()).isTrue();
    }
}
