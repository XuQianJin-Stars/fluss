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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke IT for {@link FlussClusterExtension} on the Raft metadata backend: boots a real Raft
 * coordinator + tablet server(s) with no ZooKeeper and exercises create-table / list-tables over
 * the coordinator gateway.
 */
class RaftFlussClusterExtensionITCase {

    @RegisterExtension
    public static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder()
                    .setNumOfTabletServers(1)
                    .setMetadataBackend(MetadataBackend.RAFT)
                    .build();

    @Test
    void testCreateAndListTableOnRaftCluster() throws Exception {
        assertThat(CLUSTER.getMetadataBackend()).isEqualTo(MetadataBackend.RAFT);
        assertThat(CLUSTER.getCoordinatorServer().isLeader()).isTrue();

        TablePath tablePath = TablePath.of("fluss", "raft_ext_t1");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();

        long tableId = RpcMessageTestUtils.createTable(CLUSTER, tablePath, descriptor);
        assertThat(tableId).isGreaterThanOrEqualTo(0L);

        CoordinatorGateway gateway = CLUSTER.newCoordinatorClient();
        assertThat(
                        gateway.listTables(new ListTablesRequest().setDatabaseName("fluss"))
                                .get()
                                .getTableNamesList())
                .contains("raft_ext_t1");
    }
}
