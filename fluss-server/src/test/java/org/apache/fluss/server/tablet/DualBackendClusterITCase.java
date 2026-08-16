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
import org.apache.fluss.server.testutils.DualBackendFlussClusters;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.server.testutils.RpcMessageTestUtils;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Dual-backend parameterized smoke IT (T8-1): the same create-table / list-tables path must work on
 * both {@link MetadataBackend#ZOOKEEPER} and {@link MetadataBackend#RAFT}.
 */
class DualBackendClusterITCase {

    @RegisterExtension
    public static final DualBackendFlussClusters CLUSTERS =
            DualBackendFlussClusters.builder().setNumOfTabletServers(1).build();

    @ParameterizedTest
    @EnumSource(MetadataBackend.class)
    void testCreateAndListTable(MetadataBackend backend) throws Exception {
        FlussClusterExtension cluster = CLUSTERS.get(backend);
        assertThat(cluster.getMetadataBackend()).isEqualTo(backend);
        assertThat(cluster.getCoordinatorServer().isLeader()).isTrue();

        TablePath tablePath = TablePath.of("fluss", "dual_" + backend.name().toLowerCase());
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();

        long tableId = RpcMessageTestUtils.createTable(cluster, tablePath, descriptor);
        assertThat(tableId).isGreaterThanOrEqualTo(0L);

        CoordinatorGateway gateway = cluster.newCoordinatorClient();
        assertThat(
                        gateway.listTables(new ListTablesRequest().setDatabaseName("fluss"))
                                .get()
                                .getTableNamesList())
                .contains(tablePath.getTableName());
    }
}
