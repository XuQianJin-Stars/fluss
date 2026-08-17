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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.data.BucketAssignment;
import org.apache.fluss.server.zk.data.TableAssignment;
import org.apache.fluss.server.zk.data.TableRegistration;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies {@link MetadataManager#createTable} allocates the table id first, then registers schema
 * + assignment + table in one batched store call (analysis §8.2).
 */
class MetadataManagerCreateTableTest {

    @Test
    void testCreateTableAllocatesIdBeforeBatchedRegister() throws Exception {
        MetadataStoreClient store = mock(MetadataStoreClient.class);
        when(store.databaseExists("db")).thenReturn(true);
        when(store.tableExist(any(TablePath.class))).thenReturn(false);
        when(store.getTableIdAndIncrement()).thenReturn(42L);

        MetadataManager manager =
                new MetadataManager(
                        store,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TablePath path = TablePath.of("db", "orders");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder().schema(schema).distributedBy(3).build();
        TableAssignment assignment =
                TableAssignment.builder().add(0, BucketAssignment.of(0, 1, 2)).build();

        long tableId =
                manager.createTable(path, "file:///tmp/remote", descriptor, assignment, false);

        assertThat(tableId).isEqualTo(42L);
        InOrder inOrder = inOrder(store);
        inOrder.verify(store).getTableIdAndIncrement();
        inOrder.verify(store)
                .registerTableSchemaAssignmentAndMetadata(
                        eq(path),
                        eq(schema),
                        eq(assignment),
                        org.mockito.ArgumentMatchers.argThat(
                                (TableRegistration reg) -> reg.tableId == 42L));
        verify(store, never()).registerFirstSchema(any(), any());
        verify(store, never()).registerTableAssignment(anyLong(), any());
        verify(store, never()).registerTable(any(), any());
        verify(store, never()).registerTable(any(), any(), anyBoolean());
    }

    @Test
    void testCreatePartitionedTablePassesNullAssignment() throws Exception {
        MetadataStoreClient store = mock(MetadataStoreClient.class);
        when(store.databaseExists("db")).thenReturn(true);
        when(store.tableExist(any(TablePath.class))).thenReturn(false);
        when(store.getTableIdAndIncrement()).thenReturn(7L);

        MetadataManager manager =
                new MetadataManager(
                        store,
                        new Configuration(),
                        new LakeCatalogDynamicLoader(new Configuration(), null, true));

        TablePath path = TablePath.of("db", "parted");
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .partitionedBy("id")
                        .build();

        assertThat(manager.createTable(path, "file:///tmp/remote", descriptor, null, false))
                .isEqualTo(7L);
        verify(store)
                .registerTableSchemaAssignmentAndMetadata(
                        eq(path), eq(schema), isNull(), any(TableRegistration.class));
    }
}
