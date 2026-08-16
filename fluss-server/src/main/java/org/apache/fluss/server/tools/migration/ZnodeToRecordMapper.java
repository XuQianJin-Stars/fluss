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

package org.apache.fluss.server.tools.migration;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.coordinator.raft.MetadataRecord;
import org.apache.fluss.server.zk.data.ZkData;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Maps a ZooKeeper znode path + value onto a {@link MetadataRecord}. Covers the core
 * databases/tables/assignments subset needed for offline export→import→validate.
 */
final class ZnodeToRecordMapper {

    private ZnodeToRecordMapper() {}

    static Optional<MetadataRecord> map(String path, byte[] value) {
        if (path == null || value == null) {
            return Optional.empty();
        }
        // /metadata/databases/{db} (no further slash after db name)
        if (path.startsWith(ZkData.DatabasesZNode.path() + "/")) {
            String rest = path.substring(ZkData.DatabasesZNode.path().length() + 1);
            if (!rest.contains("/")) {
                byte[] key = rest.getBytes(StandardCharsets.UTF_8);
                return Optional.of(
                        new MetadataRecord(MetadataRecord.Type.REGISTER_DATABASE, key, value));
            }
            // /metadata/databases/{db}/tables/{table}
            int tablesIdx = rest.indexOf("/tables/");
            if (tablesIdx > 0) {
                String db = rest.substring(0, tablesIdx);
                String afterTables = rest.substring(tablesIdx + "/tables/".length());
                if (!afterTables.contains("/")) {
                    TablePath tp = TablePath.of(db, afterTables);
                    byte[] key = tp.toString().getBytes(StandardCharsets.UTF_8);
                    return Optional.of(
                            new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, value));
                }
            }
        }
        // /tabletservers/tables/{tableId} assignment
        if (path.startsWith("/tabletservers/tables/")) {
            String rest = path.substring("/tabletservers/tables/".length());
            if (!rest.contains("/")) {
                byte[] key = rest.getBytes(StandardCharsets.UTF_8);
                return Optional.of(
                        new MetadataRecord(
                                MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT, key, value));
            }
        }
        return Optional.empty();
    }

    /**
     * Maps a disappeared znode path onto a DELETE/FENCE record so live-migration re-scans can
     * converge deletes (design §6.3).
     */
    static Optional<MetadataRecord> mapDelete(String path) {
        if (path == null) {
            return Optional.empty();
        }
        if (path.startsWith(ZkData.DatabasesZNode.path() + "/")) {
            String rest = path.substring(ZkData.DatabasesZNode.path().length() + 1);
            if (!rest.contains("/")) {
                byte[] key = rest.getBytes(StandardCharsets.UTF_8);
                return Optional.of(
                        new MetadataRecord(MetadataRecord.Type.DELETE_DATABASE, key, new byte[0]));
            }
            int tablesIdx = rest.indexOf("/tables/");
            if (tablesIdx > 0) {
                String db = rest.substring(0, tablesIdx);
                String afterTables = rest.substring(tablesIdx + "/tables/".length());
                if (!afterTables.contains("/")) {
                    TablePath tp = TablePath.of(db, afterTables);
                    byte[] key = tp.toString().getBytes(StandardCharsets.UTF_8);
                    return Optional.of(
                            new MetadataRecord(MetadataRecord.Type.DELETE_TABLE, key, new byte[0]));
                }
            }
        }
        if (path.startsWith("/tabletservers/tables/")) {
            String rest = path.substring("/tabletservers/tables/".length());
            if (!rest.contains("/")) {
                byte[] key = rest.getBytes(StandardCharsets.UTF_8);
                return Optional.of(
                        new MetadataRecord(
                                MetadataRecord.Type.DELETE_TABLE_ASSIGNMENT, key, new byte[0]));
            }
        }
        return Optional.empty();
    }
}
