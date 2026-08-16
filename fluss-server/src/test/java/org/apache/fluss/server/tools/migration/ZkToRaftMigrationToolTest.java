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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Offline export→import→validate fixture for {@link ZkToRaftMigrationTool}. */
class ZkToRaftMigrationToolTest {

    @TempDir Path tempDir;

    @Test
    void testExportImportValidateRoundTripOnFixture() throws Exception {
        Path export = tempDir.resolve("export.jsonl");
        String dbPayload = "{\"comment\":\"mig\"}";
        String tablePayload = "{\"table_id\":7,\"schema_id\":1,\"bucket_count\":1}";
        String assignPayload = "{\"buckets\":{\"0\":[0]}}";

        try (java.io.BufferedWriter w = Files.newBufferedWriter(export)) {
            write(w, "/metadata/databases/testdb", dbPayload);
            write(w, "/metadata/databases/testdb/tables/t1", tablePayload);
            write(w, "/tabletservers/tables/7", assignPayload);
            // Deeper child should be ignored by core validate + mapper
            write(w, "/metadata/databases/testdb/tables/t1/schemas/1", "{}");
        }

        ZkToRaftMigrationTool tool = new ZkToRaftMigrationTool();
        tool.importToRaft(export, Collections.emptyList());
        Path imported = tempDir.resolve("export.jsonl.imported.jsonl");
        assertThat(imported).exists();
        assertThat(tool.validate(export, imported)).isTrue();
    }

    private static void write(java.io.BufferedWriter w, String path, String payload)
            throws Exception {
        String b64 = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        w.write("{\"path\":\"" + path + "\",\"b64\":\"" + b64 + "\"}");
        w.newLine();
    }
}
