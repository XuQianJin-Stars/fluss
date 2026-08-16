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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MetadataRecordCodec} and {@link MetadataRecord}. */
class MetadataRecordCodecTest {

    @Test
    void testRoundTripWithKeyAndPayload() {
        byte[] key = "db.table".getBytes(StandardCharsets.UTF_8);
        byte[] payload = "{\"tableId\":42}".getBytes(StandardCharsets.UTF_8);
        MetadataRecord record =
                new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, key, payload);

        MetadataRecord decoded = MetadataRecord.fromBytes(record.toBytes());

        assertThat(decoded.type()).isEqualTo(MetadataRecord.Type.REGISTER_TABLE);
        assertThat(decoded.key()).containsExactly(key);
        assertThat(decoded.payload()).containsExactly(payload);
    }

    @Test
    void testRoundTripWithNullKey() {
        byte[] payload = "global-config".getBytes(StandardCharsets.UTF_8);
        MetadataRecord record =
                new MetadataRecord(MetadataRecord.Type.UPDATE_DYNAMIC_CONFIG, null, payload);

        MetadataRecord decoded = MetadataRecord.fromBytes(record.toBytes());

        assertThat(decoded.type()).isEqualTo(MetadataRecord.Type.UPDATE_DYNAMIC_CONFIG);
        assertThat(decoded.key()).isNull();
        assertThat(decoded.payload()).containsExactly(payload);
    }

    @Test
    void testRoundTripWithEmptyKeyIsNotNull() {
        MetadataRecord record =
                new MetadataRecord(MetadataRecord.Type.ALLOCATE_ID_RANGE, new byte[0], new byte[0]);

        MetadataRecord decoded = MetadataRecord.fromBytes(record.toBytes());

        assertThat(decoded.key()).isNotNull().isEmpty();
        assertThat(decoded.payload()).isEmpty();
    }

    @Test
    void testAllTypesRoundTrip() {
        for (MetadataRecord.Type type : MetadataRecord.Type.values()) {
            MetadataRecord record = new MetadataRecord(type, new byte[] {1, 2, 3}, new byte[] {9});
            assertThat(MetadataRecord.fromBytes(record.toBytes()).type()).isEqualTo(type);
        }
    }

    @Test
    void testDecodeRejectsUnsupportedVersion() {
        byte[] bytes = new byte[] {2, 0, 0, -1, -1, -1, -1, 0, 0, 0, 0};
        assertThatThrownBy(() -> MetadataRecordCodec.decode(bytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
    }

    @Test
    void testConstructorRejectsNulls() {
        assertThatThrownBy(() -> new MetadataRecord(null, null, new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MetadataRecord(MetadataRecord.Type.REGISTER_TABLE, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
