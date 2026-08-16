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

import org.apache.fluss.annotation.Internal;

import java.nio.ByteBuffer;

/**
 * Encodes/decodes a {@link MetadataRecord} to/from a self-describing byte array.
 *
 * <p>Wire format (big-endian):
 *
 * <pre>
 * [1B version][2B type ordinal][4B keyLen][key bytes][4B payloadLen][payload bytes]
 * </pre>
 *
 * <p>{@code keyLen == -1} encodes a {@code null} logical key. The leading version byte allows
 * additive schema evolution (see §5.11); only {@link #VERSION} {@code 1} is defined today. Bump
 * {@link #VERSION} only additively — older binaries must still decode historical records. Cluster
 * feature gating uses the separate persisted {@code metadata.version} ({@link MetadataVersions});
 * once that level is raised, downgrade to an older binary is blocked unless a metadata downgrade
 * step rewrites unsupported records.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.3 / §5.11.
 */
@Internal
public final class MetadataRecordCodec {

    /**
     * Current record wire-format version (additive only). Distinct from cluster {@link
     * MetadataVersions#CURRENT_METADATA_VERSION}.
     */
    public static final byte VERSION = 1;

    private static final int NULL_KEY = -1;
    private static final MetadataRecord.Type[] TYPES = MetadataRecord.Type.values();

    private MetadataRecordCodec() {}

    public static byte[] encode(MetadataRecord record) {
        byte[] key = record.key();
        byte[] payload = record.payload();
        int keyLen = key == null ? 0 : key.length;
        int capacity = 1 + 2 + 4 + keyLen + 4 + payload.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.put(VERSION);
        buffer.putShort((short) record.type().ordinal());
        if (key == null) {
            buffer.putInt(NULL_KEY);
        } else {
            buffer.putInt(key.length);
            buffer.put(key);
        }
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    public static MetadataRecord decode(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes must not be null");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte version = buffer.get();
        if (version != VERSION) {
            throw new IllegalArgumentException(
                    "Unsupported MetadataRecord version: "
                            + version
                            + " (expected "
                            + VERSION
                            + ")");
        }
        int typeOrdinal = buffer.getShort() & 0xFFFF;
        if (typeOrdinal < 0 || typeOrdinal >= TYPES.length) {
            throw new IllegalArgumentException(
                    "Unknown MetadataRecord type ordinal: " + typeOrdinal);
        }
        MetadataRecord.Type type = TYPES[typeOrdinal];
        int keyLen = buffer.getInt();
        byte[] key = null;
        if (keyLen != NULL_KEY) {
            if (keyLen < 0) {
                throw new IllegalArgumentException("Invalid key length: " + keyLen);
            }
            key = new byte[keyLen];
            buffer.get(key);
        }
        int payloadLen = buffer.getInt();
        if (payloadLen < 0) {
            throw new IllegalArgumentException("Invalid payload length: " + payloadLen);
        }
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        return new MetadataRecord(type, key, payload);
    }
}
