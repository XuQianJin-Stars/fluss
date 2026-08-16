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
 * Cluster {@code metadata.version} helpers (§5.11) — the KRaft feature-gate analog.
 *
 * <p>The persisted cluster version is a single {@link MetadataRecord.Type#UPDATE_METADATA_VERSION}
 * record. The leader refuses to bump it (and therefore to emit features that require a higher
 * level) until every known voter has advertised a max supported version at least as high as the
 * target. {@link #CURRENT_METADATA_VERSION} is what <em>this binary</em> understands.
 *
 * <p><b>Downgrade limitation:</b> once the cluster has written a higher {@code metadata.version},
 * downgrade to an older binary is unsafe unless a metadata downgrade step rewrites/removes records
 * that the older binary cannot apply. {@link MetadataRecordCodec#VERSION} stays additive for the
 * per-record wire format; do not reuse or drop historical record-type ordinals.
 */
@Internal
public final class MetadataVersions {

    /**
     * Highest {@code metadata.version} this coordinator binary understands and may emit. Bump only
     * when introducing a new gated feature / record type that older peers cannot apply.
     */
    public static final int CURRENT_METADATA_VERSION = 1;

    /**
     * Value returned by {@link RaftMetadataClient#getMetadataVersion()} when no {@link
     * MetadataRecord.Type#UPDATE_METADATA_VERSION} record has been committed yet.
     */
    public static final int UNSET_METADATA_VERSION = 0;

    private MetadataVersions() {}

    /** Encodes a version as a big-endian 4-byte payload. */
    public static byte[] encode(int version) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(version).array();
    }

    /** Decodes a big-endian 4-byte version payload. */
    public static int decode(byte[] payload) {
        if (payload == null || payload.length < Integer.BYTES) {
            throw new IllegalArgumentException(
                    "metadata.version payload must be at least " + Integer.BYTES + " bytes");
        }
        return ByteBuffer.wrap(payload).getInt();
    }
}
