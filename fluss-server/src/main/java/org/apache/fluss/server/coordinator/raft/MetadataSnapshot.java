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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A point-in-time terminal-state image of the metadata KvStore, tagged with the metadata offset (==
 * Raft log index) of the LAST record folded into it.
 *
 * <p>This is the payload of {@code FetchMetadataSnapshot} (§5.8.1 point 4): a subscriber whose
 * requested {@code fromOffset} is older than the leader's Raft log start (compacted) bootstraps
 * from this image, sets its applied offset to {@link #snapshotMetadataOffset()}, then resumes the
 * delta stream at {@code snapshotMetadataOffset + 1}. Because the image is a key→value terminal
 * state (not a replayable delta log), the offset tag MUST be captured atomically with the store
 * contents, otherwise the snapshot→delta seam goes off-by-one (== corruption). {@link
 * FlussMetadataStateMachine#captureSnapshot()} guarantees that atomicity.
 *
 * <p>Wire format of {@link #image()}: {@code [int count]} then {@code count} × {@code [int
 * keyLen][key bytes][int valLen][val bytes]} — the same length-prefixed layout the state machine
 * writes to its on-disk snapshot file (§5.2.5), so the two never diverge.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.1 / §5.2.5.
 */
@Internal
public final class MetadataSnapshot {

    private final long snapshotMetadataOffset;
    private final byte[] image;

    public MetadataSnapshot(long snapshotMetadataOffset, byte[] image) {
        this.snapshotMetadataOffset = snapshotMetadataOffset;
        this.image = image;
    }

    /** The metadata offset (Raft index) of the last record reflected in {@link #image()}. */
    public long snapshotMetadataOffset() {
        return snapshotMetadataOffset;
    }

    /** The serialized KvStore terminal state (see class doc for the wire format). */
    public byte[] image() {
        return image;
    }

    /** Convenience: deserializes {@link #image()} back into {@code [key, value]} pairs. */
    public List<byte[][]> entries() {
        return deserializeEntries(image);
    }

    /** Serializes {@code [key, value]} pairs into the length-prefixed image format. */
    public static byte[] serializeEntries(List<byte[][]> entries) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(entries.size());
            for (byte[][] entry : entries) {
                byte[] key = entry[0];
                byte[] value = entry[1];
                out.writeInt(key.length);
                out.write(key);
                out.writeInt(value.length);
                out.write(value);
            }
        } catch (IOException e) {
            // ByteArrayOutputStream never throws IOException; guard defensively.
            throw new UncheckedIOException("Failed to serialize metadata snapshot image", e);
        }
        return bos.toByteArray();
    }

    /** Inverse of {@link #serializeEntries}. */
    public static List<byte[][]> deserializeEntries(byte[] image) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(image))) {
            int count = in.readInt();
            List<byte[][]> entries = new ArrayList<>(Math.max(0, count));
            for (int i = 0; i < count; i++) {
                byte[] key = new byte[in.readInt()];
                in.readFully(key);
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                entries.add(new byte[][] {key, value});
            }
            return entries;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize metadata snapshot image", e);
        }
    }
}
