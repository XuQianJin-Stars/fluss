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

package org.apache.fluss.server.coordinator.gossip;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Binary codec for a {@link GossipView} (magic + version + members). */
final class GossipCodec {

    static final int MAGIC = 0xF1055001;
    static final byte VERSION = 1;

    private GossipCodec() {}

    static void write(DataOutputStream out, GossipView view) throws IOException {
        List<GossipMember> members = view.members();
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeInt(members.size());
        for (GossipMember m : members) {
            out.writeUTF(m.nodeId());
            out.writeUTF(m.rpcHost());
            out.writeInt(m.rpcPort());
            out.writeUTF(m.gossipHost());
            out.writeInt(m.gossipPort());
            out.writeLong(m.heartbeat());
            out.writeInt(m.incarnation());
        }
    }

    static GossipView read(DataInputStream in, long nowMs) throws IOException {
        int magic = in.readInt();
        if (magic != MAGIC) {
            throw new IOException("Unexpected gossip magic: 0x" + Integer.toHexString(magic));
        }
        byte version = in.readByte();
        if (version != VERSION) {
            throw new IOException("Unsupported gossip version: " + version);
        }
        int n = in.readInt();
        if (n < 0 || n > 10_000) {
            throw new IOException("Invalid gossip member count: " + n);
        }
        List<GossipMember> members = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            members.add(
                    new GossipMember(
                            in.readUTF(),
                            in.readUTF(),
                            in.readInt(),
                            in.readUTF(),
                            in.readInt(),
                            in.readLong(),
                            in.readInt()));
        }
        return GossipView.of(members, nowMs);
    }
}
