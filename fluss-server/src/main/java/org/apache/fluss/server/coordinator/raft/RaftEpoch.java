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
import org.apache.fluss.server.coordinator.CoordinatorEpoch;

/**
 * Raft-term-backed {@link CoordinatorEpoch}.
 *
 * <p>The coordinator epoch maps to the Raft leader term ({@code
 * division.getInfo().getCurrentTerm()}). Only one leader per term can commit, so the term is a
 * structurally-unique fencing token — the old {@code (epoch, zkVersion)} tuple in {@code ZkEpoch}
 * collapses to a single monotonic term.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.6.
 */
@Internal
public final class RaftEpoch implements CoordinatorEpoch {

    private final long raftTerm;

    public RaftEpoch(long raftTerm) {
        this.raftTerm = raftTerm;
    }

    /** The underlying Raft leader term. */
    public long raftTerm() {
        return raftTerm;
    }

    @Override
    public int coordinatorEpoch() {
        return Math.toIntExact(raftTerm);
    }

    @Override
    public int coordinatorEpochVersion() {
        // The (epoch, zkVersion) tuple collapses to the Raft term (§5.1.6); the single-writer
        // leader subsumes the CAS, so this value is ignored by the Raft metadata client's writes.
        return coordinatorEpoch();
    }

    @Override
    public String toString() {
        return "RaftEpoch{raftTerm=" + raftTerm + '}';
    }
}
