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

import org.apache.fluss.annotation.Internal;

/**
 * A coordinator epoch (fencing token) that is monotonically increasing across coordinator leader
 * changes.
 *
 * <p>Implemented by both backends so that {@code CoordinatorContext} accepts either without change:
 *
 * <ul>
 *   <li>{@code ZkEpoch} — backed by the ZooKeeper {@code /coordinators/epoch} znode version.
 *   <li>{@code RaftEpoch} — backed by the Raft leader term (see {@code RaftEpoch}). Only one leader
 *       per term can commit, so the term is a natural, structurally-unique fencing token.
 * </ul>
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.6.
 */
@Internal
public interface CoordinatorEpoch {

    /** The coordinator epoch value used as the fencing token. */
    int coordinatorEpoch();

    /**
     * The version paired with the epoch, used as the compare-and-set token when persisting
     * leader-and-ISR / assignment updates.
     *
     * <ul>
     *   <li>{@code ZkEpoch} — the ZooKeeper {@code /coordinators/epoch} znode version (the real CAS
     *       token that fences a stale coordinator's writes).
     *   <li>{@code RaftEpoch} — the Raft leader term (the {@code (epoch, zkVersion)} tuple
     *       collapses to the term, §5.1.6). The single-writer Raft leader subsumes the CAS, so this
     *       value is ignored by the Raft metadata client's conditional writes.
     * </ul>
     */
    int coordinatorEpochVersion();
}
