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

import java.util.function.Consumer;

/**
 * Backend-neutral coordinator leader election.
 *
 * <p>Implemented by {@code CoordinatorLeaderElection} (ZooKeeper, Curator {@code LeaderLatch}) and
 * {@code RaftLeaderSelector} (Ratis leadership). The lifecycle mirrors the existing ZooKeeper class
 * so {@code CoordinatorServer} needs no behavioral change when the backend is swapped.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.5.
 */
@Internal
public interface LeaderElection extends AutoCloseable {

    /**
     * Starts leader election asynchronously.
     *
     * @param initLeaderServices invoked once this node becomes leader (after catch-up)
     * @param cleanupLeaderServices invoked when this node loses leadership
     */
    void startElectLeaderAsync(
            Runnable initLeaderServices, Consumer<Throwable> cleanupLeaderServices);

    /** Whether this node is currently the coordinator leader. */
    boolean isLeader();

    /**
     * The current coordinator epoch (fencing token). The ZooKeeper impl returns the
     * znode-version-backed epoch; the Raft impl returns the Raft-term-backed epoch (see {@code
     * RaftEpoch}, §5.1.6). Both expose {@link CoordinatorEpoch#coordinatorEpoch()} consumed by
     * {@code CoordinatorContext}.
     */
    CoordinatorEpoch currentEpoch();

    @Override
    void close();
}
