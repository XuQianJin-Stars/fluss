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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the metadata + coordination backend for a Fluss cluster.
 *
 * <p>During the ZooKeeper-removal dual-support phase both backends coexist:
 *
 * <ul>
 *   <li>{@link #ZOOKEEPER} — the legacy backend backed by an external ZooKeeper ensemble
 *       (deprecated; still supported until a future major release removes it).
 *   <li>{@link #RAFT} — the embedded Apache-Ratis-based backend that replaces ZooKeeper. Opt in by
 *       setting {@code coordinator.seed.nodes}.
 * </ul>
 *
 * <p>There is no implicit default: exactly one of {@code zookeeper.address} or {@code
 * coordinator.seed.nodes} must be set. See {@code zookeeper-removal-design.md} §5.5.4.
 */
@Internal
public enum MetadataBackend {
    ZOOKEEPER,
    RAFT;

    private static final Logger LOG = LoggerFactory.getLogger(MetadataBackend.class);

    /**
     * Resolves the backend from configuration during the dual-support phase.
     *
     * <p>Exactly one of {@link ConfigOptions#ZOOKEEPER_ADDRESS} or {@link
     * ConfigOptions#COORDINATOR_SEED_NODES} must be set. ZooKeeper is selected by setting {@code
     * zookeeper.address} (and omitting seed nodes). Raft is selected by setting {@code
     * coordinator.seed.nodes} (and omitting the ZooKeeper address). Setting both, or setting
     * neither, is rejected so an incomplete upgrade cannot silently start a metadata backend.
     *
     * @param conf the cluster configuration
     * @return the resolved backend
     * @throws IllegalConfigurationException if both keys are set, or if neither key is set
     */
    public static MetadataBackend from(Configuration conf) {
        boolean hasZk = conf.getOptional(ConfigOptions.ZOOKEEPER_ADDRESS).isPresent();
        boolean hasRaft = conf.getOptional(ConfigOptions.COORDINATOR_SEED_NODES).isPresent();
        if (hasZk == hasRaft) {
            throw new IllegalConfigurationException(
                    "Set exactly one of '"
                            + ConfigOptions.ZOOKEEPER_ADDRESS.key()
                            + "' or '"
                            + ConfigOptions.COORDINATOR_SEED_NODES.key()
                            + "'.");
        }
        if (hasRaft) {
            return RAFT;
        }
        LOG.warn(
                "ZooKeeper metadata backend is deprecated and will be removed in a future "
                        + "major release. Migrate to the Raft backend "
                        + "({} / {}); see the ZK-to-Raft migration guide.",
                ConfigOptions.COORDINATOR_SEED_NODES.key(),
                ConfigOptions.COORDINATOR_RAFT_VOTERS.key());
        return ZOOKEEPER;
    }
}
