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
import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.coordinator.raft.RaftMetadataClient;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperUtils;

/**
 * Creates the {@link MetadataStoreClient} for the backend selected by {@link
 * MetadataBackend#from(Configuration)}.
 *
 * <p>The ZooKeeper backend returns the existing {@code ZooKeeperClient} (which {@code implements
 * MetadataStoreClient}); the Raft backend bootstraps a {@link RaftMetadataClient} (§5.5.3), which
 * starts an embedded Ratis server from the configured voter set. Because {@link
 * MetadataBackend#from} defaults to {@code ZOOKEEPER}, the Raft path is only reachable when a user
 * explicitly opts in via {@code coordinator.seed.nodes}.
 *
 * <p><b>Scope note (T4-2):</b> this factory produces a working, self-contained {@code
 * RaftMetadataClient}. Threading it into {@code CoordinatorServer} and its downstream managers (the
 * 6-constructor {@code ZooKeeperClient} → {@code MetadataStoreClient} migration, §7.3.1) is the
 * remaining integration step and is intentionally not done here.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.5.4.
 */
@Internal
public final class MetadataStoreClientFactory {

    private MetadataStoreClientFactory() {}

    /**
     * Creates the metadata store client for the configured backend.
     *
     * @param conf the effective server configuration
     * @param fatalErrorHandler handler invoked on unrecoverable backend errors
     * @return the backend-specific {@link MetadataStoreClient}
     */
    public static MetadataStoreClient create(
            Configuration conf, FatalErrorHandler fatalErrorHandler) {
        MetadataBackend backend = MetadataBackend.from(conf);
        switch (backend) {
            case ZOOKEEPER:
                return ZooKeeperUtils.startZookeeperClient(conf, fatalErrorHandler);
            case RAFT:
                try {
                    return RaftMetadataClient.create(conf);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Failed to bootstrap the Raft metadata backend.", e);
                }
            default:
                throw new IllegalStateException("Unknown metadata backend: " + backend);
        }
    }
}
