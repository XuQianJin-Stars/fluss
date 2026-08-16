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

package org.apache.fluss.server.authorizer;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.plugin.Plugin;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.ZooKeeperClient;

import java.util.Optional;

/** AuthorizePlugin. */
public interface AuthorizationPlugin extends Plugin {

    String identifier();

    Authorizer createAuthorizer(Context context);

    /** Provides session information describing the authorizer to be accessed. */
    @PublicEvolving
    interface Context {

        /** Get configuration of fluss server to authorize. */
        Configuration getConfiguration();

        /**
         * Get zookeeper client to store authorization information.
         *
         * @deprecated use {@link #getMetadataStoreClient()} which is backend-neutral (ZooKeeper or
         *     Raft). Retained for source compatibility; returns empty on non-ZooKeeper backends.
         */
        @Deprecated
        Optional<ZooKeeperClient> getZooKeeperClient();

        /**
         * Get the backend-neutral metadata store client to store authorization information.
         *
         * <p>The default implementation widens {@link #getZooKeeperClient()} so existing {@link
         * Context} implementations remain source-compatible ({@code ZooKeeperClient} already
         * implements {@link MetadataStoreClient}). Backends that are not ZooKeeper-backed override
         * this to supply their own client.
         */
        default Optional<MetadataStoreClient> getMetadataStoreClient() {
            return getZooKeeperClient().<MetadataStoreClient>map(client -> client);
        }
    }
}
