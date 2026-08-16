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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.ValidationException;
import org.apache.fluss.plugin.PluginManager;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.ZooKeeperClient;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.AUTHORIZER_ENABLED;
import static org.apache.fluss.config.ConfigOptions.AUTHORIZER_TYPE;

/** authorizer loader. */
public class AuthorizerLoader {

    /** Load authorizer. */
    public static @Nullable Authorizer createAuthorizer(
            Configuration configuration,
            @Nullable MetadataStoreClient metadataStoreClient,
            @Nullable PluginManager pluginManager) {
        if (!configuration.get(AUTHORIZER_ENABLED)) {
            return null;
        }
        String authorizerType = configuration.get(AUTHORIZER_TYPE);
        Collection<Supplier<Iterator<AuthorizationPlugin>>> pluginSuppliers = new ArrayList<>(2);
        pluginSuppliers.add(
                () ->
                        ServiceLoader.load(
                                        AuthorizationPlugin.class,
                                        AuthorizationPlugin.class.getClassLoader())
                                .iterator());

        if (pluginManager != null) {
            pluginSuppliers.add(() -> pluginManager.load(AuthorizationPlugin.class));
        }

        List<AuthorizationPlugin> matchingPlugins = new ArrayList<>();
        for (Supplier<Iterator<AuthorizationPlugin>> pluginSupplier : pluginSuppliers) {
            Iterator<AuthorizationPlugin> plugins = pluginSupplier.get();
            while (plugins.hasNext()) {
                AuthorizationPlugin plugin = plugins.next();
                if (plugin.identifier().equals(authorizerType)) {
                    matchingPlugins.add(plugin);
                }
            }
        }

        if (matchingPlugins.size() != 1) {
            throw new ValidationException(
                    String.format(
                            "Could not find same authorizer plugin for protocol '%s' in the classpath.\n\n"
                                    + "Available factory protocols are:\n\n"
                                    + "%s",
                            authorizerType,
                            matchingPlugins.stream()
                                    .map(f -> f.getClass().getName())
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        return matchingPlugins
                .get(0)
                .createAuthorizer(new DefaultContext(configuration, metadataStoreClient));
    }

    /** Default implementation of {@link AuthorizationPlugin.Context}. */
    public static class DefaultContext implements AuthorizationPlugin.Context {
        private final Configuration configuration;
        @Nullable private final MetadataStoreClient metadataStoreClient;

        public DefaultContext(
                Configuration configuration, @Nullable MetadataStoreClient metadataStoreClient) {
            this.configuration = configuration;
            this.metadataStoreClient = metadataStoreClient;
        }

        @Override
        public Configuration getConfiguration() {
            return configuration;
        }

        @Override
        public Optional<ZooKeeperClient> getZooKeeperClient() {
            return metadataStoreClient instanceof ZooKeeperClient
                    ? Optional.of((ZooKeeperClient) metadataStoreClient)
                    : Optional.empty();
        }

        @Override
        public Optional<MetadataStoreClient> getMetadataStoreClient() {
            return Optional.ofNullable(metadataStoreClient);
        }
    }
}
