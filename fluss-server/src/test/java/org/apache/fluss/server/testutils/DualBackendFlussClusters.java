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

package org.apache.fluss.server.testutils;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.utils.clock.Clock;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Starts a ZooKeeper cluster and a Raft cluster so the same HA IT can run on both metadata
 * backends.
 *
 * <p>Use with {@code @ParameterizedTest} + {@code @EnumSource(MetadataBackend.class)} (or {@link
 * #backendsAndBooleans()}) and {@link #get(MetadataBackend)}.
 */
public final class DualBackendFlussClusters
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    private final FlussClusterExtension zkCluster;
    private final FlussClusterExtension raftCluster;

    private DualBackendFlussClusters(
            FlussClusterExtension zkCluster, FlussClusterExtension raftCluster) {
        this.zkCluster = zkCluster;
        this.raftCluster = raftCluster;
    }

    /** Creates a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the cluster for {@code backend}. */
    public FlussClusterExtension get(MetadataBackend backend) {
        checkNotNull(backend, "backend");
        return backend == MetadataBackend.RAFT ? raftCluster : zkCluster;
    }

    /** Arguments for {@code @MethodSource}: one invocation per {@link MetadataBackend}. */
    public static Stream<Arguments> backends() {
        return Arrays.stream(MetadataBackend.values()).map(Arguments::of);
    }

    /**
     * Arguments for {@code @MethodSource}: cartesian product of {@link MetadataBackend} and {@code
     * true}/{@code false}.
     */
    public static Stream<Arguments> backendsAndBooleans() {
        List<Arguments> args = new ArrayList<>();
        for (MetadataBackend backend : MetadataBackend.values()) {
            args.add(Arguments.of(backend, true));
            args.add(Arguments.of(backend, false));
        }
        return args.stream();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        zkCluster.beforeAll(context);
        raftCluster.beforeAll(context);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        Exception first = null;
        try {
            raftCluster.afterAll(context);
        } catch (Exception e) {
            first = e;
        }
        try {
            zkCluster.afterAll(context);
        } catch (Exception e) {
            if (first == null) {
                first = e;
            } else {
                first.addSuppressed(e);
            }
        }
        if (first != null) {
            throw first;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        zkCluster.beforeEach(context);
        raftCluster.beforeEach(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Exception first = null;
        try {
            raftCluster.afterEach(context);
        } catch (Exception e) {
            first = e;
        }
        try {
            zkCluster.afterEach(context);
        } catch (Exception e) {
            if (first == null) {
                first = e;
            } else {
                first.addSuppressed(e);
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /** Builder mirroring {@link FlussClusterExtension.Builder} applied to both backends. */
    public static final class Builder {
        private int numOfTabletServers = 1;
        private String tabletServerListeners;
        private String coordinatorServerListeners;
        private Configuration clusterConf;
        private Clock clock;
        private String[] racks;

        /** Sets the number of tablet servers started in each backend cluster. */
        public Builder setNumOfTabletServers(int numOfTabletServers) {
            this.numOfTabletServers = numOfTabletServers;
            return this;
        }

        /** Sets cluster configuration copied independently into each backend. */
        public Builder setClusterConf(Configuration clusterConf) {
            this.clusterConf = clusterConf;
            return this;
        }

        /** Sets tablet-server listeners for both backends. */
        public Builder setTabletServerListeners(String tabletServerListeners) {
            this.tabletServerListeners = tabletServerListeners;
            return this;
        }

        /** Sets coordinator-server listeners for both backends. */
        public Builder setCoordinatorServerListeners(String coordinatorServerListeners) {
            this.coordinatorServerListeners = coordinatorServerListeners;
            return this;
        }

        /** Sets the clock used by both backend clusters. */
        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /** Sets tablet-server racks for both backends. */
        public Builder setRacks(String[] racks) {
            this.racks = racks;
            return this;
        }

        /** Builds ZooKeeper and Raft clusters with the same topology. */
        public DualBackendFlussClusters build() {
            return new DualBackendFlussClusters(
                    newCluster(MetadataBackend.ZOOKEEPER), newCluster(MetadataBackend.RAFT));
        }

        private FlussClusterExtension newCluster(MetadataBackend backend) {
            FlussClusterExtension.Builder builder =
                    FlussClusterExtension.builder()
                            .setNumOfTabletServers(numOfTabletServers)
                            .setMetadataBackend(backend);
            if (clusterConf != null) {
                builder.setClusterConf(copyConf(clusterConf));
            }
            if (tabletServerListeners != null) {
                builder.setTabletServerListeners(tabletServerListeners);
            }
            if (coordinatorServerListeners != null) {
                builder.setCoordinatorServerListeners(coordinatorServerListeners);
            }
            if (clock != null) {
                builder.setClock(clock);
            }
            if (racks != null) {
                builder.setRacks(racks);
            }
            return builder.build();
        }

        private static Configuration copyConf(Configuration source) {
            Configuration copy = new Configuration();
            source.toMap().forEach(copy::setString);
            return copy;
        }
    }
}
