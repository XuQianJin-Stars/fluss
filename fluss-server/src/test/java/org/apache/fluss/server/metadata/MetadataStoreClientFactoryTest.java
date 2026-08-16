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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link MetadataStoreClientFactory} backend dispatch. */
class MetadataStoreClientFactoryTest {

    @Test
    void testRaftBackendRequiresVoters() {
        // Selecting the Raft backend (seed.nodes present) without a voter set must fail fast with a
        // clear message rather than starting a half-configured server.
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "coordinator-1:9123");
        assertThatThrownBy(() -> MetadataStoreClientFactory.create(conf, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to bootstrap the Raft metadata backend")
                .hasRootCauseInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testBothBackendsConfiguredIsRejected() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.ZOOKEEPER_ADDRESS, "localhost:2181");
        conf.setString(ConfigOptions.COORDINATOR_SEED_NODES, "coordinator-1:9123");
        assertThatThrownBy(() -> MetadataStoreClientFactory.create(conf, null))
                .isInstanceOf(IllegalConfigurationException.class);
    }
}
