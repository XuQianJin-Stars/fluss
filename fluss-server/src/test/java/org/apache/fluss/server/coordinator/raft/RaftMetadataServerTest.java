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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.Parameters;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroupId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServerConfigKeys;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RaftMetadataServer} config mapping and relocated-Ratis usability. */
class RaftMetadataServerTest {

    private static RaftMetadataServer newServer(Configuration conf) {
        RaftPeerId peerId = RaftPeerId.valueOf("1");
        RaftPeer peer = RaftPeer.newBuilder().setId(peerId).setAddress("localhost:9124").build();
        RaftGroup group = RaftGroup.valueOf(RaftMetadataServer.RAFT_GROUP_ID, peer);
        // stateMachine is only needed at start(); buildRaftProperties() does not use it.
        return new RaftMetadataServer(conf, peerId, group, null);
    }

    @Test
    void testFixedRaftGroupIdIsDeterministic() {
        assertThat(RaftMetadataServer.RAFT_GROUP_ID)
                .isEqualTo(RaftGroupId.valueOf(RaftMetadataServer.DEFAULT_GROUP_UUID));
    }

    @Test
    void testBuildRaftPropertiesMapsConfigOptions() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-test");
        conf.set(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT, Duration.ofMillis(2000));
        conf.set(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT, Duration.ofMillis(400));

        RaftProperties p = newServer(conf).buildRaftProperties();

        // storage dir is mapped from coordinator.raft.dir
        assertThat(RaftServerConfigKeys.storageDir(p))
                .containsExactly(new File("/tmp/fluss-raft-test"));
        // election timeout max == configured value; min == half of it
        assertThat(RaftServerConfigKeys.Rpc.timeoutMax(p).toLong(TimeUnit.MILLISECONDS))
                .isEqualTo(2000L);
        assertThat(RaftServerConfigKeys.Rpc.timeoutMin(p).toLong(TimeUnit.MILLISECONDS))
                .isEqualTo(1000L);
    }

    @Test
    void testBuildRaftPropertiesUsesDefaults() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-default");

        RaftProperties p = newServer(conf).buildRaftProperties();

        // default election timeout is 1000ms (Appendix 11.2)
        assertThat(RaftServerConfigKeys.Rpc.timeoutMax(p).toLong(TimeUnit.MILLISECONDS))
                .isEqualTo(1000L);
        assertThat(GrpcConfigKeys.TLS.enabled(p)).isFalse();
        assertThat(newServer(conf).buildTlsParameters()).isNull();
    }

    /**
     * The Ratis in-flight client request cap must always leave room for a full append chunk plus
     * the single-record appends sharing the same client, otherwise a large batch would fail with
     * ResourceUnavailableException instead of being throttled (analysis §8.9.2). The cap is {@code
     * max(ratisDefault, 2 * chunkSize)}, so it is never lowered below Ratis' own default.
     */
    @Test
    void testDefaultChunkSizeGivesTwoChunksOfHeadroom() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-chunk-default");

        RaftProperties p = newServer(conf).buildRaftProperties();

        int chunk = conf.get(ConfigOptions.COORDINATOR_RAFT_APPEND_CHUNK_SIZE);
        assertThat(RaftClientConfigKeys.Async.outstandingRequestsMax(p))
                .isEqualTo(2 * chunk)
                .isGreaterThanOrEqualTo(
                        RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT);
    }

    /**
     * A chunk size small enough that Ratis' own default already suffices must not lower the cap.
     */
    @Test
    void testSmallChunkSizeKeepsRatisOutstandingDefault() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-chunk-small");
        conf.set(ConfigOptions.COORDINATOR_RAFT_APPEND_CHUNK_SIZE, 8);

        RaftProperties p = newServer(conf).buildRaftProperties();

        assertThat(RaftClientConfigKeys.Async.outstandingRequestsMax(p))
                .isEqualTo(RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT);
    }

    /**
     * async-flush is off unless asked for, and enabling it must never enable unsafe-flush: the
     * latter acknowledges an append before its flush completes and can silently lose acknowledged
     * metadata.
     */
    @Test
    void testAsyncFlushDefaultsOffAndNeverEnablesUnsafeFlush() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-flush-default");

        RaftProperties p = newServer(conf).buildRaftProperties();

        assertThat(RaftServerConfigKeys.Log.asyncFlushEnabled(p)).isFalse();
        assertThat(RaftServerConfigKeys.Log.unsafeFlushEnabled(p)).isFalse();
    }

    @Test
    void testAsyncFlushCanBeEnabledWithoutUnsafeFlush() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-flush-async");
        conf.set(ConfigOptions.COORDINATOR_RAFT_ASYNC_FLUSH_ENABLED, true);

        RaftProperties p = newServer(conf).buildRaftProperties();

        assertThat(RaftServerConfigKeys.Log.asyncFlushEnabled(p)).isTrue();
        // Ratis throws if both are set; unsafe-flush must stay off no matter what.
        assertThat(RaftServerConfigKeys.Log.unsafeFlushEnabled(p)).isFalse();
    }

    @Test
    void testLargeChunkSizeRaisesRatisOutstandingCap() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-chunk-large");
        conf.set(ConfigOptions.COORDINATOR_RAFT_APPEND_CHUNK_SIZE, 512);

        RaftProperties p = newServer(conf).buildRaftProperties();

        assertThat(RaftClientConfigKeys.Async.outstandingRequestsMax(p)).isEqualTo(1024);
    }

    @Test
    void testBuildRaftPropertiesMapsTlsOptions() {
        Configuration conf = new Configuration();
        conf.setString(ConfigOptions.COORDINATOR_RAFT_DIR, "/tmp/fluss-raft-tls");
        conf.set(ConfigOptions.COORDINATOR_RAFT_TLS_ENABLED, true);
        conf.set(ConfigOptions.COORDINATOR_RAFT_TLS_MUTUAL_AUTH_ENABLED, true);
        conf.setString(ConfigOptions.COORDINATOR_RAFT_TLS_PRIVATE_KEY_FILE, "/tls/key.pem");
        conf.setString(ConfigOptions.COORDINATOR_RAFT_TLS_CERT_CHAIN_FILE, "/tls/cert.pem");
        conf.setString(ConfigOptions.COORDINATOR_RAFT_TLS_TRUST_STORE_FILE, "/tls/ca.pem");

        RaftMetadataServer server = newServer(conf);
        RaftProperties p = server.buildRaftProperties();

        assertThat(GrpcConfigKeys.TLS.enabled(p)).isTrue();
        assertThat(GrpcConfigKeys.TLS.mutualAuthnEnabled(p)).isTrue();
        assertThat(GrpcConfigKeys.TLS.privateKeyFileName(p)).isEqualTo("/tls/key.pem");
        assertThat(GrpcConfigKeys.TLS.certChainFileName(p)).isEqualTo("/tls/cert.pem");
        assertThat(GrpcConfigKeys.TLS.trustStore(p)).isEqualTo("/tls/ca.pem");

        Parameters params = server.buildTlsParameters();
        assertThat(params).isNotNull();
        assertThat(GrpcConfigKeys.TLS.conf(params)).isNotNull();
        assertThat(GrpcConfigKeys.TLS.conf(params).getMtlsEnabled()).isTrue();
    }
}
