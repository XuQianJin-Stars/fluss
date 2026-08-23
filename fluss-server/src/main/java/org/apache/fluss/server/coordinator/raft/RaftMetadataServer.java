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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.MemorySize;
import org.apache.fluss.shaded.ratis.org.apache.ratis.RaftConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.Parameters;
import org.apache.fluss.shaded.ratis.org.apache.ratis.conf.RaftProperties;
import org.apache.fluss.shaded.ratis.org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.grpc.GrpcFactory;
import org.apache.fluss.shaded.ratis.org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroup;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftGroupId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.protocol.RaftPeerId;
import org.apache.fluss.shaded.ratis.org.apache.ratis.rpc.SupportedRpcType;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServer;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.fluss.shaded.ratis.org.apache.ratis.statemachine.StateMachine;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.SizeInBytes;
import org.apache.fluss.shaded.ratis.org.apache.ratis.util.TimeDuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Embeds an Apache Ratis {@link RaftServer} inside a CoordinatorServer, providing leader election
 * and a replicated metadata log. Replaces the ZooKeeper ensemble entirely.
 *
 * <p>A single fixed Raft group holds the whole cluster metadata; leadership drives which
 * Coordinator is the active leader (see {@code RaftLeaderSelector}); committed entries are applied
 * by the supplied {@link StateMachine} (typically {@code FlussMetadataStateMachine}).
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.2.
 */
@ThreadSafe
public class RaftMetadataServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RaftMetadataServer.class);

    // Fixed cluster-wide Raft group id. A fixed, hex-valid UUID so multiple clusters cannot
    // accidentally mix Raft groups.
    public static final UUID DEFAULT_GROUP_UUID =
            UUID.fromString("f1055c00-0000-0000-0000-000000000001");
    public static final RaftGroupId RAFT_GROUP_ID = RaftGroupId.valueOf(DEFAULT_GROUP_UUID);

    /**
     * Ratis {@code RaftServer.close()} waits up to a day for internal executors ({@code
     * ConcurrentUtils.shutdownAndWait}). Bound that wait so a stuck close cannot hang the process /
     * Surefire forever; leadership cleanup must still run first (see {@link
     * RaftLeaderSelector#close}).
     */
    private static final long CLOSE_TIMEOUT_MS = 60_000L;

    private final Configuration conf;
    private final RaftPeerId peerId;
    private final RaftGroup raftGroup;
    private final StateMachine stateMachine;
    private RaftServer server;

    public RaftMetadataServer(
            Configuration conf, RaftPeerId peerId, RaftGroup raftGroup, StateMachine stateMachine) {
        this.conf = conf;
        this.peerId = peerId;
        this.raftGroup = raftGroup;
        this.stateMachine = stateMachine;
    }

    /** Builds and starts the embedded Ratis server. */
    public synchronized void start() throws IOException {
        RaftProperties properties = buildRaftProperties();
        RaftServer.Builder builder =
                RaftServer.newBuilder()
                        .setServerId(peerId)
                        .setGroup(raftGroup)
                        .setStateMachine(stateMachine)
                        .setProperties(properties);
        Parameters tlsParameters = buildTlsParameters();
        if (tlsParameters != null) {
            builder.setParameters(tlsParameters);
            LOG.info(
                    "Raft gRPC TLS enabled for peer {} (mutualAuth={}); firewall coordinator.raft.port"
                            + " (default 9124) to coordinators only (§5.9).",
                    peerId,
                    conf.getBoolean(ConfigOptions.COORDINATOR_RAFT_TLS_MUTUAL_AUTH_ENABLED));
        }
        this.server = builder.build();
        server.start();
    }

    /** Maps Fluss config options to Ratis timing knobs and optional TLS. */
    @VisibleForTesting
    RaftProperties buildRaftProperties() {
        RaftProperties p = new RaftProperties();
        // Use gRPC transport for peer-to-peer and client RPC (matches design; explicit for
        // clarity).
        RaftConfigKeys.Rpc.setType(p, SupportedRpcType.GRPC);
        // Bind the gRPC server to this node's advertised port (parsed from its RaftPeer address);
        // otherwise GrpcConfigKeys.Server.PORT_DEFAULT == 0 makes the server listen on a random
        // ephemeral port and clients/peers cannot reach it.
        GrpcConfigKeys.Server.setPort(p, localPeerPort());
        Duration election = conf.get(ConfigOptions.COORDINATOR_RAFT_ELECTION_TIMEOUT);
        Duration firstElection = conf.get(ConfigOptions.COORDINATOR_RAFT_FIRST_ELECTION_TIMEOUT);
        // Election timeouts (min/max) derived from the configured election timeout.
        RaftServerConfigKeys.Rpc.setTimeoutMin(p, toTimeDuration(election.dividedBy(2)));
        RaftServerConfigKeys.Rpc.setTimeoutMax(p, toTimeDuration(election));
        // Faster first election on a fresh single-node cluster.
        RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(
                p, toTimeDuration(firstElection.dividedBy(2)));
        RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(p, toTimeDuration(firstElection));
        RaftServerConfigKeys.Log.setSegmentSizeMax(
                p, sizeBytes(conf.get(ConfigOptions.COORDINATOR_RAFT_LOG_SEGMENT_SIZE)));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(p, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
                p, conf.get(ConfigOptions.COORDINATOR_RAFT_SNAPSHOT_TRIGGER_THRESHOLD).longValue());
        RaftServerConfigKeys.setStorageDir(
                p,
                Collections.singletonList(new File(conf.get(ConfigOptions.COORDINATOR_RAFT_DIR))));
        applyAppendPipelineProperties(p);
        applyTlsProperties(p);
        return p;
    }

    /**
     * Raises the Ratis in-flight request caps so a full append chunk always fits.
     *
     * <p>{@code RaftJournalWriter} sends a chunk of up to {@code
     * coordinator.raft.append-chunk-size} entries before waiting for them to apply. Ratis
     * independently caps outstanding async client requests at {@code
     * RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX} (default 100) and queues server-side
     * writes at {@code Write.ELEMENT_LIMIT} / {@code Log.QUEUE_ELEMENT_LIMIT} (default 4096 each);
     * exceeding the client cap makes {@code async().send} fail with {@code
     * ResourceUnavailableException} or block the sending thread. Keeping the client cap at least
     * one chunk (plus headroom for the concurrent single-record appends that share this client)
     * means a chunk-size increase cannot silently start failing.
     */
    private void applyAppendPipelineProperties(RaftProperties p) {
        int chunkSize = conf.get(ConfigOptions.COORDINATOR_RAFT_APPEND_CHUNK_SIZE);
        int required =
                Math.max(
                        RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT, 2 * chunkSize);
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(p, required);

        // Lets concurrent appends share one fsync: the log worker still acknowledges only after the
        // flush completes, it just does not serialize writers behind it. Metadata writes are
        // fsync-dominated (~2 fsyncs per append: the data entry plus Ratis' commit-index metadata
        // entry), so this is the only lever left for concurrent writers — a serial writer has
        // nothing
        // to coalesce with. NEVER pair this with Log.setUnsafeFlushEnabled, which acknowledges
        // before
        // the flush and can silently lose acknowledged metadata; Ratis rejects enabling both
        // anyway.
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(
                p, conf.get(ConfigOptions.COORDINATOR_RAFT_ASYNC_FLUSH_ENABLED));
    }

    /** Wires {@link GrpcConfigKeys.TLS} keys from {@code coordinator.raft.tls.*} (§5.9). */
    private void applyTlsProperties(RaftProperties p) {
        boolean tlsEnabled = conf.getBoolean(ConfigOptions.COORDINATOR_RAFT_TLS_ENABLED);
        GrpcConfigKeys.TLS.setEnabled(p, tlsEnabled);
        if (!tlsEnabled) {
            return;
        }
        boolean mutualAuth =
                conf.getBoolean(ConfigOptions.COORDINATOR_RAFT_TLS_MUTUAL_AUTH_ENABLED);
        GrpcConfigKeys.TLS.setMutualAuthnEnabled(p, mutualAuth);
        String privateKey = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_PRIVATE_KEY_FILE);
        if (privateKey != null && !privateKey.isEmpty()) {
            GrpcConfigKeys.TLS.setPrivateKeyFileName(p, privateKey);
        }
        String certChain = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_CERT_CHAIN_FILE);
        if (certChain != null && !certChain.isEmpty()) {
            GrpcConfigKeys.TLS.setCertChainFileName(p, certChain);
        }
        String trustStore = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_TRUST_STORE_FILE);
        if (trustStore != null && !trustStore.isEmpty()) {
            GrpcConfigKeys.TLS.setTrustStore(p, trustStore);
        }
    }

    /**
     * When TLS is enabled and all three PEM paths are set, builds Ratis {@link Parameters} carrying
     * a file-based {@link GrpcTlsConfig} (required by {@link GrpcFactory} at channel construction).
     */
    @Nullable
    @VisibleForTesting
    Parameters buildTlsParameters() {
        if (!conf.getBoolean(ConfigOptions.COORDINATOR_RAFT_TLS_ENABLED)) {
            return null;
        }
        String privateKey = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_PRIVATE_KEY_FILE);
        String certChain = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_CERT_CHAIN_FILE);
        String trustStore = conf.getString(ConfigOptions.COORDINATOR_RAFT_TLS_TRUST_STORE_FILE);
        if (isBlank(privateKey) || isBlank(certChain) || isBlank(trustStore)) {
            LOG.warn(
                    "coordinator.raft.tls.enabled=true but private-key/cert-chain/trust-store file "
                            + "paths are incomplete; TLS property flags are set but GrpcTlsConfig "
                            + "Parameters are not. Provide all three PEM paths for peer TLS.");
            return null;
        }
        boolean mutualAuth =
                conf.getBoolean(ConfigOptions.COORDINATOR_RAFT_TLS_MUTUAL_AUTH_ENABLED);
        GrpcTlsConfig tlsConfig =
                new GrpcTlsConfig(
                        new File(privateKey),
                        new File(certChain),
                        new File(trustStore),
                        mutualAuth);
        return GrpcFactory.newRaftParameters(tlsConfig);
    }

    private static boolean isBlank(@Nullable String s) {
        return s == null || s.trim().isEmpty();
    }

    private static TimeDuration toTimeDuration(Duration duration) {
        return TimeDuration.valueOf(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Parses the gRPC listen port from this node's advertised {@link RaftPeer} address. */
    private int localPeerPort() {
        RaftPeer localPeer = raftGroup.getPeer(peerId);
        if (localPeer == null || localPeer.getAddress() == null) {
            throw new IllegalStateException(
                    "Local peer " + peerId + " has no address in group " + raftGroup.getGroupId());
        }
        String address = localPeer.getAddress();
        int colon = address.lastIndexOf(':');
        if (colon < 0 || colon == address.length() - 1) {
            throw new IllegalStateException(
                    "Malformed peer address (expected host:port): " + address);
        }
        try {
            return Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Malformed port in peer address: " + address, e);
        }
    }

    private static SizeInBytes sizeBytes(MemorySize memorySize) {
        return SizeInBytes.valueOf(memorySize.getBytes());
    }

    @VisibleForTesting
    RaftServer getRaftServer() {
        return server;
    }

    /** Fluss configuration used to build this server's Ratis properties. */
    Configuration getConf() {
        return conf;
    }

    @Override
    public synchronized void close() throws IOException {
        if (server == null) {
            return;
        }
        final RaftServer toClose = server;
        server = null;
        ExecutorService closer =
                Executors.newSingleThreadExecutor(
                        r -> {
                            Thread t = new Thread(r, "raft-metadata-server-close-" + peerId);
                            t.setDaemon(true);
                            return t;
                        });
        Future<?> future =
                closer.submit(
                        () -> {
                            toClose.close();
                            return null;
                        });
        try {
            future.get(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            LOG.error(
                    "Timed out after {} ms closing RaftServer for peer {}; abandoning close to "
                            + "avoid hanging shutdown (Ratis may leave threads).",
                    CLOSE_TIMEOUT_MS,
                    peerId,
                    e);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing RaftServer for peer " + peerId, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to close RaftServer for peer " + peerId, cause);
        } finally {
            closer.shutdownNow();
        }
    }
}
