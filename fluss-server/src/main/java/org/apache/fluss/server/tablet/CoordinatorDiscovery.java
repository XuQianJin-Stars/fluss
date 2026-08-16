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

package org.apache.fluss.server.tablet;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.cluster.ServerNode;
import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.AdminReadOnlyGateway;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.PbServerNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bootstraps the coordinator address for a TabletServer on the Raft metadata backend from the
 * configured {@code coordinator.seed.nodes} (§5.4.1). It reuses the exact {@code metadata()}
 * handshake the client performs for {@code bootstrap.servers} (cf. {@code
 * MetadataUpdater.initializeCluster}): contact any seed, read {@code
 * MetadataResponse.coordinator_server}. This is needed because a freshly-started TabletServer's
 * {@code TabletServerMetadataCache} is empty — it is populated only after the coordinator learns
 * about the server (§5.8 stream / {@code UpdateMetadata} push) — so the server cannot otherwise
 * resolve the coordinator to send its first {@code RegisterTabletServer}. Replaces the fixed ZK
 * rendezvous the ZooKeeper backend relied on.
 *
 * <p>The seeds are queried round-robin until one returns a coordinator, bounded by a timeout, so a
 * follower that has not yet learned the leader (or a coordinator still electing) is simply skipped
 * and retried. Standalone + testable (build-component-wire-later); wiring it into {@code
 * TabletServer.startServices} is a separate step.
 */
@Internal
public final class CoordinatorDiscovery {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorDiscovery.class);

    // "host:port" (bare host / IPv4, or "[ipv6]:port"); mirrors the client ClientUtils pattern.
    private static final Pattern HOST_PORT =
            Pattern.compile(".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)");

    private final List<String> seedNodes;
    private final Function<ServerNode, AdminReadOnlyGateway> gatewayFactory;

    public CoordinatorDiscovery(List<String> seedNodes, RpcClient rpcClient) {
        this(
                seedNodes,
                node ->
                        GatewayClientProxy.createGatewayProxy(
                                () -> node, rpcClient, AdminReadOnlyGateway.class));
    }

    @VisibleForTesting
    CoordinatorDiscovery(
            List<String> seedNodes, Function<ServerNode, AdminReadOnlyGateway> gatewayFactory) {
        if (seedNodes == null || seedNodes.isEmpty()) {
            throw new IllegalConfigurationException(
                    "coordinator.seed.nodes must be set for the Raft metadata backend.");
        }
        this.seedNodes = seedNodes;
        this.gatewayFactory = gatewayFactory;
    }

    /**
     * Resolves the current coordinator leader by querying the seeds round-robin until one returns a
     * {@code coordinator_server}, bounded by {@code timeoutMs}. Each attempt waits up to {@code
     * perAttemptMs} for the RPC and, on failure/empty, moves to the next seed after the same delay.
     *
     * @throws IllegalStateException if no seed yields a coordinator within {@code timeoutMs}
     */
    public ServerNode discoverCoordinatorLeader(long timeoutMs, long perAttemptMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Exception last = null;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            String seed = seedNodes.get(attempt % seedNodes.size());
            attempt++;
            try {
                ServerNode seedNode = parseSeed(seed);
                MetadataResponse response =
                        gatewayFactory
                                .apply(seedNode)
                                .metadata(new MetadataRequest())
                                .get(perAttemptMs, TimeUnit.MILLISECONDS);
                if (response.hasCoordinatorServer()) {
                    PbServerNode c = response.getCoordinatorServer();
                    ServerNode coordinator =
                            new ServerNode(
                                    c.getNodeId(),
                                    c.getHost(),
                                    c.getPort(),
                                    ServerType.COORDINATOR);
                    LOG.info("Discovered coordinator leader {} via seed {}.", coordinator, seed);
                    return coordinator;
                }
                LOG.debug("Seed {} returned no coordinator yet; retrying another seed.", seed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                last = e;
                LOG.debug(
                        "Coordinator discovery via seed {} failed; trying the next seed.", seed, e);
            }
            Thread.sleep(perAttemptMs);
        }
        throw new IllegalStateException(
                "Failed to discover the coordinator leader from seeds " + seedNodes, last);
    }

    /** Parses a {@code host:port} seed entry into an unidentified {@link ServerNode}. */
    @VisibleForTesting
    static ServerNode parseSeed(String hostPort) {
        Matcher matcher = HOST_PORT.matcher(hostPort.trim());
        if (!matcher.matches()) {
            throw new IllegalConfigurationException(
                    "Invalid coordinator.seed.nodes entry: '"
                            + hostPort
                            + "' (expected host:port).");
        }
        return new ServerNode(
                -1, matcher.group(1), Integer.parseInt(matcher.group(2)), ServerType.UNKNOWN);
    }
}
