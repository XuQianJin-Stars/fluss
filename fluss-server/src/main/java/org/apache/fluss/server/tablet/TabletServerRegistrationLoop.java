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
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * TabletServer-side registration + heartbeat-lease loop (§5.4.2) — the Raft-backend replacement for
 * the ephemeral {@code /tabletservers/ids/[serverId]} znode plus ZK session. It registers this
 * server with the coordinator via {@code RegisterTabletServer}, then renews the lease with periodic
 * {@code TabletServerHeartbeat} RPCs. If the coordinator reports the server is no longer registered
 * — its lease expired and it was fenced (a {@code FENCE_TABLET_SERVER} record removed its
 * membership), or a freshly-elected leader has no record of it after a failover — the loop
 * re-registers before continuing to heartbeat.
 *
 * <p>This is the client-side counterpart of {@code TabletServerRegistrationRpcServer}; keeping it a
 * standalone, testable unit follows the build-component-wire-later pattern used across this branch.
 * Wiring it into {@code TabletServer.startServices} and discovering the coordinator via {@code
 * coordinator.seed.nodes} (§5.4.1) instead of the (initially empty) metadata cache is a separate
 * step.
 *
 * <p><b>Threading (§5.6):</b> all RPCs are issued from a single-thread scheduler; blocking on the
 * RPC future here is safe because it is the tablet server's own thread, never the coordinator's
 * Raft apply loop.
 */
@Internal
@ThreadSafe
public class TabletServerRegistrationLoop implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerRegistrationLoop.class);

    private final CoordinatorGateway coordinatorGateway;
    private final int serverId;
    private final Supplier<RegisterTabletServerRequest> registrationSupplier;
    private final long heartbeatIntervalMs;
    private final long rpcTimeoutMs;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile ScheduledExecutorService scheduler;
    private volatile long leaderEpoch = -1L;
    private volatile boolean registered = false;

    public TabletServerRegistrationLoop(
            CoordinatorGateway coordinatorGateway,
            int serverId,
            Supplier<RegisterTabletServerRequest> registrationSupplier,
            long heartbeatIntervalMs,
            long rpcTimeoutMs) {
        this.coordinatorGateway = coordinatorGateway;
        this.serverId = serverId;
        this.registrationSupplier = registrationSupplier;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    /**
     * Registers this server with the coordinator (blocking, bounded by {@code rpcTimeoutMs}) and
     * starts the periodic heartbeat. Idempotent: a second call is a no-op.
     */
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        register();
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("tablet-server-heartbeat-" + serverId));
        scheduler.scheduleWithFixedDelay(
                this::heartbeatTick,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /** Sends a registration RPC and records the coordinator epoch it returns. */
    @VisibleForTesting
    void register() throws Exception {
        RegisterTabletServerRequest request = registrationSupplier.get();
        long epoch =
                coordinatorGateway
                        .registerTabletServer(request)
                        .get(rpcTimeoutMs, TimeUnit.MILLISECONDS)
                        .getLeaderEpoch();
        this.leaderEpoch = epoch;
        this.registered = true;
        LOG.info("Registered TabletServer {} with coordinator (leaderEpoch={}).", serverId, epoch);
    }

    /**
     * One heartbeat tick: renews the lease, and re-registers if the coordinator no longer knows
     * this server. Never throws — a transient failure (coordinator not leader, failover, network)
     * is logged and retried on the next tick.
     */
    @VisibleForTesting
    void heartbeatTick() {
        try {
            TabletServerHeartbeatResponse response =
                    coordinatorGateway
                            .tabletServerHeartbeat(
                                    new TabletServerHeartbeatRequest()
                                            .setServerId(serverId)
                                            .setLeaderEpoch(leaderEpoch))
                            .get(rpcTimeoutMs, TimeUnit.MILLISECONDS);
            this.leaderEpoch = response.getLeaderEpoch();
            if (!response.isRegistered()) {
                LOG.info(
                        "Coordinator reports TabletServer {} not registered; re-registering.",
                        serverId);
                this.registered = false;
                register();
            }
        } catch (Exception e) {
            LOG.warn("TabletServer {} heartbeat failed; will retry on the next tick.", serverId, e);
        }
    }

    @VisibleForTesting
    boolean isRegistered() {
        return registered;
    }

    @VisibleForTesting
    long leaderEpoch() {
        return leaderEpoch;
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
