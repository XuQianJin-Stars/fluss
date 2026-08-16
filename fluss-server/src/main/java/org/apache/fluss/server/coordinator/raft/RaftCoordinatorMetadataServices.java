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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * The leader-side metadata services a Raft-backed coordinator runs, assembled from a bootstrapped
 * {@link RaftMetadataClient} (which owns the embedded Raft server + state machine). This is the
 * single seam the {@code CoordinatorServer} uses on the Raft backend to make the two nullable RPC
 * handlers on {@code CoordinatorService} non-null:
 *
 * <ul>
 *   <li>{@link MetadataStreamService} + {@link MetadataStreamRpcServer} — serves the cross-process
 *       metadata stream ({@code FetchMetadata}/{@code FetchMetadataSnapshot}, §5.8) that replaces
 *       ZK watches. The service reads the Raft log by index and snapshots via the state machine;
 *       its long-poll waiters are woken by the state machine's apply listener.
 *   <li>{@link TabletServerLivenessManager} + {@link TabletServerRegistrationRpcServer} — serves
 *       TabletServer registration/heartbeat ({@code RegisterTabletServer}/{@code
 *       TabletServerHeartbeat}, §5.4.2) that replaces the ephemeral znode + ZK session, fencing a
 *       server whose heartbeat lease expires.
 * </ul>
 *
 * <p>Lifecycle: {@link #start()} begins the liveness expiry checks; {@link #close()} stops them and
 * releases parked stream long-polls. Only meaningful on the leader — the underlying store writes
 * self-guard with {@code requireLeader()} and the liveness fencer checks {@code isLeader}.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.4.2 and §5.8.
 */
@Internal
public class RaftCoordinatorMetadataServices implements AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(RaftCoordinatorMetadataServices.class);

    private final MetadataStreamService streamService;
    private final MetadataStreamRpcServer metadataStreamRpcServer;
    private final TabletServerLivenessManager livenessManager;
    private final TabletServerRegistrationRpcServer tabletServerRegistrationRpcServer;
    private final AllocateIdRpcServer allocateIdRpcServer;

    private RaftCoordinatorMetadataServices(
            MetadataStreamService streamService,
            MetadataStreamRpcServer metadataStreamRpcServer,
            TabletServerLivenessManager livenessManager,
            TabletServerRegistrationRpcServer tabletServerRegistrationRpcServer,
            AllocateIdRpcServer allocateIdRpcServer) {
        this.streamService = streamService;
        this.metadataStreamRpcServer = metadataStreamRpcServer;
        this.livenessManager = livenessManager;
        this.tabletServerRegistrationRpcServer = tabletServerRegistrationRpcServer;
        this.allocateIdRpcServer = allocateIdRpcServer;
    }

    /**
     * Assembles the leader-side services from a bootstrapped {@code client}, reading the
     * tablet-server session timeout and the liveness check interval from {@code conf}.
     */
    public static RaftCoordinatorMetadataServices create(
            RaftMetadataClient client, Configuration conf) {
        return create(client, conf, null);
    }

    /**
     * Assembles the leader-side services and, when {@code configReloadListener} is set, applies
     * committed entity-config snapshots to a standby coordinator's {@code DynamicConfigManager}
     * from the local Raft store (the #3625 parity path for ZooKeeper config-change watchers).
     */
    public static RaftCoordinatorMetadataServices create(
            RaftMetadataClient client,
            Configuration conf,
            @Nullable ConfigChangeStreamNotifier.ConfigReloadListener configReloadListener) {
        long sessionTimeoutMs =
                conf.get(ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT).toMillis();
        long checkIntervalMs =
                conf.get(ConfigOptions.COORDINATOR_RAFT_HEARTBEAT_INTERVAL).toMillis();
        long idBlockSize = conf.get(ConfigOptions.COORDINATOR_RAFT_ID_BLOCK_SIZE);
        return create(
                client,
                sessionTimeoutMs,
                checkIntervalMs,
                idBlockSize,
                SystemClock.getInstance(),
                configReloadListener);
    }

    @VisibleForTesting
    static RaftCoordinatorMetadataServices create(
            RaftMetadataClient client,
            long sessionTimeoutMs,
            long checkIntervalMs,
            long idBlockSize,
            Clock clock) {
        return create(client, sessionTimeoutMs, checkIntervalMs, idBlockSize, clock, null);
    }

    @VisibleForTesting
    static RaftCoordinatorMetadataServices create(
            RaftMetadataClient client,
            long sessionTimeoutMs,
            long checkIntervalMs,
            long idBlockSize,
            Clock clock,
            @Nullable ConfigChangeStreamNotifier.ConfigReloadListener configReloadListener) {
        FlussMetadataStateMachine stateMachine = client.ownedStateMachine();
        RaftLog raftLog = raftLogOf(client);
        // The Raft leader term is the coordinator epoch (§5.1.6); reuse it for both the stream's
        // fencing epoch and the registration/heartbeat responses.
        LongSupplier currentEpoch = () -> client.leaderSelector().currentEpoch().raftTerm();

        MetadataStreamService streamService =
                new MetadataStreamService(raftLog, currentEpoch, stateMachine::captureSnapshot);
        // Wake parked long-polls when a record commits+applies (§5.8.2), without a manual push.
        // Also refresh standby dynamic config from the local replica store (must stay cheap).
        ConfigChangeStreamNotifier configNotifier =
                configReloadListener == null
                        ? null
                        : new ConfigChangeStreamNotifier(client.store(), configReloadListener);
        stateMachine.setApplyListener(
                idx -> {
                    streamService.onApplied(idx);
                    if (configNotifier != null) {
                        // Apply-thread work must stay a cheap wakeup (§5.6). Config reload can
                        // take locks, so run it off the apply loop.
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        configNotifier.refresh();
                                    } catch (Throwable t) {
                                        LOG.warn(
                                                "Failed to refresh dynamic config after apply {}",
                                                idx,
                                                t);
                                    }
                                });
                    }
                });
        MetadataStreamRpcServer metadataStreamRpcServer =
                new MetadataStreamRpcServer(streamService);

        TabletServerLivenessManager livenessManager =
                new TabletServerLivenessManager(
                        client.journalWriter(),
                        clock,
                        sessionTimeoutMs,
                        checkIntervalMs,
                        client.leaderSelector()::isLeader);
        TabletServerRegistrationRpcServer tabletServerRegistrationRpcServer =
                new TabletServerRegistrationRpcServer(client, livenessManager, currentEpoch);
        AllocateIdRpcServer allocateIdRpcServer = new AllocateIdRpcServer(client, idBlockSize);

        return new RaftCoordinatorMetadataServices(
                streamService,
                metadataStreamRpcServer,
                livenessManager,
                tabletServerRegistrationRpcServer,
                allocateIdRpcServer);
    }

    private static RaftLog raftLogOf(RaftMetadataClient client) {
        try {
            return client.ownedServer()
                    .getRaftServer()
                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                    .getRaftLog();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to obtain the Raft log for the metadata stream", e);
        }
    }

    /** Starts the TabletServer liveness expiry checks. Idempotent. */
    public void start() {
        livenessManager.start();
    }

    /** The leader-side metadata-stream RPC handler, to set on the coordinator gateway. */
    public MetadataStreamRpcServer metadataStreamRpcServer() {
        return metadataStreamRpcServer;
    }

    /** The leader-side registration/heartbeat RPC handler, to set on the coordinator gateway. */
    public TabletServerRegistrationRpcServer tabletServerRegistrationRpcServer() {
        return tabletServerRegistrationRpcServer;
    }

    /** The leader-side id-allocation RPC handler, to set on the coordinator gateway (§5.3). */
    public AllocateIdRpcServer allocateIdRpcServer() {
        return allocateIdRpcServer;
    }

    @VisibleForTesting
    MetadataStreamService streamService() {
        return streamService;
    }

    /** The TabletServer heartbeat-lease manager (for controlled-shutdown fencing). */
    public TabletServerLivenessManager livenessManager() {
        return livenessManager;
    }

    @Override
    public void close() {
        // Release parked stream long-polls first so subscribers re-target promptly, then stop the
        // liveness scheduler.
        streamService.close();
        livenessManager.close();
    }
}
