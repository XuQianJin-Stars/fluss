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
import org.apache.fluss.cluster.Endpoint;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.rpc.messages.RegisterTabletServerResponse;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatRequest;
import org.apache.fluss.rpc.messages.TabletServerHeartbeatResponse;
import org.apache.fluss.server.metadata.MetadataStoreClient;
import org.apache.fluss.server.zk.data.TabletServerRegistration;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * Leader-side RPC handler for TabletServer registration &amp; liveness (§5.4.2): adapts the {@code
 * RegisterTabletServer}/{@code TabletServerHeartbeat} proto2 requests onto the leader's metadata
 * store and its {@link TabletServerLivenessManager}. On the Raft metadata backend these replace the
 * ephemeral {@code /tabletservers/ids/[serverId]} znode + ZK session:
 *
 * <ul>
 *   <li>{@code registerTabletServer} appends a {@code REGISTER_TABLET_SERVER} record (via {@link
 *       MetadataStoreClient#registerTabletServer}) and seeds the in-memory lease so the server is
 *       tracked for liveness immediately.
 *   <li>{@code tabletServerHeartbeat} renews the lease if the server is still registered; a server
 *       that has already been fenced (lease expired, membership record removed) is told it is no
 *       longer registered so it re-registers before its heartbeat is honored again.
 * </ul>
 *
 * <p>This is the exact logic the Raft-backed {@code CoordinatorService} delegates to; keeping it as
 * a standalone, testable unit is the build-component-wire-later pattern used throughout this branch
 * (mirrors {@link MetadataStreamRpcServer}). Only the leader serves these calls — the store writes
 * do {@code requireLeader()} and reject a stale leader with {@code NotCoordinatorLeaderException}.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.4.2.
 */
@Internal
public class TabletServerRegistrationRpcServer {

    private final MetadataStoreClient metadataStore;
    private final TabletServerLivenessManager livenessManager;
    private final LongSupplier currentEpoch;

    public TabletServerRegistrationRpcServer(
            MetadataStoreClient metadataStore,
            TabletServerLivenessManager livenessManager,
            LongSupplier currentEpoch) {
        this.metadataStore = metadataStore;
        this.livenessManager = livenessManager;
        this.currentEpoch = currentEpoch;
    }

    /** Serves {@link org.apache.fluss.rpc.protocol.ApiKeys#REGISTER_TABLET_SERVER}. */
    public CompletableFuture<RegisterTabletServerResponse> registerTabletServer(
            RegisterTabletServerRequest request) {
        CompletableFuture<RegisterTabletServerResponse> future = new CompletableFuture<>();
        try {
            String rack = request.hasRack() ? request.getRack() : null;
            List<Endpoint> endpoints =
                    request.hasListeners()
                            ? Endpoint.fromListenersString(request.getListeners())
                            : Collections.emptyList();
            int serverId = request.getServerId();
            // Bounce: membership still present (lease not expired) but the tablet process is new.
            // Fence first so watchers see Dead then New — mirrors ZK ephemeral delete+create and
            // forces processNewTabletServer to re-run OnlineReplica / LogTiering.
            if (metadataStore.getTabletServer(serverId).isPresent()
                    && metadataStore instanceof RaftMetadataClient) {
                ((RaftMetadataClient) metadataStore).fenceTabletServer(serverId);
            }
            metadataStore.registerTabletServer(
                    serverId,
                    new TabletServerRegistration(rack, endpoints, request.getRegisterTimestamp()));
            // Only reached after the append is committed+applied, so the membership record now
            // exists; start tracking the lease immediately.
            livenessManager.recordContact(serverId);
            future.complete(
                    new RegisterTabletServerResponse().setLeaderEpoch(currentEpoch.getAsLong()));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /** Serves {@link org.apache.fluss.rpc.protocol.ApiKeys#TABLET_SERVER_HEARTBEAT}. */
    public CompletableFuture<TabletServerHeartbeatResponse> tabletServerHeartbeat(
            TabletServerHeartbeatRequest request) {
        CompletableFuture<TabletServerHeartbeatResponse> future = new CompletableFuture<>();
        try {
            // The membership record is the source of truth: if it is gone the server was fenced
            // (lease expired) and must re-register — renewing an untracked lease would let a fenced
            // server silently resurrect without re-appending its registration.
            boolean registered = metadataStore.getTabletServer(request.getServerId()).isPresent();
            if (registered) {
                livenessManager.recordContact(request.getServerId());
            }
            future.complete(
                    new TabletServerHeartbeatResponse()
                            .setLeaderEpoch(currentEpoch.getAsLong())
                            .setRegistered(registered));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
