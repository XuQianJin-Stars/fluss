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
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.RegisterTabletServerRequest;
import org.apache.fluss.server.coordinator.raft.AclChangeStreamNotifier;
import org.apache.fluss.server.coordinator.raft.ConfigChangeStreamNotifier;
import org.apache.fluss.server.coordinator.raft.CoordinatorGatewayMetadataStreamTransport;
import org.apache.fluss.server.coordinator.raft.InMemoryMetadataKvStore;
import org.apache.fluss.server.coordinator.raft.MetadataStreamFeed;
import org.apache.fluss.server.coordinator.raft.MetadataStreamSubscriber;
import org.apache.fluss.server.coordinator.raft.MetadataWatchService;
import org.apache.fluss.server.coordinator.raft.RaftMetadataStore;
import org.apache.fluss.server.coordinator.raft.RpcMetadataStreamGateway;
import org.apache.fluss.server.coordinator.raft.TabletServerMetadataStreamCacheApplier;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;

/**
 * The TabletServer-side Raft metadata feed (§5.4.2 / §5.8, T6-3) — the single object that replaces
 * everything a TabletServer used to obtain from ZooKeeper: the ephemeral-znode registration + ZK
 * session, and the coordinator's {@code UpdateMetadata} push / ZK watches that populated the {@link
 * TabletServerMetadataCache}.
 *
 * <p>It wires together the previously-built, independently-Ratis-verified components into the exact
 * production object graph:
 *
 * <ul>
 *   <li>a follower {@link RaftMetadataStore} (in-memory) that a {@link MetadataStreamSubscriber}
 *       keeps in sync by replaying the ordered metadata stream pulled from the coordinator leader
 *       via the production {@link CoordinatorGatewayMetadataStreamTransport} over the {@code
 *       CoordinatorGateway};
 *   <li>a {@link MetadataStreamFeed} that drives that subscriber on its own daemon thread and,
 *       after each advancing batch, re-derives the cache via {@link
 *       TabletServerMetadataStreamCacheApplier} and (optionally) reconciles dynamic config / ACLs
 *       via the {@link ConfigChangeStreamNotifier} / {@link AclChangeStreamNotifier};
 *   <li>a {@link TabletServerRegistrationLoop} that registers this server with the coordinator and
 *       renews the heartbeat lease (re-registering if the leader ever fences/forgets it).
 * </ul>
 *
 * <p>Lifecycle: {@link #start()} registers this server (blocking on the first {@code
 * RegisterTabletServer}) and then starts the background feed + heartbeat; {@link #close()} stops
 * both. Only used on the Raft metadata backend — on ZooKeeper the TabletServer keeps its existing
 * ephemeral-znode registration and coordinator-push metadata path unchanged.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.4.2 and §5.8.
 */
@Internal
@ThreadSafe
public final class TabletServerRaftMetadataFeed implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerRaftMetadataFeed.class);

    /** Records pulled per FetchMetadata; the stream is low-volume so a generous batch is fine. */
    private static final int MAX_RECORDS_PER_FETCH = 1000;

    private final int serverId;
    private final MetadataStreamFeed feed;
    private final TabletServerRegistrationLoop registrationLoop;

    private TabletServerRaftMetadataFeed(
            int serverId, MetadataStreamFeed feed, TabletServerRegistrationLoop registrationLoop) {
        this.serverId = serverId;
        this.feed = feed;
        this.registrationLoop = registrationLoop;
    }

    /**
     * Assembles the feed from a resolved {@code coordinatorGateway} (a leader-following proxy) and
     * this server's {@link TabletServerMetadataCache}. The optional config/ACL reload listeners, if
     * non-null, are reconciled from the streamed store after each batch (the cross-process
     * replacement for the ZooKeeper config/ACL change-notification watchers); pass null to skip.
     */
    public static TabletServerRaftMetadataFeed create(
            Configuration conf,
            int serverId,
            CoordinatorGateway coordinatorGateway,
            TabletServerMetadataCache metadataCache,
            java.util.function.Supplier<RegisterTabletServerRequest> registrationSupplier,
            @Nullable ConfigChangeStreamNotifier.ConfigReloadListener configReloadListener,
            @Nullable AclChangeStreamNotifier.AclReloadListener aclReloadListener) {
        return create(
                conf,
                serverId,
                coordinatorGateway,
                metadataCache,
                registrationSupplier,
                configReloadListener,
                aclReloadListener,
                null);
    }

    /**
     * Same as {@link #create(Configuration, int, CoordinatorGateway, TabletServerMetadataCache,
     * java.util.function.Supplier, ConfigChangeStreamNotifier.ConfigReloadListener,
     * AclChangeStreamNotifier.AclReloadListener)} but lets the caller supply the follower {@link
     * RaftMetadataStore} the subscriber feeds. Pass the SAME store that backs a TabletServer's
     * read-only follower metadata client so table/schema metadata replayed from the stream is
     * visible to that server's reads; pass {@code null} to have this method own a fresh store.
     */
    public static TabletServerRaftMetadataFeed create(
            Configuration conf,
            int serverId,
            CoordinatorGateway coordinatorGateway,
            TabletServerMetadataCache metadataCache,
            java.util.function.Supplier<RegisterTabletServerRequest> registrationSupplier,
            @Nullable ConfigChangeStreamNotifier.ConfigReloadListener configReloadListener,
            @Nullable AclChangeStreamNotifier.AclReloadListener aclReloadListener,
            @Nullable RaftMetadataStore externalFollowerStore) {
        long longPollMs = conf.get(ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT).toMillis();
        long sessionTimeoutMs =
                conf.get(ConfigOptions.COORDINATOR_TABLET_SERVER_SESSION_TIMEOUT).toMillis();
        // Heartbeat comfortably within the lease (roughly a third of the session timeout) so a
        // couple of dropped heartbeats do not fence a live server; bound below by 1s.
        long heartbeatIntervalMs = Math.max(1_000L, sessionTimeoutMs / 3);
        // A single fetch/heartbeat RPC must resolve well within a session; reuse the session
        // timeout as a generous upper bound.
        long rpcTimeoutMs = sessionTimeoutMs;
        long reconnectBackoffMs = Math.max(50L, longPollMs / 2);

        // Follower store fed by the subscriber; read directly by the applier/notifiers (it is an
        // exact replica of the leader's committed state, so no leader guard is needed). Shared with
        // the TabletServer's follower metadata client when supplied.
        RaftMetadataStore followerStore =
                externalFollowerStore != null
                        ? externalFollowerStore
                        : new RaftMetadataStore(
                                new InMemoryMetadataKvStore(), new MetadataWatchService());

        RpcMetadataStreamGateway streamGateway =
                new RpcMetadataStreamGateway(
                        new CoordinatorGatewayMetadataStreamTransport(coordinatorGateway),
                        serverId);
        MetadataStreamSubscriber subscriber =
                new MetadataStreamSubscriber(
                        streamGateway,
                        followerStore,
                        MAX_RECORDS_PER_FETCH,
                        longPollMs,
                        reconnectBackoffMs,
                        null);

        TabletServerMetadataStreamCacheApplier applier =
                new TabletServerMetadataStreamCacheApplier(followerStore, metadataCache);

        List<Runnable> refreshers = new ArrayList<>(3);
        refreshers.add(applier::refresh);
        if (configReloadListener != null) {
            ConfigChangeStreamNotifier configNotifier =
                    new ConfigChangeStreamNotifier(followerStore, configReloadListener);
            refreshers.add(configNotifier::refresh);
        }
        if (aclReloadListener != null) {
            AclChangeStreamNotifier aclNotifier =
                    new AclChangeStreamNotifier(followerStore, aclReloadListener);
            refreshers.add(aclNotifier::refresh);
        }

        MetadataStreamFeed feed =
                new MetadataStreamFeed(
                        subscriber,
                        refreshers,
                        reconnectBackoffMs,
                        "tablet-server-metadata-feed-" + serverId);

        TabletServerRegistrationLoop registrationLoop =
                new TabletServerRegistrationLoop(
                        coordinatorGateway,
                        serverId,
                        registrationSupplier,
                        heartbeatIntervalMs,
                        rpcTimeoutMs);

        return new TabletServerRaftMetadataFeed(serverId, feed, registrationLoop);
    }

    /**
     * Starts the background metadata-stream feed first (so table/schema metadata can converge),
     * then registers this server with the coordinator. Register still runs early in boot, but
     * starting the feed first reduces the race where {@code NotifyLeaderAndIsr} arrives before the
     * follower store has table metadata.
     */
    public void start() throws Exception {
        LOG.info("Starting Raft metadata feed for TabletServer {}.", serverId);
        feed.start();
        registrationLoop.start();
        LOG.info("Raft metadata feed for TabletServer {} started.", serverId);
    }

    @Override
    public void close() {
        // Stop pulling/refreshing first, then stop heartbeating; both are best-effort.
        try {
            feed.close();
        } catch (Throwable t) {
            LOG.warn("Failed to close the metadata-stream feed for TabletServer {}.", serverId, t);
        }
        try {
            registrationLoop.close();
        } catch (Throwable t) {
            LOG.warn("Failed to close the registration loop for TabletServer {}.", serverId, t);
        }
    }

    @VisibleForTesting
    TabletServerRegistrationLoop registrationLoop() {
        return registrationLoop;
    }
}
