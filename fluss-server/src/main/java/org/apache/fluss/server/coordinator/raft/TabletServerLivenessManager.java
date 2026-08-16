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
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Leader-side heartbeat-lease fencing for TabletServers (§5.4.2) — the deterministic replacement
 * for ZooKeeper ephemeral-node expiry, following the KRaft {@code BrokerHeartbeatManager} model.
 *
 * <p>A TabletServer registers ({@code RegisterTabletServer}) and then renews a lease with periodic
 * {@code TabletServerHeartbeat} RPCs. The leader tracks {@code lastContactMs} per server IN MEMORY
 * (never in the log). On a scheduled tick, any server whose last contact is older than {@code
 * sessionTimeoutMs} is fenced by appending a {@code FENCE_TABLET_SERVER} record — because the fence
 * is a replicated log entry, every Coordinator observes the same membership change
 * deterministically (unlike ZK where expiry was observed independently per watcher).
 *
 * <p><b>Leader-only:</b> fencing appends run only while this node is the Raft leader ({@code
 * isLeader}); a non-leader's in-memory contact map is irrelevant (heartbeats target the leader). On
 * gaining leadership the coordinator must call {@link #seedRegisteredServers} with the store's
 * current membership so offline members (REGISTER still present, no heartbeats) expire after {@code
 * sessionTimeoutMs} — ZooKeeper ephemeral-node expiry parity.
 *
 * <p><b>Thread rule (§5.6):</b> the scheduled tick calls {@link RaftJournalWriter#append} (async)
 * and never blocks on the apply loop; the returned future is observed only for error logging.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.4.2.
 */
@Internal
@ThreadSafe
public class TabletServerLivenessManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TabletServerLivenessManager.class);

    private static final byte[] EMPTY = new byte[0];

    private final RaftJournalWriter writer;
    private final Clock clock;
    private final long sessionTimeoutMs;
    private final long checkIntervalMs;
    private final BooleanSupplier isLeader;

    // serverId -> last contact (register/heartbeat) wall-clock ms. Never persisted (§5.4.2).
    private final Map<Integer, Long> lastContact = new ConcurrentHashMap<>();

    private volatile ScheduledExecutorService scheduler;

    public TabletServerLivenessManager(
            RaftJournalWriter writer,
            Clock clock,
            long sessionTimeoutMs,
            long checkIntervalMs,
            BooleanSupplier isLeader) {
        this.writer = writer;
        this.clock = clock;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.checkIntervalMs = checkIntervalMs;
        this.isLeader = isLeader;
    }

    /** Starts the periodic expiry check. Idempotent. */
    public synchronized void start() {
        if (scheduler != null) {
            return;
        }
        this.scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("tablet-server-liveness"));
        scheduler.scheduleWithFixedDelay(
                this::checkExpiredServersSafely,
                checkIntervalMs,
                checkIntervalMs,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Records contact from a TabletServer (registration or heartbeat), renewing its lease. Called
     * on the RPC handler thread.
     */
    public void recordContact(int serverId) {
        lastContact.put(serverId, clock.milliseconds());
    }

    /**
     * Seeds lease tracking for TabletServers already present in the metadata store when this node
     * becomes leader (T6). Uses {@link Map#putIfAbsent} so an in-flight heartbeat is not reset.
     *
     * <p>Servers that never renew within {@code sessionTimeoutMs} are then fenced by the periodic
     * tick — ZooKeeper ephemeral-expiry parity for members that were offline while this coordinator
     * was down (REGISTER record still in the log, no heartbeats).
     */
    public void seedRegisteredServers(Iterable<Integer> serverIds) {
        long now = clock.milliseconds();
        for (Integer serverId : serverIds) {
            if (serverId == null) {
                continue;
            }
            Long previous = lastContact.putIfAbsent(serverId, now);
            if (previous == null) {
                LOG.info(
                        "Seeded TabletServer {} lease at leadership with grace until +{}ms",
                        serverId,
                        sessionTimeoutMs);
            }
        }
    }

    /** Stops tracking a server (e.g. on graceful controlled-shutdown, before it is fenced). */
    public void forget(int serverId) {
        lastContact.remove(serverId);
    }

    /**
     * Immediately fences a TabletServer (ZK ephemeral-delete parity for controlled shutdown / clean
     * stop). Stops lease tracking first so the periodic tick cannot double-fence.
     */
    public void fenceImmediately(int serverId) {
        forget(serverId);
        fence(serverId, "controlled shutdown / immediate fence");
    }

    @VisibleForTesting
    boolean isTracked(int serverId) {
        return lastContact.containsKey(serverId);
    }

    private void checkExpiredServersSafely() {
        try {
            checkExpiredServers();
        } catch (Throwable t) {
            // Never let an exception kill the scheduled task.
            LOG.warn("TabletServer liveness check failed", t);
        }
    }

    /** Fences every server whose lease has expired. Leader-only; runs on the scheduler thread. */
    @VisibleForTesting
    void checkExpiredServers() {
        if (!isLeader.getAsBoolean()) {
            return;
        }
        long now = clock.milliseconds();
        for (Map.Entry<Integer, Long> entry : lastContact.entrySet()) {
            if (now - entry.getValue() > sessionTimeoutMs) {
                int serverId = entry.getKey();
                // Remove first so a slow append cannot cause a second fence next tick; the server
                // must re-register to be tracked again anyway.
                lastContact.remove(serverId);
                fence(serverId, "heartbeat lease expiry");
            }
        }
    }

    private void fence(int serverId, String reason) {
        LOG.info(
                "Fencing TabletServer {} ({}; sessionTimeout={}ms)",
                serverId,
                reason,
                sessionTimeoutMs);
        writer.append(
                        new MetadataRecord(
                                MetadataRecord.Type.FENCE_TABLET_SERVER,
                                serverIdKey(serverId),
                                EMPTY))
                .exceptionally(
                        t -> {
                            // Typically NotLeaderException if leadership was lost mid-tick; the new
                            // leader fences based on its own tracking. Safe to log and drop.
                            LOG.warn("Failed to append FENCE_TABLET_SERVER for {}", serverId, t);
                            return null;
                        });
    }

    /**
     * Logical key body for a tablet-server membership record. MUST match the encoding used by
     * {@code RaftMetadataClient.registerTabletServer} (4-byte big-endian id) so a {@code
     * FENCE_TABLET_SERVER} deletes exactly the key a {@code REGISTER_TABLET_SERVER} created.
     */
    private static byte[] serverIdKey(int serverId) {
        return ByteBuffer.allocate(4).putInt(serverId).array();
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
