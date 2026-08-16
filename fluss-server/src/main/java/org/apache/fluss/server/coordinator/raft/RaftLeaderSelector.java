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

import org.apache.fluss.exception.CoordinatorEpochFencedException;
import org.apache.fluss.server.coordinator.LeaderElection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Raft-leadership-backed {@link LeaderElection}, replacing the Curator-LeaderLatch-based {@code
 * CoordinatorLeaderElection}. The public surface
 * (startElectLeaderAsync/isLeader/currentEpoch/close) is the {@link LeaderElection} contract, so
 * the CoordinatorServer integration is a single swap.
 *
 * <p>On gaining leadership we first wait for the state machine to catch up to the leader's
 * committed index, then run {@code initLeaderServices}; on losing it we run {@code
 * cleanupLeaderServices}. With the pure commit-apply model (§5.1.4) there is no ignore-applys
 * toggle — the store is always a function of the committed log.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.1.5.
 */
public class RaftLeaderSelector implements LeaderElection {

    private static final Logger LOG = LoggerFactory.getLogger(RaftLeaderSelector.class);

    /**
     * Matches {@code CoordinatorLeaderElection} close wait; keeps Raft teardown from hanging CI.
     */
    private static final long DEFAULT_CLOSE_TIMEOUT_MS = 10_000L;

    private final String serverId;
    private final RaftMetadataServer raftServer;
    private final FlussMetadataStateMachine stateMachine;
    private final long catchUpTimeoutMs; // COORDINATOR_RAFT_CATCHUP_TIMEOUT (Appendix 11.2)
    private final long closeTimeoutMs;
    // Whether the catch-up-and-serve task has been scheduled for the current leadership term (reset
    // on losing leadership), so it runs exactly once per term.
    private final AtomicBoolean caughtUpScheduled = new AtomicBoolean(false);
    // Whether the leader-init callback has run for the current leadership term. Independent of
    // isLeader: a node can be a serving Raft leader (isLeader=true, for requireLeader guards) with
    // no init callback at all (e.g. RaftMetadataClient used standalone without a
    // CoordinatorServer).
    private final AtomicBoolean initRan = new AtomicBoolean(false);
    // Whether this node is a caught-up, serving Raft leader. Set true after catch-up on gaining
    // Raft leadership, INDEPENDENT of whether init callbacks were registered — this is what
    // requireLeader() (reads/writes, §5.7) checks. Do not couple it to startElectLeaderAsync.
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    // Set on close(); suppresses gain/lose primacy and makes {@link #isLeader()} false so Raft
    // server shutdown (inside CoordinatorServer's lock) cannot re-enter cleanupCoordinatorLeader
    // and deadlock.
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final ExecutorService callbackExecutor;
    private final AtomicReference<CompletableFuture<Void>> pendingCleanup =
            new AtomicReference<>(CompletableFuture.completedFuture(null));

    private volatile Runnable initLeaderServices;
    private volatile Consumer<Throwable> cleanupLeaderServices;

    public RaftLeaderSelector(
            String serverId,
            RaftMetadataServer raftServer,
            FlussMetadataStateMachine stateMachine,
            long catchUpTimeoutMs) {
        this(serverId, raftServer, stateMachine, catchUpTimeoutMs, DEFAULT_CLOSE_TIMEOUT_MS);
    }

    RaftLeaderSelector(
            String serverId,
            RaftMetadataServer raftServer,
            FlussMetadataStateMachine stateMachine,
            long catchUpTimeoutMs,
            long closeTimeoutMs) {
        this.serverId = serverId;
        this.raftServer = raftServer;
        this.stateMachine = stateMachine;
        this.catchUpTimeoutMs = catchUpTimeoutMs;
        this.closeTimeoutMs = closeTimeoutMs;
        this.callbackExecutor =
                Executors.newCachedThreadPool(
                        r -> {
                            Thread t = new Thread(r, "coordinator-leader-callback-" + serverId);
                            t.setDaemon(true);
                            return t;
                        });
    }

    @Override
    public void startElectLeaderAsync(
            Runnable initLeaderServices, Consumer<Throwable> cleanupLeaderServices) {
        this.initLeaderServices = initLeaderServices;
        this.cleanupLeaderServices = cleanupLeaderServices;
        // No explicit start(): the embedded Ratis server elects automatically; leadership arrives
        // via FlussMetadataStateMachine.notifyLeaderChanged -> notifyLeadershipChanged below. If
        // this
        // node is ALREADY a caught-up leader (it was elected during RaftMetadataClient bootstrap,
        // before these callbacks were registered), run the leader-init callback now.
        tryRunInit();
    }

    /** Invoked by the state machine on every Raft role change. */
    void notifyLeadershipChanged(boolean nowLeader) {
        if (closing.get()) {
            return;
        }
        if (nowLeader) {
            scheduleGainPrimacy();
            return;
        }
        // Ratis may fire notifyNotLeader while this node is already (or still) the elected
        // leader — for example around a transfer or a follower-to-leader transition. Dropping
        // primacy then leaves CoordinatorServer.isLeader() false forever while the division
        // remains leader. If we have not yet become a serving leader, still run catch-up.
        if (raftDivisionIsLeader()) {
            LOG.debug("Ignoring lose-primacy for {}; Ratis division is still leader.", serverId);
            scheduleGainPrimacy();
            return;
        }
        // Allow catch-up + init to run again on a future term.
        caughtUpScheduled.set(false);
        initRan.set(false);
        losePrimacy();
    }

    /**
     * On gaining Raft leadership, schedules (once per term) the catch-up and then marks this node a
     * serving leader ({@link #isLeader}) so leader-guarded reads/writes (§5.7) succeed — regardless
     * of whether a {@code CoordinatorServer} registered init callbacks. It then attempts the
     * leader-init callback (a no-op if none is registered yet; {@link #startElectLeaderAsync} runs
     * it if it registers later).
     */
    private void scheduleGainPrimacy() {
        if (closing.get() || !caughtUpScheduled.compareAndSet(false, true)) {
            return;
        }
        CompletableFuture<Void> cleanup = pendingCleanup.get();
        try {
            callbackExecutor.execute(
                    () -> {
                        try {
                            if (closing.get()) {
                                return;
                            }
                            cleanup.get(); // wait for prior cleanup to finish
                            // Wait until this node's applied index reaches the leader's committed
                            // index so reads/writes see a complete view before serving as leader.
                            awaitCatchUp();
                            if (closing.get()) {
                                return;
                            }
                            isLeader.set(true);
                            tryRunInit();
                        } catch (CoordinatorEpochFencedException e) {
                            LOG.warn("Coordinator {} fenced, stepping down.", serverId);
                            caughtUpScheduled.set(false);
                            losePrimacy();
                        } catch (Exception e) {
                            LOG.error("Catch-up failed for {}", serverId, e);
                            caughtUpScheduled.set(false);
                            if (raftDivisionIsLeader()) {
                                scheduleGainPrimacy();
                            } else {
                                losePrimacy();
                            }
                        }
                    });
        } catch (RejectedExecutionException e) {
            caughtUpScheduled.set(false);
            LOG.debug("Callback executor rejected gainPrimacy for {}.", serverId, e);
        }
    }

    /**
     * Runs the leader-init callback exactly once per leadership term, but only once BOTH conditions
     * hold: this node is a caught-up serving leader ({@link #isLeader}) AND a {@code
     * CoordinatorServer} has registered its callbacks ({@link #initLeaderServices} != null). Called
     * from both the catch-up task and {@link #startElectLeaderAsync}, whichever arrives second; the
     * {@link #initRan} CAS makes it idempotent and thread-safe. Runs on the callback executor so it
     * never blocks the caller's thread (e.g. the CoordinatorServer start thread).
     */
    private void tryRunInit() {
        if (closing.get() || !isLeader.get() || initLeaderServices == null) {
            return;
        }
        if (!initRan.compareAndSet(false, true)) {
            return;
        }
        Runnable init = initLeaderServices;
        try {
            callbackExecutor.execute(
                    () -> {
                        try {
                            if (closing.get()) {
                                return;
                            }
                            // initLeaderServices legitimately performs leader-guarded metadata
                            // writes (e.g. registerCoordinatorLeader), which go through
                            // requireLeader() -> isLeader() — already true here.
                            init.run();
                        } catch (CoordinatorEpochFencedException e) {
                            LOG.warn("Coordinator {} fenced during init, stepping down.", serverId);
                            initRan.set(false);
                            losePrimacy();
                        } catch (Exception e) {
                            LOG.error("initLeaderServices failed for {}", serverId, e);
                            initRan.set(false);
                            losePrimacy();
                        }
                    });
        } catch (RejectedExecutionException e) {
            initRan.set(false);
            LOG.debug("Callback executor rejected tryRunInit for {}.", serverId, e);
        }
    }

    /**
     * Blocks until this node has applied every entry that was committed at the moment it gained
     * leadership. Mandatory under pure commit-apply (§5.1.4): a freshly-elected leader may still be
     * replaying a committed-but-not-yet-applied backlog, and leader-only reads (§5.7) must not
     * observe a half-applied store. On timeout this throws, so gainPrimacy falls back to
     * losePrimacy.
     */
    private void awaitCatchUp() throws Exception {
        long committedIndex =
                raftServer
                        .getRaftServer()
                        .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                        .getRaftLog()
                        .getLastCommittedIndex();
        stateMachine.awaitApplied(committedIndex).get(catchUpTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Raft-term-backed coordinator epoch (see {@link RaftEpoch}, §5.1.6). */
    @Override
    public RaftEpoch currentEpoch() {
        try {
            long term =
                    raftServer
                            .getRaftServer()
                            .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                            .getInfo()
                            .getCurrentTerm();
            return new RaftEpoch(term);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read current Raft term", e);
        }
    }

    private void losePrimacy() {
        if (closing.get() || !isLeader.compareAndSet(true, false)) {
            return;
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        pendingCleanup.set(f);
        Runnable cleanupTask =
                () -> {
                    try {
                        Consumer<Throwable> cleanup = cleanupLeaderServices;
                        if (cleanup != null) {
                            cleanup.accept(null);
                        }
                    } finally {
                        f.complete(null);
                    }
                };
        try {
            callbackExecutor.execute(cleanupTask);
        } catch (RejectedExecutionException e) {
            // close() may shut down the executor while init/losePrimacy is in flight
            // (e.g. CoordinatorServerRaftBootITCase teardown). Run cleanup inline.
            LOG.debug(
                    "Callback executor rejected losePrimacy for {}; running cleanup inline.",
                    serverId,
                    e);
            cleanupTask.run();
        }
    }

    @Override
    public boolean isLeader() {
        if (closing.get()) {
            return false;
        }
        // Ratis may elect this node without delivering notifyLeaderChanged (observed after a
        // leader process is closed while tablets are still connected). Nudge catch-up; the
        // per-term CAS in scheduleGainPrimacy makes this cheap and idempotent.
        if (!isLeader.get() && raftDivisionIsLeader()) {
            scheduleGainPrimacy();
        }
        return isLeader.get();
    }

    /**
     * Stops serving as leader and runs {@code cleanupLeaderServices} before tearing down the
     * callback executor — same contract as {@code CoordinatorLeaderElection#close}. Must run
     * <em>outside</em> {@code CoordinatorServer}'s shutdown lock: cleanup re-acquires that lock,
     * and Raft server close (later, under the lock) can fire {@link #notifyLeadershipChanged
     * (false)}; with leadership already cleared that path is a no-op and avoids deadlock.
     */
    @Override
    public void close() {
        LOG.info("Closing RaftLeaderSelector for {}.", serverId);
        if (!closing.compareAndSet(false, true)) {
            awaitCallbackExecutorTermination();
            return;
        }

        // Drop serving-leader flag first so requireLeader fails; then run cleanup (best-effort
        // unregister may warn) and wait, matching Curator LeaderLatch close ordering intent.
        boolean wasLeader = isLeader.getAndSet(false);
        caughtUpScheduled.set(false);
        initRan.set(false);

        CompletableFuture<Void> closeCleanup = new CompletableFuture<>();
        pendingCleanup.set(closeCleanup);
        Runnable cleanupTask =
                () -> {
                    try {
                        if (wasLeader) {
                            Consumer<Throwable> cleanup = cleanupLeaderServices;
                            if (cleanup != null) {
                                cleanup.accept(null);
                            }
                        }
                    } finally {
                        closeCleanup.complete(null);
                    }
                };
        try {
            callbackExecutor.execute(cleanupTask);
        } catch (RejectedExecutionException e) {
            cleanupTask.run();
        }

        try {
            closeCleanup.get(closeTimeoutMs, TimeUnit.MILLISECONDS);
            callbackExecutor.shutdown();
        } catch (Exception e) {
            LOG.warn(
                    "Timed out or failed waiting for leader cleanup for {} after {} ms; forcing "
                            + "executor shutdown.",
                    serverId,
                    closeTimeoutMs,
                    e);
            callbackExecutor.shutdownNow();
        }
        awaitCallbackExecutorTermination();
    }

    private void awaitCallbackExecutorTermination() {
        try {
            if (!callbackExecutor.awaitTermination(closeTimeoutMs, TimeUnit.MILLISECONDS)) {
                callbackExecutor.shutdownNow();
                if (!callbackExecutor.awaitTermination(closeTimeoutMs, TimeUnit.MILLISECONDS)) {
                    LOG.warn(
                            "Leader callback executor for {} did not terminate within {} ms.",
                            serverId,
                            closeTimeoutMs);
                }
            }
        } catch (InterruptedException e) {
            callbackExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            LOG.warn(
                    "Interrupted while waiting for leader callback executor for {} to terminate.",
                    serverId);
        }
    }

    private boolean raftDivisionIsLeader() {
        try {
            return raftServer
                    .getRaftServer()
                    .getDivision(RaftMetadataServer.RAFT_GROUP_ID)
                    .getInfo()
                    .isLeader();
        } catch (Exception e) {
            return false;
        }
    }
}
