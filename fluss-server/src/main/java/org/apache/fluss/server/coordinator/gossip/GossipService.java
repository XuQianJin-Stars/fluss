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

package org.apache.fluss.server.coordinator.gossip;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optional SWIM-style gossip used so coordinators (and TabletServers pulling membership) can
 * discover peers without a complete static seed list (design §1.2 / §5.4.4).
 *
 * <p>Each round: bump the local heartbeat, pick a random known peer (or a configured seed if the
 * view is still empty), push-pull views, merge, expire silent members.
 */
@Internal
public final class GossipService implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GossipService.class);

    private final GossipMember self;
    private final List<GossipMember> seeds;
    private final GossipView view;
    private final GossipTransport transport;
    private final long intervalMs;
    private final long failureTimeoutMs;
    private final Clock clock;
    private final Random random = new Random();
    private final AtomicLong heartbeat = new AtomicLong(0L);
    private final AtomicBoolean started = new AtomicBoolean(false);

    private ScheduledExecutorService executor;

    GossipService(
            GossipMember self,
            List<GossipMember> seeds,
            GossipView view,
            GossipTransport transport,
            long intervalMs,
            long failureTimeoutMs,
            Clock clock) {
        this.self = self;
        this.seeds = seeds == null ? Collections.emptyList() : new ArrayList<>(seeds);
        this.view = view;
        this.transport = transport;
        this.intervalMs = intervalMs;
        this.failureTimeoutMs = failureTimeoutMs;
        this.clock = clock;
        this.view.putLocal(self, clock.milliseconds());
    }

    /**
     * Start a TCP gossip listener from coordinator config and advertise {@code self}'s RPC address.
     * {@code coordinator.gossip.port=0} binds an ephemeral port (tests).
     *
     * @param self this coordinator's identity (gossip port may be 0)
     * @param seeds contact points derived from {@code coordinator.seed.nodes}
     * @param conf coordinator configuration
     * @return started service (caller must {@link #close()})
     */
    public static GossipService start(
            GossipMember self, List<GossipMember> seeds, Configuration conf) throws IOException {
        return start(self, seeds, conf, SystemClock.getInstance());
    }

    /**
     * Start a TCP gossip listener.
     *
     * @param self this coordinator's identity
     * @param seeds contact points
     * @param conf coordinator configuration
     * @param clock wall clock used for expiry
     * @return started service
     */
    public static GossipService start(
            GossipMember self, List<GossipMember> seeds, Configuration conf, Clock clock)
            throws IOException {
        GossipView view = new GossipView();
        TcpGossipTransport transport = new TcpGossipTransport(view);
        int bound = transport.bind(self.gossipPort());
        GossipMember boundSelf = self.withGossipPort(bound);
        view.putLocal(boundSelf, clock.milliseconds());
        long intervalMs = conf.get(ConfigOptions.COORDINATOR_GOSSIP_INTERVAL).toMillis();
        long failureMs = conf.get(ConfigOptions.COORDINATOR_GOSSIP_FAILURE_TIMEOUT).toMillis();
        GossipService service =
                new GossipService(boundSelf, seeds, view, transport, intervalMs, failureMs, clock);
        service.startScheduler();
        LOG.info(
                "Started coordinator gossip on {} advertising RPC {}.",
                boundSelf.gossipAddress(),
                boundSelf.rpcAddress());
        return service;
    }

    /**
     * In-process service for tests (no TCP). Caller drives rounds with {@link #gossipOnce()} or
     * starts the scheduler.
     */
    @VisibleForTesting
    static GossipService createForTest(
            GossipMember self,
            List<GossipMember> seeds,
            GossipView view,
            GossipTransport transport,
            long intervalMs,
            long failureTimeoutMs,
            Clock clock) {
        return new GossipService(self, seeds, view, transport, intervalMs, failureTimeoutMs, clock);
    }

    void startScheduler() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        executor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "fluss-gossip-" + self.nodeId());
                            t.setDaemon(true);
                            return t;
                        });
        long delay = Math.max(1L, intervalMs);
        executor.scheduleAtFixedRate(this::safeGossipOnce, delay, delay, TimeUnit.MILLISECONDS);
    }

    /** One push-pull round. Package-visible for tests. */
    @VisibleForTesting
    void gossipOnce() throws IOException {
        long now = clock.milliseconds();
        GossipMember local = self.withHeartbeat(heartbeat.incrementAndGet());
        view.putLocal(local, now);

        GossipMember peer = pickPeer();
        if (peer == null) {
            view.expire(self.nodeId(), now, failureTimeoutMs);
            return;
        }
        GossipView remote = transport.exchange(peer, view.snapshot());
        view.merge(remote, clock.milliseconds());
        view.expire(self.nodeId(), clock.milliseconds(), failureTimeoutMs);
    }

    /** Current membership (including self). */
    public GossipView view() {
        return view;
    }

    /** This node's advertised identity after bind. */
    public GossipMember self() {
        return self;
    }

    /** Bound gossip TCP port, or {@code -1} when the transport is not TCP. */
    public int gossipPort() {
        if (transport instanceof TcpGossipTransport) {
            return ((TcpGossipTransport) transport).localPort();
        }
        return self.gossipPort();
    }

    /** Stops the gossip scheduler and closes the transport. */
    @Override
    public void close() {
        started.set(false);
        if (executor != null) {
            executor.shutdownNow();
        }
        try {
            transport.close();
        } catch (IOException e) {
            LOG.debug("Error closing gossip transport", e);
        }
    }

    private void safeGossipOnce() {
        try {
            gossipOnce();
        } catch (Throwable t) {
            LOG.debug("Gossip round failed for {}", self.nodeId(), t);
        }
    }

    private GossipMember pickPeer() {
        List<GossipMember> known = new ArrayList<>();
        for (GossipMember m : view.members()) {
            if (!m.nodeId().equals(self.nodeId()) && m.gossipPort() > 0) {
                known.add(m);
            }
        }
        if (known.isEmpty()) {
            known.addAll(seeds);
        }
        if (known.isEmpty()) {
            return null;
        }
        return known.get(random.nextInt(known.size()));
    }
}
