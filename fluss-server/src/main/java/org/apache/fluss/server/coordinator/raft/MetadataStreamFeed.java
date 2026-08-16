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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Drives a {@link MetadataStreamSubscriber} on a dedicated daemon thread and, after each batch that
 * advances the applied offset, runs a fixed set of derived-view refreshers (§5.8). On the Raft
 * backend this is the tablet server's (or a standby coordinator's) whole metadata feed: the loop
 * replays the ordered stream into the subscriber's follower store, then re-derives every view built
 * from that store — typically a {@link TabletServerMetadataStreamCacheApplier#refresh()} plus a
 * {@link ConfigChangeStreamNotifier#refresh()} and {@link AclChangeStreamNotifier#refresh()} — so a
 * single subscriber thread keeps the cache, dynamic config, and ACLs in sync from one stream.
 *
 * <p>The refreshers run only when the subscriber's {@link MetadataStreamSubscriber#appliedOffset()}
 * changed since the last successful refresh (a snapshot bootstrap advances it too), so an empty
 * long-poll costs nothing; a forced initial refresh primes the views on cold start. Because every
 * refresher re-derives the full view from the local store, they are idempotent — a refresher that
 * throws is retried on the next iteration (after a backoff) rather than losing the update.
 *
 * <p>Transport failures ({@link IOException} from the subscriber) are handled exactly as {@link
 * MetadataStreamSubscriber#runLoop()} would: back off and retry from the same cursor so a leader
 * failover self-heals. This class owns its own loop (rather than reusing {@code runLoop()}) only so
 * it can run the refreshers after each advancing batch.
 *
 * <p>See {@code zookeeper-removal-design.md} §5.8.
 */
@Internal
public final class MetadataStreamFeed implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataStreamFeed.class);

    private final MetadataStreamSubscriber subscriber;
    private final List<Runnable> refreshers;
    private final long reconnectBackoffMs;
    private final String threadName;

    private volatile boolean running = false;
    private Thread thread;

    // Loop-thread-only state (also read by pollAndRefreshOnce in single-threaded tests).
    private long lastRefreshedOffset = Long.MIN_VALUE;
    private boolean pendingRefresh = true;

    /**
     * @param subscriber the metadata-stream subscriber to drive (single-consumer)
     * @param refreshers derived-view refreshers to run after each advancing batch, in order
     * @param reconnectBackoffMs backoff before retrying after a transport or refresher failure
     * @param threadName name for the dedicated subscriber thread
     */
    public MetadataStreamFeed(
            MetadataStreamSubscriber subscriber,
            List<Runnable> refreshers,
            long reconnectBackoffMs,
            String threadName) {
        this.subscriber = checkNotNull(subscriber, "subscriber");
        this.refreshers = new ArrayList<>(checkNotNull(refreshers, "refreshers"));
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.threadName = checkNotNull(threadName, "threadName");
    }

    /** Starts the subscriber thread. Idempotent. */
    public synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        thread = new Thread(this::runLoop, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    private void runLoop() {
        LOG.info("Metadata stream feed '{}' started", threadName);
        while (running) {
            try {
                pollAndRefreshOnce();
            } catch (IOException e) {
                if (!running) {
                    break;
                }
                LOG.warn(
                        "Metadata stream feed '{}' fetch failed; retrying after {}ms",
                        threadName,
                        reconnectBackoffMs,
                        e);
                sleepBackoff();
            } catch (RuntimeException e) {
                if (!running) {
                    break;
                }
                // A refresher failed. Views re-derive from the local store, so keep pendingRefresh
                // set and retry on the next iteration after a backoff rather than dropping it.
                LOG.error(
                        "Metadata stream feed '{}' refresher failed; retrying after {}ms",
                        threadName,
                        reconnectBackoffMs,
                        e);
                sleepBackoff();
            }
        }
        LOG.info("Metadata stream feed '{}' stopped", threadName);
    }

    /**
     * Performs one fetch+replay round and, iff the applied offset advanced since the last
     * successful refresh (or a refresh is still pending from a prior failure), runs the refreshers
     * in order. Package-private so a test can drive iterations deterministically without the
     * thread.
     */
    void pollAndRefreshOnce() throws IOException {
        subscriber.pollOnce();
        long applied = subscriber.appliedOffset();
        if (applied != lastRefreshedOffset) {
            pendingRefresh = true;
        }
        if (pendingRefresh) {
            for (Runnable refresher : refreshers) {
                refresher.run();
            }
            lastRefreshedOffset = applied;
            pendingRefresh = false;
        }
    }

    private void sleepBackoff() {
        try {
            Thread.sleep(reconnectBackoffMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    /** Stops the subscriber thread (best-effort) and closes the subscriber. Idempotent. */
    @Override
    public synchronized void close() {
        running = false;
        subscriber.close();
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }
}
