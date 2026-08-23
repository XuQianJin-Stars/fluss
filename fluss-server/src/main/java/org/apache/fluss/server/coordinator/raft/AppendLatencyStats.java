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

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Splits the metadata append latency into the two segments Fluss actually controls, so the cost of
 * a single write can be attributed instead of guessed (analysis §8.9.1).
 *
 * <p>A {@code RaftJournalWriter.append} spans:
 *
 * <pre>
 *   |---------------------- total ----------------------|
 *   |------- send→reply -------|----- reply→applied ----|
 *   send to Ratis ......... majority commit ......... local apply observed
 * </pre>
 *
 * <ul>
 *   <li><b>send→reply</b> — gRPC loopback encode/decode, log append, fsync, majority ack. Ratis
 *       reports finer-grained pieces of this segment itself ({@code flushTime}, {@code syncTime},
 *       {@code appendEntryLatency}, {@code queueingDelay} on the {@code log_worker} registry); this
 *       class deliberately does not duplicate them, it only bounds the whole segment.
 *   <li><b>reply→applied</b> — commit-to-apply handoff plus the apply loop itself (store mutation,
 *       watch dispatch, waiter completion). This is the segment §8.9.4 C1/C2/C3 shrink.
 * </ul>
 *
 * <p>Accumulates sums and counts only (no reservoir), so it is allocation-free and safe on the hot
 * path; {@link #snapshot()} yields the derived averages. Sampling is unconditional but costs two
 * {@link System#nanoTime()} reads and three atomic adds per append — negligible against a
 * millisecond-scale append, and it keeps the measurement available in production rather than only
 * under a benchmark flag.
 */
@Internal
@ThreadSafe
public final class AppendLatencyStats {

    private final AtomicLong appendCount = new AtomicLong();
    private final AtomicLong sendToReplyNanos = new AtomicLong();
    private final AtomicLong replyToAppliedNanos = new AtomicLong();
    private final AtomicLong totalNanos = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();

    /**
     * Records one successful append.
     *
     * @param sendNanos {@link System#nanoTime()} taken immediately before handing the record to
     *     Ratis
     * @param replyNanos {@link System#nanoTime()} taken when the Raft reply (majority commit) was
     *     observed
     * @param appliedNanos {@link System#nanoTime()} taken when the local apply was observed
     */
    void record(long sendNanos, long replyNanos, long appliedNanos) {
        appendCount.incrementAndGet();
        sendToReplyNanos.addAndGet(replyNanos - sendNanos);
        replyToAppliedNanos.addAndGet(appliedNanos - replyNanos);
        totalNanos.addAndGet(appliedNanos - sendNanos);
    }

    /** Records an append that failed (send rejected, not leader, apply error). */
    void recordFailure() {
        failureCount.incrementAndGet();
    }

    /** Number of successful appends observed. */
    public long appendCount() {
        return appendCount.get();
    }

    /** Number of failed appends observed. */
    public long failureCount() {
        return failureCount.get();
    }

    /** Mean microseconds from send to Raft reply; {@code 0} when nothing was recorded. */
    public double avgSendToReplyMicros() {
        return avgMicros(sendToReplyNanos.get());
    }

    /** Mean microseconds from Raft reply to local apply; {@code 0} when nothing was recorded. */
    public double avgReplyToAppliedMicros() {
        return avgMicros(replyToAppliedNanos.get());
    }

    /** Mean microseconds for the whole append; {@code 0} when nothing was recorded. */
    public double avgTotalMicros() {
        return avgMicros(totalNanos.get());
    }

    /**
     * Fraction of the total append latency spent before the Raft reply, in {@code [0, 1]}. This is
     * the number that decides whether §8.9.3 B2 (bypassing the gRPC loopback) is worth its
     * complexity: it caps how much of an append that change could possibly address.
     */
    public double sendToReplyShare() {
        long total = totalNanos.get();
        return total <= 0 ? 0.0 : (double) sendToReplyNanos.get() / (double) total;
    }

    /** Resets all counters; intended for benchmarks measuring a specific phase. */
    public void reset() {
        appendCount.set(0);
        sendToReplyNanos.set(0);
        replyToAppliedNanos.set(0);
        totalNanos.set(0);
        failureCount.set(0);
    }

    /** Human-readable one-line summary for logs and benchmark output. */
    public String snapshot() {
        long count = appendCount.get();
        if (count == 0) {
            return "appends=0";
        }
        return String.format(
                "appends=%d failures=%d total=%.1fus sendToReply=%.1fus(%.1f%%) replyToApplied=%.1fus(%.1f%%)",
                count,
                failureCount.get(),
                avgTotalMicros(),
                avgSendToReplyMicros(),
                sendToReplyShare() * 100.0,
                avgReplyToAppliedMicros(),
                (1.0 - sendToReplyShare()) * 100.0);
    }

    private double avgMicros(long totalNanosForSegment) {
        long count = appendCount.get();
        return count == 0 ? 0.0 : totalNanosForSegment / (double) count / 1000.0;
    }
}
