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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AppendLatencyStats} segment attribution (analysis §8.9.1). */
class AppendLatencyStatsTest {

    private static final long US = 1_000L; // nanos per microsecond

    @Test
    void testEmptyStatsReportZero() {
        AppendLatencyStats stats = new AppendLatencyStats();

        assertThat(stats.appendCount()).isZero();
        assertThat(stats.failureCount()).isZero();
        assertThat(stats.avgTotalMicros()).isZero();
        assertThat(stats.avgSendToReplyMicros()).isZero();
        assertThat(stats.avgReplyToAppliedMicros()).isZero();
        assertThat(stats.sendToReplyShare()).isZero();
        assertThat(stats.snapshot()).isEqualTo("appends=0");
    }

    /** The two segments must sum to the total, otherwise attribution is meaningless. */
    @Test
    void testSegmentsSumToTotal() {
        AppendLatencyStats stats = new AppendLatencyStats();
        // send at 0us, reply at 8000us, applied at 8500us
        stats.record(0L, 8000 * US, 8500 * US);

        assertThat(stats.appendCount()).isEqualTo(1);
        assertThat(stats.avgSendToReplyMicros()).isEqualTo(8000.0);
        assertThat(stats.avgReplyToAppliedMicros()).isEqualTo(500.0);
        assertThat(stats.avgTotalMicros()).isEqualTo(8500.0);
        assertThat(stats.avgSendToReplyMicros() + stats.avgReplyToAppliedMicros())
                .isEqualTo(stats.avgTotalMicros());
    }

    /**
     * The share caps how much of an append §8.9.3 B2 (bypassing gRPC) could possibly address, so it
     * must be the send→reply fraction of the total.
     */
    @Test
    void testSendToReplyShare() {
        AppendLatencyStats stats = new AppendLatencyStats();
        stats.record(0L, 8000 * US, 10_000 * US);

        assertThat(stats.sendToReplyShare()).isEqualTo(0.8);
    }

    @Test
    void testAveragesAcrossMultipleAppends() {
        AppendLatencyStats stats = new AppendLatencyStats();
        stats.record(0L, 1000 * US, 2000 * US);
        stats.record(0L, 3000 * US, 4000 * US);

        assertThat(stats.appendCount()).isEqualTo(2);
        assertThat(stats.avgSendToReplyMicros()).isEqualTo(2000.0); // (1000 + 3000) / 2
        assertThat(stats.avgReplyToAppliedMicros()).isEqualTo(1000.0); // (1000 + 1000) / 2
        assertThat(stats.avgTotalMicros()).isEqualTo(3000.0);
    }

    /** Failures must be counted separately and must not skew the latency averages. */
    @Test
    void testFailuresDoNotAffectAverages() {
        AppendLatencyStats stats = new AppendLatencyStats();
        stats.record(0L, 1000 * US, 2000 * US);
        stats.recordFailure();
        stats.recordFailure();

        assertThat(stats.appendCount()).isEqualTo(1);
        assertThat(stats.failureCount()).isEqualTo(2);
        assertThat(stats.avgTotalMicros()).isEqualTo(2000.0);
    }

    @Test
    void testResetClearsEverything() {
        AppendLatencyStats stats = new AppendLatencyStats();
        stats.record(0L, 1000 * US, 2000 * US);
        stats.recordFailure();

        stats.reset();

        assertThat(stats.appendCount()).isZero();
        assertThat(stats.failureCount()).isZero();
        assertThat(stats.avgTotalMicros()).isZero();
        assertThat(stats.snapshot()).isEqualTo("appends=0");
    }

    @Test
    void testSnapshotReportsBothSegments() {
        AppendLatencyStats stats = new AppendLatencyStats();
        stats.record(0L, 8000 * US, 10_000 * US);

        assertThat(stats.snapshot())
                .contains("appends=1")
                .contains("total=10000.0us")
                .contains("sendToReply=8000.0us(80.0%)")
                .contains("replyToApplied=2000.0us(20.0%)");
    }
}
