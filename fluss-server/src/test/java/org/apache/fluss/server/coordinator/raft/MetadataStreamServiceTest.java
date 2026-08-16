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
 * See the License for the specific language regarding copyright ownership.
 */

package org.apache.fluss.server.coordinator.raft;

import org.apache.fluss.rpc.messages.FetchMetadataRequest;
import org.apache.fluss.rpc.messages.FetchMetadataResponse;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.fluss.shaded.ratis.org.apache.ratis.server.raftlog.RaftLog;
import org.apache.fluss.shaded.ratis.org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MetadataStreamService} async FetchMetadata parking: the RPC future is
 * returned immediately, timeouts and apply wakeups complete it once on the stream pool, and the
 * apply thread only enqueues (§5.6 / analysis §8.3).
 */
class MetadataStreamServiceTest {

    private FakeLog fakeLog;
    private ExecutorService streamExec;
    private RecordingExecutor recordingExecutor;
    private ManuallyTriggeredScheduledExecutorService timeoutScheduler;
    private MetadataStreamService stream;

    @BeforeEach
    void setUp() {
        fakeLog = new FakeLog();
        streamExec =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory.Builder()
                                .setPoolName("test-metadata-stream")
                                .setExceptionHandler(
                                        (t, e) -> {
                                            // Do not FatalExit the test JVM.
                                        })
                                .build());
        recordingExecutor = new RecordingExecutor(streamExec);
        timeoutScheduler = new ManuallyTriggeredScheduledExecutorService();
        stream =
                new MetadataStreamService(
                        fakeLog.raftLog(), () -> 7L, null, recordingExecutor, timeoutScheduler);
    }

    @AfterEach
    void tearDown() {
        if (stream != null) {
            stream.close();
        }
        if (streamExec != null) {
            streamExec.shutdownNow();
        }
    }

    @Test
    void testFetchAsyncReturnsImmediatelyWhenCaughtUp() {
        fakeLog.setLastCommitted(5L);

        long t0 = System.nanoTime();
        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 60_000L);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(elapsedMs).isLessThan(200L);
        assertThat(parked).isNotDone();
        assertThat(fakeLog.getThreads()).isEmpty();
    }

    @Test
    void testTimeoutCompletesEmptyUpToDateResponse() throws Exception {
        fakeLog.setLastCommitted(5L);

        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 1_000L);
        assertThat(parked).isNotDone();

        timeoutScheduler.triggerNonPeriodicScheduledTask();
        MetadataStreamService.FetchResult result = parked.get(5, TimeUnit.SECONDS);

        assertThat(result.snapshotRequired()).isFalse();
        assertThat(result.records()).isEmpty();
        assertThat(result.nextOffset()).isEqualTo(6L);
        assertThat(result.leaderEpoch()).isEqualTo(7L);
        assertThat(result.highMetadataOffset()).isEqualTo(5L);
    }

    @Test
    void testDataArrivalCompletesBeforeTimeout() throws Exception {
        fakeLog.setLastCommitted(5L);
        byte[] payload = recordBytes("db.d", "4");

        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 60_000L);
        assertThat(parked).isNotDone();

        fakeLog.appendRecord(6L, payload);
        stream.onApplied(6L);

        MetadataStreamService.FetchResult result = parked.get(5, TimeUnit.SECONDS);
        assertThat(result.records()).hasSize(1);
        assertThat(result.records().get(0).offset()).isEqualTo(6L);
        assertThat(result.records().get(0).bytes()).isEqualTo(payload);
        assertThat(result.nextOffset()).isEqualTo(7L);
        assertThat(timeoutScheduler.getActiveNonPeriodicScheduledTask()).isEmpty();
    }

    @Test
    void testApplyThreadNotifyDoesNotRunHeavyWork() throws Exception {
        fakeLog.setLastCommitted(5L);
        byte[] payload = recordBytes("db.e", "5");

        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 60_000L);
        assertThat(parked).isNotDone();
        assertThat(fakeLog.getThreads()).isEmpty();

        fakeLog.setSlowGetMs(200);
        fakeLog.appendRecord(6L, payload);

        Thread apply = new Thread(() -> stream.onApplied(6L), "ratis-apply");
        long t0 = System.nanoTime();
        apply.start();
        apply.join(5_000);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(apply.isAlive()).isFalse();
        assertThat(elapsedMs).isLessThan(100L);
        assertThat(recordingExecutor.submitters).contains(apply);

        MetadataStreamService.FetchResult result = parked.get(5, TimeUnit.SECONDS);
        assertThat(result.records()).hasSize(1);
        assertThat(fakeLog.getThreads()).isNotEmpty();
        assertThat(fakeLog.getThreads()).doesNotContain(apply);
        for (Thread getThread : fakeLog.getThreads()) {
            assertThat(getThread.getName()).isNotEqualTo("ratis-apply");
        }
    }

    @Test
    void testTimeoutAndApplyDoNotDoubleComplete() throws Exception {
        fakeLog.setLastCommitted(5L);
        byte[] payload = recordBytes("db.f", "6");

        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 1_000L);
        AtomicInteger completions = new AtomicInteger();
        CompletableFuture<MetadataStreamService.FetchResult> observed =
                parked.whenComplete((r, t) -> completions.incrementAndGet());

        fakeLog.appendRecord(6L, payload);
        Thread timeoutThread =
                new Thread(() -> timeoutScheduler.triggerNonPeriodicScheduledTasks());
        Thread applyThread = new Thread(() -> stream.onApplied(6L), "ratis-apply");
        timeoutThread.start();
        applyThread.start();
        timeoutThread.join(5_000);
        applyThread.join(5_000);
        stream.onApplied(6L);
        timeoutScheduler.triggerNonPeriodicScheduledTasks();

        MetadataStreamService.FetchResult result = observed.get(5, TimeUnit.SECONDS);
        assertThat(result.snapshotRequired()).isFalse();
        assertThat(completions.get()).isEqualTo(1);
        assertThat(parked.complete(result)).isFalse();
        assertThat(completions.get()).isEqualTo(1);
    }

    @Test
    void testNotifyLeadershipLostCompletesParkedWaiters() throws Exception {
        fakeLog.setLastCommitted(5L);

        CompletableFuture<MetadataStreamService.FetchResult> parked =
                stream.fetchAsync(6L, 100, 60_000L);
        assertThat(parked).isNotDone();

        stream.notifyLeadershipLost();
        MetadataStreamService.FetchResult result = parked.get(5, TimeUnit.SECONDS);
        assertThat(result.records()).isEmpty();
        assertThat(result.nextOffset()).isEqualTo(6L);
    }

    @Test
    void testRpcFetchMetadataDoesNotBlockWhenCaughtUp() {
        fakeLog.setLastCommitted(5L);
        MetadataStreamRpcServer rpc = new MetadataStreamRpcServer(stream);
        FetchMetadataRequest request = MetadataStreamRpcs.buildFetchRequest(1, 6L, 0L, 100, 5_000);

        long t0 = System.nanoTime();
        CompletableFuture<FetchMetadataResponse> future = rpc.fetchMetadata(request);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        assertThat(elapsedMs).isLessThan(200L);
        assertThat(future).isNotDone();
    }

    @Test
    void testImmediateFetchWhenDataAlreadyPresent() throws Exception {
        byte[] payload = recordBytes("db.a", "1");
        fakeLog.appendRecord(1L, payload);

        CompletableFuture<MetadataStreamService.FetchResult> future =
                stream.fetchAsync(1L, 100, 5_000L);
        MetadataStreamService.FetchResult result = future.get(5, TimeUnit.SECONDS);

        assertThat(future).isCompleted();
        assertThat(result.records()).hasSize(1);
        assertThat(result.records().get(0).bytes()).isEqualTo(payload);
        assertThat(timeoutScheduler.getAllNonPeriodicScheduledTask()).isEmpty();
    }

    private static byte[] recordBytes(String key, String payload) {
        return new MetadataRecord(
                        MetadataRecord.Type.REGISTER_TABLE,
                        key.getBytes(StandardCharsets.UTF_8),
                        payload.getBytes(StandardCharsets.UTF_8))
                .toBytes();
    }

    private static final class RecordingExecutor implements Executor {
        private final Executor delegate;
        private final List<Thread> submitters = new CopyOnWriteArrayList<>();

        private RecordingExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            submitters.add(Thread.currentThread());
            delegate.execute(command);
        }
    }

    /**
     * Scriptable {@link RaftLog} covering the three methods {@link MetadataStreamService} reads.
     */
    private static final class FakeLog {
        private final RaftLog mock = mock(RaftLog.class);
        private final Map<Long, LogEntryProto> entries = new ConcurrentHashMap<>();
        private final List<Thread> getThreads = new CopyOnWriteArrayList<>();
        private volatile long startIndex = 1L;
        private volatile long lastCommitted = 0L;
        private volatile long slowGetMs;

        private FakeLog() {
            try {
                when(mock.getStartIndex()).thenAnswer(inv -> startIndex);
                when(mock.getLastCommittedIndex()).thenAnswer(inv -> lastCommitted);
                when(mock.get(anyLong()))
                        .thenAnswer(
                                inv -> {
                                    getThreads.add(Thread.currentThread());
                                    long slow = slowGetMs;
                                    if (slow > 0) {
                                        Thread.sleep(slow);
                                    }
                                    long idx = (Long) inv.getArgument(0);
                                    LogEntryProto entry = entries.get(idx);
                                    if (entry != null) {
                                        return entry;
                                    }
                                    return LogEntryProto.newBuilder()
                                            .setIndex(idx)
                                            .setTerm(1L)
                                            .build();
                                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private RaftLog raftLog() {
            return mock;
        }

        private void setLastCommitted(long index) {
            lastCommitted = index;
        }

        private void setSlowGetMs(long ms) {
            slowGetMs = ms;
        }

        private void appendRecord(long index, byte[] data) {
            entries.put(
                    index,
                    LogEntryProto.newBuilder()
                            .setIndex(index)
                            .setTerm(1L)
                            .setStateMachineLogEntry(
                                    StateMachineLogEntryProto.newBuilder()
                                            .setLogData(ByteString.copyFrom(data))
                                            .build())
                            .build());
            if (index > lastCommitted) {
                lastCommitted = index;
            }
        }

        private List<Thread> getThreads() {
            return new ArrayList<>(getThreads);
        }
    }
}
