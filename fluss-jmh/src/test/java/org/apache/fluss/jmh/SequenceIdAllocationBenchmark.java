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

package org.apache.fluss.jmh;

import org.apache.fluss.server.metadata.MetadataStoreClient;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

/**
 * Sequence-id allocation: ZooKeeper 1:1 CAS vs Raft block-size 1 (one append/id) vs Raft block-size
 * 64 (KRaft-style pre-reservation). See {@code zookeeper-removal-design.md} §5.3 / §8.4.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
@Threads(1)
public class SequenceIdAllocationBenchmark {

    /**
     * {@code ZOOKEEPER} (always 1:1), {@code RAFT_1} (one Raft append per id), {@code RAFT_64}
     * (block of 64).
     */
    @Param({"ZOOKEEPER", "RAFT_1", "RAFT_64"})
    public String mode;

    private MetadataJmhSupport.StoreSession session;
    private MetadataStoreClient store;

    /** Starts the backend implied by {@link #mode}. */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        if (MetadataJmhSupport.ZK.equals(mode)) {
            session = MetadataJmhSupport.StoreSession.start(MetadataJmhSupport.ZK, 1L);
        } else if ("RAFT_1".equals(mode)) {
            session = MetadataJmhSupport.StoreSession.start(MetadataJmhSupport.RAFT, 1L);
        } else if ("RAFT_64".equals(mode)) {
            session = MetadataJmhSupport.StoreSession.start(MetadataJmhSupport.RAFT, 64L);
        } else {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }
        store = session.store;
        store.getTableIdAndIncrement();
    }

    /** Closes the embedded metadata backend. */
    @TearDown(Level.Trial)
    public void teardown() throws Exception {
        if (session != null) {
            session.close();
        }
    }

    /** One sequence-id increment (ZK CAS, or Raft block-local / append). */
    @Benchmark
    public long allocateSequenceId() throws Exception {
        return store.getTableIdAndIncrement();
    }

    /**
     * Runs this benchmark with the documented short JMH flags.
     *
     * @param args unused
     */
    public static void main(String[] args) throws RunnerException {
        new Runner(
                        MetadataJmhSupport.shortRunOptions(
                                ".*" + SequenceIdAllocationBenchmark.class.getSimpleName() + ".*"))
                .run();
    }
}
