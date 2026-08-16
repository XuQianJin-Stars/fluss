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

import org.apache.fluss.server.metadata.MetadataBackend;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/**
 * Single-node coordinator startup (embedded ZK TestingServer vs 1-voter Ratis, no tablet servers).
 * Uses {@link Mode#SingleShotTime} because each sample starts a fresh cluster.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(0)
@Threads(1)
public class CoordinatorStartupBenchmark {

    /** {@code ZOOKEEPER} or {@code RAFT}. */
    @Param({"ZOOKEEPER", "RAFT"})
    public String backend;

    private FlussClusterExtension cluster;

    /** Stops the cluster started by the previous invocation (not counted in the sample). */
    @TearDown(Level.Invocation)
    public void stopCluster() throws Exception {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    /** Starts coordinator + metadata backend and waits until the leader is serving. */
    @Benchmark
    public FlussClusterExtension startup() throws Exception {
        cluster =
                FlussClusterExtension.builder()
                        .setNumOfTabletServers(0)
                        .setRacks(new String[0])
                        .setMetadataBackend(MetadataBackend.valueOf(backend))
                        .build();
        cluster.start();
        return cluster;
    }

    /**
     * Runs this benchmark with the documented short JMH flags (single-shot, in-process).
     *
     * @param args unused
     */
    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + CoordinatorStartupBenchmark.class.getSimpleName() + ".*")
                        .warmupIterations(1)
                        .measurementIterations(3)
                        .forks(0)
                        .build();
        new Runner(opt).run();
    }
}
