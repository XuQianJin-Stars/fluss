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

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/**
 * Runs every §8.4 ZooKeeper vs Raft metadata-path JMH class with the documented short flags.
 *
 * <p>Startup uses {@code SingleShotTime} (class annotations); the other benches use AverageTime
 * with 2×1s warmup and 3×1s measurement, {@code forks=0}.
 */
public final class MetadataBackendJmhMain {

    private MetadataBackendJmhMain() {}

    /**
     * Entry point for a combined short run of all metadata-backend benches.
     *
     * @param args unused
     */
    public static void main(String[] args) throws RunnerException {
        try {
            Options timed =
                    new OptionsBuilder()
                            .verbosity(VerboseMode.NORMAL)
                            .include(
                                    "org.apache.fluss.jmh.(MetadataPathBenchmark|MetadataStoreBenchmark|"
                                            + "SequenceIdAllocationBenchmark)")
                            .warmupIterations(2)
                            .warmupTime(TimeValue.seconds(1))
                            .measurementIterations(3)
                            .measurementTime(TimeValue.seconds(1))
                            .forks(0)
                            .timeUnit(TimeUnit.MICROSECONDS)
                            .build();
            new Runner(timed).run();

            Options startup =
                    new OptionsBuilder()
                            .verbosity(VerboseMode.NORMAL)
                            .include(
                                    ".*" + CoordinatorStartupBenchmark.class.getSimpleName() + ".*")
                            .warmupIterations(1)
                            .measurementIterations(3)
                            .forks(0)
                            .timeUnit(TimeUnit.MILLISECONDS)
                            .build();
            new Runner(startup).run();
        } finally {
            MetadataJmhSupport.closeShared();
        }
    }
}
