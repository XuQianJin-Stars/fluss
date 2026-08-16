<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache Fluss : JMH

Microbenchmarks for Fluss. Classes live under `src/test/java` (same pattern as
`LogScannerBenchmark`) and are **not** executed by Surefire (`*Test` / `*ITCase`
only).

## §8.4 ZooKeeper vs Raft metadata paths

Compares the same metadata operations on **embedded ZooKeeper** (`TestingServer`)
and **1-voter embedded Ratis** (loopback). This is a laptop/CI baseline, not a
3-voter same-AZ NVMe production measurement. Leader-election timing stays in
`RaftLeaderElectionTimingITCase` (3 voters).

| Class | What it measures |
|-------|------------------|
| `MetadataPathBenchmark` | Admin DDL/list/get: create/drop/alter database & table, create/drop partition, listTables, getTableInfo, … (1 coordinator + 1 tablet server) |
| `MetadataStoreBenchmark` | Every `MetadataStoreClient` write/read that can run without a tablet cluster |
| `SequenceIdAllocationBenchmark` | ZK 1:1 vs Raft blockSize=1 vs blockSize=64 |
| `CoordinatorStartupBenchmark` | Coordinator + metadata backend start (0 tablet servers) |

### Short-run flags (used for the design-doc table)

- Warmup: 2 iterations × 1s (startup: 1 single-shot)
- Measurement: 3 iterations × 1s (startup: 3 single-shots)
- Forks: 0 (in-process, required so the cluster stays in the JMH JVM). Store and Admin
  sessions are reused per backend for the JVM, then closed at the end of
  `MetadataBackendJmhMain`.
- Threads: 1
- Mode: AverageTime (µs/op), except startup = SingleShotTime (ms/op)

### How to run

From the repository root, Java 11:

```bash
export JAVA_HOME=/path/to/jdk-11

# Compile this module and its dependencies (fluss-server test-jar is required)
./mvnw -pl fluss-jmh -am test-compile -DskipTests

# All §8.4 benches (several minutes)
./mvnw -pl fluss-jmh exec:java

# One class
./mvnw -pl fluss-jmh exec:java -Djmh.main=org.apache.fluss.jmh.MetadataPathBenchmark
```

Equivalent include regex if you invoke JMH `Main` yourself:

```
org.apache.fluss.jmh.Metadata(Path|Store|Backend).*|.*SequenceIdAllocationBenchmark|.*CoordinatorStartupBenchmark
```

Existing Arrow / log-scanner benches keep their own `main` methods:

```bash
./mvnw -pl fluss-jmh exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=org.apache.fluss.jmh.LogScannerBenchmark
```
