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

import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.metadata.DatabaseChange;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.DatabaseInfo;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;

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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end Admin metadata path on ZooKeeper vs 1-voter embedded Raft (loopback). See {@code
 * zookeeper-removal-design.md} §8.4.
 *
 * <p>Covers the main DDL/list/get Admin APIs that {@link FlussClusterExtension} can run without a
 * 3-replica table. ISR / leaderAndIsr churn, rebalance, and lake/kv snapshot RPCs are not measured
 * (need live replica traffic).
 *
 * <p>Conditions: 1 coordinator + 1 tablet server; in-process Curator {@code TestingServer} vs
 * single-voter Ratis; warm JVM; {@code @Fork(0)}. Not a 3-voter same-AZ NVMe production baseline.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(0)
@Threads(1)
public class MetadataPathBenchmark {

    private static final String ADMIN_DB = "jmh_admin_db";

    /** {@code ZOOKEEPER} or {@code RAFT}. */
    @Param({"ZOOKEEPER", "RAFT"})
    public String backend;

    private MetadataJmhSupport.AdminSession session;
    private Admin admin;
    private TableDescriptor descriptor;
    private TableDescriptor partitionedDescriptor;
    private TablePath readPath;
    private TablePath alterPath;
    private TablePath partitionedPath;
    private final AtomicLong seq = new AtomicLong();

    /** Starts a 1-node cluster for the parameterized backend and seeds tables for reads. */
    @Setup(Level.Trial)
    public void setup() throws Exception {
        session = MetadataJmhSupport.sharedAdmin(backend);
        admin = session.admin;
        descriptor = MetadataJmhSupport.benchmarkTableDescriptor();
        partitionedDescriptor = MetadataJmhSupport.benchmarkPartitionedTableDescriptor();
        readPath = TablePath.of(ADMIN_DB, "read_table");
        alterPath = TablePath.of(ADMIN_DB, "alter_table");
        partitionedPath = TablePath.of(ADMIN_DB, "part_table");
        if (!session.seeded) {
            admin.createDatabase(ADMIN_DB, DatabaseDescriptor.EMPTY, false).get();
            admin.createTable(readPath, descriptor, false).get();
            admin.createTable(alterPath, descriptor, false).get();
            admin.createTable(partitionedPath, partitionedDescriptor, false).get();
            admin.createPartition(partitionedPath, partitionSpec("seed"), false).get();
            session.seeded = true;
        }
    }

    /** Shared {@code @Fork(0)} cluster is reused across methods; do not close per trial. */
    @TearDown(Level.Trial)
    public void teardown() {
        // Shared cluster lives for the in-process JVM.
    }

    private PartitionSpec partitionSpec(String value) {
        return new PartitionSpec(Collections.singletonMap("pt", value));
    }

    /** Full DDL create-table path (RPC + coordinator event + metadata write). */
    @Benchmark
    public Void createTable() throws Exception {
        TablePath path = TablePath.of(ADMIN_DB, "t" + seq.incrementAndGet());
        return admin.createTable(path, descriptor, false).get();
    }

    /** Leader metadata read via Admin {@code getTableInfo}. */
    @Benchmark
    public TableInfo getTableInfo() throws Exception {
        return admin.getTableInfo(readPath).get();
    }

    /** Create then drop a unique table. */
    @Benchmark
    public Void dropTable() throws Exception {
        TablePath path = TablePath.of(ADMIN_DB, "tdel" + seq.incrementAndGet());
        admin.createTable(path, descriptor, false).get();
        return admin.dropTable(path, false).get();
    }

    /** Create a unique database. */
    @Benchmark
    public Void createDatabase() throws Exception {
        return admin.createDatabase("adb" + seq.incrementAndGet(), DatabaseDescriptor.EMPTY, false)
                .get();
    }

    /** Create then drop a unique empty database. */
    @Benchmark
    public Void dropDatabase() throws Exception {
        String name = "dbdel" + seq.incrementAndGet();
        admin.createDatabase(name, DatabaseDescriptor.EMPTY, false).get();
        return admin.dropDatabase(name, false, false).get();
    }

    /** Alter the seeded database comment. */
    @Benchmark
    public Void alterDatabase() throws Exception {
        return admin.alterDatabase(
                        ADMIN_DB,
                        Collections.singletonList(
                                DatabaseChange.updateComment("c" + seq.incrementAndGet())),
                        false)
                .get();
    }

    /** Read the seeded database. */
    @Benchmark
    public DatabaseInfo getDatabaseInfo() throws Exception {
        return admin.getDatabaseInfo(ADMIN_DB).get();
    }

    /** Existence check for the seeded database. */
    @Benchmark
    public Boolean databaseExists() throws Exception {
        return admin.databaseExists(ADMIN_DB).get();
    }

    /** List database names. */
    @Benchmark
    public List<String> listDatabases() throws Exception {
        return admin.listDatabases().get();
    }

    /** Existence check for the seeded table. */
    @Benchmark
    public Boolean tableExists() throws Exception {
        return admin.tableExists(readPath).get();
    }

    /** List tables in the seeded database. */
    @Benchmark
    public List<String> listTables() throws Exception {
        return admin.listTables(ADMIN_DB).get();
    }

    /** Alter a custom property on the seeded alter-table. */
    @Benchmark
    public Void alterTable() throws Exception {
        return admin.alterTable(
                        alterPath,
                        Collections.singletonList(
                                TableChange.set("client.connect-timeout", "240s")),
                        false)
                .get();
    }

    /** Create a unique partition on the seeded partitioned table. */
    @Benchmark
    public Void createPartition() throws Exception {
        return admin.createPartition(
                        partitionedPath, partitionSpec("p" + seq.incrementAndGet()), false)
                .get();
    }

    /** Create then drop a unique partition. */
    @Benchmark
    public Void dropPartition() throws Exception {
        String name = "pdel" + seq.incrementAndGet();
        admin.createPartition(partitionedPath, partitionSpec(name), false).get();
        return admin.dropPartition(partitionedPath, partitionSpec(name), false).get();
    }

    /** List partitions of the seeded partitioned table. */
    @Benchmark
    public List<PartitionInfo> listPartitionInfos() throws Exception {
        return admin.listPartitionInfos(partitionedPath).get();
    }

    /**
     * Runs this benchmark with the documented short JMH flags.
     *
     * @param args optional JMH include regex (default: this class)
     */
    public static void main(String[] args) throws RunnerException {
        String include =
                args.length > 0
                        ? args[0]
                        : ".*" + MetadataPathBenchmark.class.getSimpleName() + ".*";
        try {
            new Runner(MetadataJmhSupport.shortRunOptions(include)).run();
        } finally {
            MetadataJmhSupport.closeShared();
        }
    }
}
