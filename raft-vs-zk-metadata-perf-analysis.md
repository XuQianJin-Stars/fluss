<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Raft 元数据路径慢于 ZooKeeper 的原因分析

| 字段 | 内容 |
|------|------|
| **性质** | 只读分析报告。**不改生产 / 测试 / JMH Java。** §8 是代码级实现指南（patch sketch），未落地。 |
| **对照设计文档** | [`zookeeper-removal-design.md`](zookeeper-removal-design.md) **§8.4**（v3.12，2026-08-16 全量 JMH catalog） |
| **分支** | `feature/zookeeper-removal-raft` |
| **测量日期** | 2026-08-16 |
| **结论一句话** | 在 1-voter loopback 上，Raft **单条 durable 写**大约是 ZK TestingServer 的 **1.7–1.8×**（`registerTable` 4815µs vs 8582µs）。这不是「Raft 实现写坏了」，而是 **每条写都走完整 commit-apply 流水线 + 空队列立即 fsync + `.get()` 等到本地 apply**；读路径和 block 分配 ID 反而更快。**不能把这组数字外推到 3 节点跨 AZ 生产集群。** |

本文只解释 **JMH 里 Raft 明显更慢的那些操作**，并附「Raft 更快」对照，避免把 catalog 读成「Raft 全面更慢」。

---

## 1. 范围与测量环境

### 1.1 测的是什么、不是什么

§8.4 原文已经写明：原表里的 Raft「&lt;5ms p50 / 3-voter same-AZ NVMe」是 **生产目标**，本次填进去的实测列是 **1 节点 loopback JMH baseline**：

- ZooKeeper：**进程内 Curator `TestingServer`**（`ZooKeeperTestUtils.createAndStartZookeeperTestingServer`）
- Raft：**单 voter 嵌入式 Ratis**（loopback `127.0.0.1:<ephemeral>`）

**不能外推：**

| 维度 | 本次 JMH | 生产（未测） |
|------|----------|--------------|
| 拓扑 | 1 voter / 1 ZK | 3 或 5 voter 多数派 |
| 网络 | 本机 loopback | 同 AZ RTT 或跨 AZ RTT |
| 磁盘 | 开发机 Darwin arm64 | 生产 NVMe + 多数派 fsync |
| 并发 | JMH `@Threads(1)` | 多 RPC 线程、心跳、snapshot |
| 语义 | AverageTime ≈ 单峰数据的 p50 点估计 | 需要 p50/p99、负载形状 |

3-voter 写延迟的物理下界是 **多数派 RTT + 多数派 fsync**。跨 AZ 写 **没有测量**；§8.4 写的是「bounded below by majority RTT」。1-voter 已经比 ZK 慢的操作，在 3-voter 上 **只可能更慢**（多一次 follower 复制 + 多数派确认），不能用本次数字声称「生产已经对齐 ZK」。

### 1.2 实测环境（§8.4 Environment）

| 字段 | 值 |
|------|----|
| Date | 2026-08-16 |
| OS / arch | Darwin 25.5.0, arm64 (macOS) |
| Java | TencentKona JDK 11.0.23 |
| JMH | 1.27；AverageTime µs/op（启动：SingleShotTime ms/op） |
| Flags | warmup 2×1s，measurement 3×1s，`@Fork(0)` in-process，1 thread（启动：1 warmup + 3 single-shots） |
| Topology | **1-node**：local ZK `TestingServer` vs 1-voter embedded Ratis on loopback |
| Classes | `fluss-jmh`：`MetadataPathBenchmark`、`MetadataStoreBenchmark`、`SequenceIdAllocationBenchmark`、`CoordinatorStartupBenchmark` |

§8.4 明确：score 是 3 次 measurement 的 AverageTime；± 是 JMH 99.9% CI，**n=3 时区间很宽，当作点估计**。

### 1.3 两层测量必须分开读

| 层 | JMH 类 | 测到的东西 |
|----|--------|------------|
| **Store-level** | `MetadataStoreBenchmark` | `MetadataStoreClient` 直调：ZK `create/setData` vs Raft `writer.append().get()`。**隔离共识**，没有 Admin RPC、没有 tablet assignment/ISR。 |
| **Admin / RPC** | `MetadataPathBenchmark` | 1 coordinator + 1 tablet，`Admin.*().get()`。DDL 还包含 assignment、ISR、tablet RPC。 |
| **ID** | `SequenceIdAllocationBenchmark` | ZK 1:1 CAS vs Raft `blockSize=1` vs `blockSize=64` |
| **启动** | `CoordinatorStartupBenchmark` | 0 tablet；`FlussClusterExtension.start()` 等到 leader 可服务 |

**读 catalog 时：** `createTable` Raft 更快（24.7ms vs 46.6ms）**不能**用来否定 `registerTable` Raft 更慢（8.6ms vs 4.8ms）。前者被 tablet DDL 主导，后者才是共识写。

---

## 2. 哪些操作 Raft 更慢

判定：「明显更慢」= Raft/ZK ≳ **1.3×**，或绝对差 ≳ **1ms**（写）/ ≳ **200µs**（读）。数字全部来自 §8.4 2026-08-16 catalog。

### 2.1 Store-level：单 key durable 写（共识热路径）

这是最干净的对照。几乎所有「一个 Raft append」的写都落在同一条带上：**ZK ~4.7–5.3ms，Raft ~8.2–9.3ms，约 1.7×**。

| 操作 | ZK µs | Raft µs | Raft/ZK | 备注 |
|------|------:|--------:|--------:|------|
| `registerTable` (`metadataWrite`) | **4815** | **8582** | 1.78× | §8.4 主指标；下文 §3.1 拆路径 |
| `updateTable` | 4669 | 8379 | 1.79× | 同一条 append |
| `registerDatabase` | 4865 | 8372 | 1.72× | |
| `updateDatabase` | 4777 | 8561 | 1.79× | |
| `registerSchema` | 4741 | 9335 | 1.97× | 后续 schema id（非 first） |
| `registerTableAssignment` | 4922 | 8709 | 1.77× | |
| `updateTableAssignment` | 4928 | 8163 | 1.66× | `MATCH_ANY` |
| `updatePartitionAssignment` | 4903 | 8253 | 1.68× | |
| `updateLeaderAndIsr` | 5009 | 8528 | 1.70× | |
| `registerCoordinatorLeader` | 9748 | 8346 | 0.86× | ZK 是 unregister+create；**这条 Raft 并不慢**，列在此仅避免和 8.3ms 带混淆 |
| `registerTabletServer` | 4768 | 8675 | 1.82× | unique id |
| `createResourceAcl` | 4709 | 8920 | 1.89× | |
| `updateResourceAcl` | 4695 | 8649 | 1.84× | |
| `registerRebalanceTask` | 4979 | 8463 | 1.70× | |
| `updateServerTags` | 4677 | 8772 | 1.88× | |
| `registerKvSnapshotLeaseMetadata` | 4984 | 8362 | 1.68× | |
| `updateKvSnapshotLeaseMetadata` | 4739 | 8461 | 1.79× | |
| `tryRegisterProducerOffsets` | 4831 | 8557 | 1.77× | |
| `upsertRemoteLogManifestHandle` | 5312 | 8380 | 1.58× | |
| `upsertLakeTable` | 4811 | 8373 | 1.74× | |
| `registerTableBucketSnapshot` | 5002 | 8544 | 1.71× | |

**解释：** 这些行不是 N 个独立瓶颈，而是 **同一种写** 换了 record type。只要理解 `registerTable`，其余单 append 写都可以用同一套根因。

### 2.2 Store-level：多 append / 级联删除（比单写更慢）

JMH 的 delete 行 **包含前面的 unique-key create**（`MetadataStoreBenchmark` javadoc / §8.4）。Raft 侧再叠加「一个逻辑 API = 多次 `append().get()`」。

| 操作 | ZK µs | Raft µs | Raft/ZK | 为何比单写更差 |
|------|------:|--------:|--------:|----------------|
| `deleteDatabase` | 9827 | 17731 | 1.80× | create+delete ≈ 2× 单写 |
| `deleteTable` | 9373 | 16268 | 1.74× | create + `deleteTable` 先扫 schema/partition 再逐条 append |
| `registerPartition` | 13973 | 16993 | 1.22× | Raft **两次串行 append**（assignment 然后 metadata）；ZK 一次 `transaction()` |
| `deletePartition` | 19131 | 24953 | 1.30× | create（2 append）+ delete |
| `deleteTableAssignment` | 9626 | 25441 | 2.64× | create + 级联 `DELETE_LEADER_AND_ISR`（每个 bucket 一条）+ `DELETE_TABLE_ASSIGNMENT` |
| `deletePartitionAssignment` | 19557 | 32866 | 1.68× | 先 `registerPartitionAssignmentAndMetadata`（2 append）再删 assignment |
| `conditionalDeleteResourceAcl` | 9733 | 16653 | 1.71× | create+delete |
| `deleteRebalanceTask` | 9638 | 16597 | 1.72× | |
| `deleteServerTags` | 9819 | 16887 | 1.72× | delete+restore |
| `unregisterCoordinatorLeader` | 9866 | 17221 | 1.75× | unregister + 再 register |
| `deleteKvSnapshotLease` | 9264 | 16222 | 1.75× | |
| `deleteProducerOffsets` | 9403 | 16886 | 1.80× | |
| `deleteProducerSnapshotIfVersion` | 9431 | 16597 | 1.76× | |
| `deleteTableBucketSnapshot` | 9355 | 16535 | 1.77× | |

`registerPartition` 的倍数只有 1.22×，是因为 ZK 这条本身就贵（父节点 create + 2-op transaction，13973µs），Raft 两次 8.5ms append 叠成 ~17ms，差距被 ZK 的「已经很慢」缩小。语义上 Raft 仍然更吃亏：**ZK 一次 round-trip 提交两 key，Raft 两次 commit-apply**。

### 2.3 Store-level：前缀扫描读

**2026-08-16 晚已落地 prefix iterator**（`MetadataKvStore.scanPrefixEntries` seek+stop）。下表 **After** 是同机、同样短 flags 的复跑；ZK 行在噪声内不变。完整对照见 **§8.6**。

| 操作 | ZK µs（catalog / 复跑） | Raft before | Raft after | 机制 |
|------|------:|--------:|--------:|------|
| `listDatabases` | 101 / 107 | 328 | **0.89** | seek `REGISTER_DATABASE` header |
| `listDatabaseSummaries` | 131 / 134 | 330 | **2.28** | |
| `getCurrentSchemaId` | 98 / 106 | 325 | **1.17** | `maxSchemaId` → prefix iterator |
| `getLatestSchemas` | 199 / 222 | 331 | **3.24** | |
| `listTables` | 101 / 107 | 329 | **1.39** | |
| `getPartitions` | 99 / 112 | 331 | **1.57** | |
| `getPartitionNumber` | 99 / 106 | 331 | **1.23** | |
| `getPartitionRegistrations` | 198 / 219 | 327 | **1.59** | |
| `listAssignedTableIds` | 98 / 106 | 328 | **1.13** | |
| `getSortedTabletServerList` | 98 / 105 | 326 | **1.18** | |
| `listResourcesByType` | 139 / 107 | 335 | **0.91** | |

原先 §8.4 写「Raft prefix scan ~330µs」是全量 `entries()` 的常数。落地后 Raft list* 回到 **点读量级（~1µs）**，并快于 ZK `getChildren`（~100µs）。

### 2.4 Admin / RPC 路径（1 coordinator + 1 tablet）

只列 Raft 更慢的。**完整 DDL（create/drop table/partition）Raft 更快，见 §2.6。**

| 操作 | ZK µs | Raft µs | Raft/ZK | 层 |
|------|------:|--------:|--------:|----|
| `createDatabase` | 5379 | 8145 | 1.51× | 接近 store `registerDatabase` |
| `alterDatabase` | 5838 | 8375 | 1.43× | 接近 store `updateDatabase` |
| `dropDatabase` | 11382 | 16338 | 1.44× | 接近 store create+delete |
| **`getTableInfo`** | **356 → 443**（复跑） | **842 → 89**（warm） | **was 2.37×, now 0.20×** | prefix iterator 后 Raft 更快。孤立跑第一条 Admin 读仍可能叠 `metadata.stream.max-wait=100ms`（本次冷跑 104ms，不算优化失败） |

Admin 写里，只有「几乎等于一次 metadata store 写」的 database DDL 跟着共识变慢。`createTable` 24.7ms vs 46.6ms 已经不是共识在主导。

### 2.5 启动与 1:1 ID

| 操作 | ZK | Raft | Raft/ZK |
|------|----|------|--------:|
| Coordinator 启动（0 tablet，`CoordinatorStartupBenchmark`） | **99.3 ms** | **194 ms** | 1.95× |
| Sequence id 1:1（`SequenceIdAllocationBenchmark`） | **4871 µs/op**（~205 ops/s） | **8194 µs/op**（blockSize=1，~122 ops/s） | 1.68× |

生产 Raft 默认 `coordinator.raft.id-block-size=1000` 时，`getTableIdAndIncrement` 是 **8.5µs**（§8.4 / `MetadataStoreBenchmark.allocateId`），**比 ZK 快约 590×**。1:1 慢只说明「把 Raft 当 ZK CAS 用会慢」，不是生产默认。

### 2.6 对照：Raft 更快（避免误读 catalog）

**不要**从「Raft 写慢」推出「用户 DDL 也慢」。

#### Admin / RPC（用户可感知的热路径多数在这里）

| 操作 | ZK µs | Raft µs | 说明 |
|------|------:|--------:|------|
| `createTable` | 46581 | 24713 | 全量 DDL：assignment + ISR + tablet RPC |
| `dropTable` | 89312 | 43684 | create+drop |
| `createPartition` | 28827 | 16999 | 1-bucket partitioned log |
| `dropPartition` | 57902 | 26147 | |
| `listPartitionInfos` | 18535 | 500 | ZK 走大量 znode；Raft 本地/缓存 |
| `alterTable` | 351 | 81 | 幂等 set 之后 |
| `getDatabaseInfo` | 174 | 68 | |
| `databaseExists` | 166 | 62 | |
| `listDatabases` | 245 | 133 | Admin 层；store 层 list 反而是 Raft 慢 |
| `tableExists` | 160 | 63 | |
| `listTables` | 283 | 113 | 同上 |

#### Store 点读

| 操作 | ZK µs | Raft µs |
|------|------:|--------:|
| `getTable` (`metadataRead`) | **102** | **1.6** |
| `getDatabase` / `tableExist` / `getSchemaById` / 多数 `get*` 点读 | ~98–108 | ~0.3–2.1 |

#### Store 写：ZK 路径更重，Raft 反而赢

| 操作 | ZK µs | Raft µs | 原因 |
|------|------:|--------:|------|
| `registerFirstSchema` | 20393 | 8784 | ZK `creatingParentsIfNeeded` 多层父节点 |
| `registerLeaderAndIsr` | 15571 | 8518 | ZK `createRecursiveWithEpochCheck` epoch 包装 |
| `registerCoordinatorServer` | 4957 | 0.67 | Raft **no-op**（membership = voter set） |
| `insertAclChangeNotification` | 4998 | 0.002 | Raft **no-op**（stream 即通知） |
| `getTableIdAndIncrement`（生产 block=1000） | 5037 | **8.5** | 块内本地递增 |

#### Sequence block

| Mode | µs/op | ops/s |
|------|------:|------:|
| ZooKeeper 1:1 CAS | 4871 | ~205 |
| Raft blockSize=1 | 8194 | ~122 |
| Raft blockSize=64 | **127** | **~7.9k** |
| Raft 生产 default 1000（store `allocateId`） | **8.5** | ~118k |

---

## 3. 逐操作拆路径

### 3.1 `registerTable`（store `metadataWrite`：ZK 4815µs vs Raft 8582µs）

这是 §8.4 的主写指标。差 **~3.8ms / 1.78×**。

#### ZooKeeper 路径（一次 IPC + 一次 txn log）

`ZooKeeperClient.registerTable`（`needCreateNode=true` 时）：

```790:807:fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        String path = TableZNode.path(tablePath);
        byte[] tableBytes = TableZNode.encode(tableRegistration);
        if (needCreateNode) {
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, tableBytes);
        } else {
            zkClient.setData().forPath(path, tableBytes);
        }
        // ...
    }
```

JMH store 会话用的是 **本机 `TestingServer`**（`MetadataJmhSupport.StoreSession.start` → `ZooKeeperTestUtils.createAndStartZookeeperTestingServer`）。请求路径：

1. Curator 客户端 → 本机 ZK 端口  
2. ZK 把事务写入 `FileTxnLog`（TestingServer 默认落临时目录，**forceSync 开**）  
3. 更新内存 `DataTree`  
4. 应答返回  

**没有** 第二套状态机、没有 gRPC、没有「commit 之后再 apply 到 RocksDB」的第二跳。父节点若已存在（JMH 先 seed 了 `jmh_store_db`），`creatingParentsIfNeeded` 基本是一次 create。

#### Raft 路径（必须走完 commit **且** local apply）

调用栈：

```
MetadataStoreBenchmark.metadataWrite
  → RaftMetadataClient.registerTable
       requireLeader()
       JSON 序列化 TableRegistration
       append(REGISTER_TABLE, key, payload)
         → writer.append(record).get()          // 调用线程阻塞
              → RaftClient.async().send(msg)    // gRPC，即使 1-voter loopback
              → 多数派 commit（1-voter = 自己）
              → FlussMetadataStateMachine.applyTransaction
                   store.apply → RocksDB put
              → stateMachine.awaitApplied(logIndex)
```

`registerTable` 本身：

```1066:1083:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        requireLeader();
        // needCreateNode is a ZooKeeper znode-lifecycle flag; the Raft backend has no separate node
        // to create — the replicated record IS the registration — so the flag is not applicable.
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tableRegistration, TableRegistrationJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath), payload);
    }
```

`append` **同步等到 apply**：

```2533:2536:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    private void append(MetadataRecord.Type type, byte[] key, byte[] payload) throws Exception {
        // append resolves after commit+local-apply; NO separate store.apply (single-writer,
        // §5.1.4).
        writer.append(new MetadataRecord(type, key, payload)).get();
    }
```

`RaftJournalWriter.append` 把「Ratis send 成功」和「本地 applied」串起来：

```57:71:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftJournalWriter.java
    public CompletableFuture<Long> append(MetadataRecord record) {
        Message msg = Message.valueOf(ByteString.copyFrom(record.toBytes()));
        return raftClient
                .async()
                .send(msg)
                .thenCompose(
                        (RaftClientReply reply) -> {
                            if (!reply.isSuccess()) {
                                CompletableFuture<Long> f = new CompletableFuture<>();
                                f.completeExceptionally(reply.getException());
                                return f;
                            }
                            // wait until our own committed index has been applied locally
                            return stateMachine.awaitApplied(reply.getLogIndex());
                        });
    }
```

State machine 是 **纯 commit-apply**（设计 §5.1.4 明确拒绝 leader pre-apply）：

```120:141:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/FlussMetadataStateMachine.java
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        try {
            LogEntryProto entry = trx.getLogEntry();
            byte[] data = trx.getStateMachineLogEntry().getLogData().toByteArray();
            applyEntry(MetadataRecord.fromBytes(data), entry.getIndex());
            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            return EMPTY;
        } catch (Exception e) {
            // ...
        }
    }

    synchronized void applyEntry(MetadataRecord record, long index) {
        if (index <= lastAppliedIndex) {
            return; // duplicate (retried write)
        }
        store.apply(record); // deterministic mutation of the KvStore + fire local watches
        lastAppliedIndex = index;
        completeAppliedWaiters(index);
        // ...
    }
```

`RaftMetadataServer.buildRaftProperties` **没有**覆盖 Ratis 的 log flush 旋钮，因此走 Ratis 2.4.1 默认（见 §4.2）。它只设置了 gRPC、election timeout、segment size、snapshot、storage dir、TLS：

```128:155:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataServer.java
    RaftProperties buildRaftProperties() {
        RaftProperties p = new RaftProperties();
        RaftConfigKeys.Rpc.setType(p, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setPort(p, localPeerPort());
        // election / first-election / segment / snapshot / storageDir / TLS
        // 没有 setForceSyncNum / unsafe-flush / async-flush
        return p;
    }
```

生产 KvStore 是 **RocksDB**（`RaftMetadataClient.create` → `openProductionKvStore` → `coordinator.raft.dir/metadata-kv/`）。`WriteOptions` 用默认构造（**WAL 开、sync 默认 false**）：apply 路径会写 WAL，但不在 apply 里再 `fsync`。耐久性绑定在 **Raft log fsync** 上；RocksDB put 仍是额外用户态 I/O 和迭代器/memtable 开销。

**1-voter 并不能跳过 fsync。** §8.4 原文：「1-voter Raft still fsyncs the local log；3-voter + RTT will be higher」。

### 3.2 1:1 allocate id（ZK 4871µs vs Raft blockSize=1 的 8194µs）

两条实现的语义不同。

**ZK**（`ZkSequenceIDCounter`）：每次 `getAndIncrement` 都是 Curator `DistributedAtomicLong.increment()` —— **一次 ZK CAS round-trip**，失败抛异常。没有块缓存。

```61:70:fluss-server/src/main/java/org/apache/fluss/server/zk/ZkSequenceIDCounter.java
    public long getAndIncrement() throws Exception {
        AtomicValue<Long> incrementValue = sequenceIdCounter.increment();
        if (incrementValue.succeeded()) {
            return incrementValue.preValue();
        } else {
            throw new Exception(/* ... */);
        }
    }
```

**Raft**（`RaftSequenceIDCounter`）：先 `reserveThrough` 把 high-water mark 用一条 `ALLOCATE_ID_RANGE` 推进 `blockSize`，块内再本地发号。`writer.append(...).get()` **同样要 commit+apply 之后才能发号**（§5.3.3 no-reuse）：

```103:114:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftSequenceIDCounter.java
    private void reserveThrough(long target) throws Exception {
        long newEnd = Math.max(target, blockEnd + blockSize);
        MetadataRecord record =
                new MetadataRecord(
                        MetadataRecord.Type.ALLOCATE_ID_RANGE,
                        counterName.getBytes(StandardCharsets.UTF_8),
                        longToBytes(newEnd));
        writer.append(record).get();
        blockEnd = newEnd;
    }
```

因此：

- `blockSize=1`：每个 id 一次 Raft append ≈ `registerTable` 同量级（8194 vs 8582µs）。**比 ZK CAS 还慢**，因为 Raft 流水线比 TestingServer CAS 重。
- `blockSize=64`：摊销到 127µs/id（§8.4）。
- 生产 `id-block-size=1000`：8.5µs/id。JMH `MetadataStoreBenchmark.allocateId` 用的就是这个默认值。

**结论：** 「Raft ID 慢」只在 **故意关掉块分配** 时成立。生产路径已经按设计避开了这条慢路径。

### 3.3 Coordinator 启动（ZK 99.3ms vs Raft 194ms）

`CoordinatorStartupBenchmark` 每个 sample 都 `FlussClusterExtension.builder().setNumOfTabletServers(0).build(); cluster.start();`，SingleShotTime。

`FlussClusterExtension.start()`：

- **ZK：** 先起 `TestingServer` + `ZooKeeperClient`，再 `startCoordinatorServer()`，再 `waitUntilCoordinatorServerElected()`。
- **Raft：** 直接 `startCoordinatorServer()`（内部 `applyRaftCoordinatorConfig`：election timeout 500ms、first-election 150ms、heartbeat 150ms、`metadata.stream.max-wait=100ms`），再 wait。

Wait 条件 **Raft 比 ZK 更重**：

```1221:1257:fluss-server/src/test/java/org/apache/fluss/server/testutils/FlussClusterExtension.java
    private void waitUntilCoordinatorServerElected() {
        if (metadataBackend == MetadataBackend.RAFT) {
            waitUntil(
                    () -> {
                        // isLeader() flips true as soon as Raft elects; initCoordinatorLeader
                        // (event processor + RPC leadership) finishes later. Wait for both so
                        // metadata RPCs do not hit "CoordinatorEventProcessor is not
                        // initialized yet".
                        // ... getCoordinatorEventProcessor() must succeed
                        return coordinatorServer.isLeader()
                                && meta != null
                                && meta.getCoordinatorLeaderAddress().isPresent()
                                && meta.listDatabases()
                                        .contains(CoordinatorServer.DEFAULT_DATABASE);
                    },
                    Duration.ofSeconds(60),
                    "Fail to wait Raft coordinator server elected");
            return;
        }
        waitUntil(() -> zooKeeperClient.getCoordinatorLeaderAddress().isPresent(), /* 10s */);
        waitUntil(() -> coordinatorServer.getCoordinatorService().isLeader(), /* 10s */);
    }
```

Raft 启动额外工作（`CoordinatorServer` + `RaftLeaderSelector`）：

1. `RaftMetadataClient.create`：开 RocksDB KvStore、起 Ratis gRPC server、RaftClient。  
2. **第一次选举**：`coordinator.raft.first-election.timeout` 默认 300ms（夹具改成 150ms；min = 一半）。1-voter 也要等 first-election 窗口，不能像 ZK LeaderLatch 那样几乎立刻拿到 ephemeral。  
3. `scheduleGainPrimacy` → `awaitCatchUp()`：等到 `stateMachine.awaitApplied(lastCommittedIndex)`（§5.1.4 纯 commit-apply 的强制条件）。  
4. `initCoordinatorLeader`：`registerCoordinatorLeader()` —— **又一次 Raft append**（~8ms 量级）。  
5. 夹具还要等到 default database 出现、event processor 已初始化。

ZK 侧 TestingServer 已经在 coordinator 之前起来；coordinator 主要是 LeaderLatch + 写 ephemeral。99ms vs 194ms 的差（~95ms）和 **first-election 窗口 + Ratis/RocksDB 初始化 + catch-up + 一次 leader 注册 append** 同一量级，**不是**「Raft 元数据写 8ms vs 4ms」那种热路径问题。

生产 3-voter 选举更慢（§8.4：`RaftLeaderElectionTimingITCase` 软断言 &lt;5s；历史 ZK 选举 ~5s）。本次 1-node 启动数字 **不能**用来验收「&lt;1s 3-voter election」目标。

### 3.4 Admin `getTableInfo`（ZK 356µs vs Raft 842µs）—— 若仍慢

Store 层 `getTable` 是 Raft **更快**（1.6µs vs 102µs）。Admin 层反而 Raft 慢 2.4×。这是 **层混淆** 的典型例子。

`FlussAdmin.getTableInfo` **强制打 coordinator write gateway**，不打随机 tablet：

```329:338:fluss-client/src/main/java/org/apache/fluss/client/admin/FlussAdmin.java
    public CompletableFuture<TableInfo> getTableInfo(TablePath tablePath) {
        // Use the coordinator (write) gateway, not a random tablet. Tablet Admin reads go through
        // the Raft follower metadata store / stream and can lag the leader's create/alter, so
        // createTable().get() then getTableInfo().get() would otherwise flake with
        // TableNotExistException under Raft (ZK UpdateMetadata push was usually fast enough).
        return gateway.getTableInfo(request)
```

Coordinator `RpcServiceBase.getTableInfo` → `MetadataManager.getTable`：

1. `getTable` 点读（Raft ~1.6µs）  
2. `getLatestSchema` → **`getCurrentSchemaId` + `getSchemaById`**

Raft `getCurrentSchemaId` 不是点读，是扫 schema 前缀：

```1282:1319:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public int getCurrentSchemaId(TablePath tablePath) {
        requireLeader();
        return maxSchemaId(tablePath);
    }
    private List<Integer> schemaIds(TablePath tablePath) {
        byte[] prefix = keyOf(MetadataRecord.Type.REGISTER_SCHEMA, schemaTableScanBody(tablePath));
        List<Integer> ids = new ArrayList<>();
        for (byte[][] entry : store.scanPrefixEntries(prefix)) {
            ids.add(intFromBytes(entry[0], prefix.length));
        }
        return ids;
    }
```

而 `RaftMetadataStore.scanPrefixEntries` **曾经**不用 `kvStore.scanPrefix`（RocksDB iterator seek），而是 **全量 `kvStore.entries()` 再 filter**（已改，见 §8.6）：

```100:107:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataStore.java
    public List<byte[][]> scanPrefixEntries(byte[] prefix) {
        List<byte[][]> out = new ArrayList<>();
        for (byte[][] entry : kvStore.entries()) {
            if (startsWith(entry[0], prefix)) {
                out.add(entry);
            }
        }
        return out;
    }
```

这和 §8.4 里所有 prefix 行都贴在 **~330µs** 一致。

粗算（store 数字）：

| | ZK | Raft |
|--|----|------|
| `getTable` | 102 | 1.6 |
| `getCurrentSchemaId` | 98 | 325 |
| `getSchemaById` | 103 | 2.1 |
| 三者之和 | ~303 | ~329 |
| Admin `getTableInfo` | **356** | **842** |
| Admin − store 之和 | ~53µs（RPC 开销合理） | **~513µs** |

Raft Admin 多出来的 ~0.5ms **不能**单用 schema 扫描解释完。更可能的叠加：

1. **`@Fork(0)` 共享集群污染（§4.4）**：`MetadataPathBenchmark` 的 `createTable` 会留下大量表；后续 `getTableInfo` 的 `entries()` 全表扫描随 catalog 变大。ZK `getChildren(SchemasZNode.path)` 只列一个表的 schema 子节点，不受「库里有多少其它表」影响。  
2. **Coordinator 与 Ratis gRPC / metadata-stream 同进程争用**：1 tablet 的 `TabletServerRaftMetadataFeed` 按 `coordinator.metadata-stream.max-wait` **long-poll**（生产默认 **500ms**，夹具 **100ms**）。Warm JMH 不应把 100ms 加进每次 `getTableInfo`（否则会看到 ~100ms 而不是 842µs），但 long-poll 线程、apply 唤醒、`onApplied` 仍会扰动。  
3. **冷集群 caveat（§8.4 原文）：** 「cold IT cluster can add `metadata.stream.max-wait=100ms`」。若 Admin 误打到 **tablet** 的 follower 视图、或 cache 尚未追上 leader，一次 miss 可以睡满 long-poll。当前 `FlussAdmin` 已改走 coordinator，**warm JMH 的 842µs 不应再含 100ms**；IT 冷启动 / 旧客户端打 tablet 仍可能。

**2026-08-16 晚更新：** prefix iterator 落地后，warm `getTableInfo` Raft **89µs** vs ZK **443µs**（复跑；catalog 原 ZK 356µs）。store `getCurrentSchemaId` 从 325µs 降到 1.2µs，§3.4 里那笔 ~0.5ms 差消失了。孤立把 `getTableInfo` 当 JVM 里第一条 Admin 读仍可能叠夹具 `max-wait=100ms`（本次冷跑 104ms）——那是 stream 追上问题，不是 scan。

**诚实结论（更新后）：** warm 路径上 `getTableInfo` 不再是「Raft 更慢的读」。它和 `registerTable` 慢 3.8ms **从来不是同一个根因**。

---

## 4. 根因分层

五层是叠加关系，不是五选一。对 **单条 durable 写**，第 1–3 层加起来就能解释 4.8ms → 8.6ms。

### 4.1 协议流水线（即使 1-voter 也在）

| 阶段 | ZK TestingServer | 1-voter Ratis（本次 JMH） |
|------|------------------|---------------------------|
| 客户端到共识 | 本机 ZK 会话 / NIO | **Ratis gRPC** `RaftClient.async().send`（loopback 仍编解码） |
| 复制 | 单节点 txn | Raft append；1-voter 无网络复制，但仍走 replicator/state machine 状态机 |
| 提交门槛 | 本地 txn 完成 | 多数派（自己）commit |
| 可见性 | `DataTree` 在同一条处理路径里更新 | **禁止 pre-apply**：必须 `applyTransaction` 之后 KvStore 才可见（§5.1.4） |
| 调用方 | `create().forPath` 返回 | `append().get()` 再等 `awaitApplied` |

设计文档把 pre-apply 明确标成「可省几 ms 的正确性风险」。JMH 看到的 3.8ms 差值里，**有一部分就是这笔故意付的正确性税**。1-voter 省掉的是 RTT，不是这条流水线。

### 4.2 每条 fsync：`force.sync.num=128` 挡不住空队列

Fluss 使用 **Apache Ratis 2.4.1**（`fluss-shaded-ratis/pom.xml` `<ratis.version>2.4.1</ratis.version>`）。`buildRaftProperties` 不设置 flush 相关 key，因此：

| Ratis key | 默认 | 含义 |
|-----------|------|------|
| `raft.server.log.force.sync.num` | **128**（`RaftServerConfigKeys.Log.FORCE_SYNC_NUM_DEFAULT`） | 积压这么多未 flush 条目才强制 sync |
| `raft.server.log.unsafe-flush.enabled` | **false** | 必须等 `FileChannel.force` |
| `raft.server.log.use.memory` | **false** | 真正写段文件 |

`SegmentedRaftLogWorker.shouldFlush()`（Ratis 2.4.1）是：

```text
if (out == null) return false;
else if (pendingFlushNum >= forceSyncNum) return true;   // 128 条才强制
return pendingFlushNum > 0 && queue.isEmpty();           // 队列空 → 立刻 flush
```

JMH **`@Threads(1)` + 每次 `append().get()`**：调用方发下一条之前，上一条已经走完 commit+apply。Log worker 的 task queue **经常是空的**。于是 `shouldFlush` 的第二句为真：**几乎每条 metadata 写都 fsync**，128 的 batch 阈值根本用不上。

这是 Ratis 为了延迟（不把唯一一条写晾在 buffer 里）做的正确选择，但对「串行同步写」的 JMH 是最坏负载形状。ZK TestingServer 同样每条 txn fsync；Raft 更慢是因为 **fsync 之外还有 gRPC + apply + awaitApplied + RocksDB put**，不是「只有 Raft 在 fsync、ZK 没有」。

3-voter 生产若有并发 metadata 写，`force.sync.num=128` 才可能把多次写合成一次 flush；**本次数字没有覆盖那种形状。**

### 4.3 `awaitApplied`：线性化读的额外一跳

`RaftJournalWriter` 在 `send` 成功后还要 `stateMachine.awaitApplied(reply.getLogIndex())`。

- 目的（§5.7）：leader 上随后的本地读必须看到自己刚提交的写。  
- 代价：调用线程从「多数派已提交」再等到 apply 线程跑完 `store.apply` + waiter complete。1-voter 上 commit 和 apply 仍是 **不同线程**（Ratis log worker vs apply thread），`.get()` 至少一次上下文切换。  
- ZK：create 返回时 DataTree 已更新，没有这层 future。

`awaitApplied` 还有启动路径用法：`RaftLeaderSelector.awaitCatchUp` 等到 leadership-gain 时的 commit index，避免半应用 store 对外服务。这影响 **启动**，不影响稳态 `registerTable`。

### 4.4 启动选举

见 §3.3。稳态写不走选举；**只污染 `CoordinatorStartupBenchmark`**。夹具把 first-election 收到 150ms，生产默认 300ms，3-voter 还要加随机化 election timeout（生产默认上限 1s）。

### 4.5 测试夹具污染（会扭曲读，也会让「共享 JVM」写数字偏乐观/悲观）

这些是测量偏差，不是 Raft 协议本身：

| 夹具行为 | 代码 | 对数字的影响 |
|----------|------|----------------|
| `@Fork(0)` 复用 backend | `MetadataJmhSupport.sharedStore` / `sharedAdmin`；避免 Ratis 端口泄漏 | 同一 JVM 里后面的 trial 看到前面 create 留下的 key；**prefix/`entries()` 扫描变慢** |
| Admin 集群 1 tablet + long-poll | `FlussClusterExtension.applyRaftCoordinatorConfig` 设 `COORDINATOR_METADATA_STREAM_MAX_WAIT=100ms` | warm 路径不应 +100ms；冷 IT / 打 tablet 可以 |
| 生产 long-poll 默认 500ms | `ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT` | 若 getTableInfo 打到未追上的 tablet，可多 500ms |
| JMH 单线程同步 `.get()` | `@Threads(1)` | 最大化「空队列每条 fsync」，最小化 batch |
| TestingServer vs 真 ZK 集群 | 本机单节点 | ZK 数字已经是「最快 ZK」；生产 ZK 多数派会更慢，**相对差距会变** |
| n=3、warmup 2s | §8.4 | 点估计；不能当 p99 |
| `registerCoordinatorServer` / `insertAclChangeNotification` Raft no-op | `RaftMetadataClient` | catalog 里 Raft ~0µs **不是**共识赢了，是 API 空操作 |

`scanPrefixEntries` **曾经**全表扫描；2026-08-16 晚已改成 KvStore prefix iterator（§8.6）。共享 store 残留其它类型的 key **不再**拖慢 `list*` / `getCurrentSchemaId`。

---

## 5. 为什么读和 block ID 反而快

### 5.1 点读：进程内 KvStore vs ZK RPC

Raft leader 读（§5.7）是 `requireLeader()` + `store.get(key)`。生产路径是 **同进程 RocksDB `db.get`**，JMH 约 **1µs**。

ZK 读是 Curator `getData`/`exists`/`getChildren` 进 TestingServer，约 **100µs**。即使 loopback，也是一次 IPC + 编码。

所以 `getTable` 102µs vs 1.6µs 是 **预期的架构差**，不是测量噪声。

### 5.2 Prefix scan（已修）

点读走 `get`。`list*` / `getCurrentSchemaId` 需要从 key 解码 id，所以必须带 key 的前缀扫描。修复前 `scanPrefixEntries` 全量 `entries()` → ~330µs。修复后委托 `MetadataKvStore.scanPrefixEntries`（RocksDB seek / in-memory subMap）→ **~1µs**，快于 ZK `getChildren`。Admin `listTables` 本来就 Raft 更快（RPC 层），现在 store 层也对齐了。

### 5.3 Block ID：一次 Raft 换一千次 ZK CAS

ZK 每个 id 一次 CAS（~5ms）。Raft 一次 `ALLOCATE_ID_RANGE` append（~8ms）换 `blockSize` 个本地 id。默认 1000 → 8.5µs。这是 §5.3 的设计目标，JMH 已验证。

### 5.4 重 DDL 为什么 Raft 可以更快

`createTable` 46ms vs 25ms：共识写只占其中一次（或几次）8ms 级 append，其余是 tablet 建副本、ISR、RPC。ZK 模式 tablet 靠 ZK watch / 多次 znode；Raft 模式 metadata stream + 更少的父节点 create。`listPartitionInfos` 18.5ms vs 0.5ms 更极端：ZK 树走访 vs 本地结构。

**用户可感知的建表，本次 1-node JMH 上 Raft 并不慢。** 慢的是 **纯 metadata store 写** 和 **database 这种几乎等于纯 store 写的 Admin API**。

---

## 6. JMH 局限

1. **1-voter loopback ≠ 3-voter 同 AZ ≠ 跨 AZ。** 写延迟在 3-voter 上会加上多数派 RTT 和至少一个 follower fsync。本次 Raft 已经比 ZK 慢的写，生产只会更慢，除非 ZK 也换成多节点并跨网络。  
2. **AverageTime、3 次 1s、`@Fork(0)`。** 不是 p50/p99；CI 宽；JIT/GC/端口复用都在同一 JVM。  
3. **单线程同步写** 专门打中 Ratis「queue empty → flush now」。并发 pipelined `append`（不在每次后 `.get()`）可能完全不同。  
4. **共享 session 污染 prefix 读和 `getTableInfo`。**  
5. **TestingServer 是偏快的 ZK。** 生产 ZK 3 节点 + `forceSync` + 真实磁盘，ZK 写也会从 ~5ms 往上走；相对比会变。  
6. **没有测：** 跨 AZ、3-voter 选举（只有 IT 软断言）、LeaderAndIsr 热更新 RPC、authorizer ACL Admin、rebalance、湖/kv snapshot Admin。§8.4「Not measured」表已列。  
7. **No-op 行**（`registerCoordinatorServer`、`insertAclChangeNotification`）不能当吞吐胜利。  
8. **启动 bench 含夹具 wait**（event processor、default DB、leader address），不是纯 `CoordinatorServer.startServices()`。  
9. **macOS arm64 笔记本磁盘** 不能代表生产 NVMe 的 fsync 成本。

---

## 7. 结论与「若要对齐生产该怎么测」

### 7.1 结论（对当前代码与当前数字诚实）

1. **Raft 输在「单条 durable 元数据写」和「把 Raft 当 1:1 CAS」。** `registerTable` 4815µs vs 8582µs 是代表性数字；同带上几乎所有单 append 写都是 ~1.7×。  
2. **根因是设计内的：** 纯 commit-apply、每条写 `awaitApplied`、Ratis 空队列立即 fsync、gRPC 即使 loopback、apply 再写 RocksDB。不是漏了索引或「实现比 ZK 拙劣一截」那么简单。  
3. **Raft 赢在点读、block ID、以及被 tablet DDL 主导的 `createTable`/`dropTable`。** 用 Admin 建表数字说「Raft 更快」或用 store `registerTable` 说「Raft 不能用」，都是错的层。  
4. **`getTableInfo` 曾慢是读路径实现（全表 `entries()`），不是 fsync。** prefix iterator 落地后 warm Raft 89µs &lt; ZK 443µs。冷集群 `metadata.stream.max-wait=100ms` 仍是独立 caveat。  
5. **1-node JMH 不能验收 §8.4 的 3-voter NVMe 目标（写 p50 &lt;5ms）。** 1-voter 已经 8.6ms；3-voter 同 AZ 若 RTT+fsync 做得好，有可能回到 5ms 附近，**需要另测**；跨 AZ 由物理 RTT 决定。  
6. **生产 Java 仍未改。** 代码级怎么改见 **§8**（prefix iterator、FetchMetadata 异步停车、writer pipeline、`async-flush`；明确禁止 `unsafe-flush`）。任何一项都有正确性或运维代价。

### 7.2 建议的生产对齐测法（只建议，不改仓库）

要对齐「生产元数据写是否可接受」，建议单独跑一套 **不替换本次 JMH 代码** 的实验：

1. **3-voter，同 AZ，本地 NVMe**，真实 `coordinator.raft.dir`，不要 TestingServer。  
2. 报 **p50/p99**，不要只报 AverageTime；warmup 拉长；**`@Fork(1)` 或独立进程**，避免共享 store 污染。  
3. 两种负载：  
   - 串行 `append().get()`（对照本次）  
   - 并发 pipelined 写（更像 coordinator RPC 池）  
4. 分别测 store `registerTable` 和 Admin `createTable`，继续分栏。  
5. 打开（或记录）Ratis `raft.appendLatency` / flush metrics（设计 §5.12），区分 fsync vs apply vs gRPC。  
6. **跨 AZ** 只测 round-trip 下界：`p50 ≈ majority RTT + fsync`，不要和 loopback 比绝对值。  
7. `getTableInfo`：空集群 vs 大 catalog；coordinator gateway vs 故意打 tablet（验证 long-poll caveat）。  
8. 启动：把夹具 `waitUntil*` 和 `CoordinatorServer.startServices()` 分开计时；3-voter 选举用 `RaftLeaderElectionTimingITCase` 的毫秒日志，而不是 1-node SingleShot。

在那套数字出来之前，唯一站得住的说法是：

> 在 2026-08-16 的 1-voter loopback JMH 上，Raft 单条 metadata 写约为 ZK TestingServer 的 1.7×（8.6ms vs 4.8ms）；点读和默认块分配 ID 明显更快；完整 `createTable` 在该环境反而 Raft 更快。这不能证明生产 3-voter 已经达标或未达标。

---

## 8. 代码级实现指南（只规划，不改生产 / JMH 代码）

本节把 §4 / §7.1 的推论落到 **具体类、方法和可编译的 Java 8 patch sketch**。**本文不落地行为变更**；sketch 仅供评审。验证一律走现有 JMH（`MetadataStoreBenchmark` / `MetadataPathBenchmark`，`@Threads(1)` AverageTime µs/op），并标明哪一行会动、哪一行根本动不了。

约束与现有代码一致：源码 Java 8（禁止 `var` / `List.of` / `Stream.toList`）；shaded Ratis 包名 `org.apache.fluss.shaded.ratis.org.apache.ratis.*`；apply 线程规则见设计文档 **§5.6**。

建议落地顺序（若将来开 PR）：**8.4 prefix iterator（S）→ 8.3 FetchMetadata 异步停车（M）→ 8.1 仅 `async-flush` 属性（S）+ 8.2 writer pipeline（M）**。不要先开 `unsafe-flush`。不要先加 `MULTI_OP` 记录类型（见 8.2.3）。

---

### 8.1 Group-commit / flush batching（串行 `append().get()`）

#### 8.1.1 当前调用链

JMH `MetadataStoreBenchmark.metadataWrite`（`@Threads(1)`）→ `store.registerTable` → `RaftMetadataClient.registerTable` → 私有 `append` 里 **`writer.append(record).get()`** → `RaftJournalWriter.append`：`raftClient.async().send` 成功后再 `stateMachine.awaitApplied(logIndex)`。

```1073:1083:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public void registerTable(
            TablePath tablePath, TableRegistration tableRegistration, boolean needCreateNode)
            throws Exception {
        requireLeader();
        byte[] payload =
                JsonSerdeUtils.writeValueAsBytes(
                        tableRegistration, TableRegistrationJsonSerde.INSTANCE);
        append(MetadataRecord.Type.REGISTER_TABLE, tablePathBytes(tablePath), payload);
    }
```

```2533:2537:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    private void append(MetadataRecord.Type type, byte[] key, byte[] payload) throws Exception {
        // append resolves after commit+local-apply; NO separate store.apply (single-writer,
        // §5.1.4).
        writer.append(new MetadataRecord(type, key, payload)).get();
    }
```

```57:71:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftJournalWriter.java
    public CompletableFuture<Long> append(MetadataRecord record) {
        Message msg = Message.valueOf(ByteString.copyFrom(record.toBytes()));
        return raftClient
                .async()
                .send(msg)
                .thenCompose(
                        (RaftClientReply reply) -> {
                            if (!reply.isSuccess()) {
                                CompletableFuture<Long> f = new CompletableFuture<>();
                                f.completeExceptionally(reply.getException());
                                return f;
                            }
                            return stateMachine.awaitApplied(reply.getLogIndex());
                        });
    }
```

`RaftJournalWriter.append` **已经是异步的**（返回 `CompletableFuture`，符合 §5.6）。阻塞点不在 writer，而在 **`RaftMetadataClient.append` 的 `.get()`**：下一条 metadata 写要等上一条 **多数派 commit + 本地 apply** 全部结束。JMH `@Threads(1)` 把这条路径打成「空队列立即 fsync」（§4.2）。

#### 8.1.2 `buildRaftProperties()` 现在设了什么、没设什么

```128:155:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataServer.java
    RaftProperties buildRaftProperties() {
        RaftProperties p = new RaftProperties();
        RaftConfigKeys.Rpc.setType(p, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setPort(p, localPeerPort());
        // election min/max, first-election, segment size, snapshot auto-trigger, storageDir, TLS
        applyTlsProperties(p);
        return p;
    }
```

对照 **Apache Ratis 2.4.1** `RaftServerConfigKeys.Log`（`ratis-server-api-2.4.1.jar`，Fluss 经 `fluss-shaded-ratis` 重定位）。`buildRaftProperties` **一个 flush 键都没设**，因此全部走默认：

| Ratis key | setter | 2.4.1 默认 | 含义 |
|-----------|--------|------------|------|
| `raft.server.log.force.sync.num` | `Log.setForceSyncNum(p, n)` | **128** | `pendingFlushNum >= n` 时强制 `FileChannel.force` |
| `raft.server.log.async-flush.enabled` | `Log.setAsyncFlushEnabled(p, true)` | **false** | `force` 丢到独立 `flushExecutor`；**仍等 force 完成才推进 flushed index** |
| `raft.server.log.unsafe-flush.enabled` | `Log.setUnsafeFlushEnabled(p, true)` | **false** | **不等** `FileChannel.force` 就推进 flushed index |
| `raft.server.log.use.memory` | `Log.setUseMemory` | **false** | 内存 log，无段文件 |

Ratis 源码 `SegmentedRaftLogWorker.shouldFlush()`（2.4.1）仍是：

```text
if (out == null) return false;
else if (pendingFlushNum >= forceSyncNum) return true;   // 128
return pendingFlushNum > 0 && queue.isEmpty();           // 队列空 → 立刻 flush
```

`flushIfNecessary()` 三条路径（同文件）：

- 默认：`flushOutStream()`（同步 `force`）→ `updateFlushedIndexIncreasingly()`
- `asyncFlush`：`out.asyncFlush(flushExecutor).thenCombine(...).whenComplete(updateFlushedIndex…)` —— **耐久性仍绑定 force 完成**
- `unsafeFlush`：`unsafeFlushOutStream()` 后 **立刻** `updateFlushedIndexIncreasingly()` —— commit 可在 force 之前完成
- 构造器：`if (asyncFlush && unsafeFlush) throw IllegalStateException` —— **不能同时开**

#### 8.1.3 改 properties vs 改 writer pipeline：哪条才打得中 JMH

| 手段 | 对 `@Threads(1)` + 每次 `.get()` 的 `registerTable` | 对生产多 RPC worker 并发写 | 对一次 DDL 内多条 append（见 8.2） |
|------|------------------------------------------------------|------------------------------|-------------------------------------|
| 只改 `force.sync.num`（128→更大/更小） | **几乎为 0**。队列每次都空，`shouldFlush` 第二句为真，128 用不上 | 并发时才可能把多条合成一次 force | 仍被每条 `.get()` 拆开 |
| 打开 `async-flush` | **很小**。fsync 与 apply 略重叠，但 `.get()` 仍等 flushed index | **有用**：log worker 在 force 期间能继续吃队列 | 单独不够；要配合 pipeline |
| 打开 `unsafe-flush` | 数字会好看 | 数字会好看 | 数字会好看 |
| writer / client **pipeline**（多条 `send` 后再 `allOf().get()`） | 单条 `metadataWrite` **仍 1 次 fsync**，那一行不变 | 自然 group-commit | **这才是 createTable / registerPartition 的杠杆** |

结论：**不要指望调 `force.sync.num` 让当前 JMH `metadataWrite` 从 8582µs 掉下来。** 那一行的负载形状专门制造空队列。要对齐生产，要么加并发 JMH（现有 catalog 没有 `@Threads(N)` 写），要么在 **一条 RPC 里 pipeline 多条 record**（8.2）。

#### 8.1.4 属性层 patch sketch（仅 `async-flush`；Java 8）

在 `buildRaftProperties()` 末尾、`return p` 之前（shaded import 已存在于该文件）：

```java
// Optional, production-safe overlap: FileChannel.force still happens; flushed
// index advances only after force completes. Does NOT fix @Threads(1) .get().
RaftServerConfigKeys.Log.setAsyncFlushEnabled(p, true);
```

若希望可配，新增 `ConfigOption<Boolean>`（例如 `coordinator.raft.log.async-flush.enabled`，默认 `false` 直到 3-voter 测过），再 `setAsyncFlushEnabled(p, conf.getBoolean(option))`。不要把 `force.sync.num` 当成「关掉每条 fsync」的旋钮。

#### 8.1.5 什么绝对不要开：`unsafe-flush`

**禁止**在 metadata Raft 上设置 `Log.setUnsafeFlushEnabled(p, true)`。

- 语义：flushed index / commit 可以发生在 `FileChannel.force` 之前。掉电或 `kill -9` 后，多数派已「提交」的 metadata 可能从段文件里消失。
- 设计 §5.1.4 把耐久性绑在 **Raft log fsync** 上；RocksDB apply 的 `WriteOptions` 默认 **不** 再 sync。`unsafe-flush` 等于拆掉这唯一的耐久性锚。
- 和 `async-flush` 互斥；开了会直接 `IllegalStateException`。
- 若有人用它「优化」JMH：`registerTable` 会变快，那是 **丢掉 durability 换来的假分数**，不能进生产、不能写进 §8.4 验收。

#### 8.1.6 风险 / 工作量 / 用现有 JMH 怎么验

| | |
|--|--|
| **工作量** | **S**（只加 `setAsyncFlushEnabled` + 一项 IT 确认 1-voter 仍能选主、写后读）。Pipeline 见 8.2，**M**。 |
| **JMH** | `MetadataStoreBenchmark.metadataWrite` / `registerTable` / `updateTable` / `registerDatabase`：**不要预期** `@Threads(1)` 有显著下降。`MetadataPathBenchmark.createTable`：属性单独改 **几乎不动**（24.7ms 被 tablet DDL 主导；其中 3 次 store append 仍串行 `.get()`）。 |
| **生产风险** | `async-flush`：低–中（Ratis 支持的安全重叠；需 3-voter 看 commit 延迟与 flush 线程异常路径）。`unsafe-flush`：**不可接受**（静默丢元数据）。 |

现有 JMH 命令（不改 JMH 源码）：

```bash
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.metadataWrite
./mvnw -pl fluss-jmh test -Dtest=MetadataPathBenchmark.createTable
```

若只改了 `async-flush` 而 `metadataWrite` 几乎不变，这是 **预期**，不是实现失败。

---

### 8.2 一次 DDL 内 pipeline / 批量多条 metadata record

#### 8.2.1 当前调用链：`createTable` 四步，每步一次同步写

```408:454:fluss-server/src/main/java/org/apache/fluss/server/coordinator/MetadataManager.java
    public long createTable(...) {
        // ...
        zookeeperClient.registerFirstSchema(tablePath, tableToCreate.getSchema());
        return uncheck(
                () -> {
                    long tableId = zookeeperClient.getTableIdAndIncrement();
                    if (tableAssignment != null) {
                        zookeeperClient.registerTableAssignment(tableId, tableAssignment);
                    }
                    zookeeperClient.registerTable(
                            tablePath,
                            TableRegistration.newTable(tableId, remoteDataDir, tableToCreate),
                            false);
                    return tableId;
                },
                "Fail to create table " + tablePath);
    }
```

`zookeeperClient` 在 Raft 后端就是 `RaftMetadataClient`（`MetadataStoreClient`）。每一步都是 **一条** `MetadataRecord` + 一次 `append().get()`：

| 步骤 | 方法 | record type | 是否一定打 Raft |
|------|------|-------------|-----------------|
| 1 | `registerFirstSchema` | `REGISTER_SCHEMA` | 是 |
| 2 | `getTableIdAndIncrement` | `ALLOCATE_ID_RANGE` | **仅当本地 block 用尽**（生产 `id-block-size=1000` 时多数时候是纯内存 `nextId++`） |
| 3 | `registerTableAssignment` | `UPDATE_TABLE_ASSIGNMENT` | 非分区表才有 |
| 4 | `registerTable` | `REGISTER_TABLE` | 是 |

```1251:1264:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public int registerFirstSchema(TablePath tablePath, Schema schema) throws Exception {
        return registerSchema(tablePath, schema, DEFAULT_SCHEMA_ID);
    }
    public int registerSchema(...) {
        requireLeader();
        append(MetadataRecord.Type.REGISTER_SCHEMA, schemaKeyBytes(tablePath, schemaId),
                schema.toJsonBytes());
        return schemaId;
    }
```

```2334:2337:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public long getTableIdAndIncrement() throws Exception {
        requireLeader();
        return tableIdCounter.getAndIncrement();
    }
```

```1424:1431:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public void registerTableAssignment(long tableId, TableAssignment tableAssignment)
            throws Exception {
        requireLeader();
        append(MetadataRecord.Type.UPDATE_TABLE_ASSIGNMENT, longToBytes(tableId), payload);
    }
```

依赖：步骤 3/4 需要步骤 2 的 `tableId`。步骤 1 与 2 **无数据依赖**（schema 键是 `tablePath`，不是 `tableId`）。ZK 注释已写明：schema 先注册、表后失败「再注册一次 schema 无害」—— **今天的 createTable 也不是原子事务**。

`registerPartition` 同构：JMH 注明「assignment + metadata txn (2 keys)」，Raft 16993µs ≈ 两次 8.5ms append。

#### 8.2.2 现有 `MetadataRecord.Type`：没有 MultiOp / batch

```38:101:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataRecord.java
    public enum Type {
        REGISTER_DATABASE,
        // ...
        DELETE_SCHEMA
        // Append-only: new types MUST be added at the end (codec uses enum ordinal).
    }
```

Codec 是 `[1B version][2B type ordinal][4B keyLen][key][4B payloadLen][payload]`（`MetadataRecordCodec`）。`apply` 是 **一 record → 一 key put/delete**（`RaftMetadataStore.apply`）。流上 **一个 Raft index = 一条** `StreamRecord`（`MetadataStreamService.fetch`），subscriber 用 `MetadataRecord.fromBytes` 再 `store.apply`。

因此：**今天没有、也不该轻易加「一条 log 里塞 N 个 mutation」的类型。** 未知 ordinal 会让旧二进制 `decode` 直接抛错；混版本 quorum 还要抬 `MetadataVersions.CURRENT_METADATA_VERSION`（现为 1）。即便加上 `MULTI_OP`，tablet 的 `MetadataStreamCacheApplier` 也必须会拆包，否则 follower 视图会丢 mutation。

#### 8.2.3 推荐的最小 API：N 条独立 log entry，pipeline 后再等 apply

**不要**新 `Type`。复用 `RaftJournalWriter.append` 已返回的 future，在 client 侧一次发出去、最后 `allOf`。

`RaftJournalWriter` 增加（Java 8）：

```java
public CompletableFuture<Long> appendAll(List<MetadataRecord> records) {
    if (records == null || records.isEmpty()) {
        throw new IllegalArgumentException("records must not be empty");
    }
    List<CompletableFuture<Long>> futures = new ArrayList<CompletableFuture<Long>>(records.size());
    for (int i = 0; i < records.size(); i++) {
        futures.add(append(records.get(i)));
    }
    CompletableFuture<Void> all =
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    return all.thenApply(
            ignored -> {
                Long last = null;
                for (int i = 0; i < futures.size(); i++) {
                    last = futures.get(i).join();
                }
                return last;
            });
}
```

`RaftMetadataClient`：

```java
private void appendAll(List<MetadataRecord> records) throws Exception {
    requireLeader();
    writer.appendAll(records).get();
}
```

`createTable` 热路径（tableId 已在本地 block 内时）可收成 **一次** `appendAll`（schema + 可选 assignment + table）。若 `getAndIncrement` 需要 `reserveThrough`，**先**让 `ALLOCATE_ID_RANGE` `.get()` 完成（no-reuse，§5.3.3），再 `appendAll` 其余记录——不要把「尚未持久化的 id」放进后面的 record。

`MetadataManager.createTable` 可以继续逐步调现有四个方法（语义不变）；真正的收益来自 **Raft 后端把 3 次 `.get()` 合成 1 次 `appendAll().get()`**。若不想改 `MetadataManager`，可在 `RaftMetadataClient` 增加 `createTableRecords(...)` 包一层，由将来的 coordinator 路径调用。第一刀甚至可以只改 client、让 `MetadataManager` 仍逐步调用——那就 **没有** pipeline 收益。要收益，必须在 **同一次** `appendAll` 里发出去。

**明确不做（v1）：**

- 新 `Type.MULTI_OP` / 嵌套 payload：要 bump `metadata.version`、改 `apply`、改 stream applier、混版本滚动。正确性收益（原子 DDL）ZK 路径也没有，不值得当 perf 的第一刀。
- 在 apply 线程里 `append` 或 `.get()`（§5.6 自死锁）。
- 把 `ALLOCATE_ID_RANGE` 和「用新 id 的 REGISTER_*」放进 **同一条** 未等待 apply 的 batch 且在 client 侧提前把 id 交给调用方——违反 no-reuse。

#### 8.2.4 风险 / 工作量 / JMH

| | |
|--|--|
| **工作量** | **M**（writer + client API + `createTable`/`registerPartition` 接线 + 失败部分提交测试）。`MULTI_OP` 是 **L**，不建议作为 perf PR。 |
| **JMH** | `MetadataPathBenchmark.createTable`：store 部分从 ~3 次 8.5ms 串行 fsync 变成 ~1 次；绝对值仍被 tablet ISR 主导，可能从 24.7ms 掉几个 ms，**不要预期腰斩**。`MetadataStoreBenchmark.registerPartition`（16993µs）：两 key → 一次 flush，**最干净的 batch 验收行**。`registerFirstSchema`+`registerTable` 各自单行 **不变**（仍是一条 append）。`metadataWrite` **不变**。 |
| **生产风险** | 中。部分失败语义与今天 ZK 相同（schema 可能留下）；pipeline 后失败窗口更短但诊断要按「一批」来。Ratis 客户端对同 group 的有序 `async().send` 必须保持 FIFO（Ratis 默认 ordered stream）。IT：`RaftMetadataClientITCase` 建表后 `getTable` / `getSchemaById` / assignment 三件套仍可见。 |

```bash
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.registerPartition
./mvnw -pl fluss-jmh test -Dtest=MetadataPathBenchmark.createTable
```

---

### 8.3 `FetchMetadata` 不得阻塞 Netty worker

#### 8.3.1 当前调用链：long-poll 发生在 worker 上，不是 selector 上

**好消息：** Netty I/O 线程（`fluss-netty-server-selector`）**没有**在 `wait()`。`NettyServerHandler` 解析完请求后 `requestChannel.putRequest(request)`，响应用 `future.whenCompleteAsync(..., ctx.executor())` 写回。

**坏消息：** 真正处理 RPC 的是 **`fluss-netty-server-worker`**（默认 **8** 条，`netty.server.num-worker-threads`）。`RequestProcessor.run` 从 channel `poll` 后同步调用 `handler.processRequest`。`FlussRequestHandler` 反射调用 gateway 方法，再 `whenComplete` 填响应——**前提是该方法立刻返回一个（可能未完成的）future**。

```54:79:fluss-rpc/src/main/java/org/apache/fluss/rpc/netty/server/FlussRequestHandler.java
    public void processRequest(FlussRequest request) {
        // ...
        CompletableFuture<?> responseFuture =
                (CompletableFuture<?>) api.getMethod().invoke(service, message);
        responseFuture.whenComplete(
                (response, throwable) -> { /* request.complete / fail */ });
    }
```

```52:63:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataStreamRpcServer.java
    public CompletableFuture<FetchMetadataResponse> fetchMetadata(FetchMetadataRequest request) {
        CompletableFuture<FetchMetadataResponse> future = new CompletableFuture<>();
        try {
            MetadataStreamService.FetchResult result =
                    streamService.fetch(request.getFromOffset(), maxRecords, maxWaitMs);
            future.complete(MetadataStreamRpcs.toResponse(result));
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }
```

`streamService.fetch` 在调用线程上跑完，包括 long-poll：

```123:126:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataStreamService.java
        if (fromOffset > raftLog.getLastCommittedIndex() && maxWaitMs > 0) {
            awaitNewData(fromOffset, maxWaitMs);
        }
```

```189:207:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataStreamService.java
    private void awaitNewData(long fromOffset, long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        synchronized (wakeupLock) {
            while (serving && fromOffset > raftLog.getLastCommittedIndex() /* && epoch */) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return;
                }
                wakeupLock.wait(remaining);
            }
        }
    }
```

因此：`fetchMetadata` 返回的 future **在 worker 返回之前就已经 completed**。`RequestProcessor` 在 `wait(maxWaitMs)` 期间 **不能** 处理同一 processor 队列里的 `createTable` / `getTableInfo` / 其它 RPC。

超时预算：生产 `coordinator.metadata-stream.max-wait` 默认 **500ms**；IT/JMH 夹具 **100ms**（`FlussClusterExtension.applyRaftCoordinatorConfig`）。每个追上水位的 tablet 订阅占 **1 个 worker ≤ 500ms**。默认 8 worker；tablet 数上去会把 coordinator RPC 打满。

设计 §5.6 写的「metadata-stream pool」和 `MetadataStreamService` javadoc「Runs on the metadata-stream pool」**尚未实现**：fetch 跑在通用 Netty worker 上。`onApplied` 已正确（apply 线程只 `notifyAll`）：

```172:176:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataStreamService.java
    public void onApplied(long metadataOffset) {
        synchronized (wakeupLock) {
            wakeupLock.notifyAll();
        }
    }
```

`FlussMetadataStateMachine.setApplyListener` 也写明：listener **必须便宜、非阻塞**，不得在 apply 上等待新 append。

#### 8.3.2 异步停车 sketch（CompletableFuture + scheduled timeout，遵守 §5.6）

目标：`fetchMetadata` **立刻**返回未完成的 future，worker 马上 `poll` 下一条；到期或 `onApplied` 之后在 **非 apply** 线程读 log、完成 future。

要点：

1. **不要**在 apply 线程 `future.complete(response)`。`FlussRequestHandler.whenComplete` 跑在 **完成 future 的那个线程** 上；在 apply 上 complete 会把响应序列化/metrics 拽进 apply loop。
2. `onApplied` 只允许：记下 waiter、`notify`、或把「去读 log」的任务 `execute` 到 stream 线程池。禁止 `writer.append`、禁止任何 append future 的 `.get()`、禁止 `raftLog.get` 的大段扫描。
3. 超时用 `ScheduledExecutorService.schedule`，**不是** apply 线程 sleep。
4. 数据已在水位之上：同步读 log 后 complete 即可（短路径仍可留在 worker；或统一丢给 stream pool，避免两条路径）。

`MetadataStreamRpcServer`：

```java
public CompletableFuture<FetchMetadataResponse> fetchMetadata(FetchMetadataRequest request) {
    int maxRecords = request.hasMaxRecords() ? request.getMaxRecords() : 0;
    long maxWaitMs = request.hasMaxWaitMs() ? request.getMaxWaitMs() : 0L;
    return streamService
            .fetchAsync(request.getFromOffset(), maxRecords, maxWaitMs)
            .thenApply(MetadataStreamRpcs::toResponse);
}
```

`MetadataStreamService` 核心（Java 8；`streamExecutor` / `timeoutScheduler` 在构造时注入，daemon 线程）：

```java
public CompletableFuture<FetchResult> fetchAsync(
        final long fromOffset, final int maxRecords, final long maxWaitMs) {
    CompletableFuture<FetchResult> future = new CompletableFuture<FetchResult>();
    try {
        FetchResult immediate = tryFetchNow(fromOffset, maxRecords);
        if (immediate != null
                && (!immediate.records().isEmpty()
                        || immediate.snapshotRequired()
                        || maxWaitMs <= 0)) {
            future.complete(immediate);
            return future;
        }
        final Waiter waiter = new Waiter(fromOffset, maxRecords, future);
        synchronized (wakeupLock) {
            waiters.add(waiter);
        }
        timeoutScheduler.schedule(
                new Runnable() {
                    @Override
                    public void run() {
                        completeWaiter(waiter);
                    }
                },
                maxWaitMs,
                TimeUnit.MILLISECONDS);
    } catch (IOException e) {
        future.completeExceptionally(e);
    }
    return future;
}

public void onApplied(long metadataOffset) {
    // APPLY THREAD: enqueue only.
    streamExecutor.execute(
            new Runnable() {
                @Override
                public void run() {
                    drainWaiters();
                }
            });
}

private void completeWaiter(final Waiter waiter) {
    if (!waiter.claimed.compareAndSet(false, true)) {
        return; // timeout vs onApplied: only one winner
    }
    streamExecutor.execute(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        waiter.future.complete(doFetch(waiter.fromOffset, waiter.maxRecords));
                    } catch (IOException e) {
                        waiter.future.completeExceptionally(e);
                    }
                }
            });
}
```

`Waiter.claimed`（`AtomicBoolean`）保证超时与 wakeup **只完成一次**；真正的 `FetchResult` 只在 `streamExecutor` 上 `complete`，避免 apply 线程跑 `FlussRequestHandler.whenComplete`。`doFetch` 复用现有 `fetch` 里从 `raftLog.get(index)` 扫描的循环，**去掉** `awaitNewData`。

`MetadataStreamGateway.fetch` 今日是同步接口（测试 / in-process subscriber）。可保留同步包装：`fetchAsync(...).get()`，但 **RPC 路径禁止再 `.get()`**。

Worker 数无需为了本优化去改默认 8；异步之后 long-poll 不再占用 worker。`netty.server.num-network-threads`（默认 3）本来就不是瓶颈。

#### 8.3.3 风险 / 工作量 / JMH

| | |
|--|--|
| **工作量** | **M**（waiter 集合、超时、leadership-lost 取消、与现有 `notifyLeadershipLost` 对齐）。不是 L：不改协议。 |
| **JMH** | **现有 catalog 没有 `fetchMetadata` 行。** 不要用 `metadataWrite` 验收本项。可能的间接影响：`MetadataPathBenchmark.getTableInfo`（842µs）若含 worker 与 1 个 tablet long-poll 的争用，异步后或可掉一点，但 §3.4 主因是 `scanPrefixEntries`，**本项不是 getTableInfo 的主修复**。验收以 IT 为主：`MetadataStreamRpcITCase`、`MetadataStreamServiceITCase`、`MetadataStreamFeedITCase`（long-poll 被新 append 唤醒、超时返回空、leadership lost 立刻返回）。 |
| **生产风险** | 中。竞态：超时与 `onApplied` 双 complete；apply 误 complete RPC；waiter 泄漏；`notifyLeadershipLost` 必须 `complete` 所有 waiter 否则 tablet 会卡满 `maxWaitMs`。错误地在 apply 上读 log / 写响应 → 延迟抖动或 §5.6 死锁。 |

---

### 8.4 `list*` / prefix scan：`scanPrefixEntries` 在全量 `entries()` 上过滤

#### 8.4.1 当前：values 前缀扫描是真 iterator；entries 前缀扫描是全表扫

`MetadataKvStore.scanPrefix`（values only）在 RocksDB 上已经是 `seek(prefix)` + `startsWith` 停止：

```171:186:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RocksDBMetadataKvStore.java
    public List<byte[]> scanPrefix(byte[] prefix) {
        ensureOpen();
        List<byte[]> result = new ArrayList<>();
        try (RocksIterator it = db.newIterator(defaultColumnFamilyHandle)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                result.add(it.value());
                it.next();
            }
        }
        return result;
    }
```

`InMemoryMetadataKvStore.scanPrefix` 用 `ConcurrentSkipListMap.subMap`。

但几乎所有 `list*` 需要 **key+value**（从 key 里解析 db 名 / table 名 / schema id / tablet id）。**已落地：** `scanPrefixEntries` 委托 KvStore 前缀迭代，不再全量 `entries()`：

```100:102:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataStore.java
    public List<byte[][]> scanPrefixEntries(byte[] prefix) {
        return kvStore.scanPrefixEntries(prefix);
    }
```

`kvStore.entries()` 仍只给 snapshot 序列化用。JMH 里「Raft prefix scan」从 ~328–335µs 降到 ~1µs（§8.6）。

调用方（均在 `RaftMetadataClient`）包括：

| 方法 | 前缀 | JMH 行（Raft µs） |
|------|------|-------------------|
| `listDatabases` | `prefixOf(REGISTER_DATABASE)` | store `listDatabases` **328** |
| `listTables` | `REGISTER_TABLE` + `database.` | store `listTables` **329** |
| `getCurrentSchemaId` / `schemaIds` | `REGISTER_SCHEMA` + tablePath | `getCurrentSchemaId` **325**；Admin `getTableInfo` **842**（叠加） |
| `getSortedTabletServerList` | `REGISTER_TABLET_SERVER` | **326** |
| `listAssignedTableIds` / `listAssignedPartitionIds` | assignment types | **328** |
| `getPartitions` 等 | `REGISTER_PARTITION` | `getPartitions` **331** |
| `listResourcesByType` | ACL | **335** |
| `listDatabaseSummaries` | 先扫全部 `REGISTER_TABLE` | **330** |

`RaftMetadataStore.scanPrefix(prefix)`（只返回 value）几乎没人用于这些 list API。

#### 8.4.2 最小补丁：把「带 key 的前缀迭代」下沉到 `MetadataKvStore`

接口增加（Java 8）：

```java
/** Like {@link #scanPrefix} but returns {@code [key, value]} pairs, seek+stop. */
List<byte[][]> scanPrefixEntries(byte[] prefix);
```

`RocksDBMetadataKvStore`：复制 `scanPrefix` 循环，`result.add(new byte[][] {key, it.value()})`。`InMemoryMetadataKvStore`：与 `scanPrefix` 同一 `subMap`，产出 key+value clone。

`RaftMetadataStore.scanPrefixEntries` 改为 **一行委托** `return kvStore.scanPrefixEntries(prefix);`，删掉 `entries()` + `startsWith` 过滤。保留 `snapshotEntries()` → `kvStore.entries()`（快照仍要全量）。

**不要**改 key 编码：`MetadataKeys.prefixOf(Type)` 已经是 3 字节 header，RocksDB / SkipList 按 unsigned byte 排序，前缀是连续区间。

#### 8.4.3 风险 / 工作量 / JMH

| | |
|--|--|
| **工作量** | **S**。接口 + 两处 impl + `RaftMetadataStore` 委托；单测已有 `RocksDBMetadataKvStoreITCase` 的 `scanPrefix`。给 `scanPrefixEntries` 加「插入无关 key 后前缀结果不变」即可。 |
| **JMH** | 所有 store「Raft prefix scan」行：328µs → 应接近点读量级（空/小 catalog 下数十 µs 或更低；随 **该前缀的 key 数** 增长，而不再随 **整个 catalog**）。Admin `getTableInfo`（842µs）：`getCurrentSchemaId` 从 325µs 降下来之后，Raft 应更接近 ZK 的 356µs；剩余差才是 RPC。Admin `listDatabases` / `listTables` 已经 Raft 更快，本修复主要救 **store 层** 和 **大 catalog 下的 getTableInfo**。`@Fork(0)` 污染（§4.4）在全表扫时被放大；iterator 后共享 JVM 残留其它表 **不应**再拖慢 `getCurrentSchemaId`。 |
| **生产风险** | **低**。只读路径；写路径 / Raft log / apply 不动。注意 iterator 与 apply 并发：RocksDB 默认 iterator 是 snapshot isolation，可接受。不要在 apply 线程做大扫描（list 已在 RPC 线程）。 |

```bash
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.listDatabases
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.listTables
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.getCurrentSchemaId
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.getSortedTabletServerList
./mvnw -pl fluss-jmh test -Dtest=MetadataPathBenchmark.getTableInfo
```

---

### 8.5 汇总

| # | 优化 | 工作量 | 现有 JMH 上预期 | 生产风险 | 不要做的事 |
|---|------|--------|-----------------|----------|------------|
| 8.1 属性 | `async-flush=true` | **S** | `metadataWrite` **基本不变** | 低–中 | **`unsafe-flush`**；幻想调 `force.sync.num` 修好串行 `.get()` |
| 8.1+8.2 writer | `appendAll` pipeline | **M** | `registerPartition` 接近腰斩；`createTable` 掉数 ms；`metadataWrite` 仍单条 | 中（部分失败窗口） | apply 上 `.get()`；未 apply 的 id 外泄 |
| 8.2 MultiOp 类型 | 新 `Type` + codec | **L** | 与 pipeline 同类，额外原子性 | 高（混版本 / stream） | 当 perf 第一刀 |
| 8.3 FetchMetadata async | future + scheduler | **M** | **无直接 JMH 行**；`getTableInfo` 最多间接 | 中（双 complete / apply 误 complete） | 在 apply 线程 `complete` RPC 或扫 log |
| 8.4 prefix iterator | `KvStore.scanPrefixEntries` | **S** | 所有 ~330µs prefix 行；`getTableInfo` 主修复 | **低** | 继续用 `entries()` 做 list |

**第一刀已落地（§8.6）：8.4 prefix iterator。** 正确性面最小；store list* 从 ~330µs 降到 ~1µs；warm `getTableInfo` Raft 快于 ZK。第二刀仍是 8.3，避免 tablet 把 8 个 worker long-poll 占满。写路径先 pipeline（8.2）再考虑 `async-flush`（8.1）；**永远不要**为分数打开 `unsafe-flush`。

---

### 8.6 First-cut 落地后 JMH（2026-08-16 晚，同机同 flags）

实现：`MetadataKvStore.scanPrefixEntries`（RocksDB `seek` + stop；InMemory `subMap`）；`RaftMetadataStore.scanPrefixEntries` 一行委托。`snapshotEntries()` 仍走全量 `entries()`。

**Flags：** warmup 2×1s，measurement 3×1s，`@Fork(0)`，1 thread，AverageTime µs/op。JAVA_HOME = TencentKona 11.0.23。只复跑 prefix / `getTableInfo` 行（外加一次 warm `createTable` 给 Admin 读垫热）。

**ZK 行没有被这次补丁改到**；复跑与 catalog 的差是 JMH 噪声（n=3）。

#### Store-level prefix（`MetadataStoreBenchmark`）

| 操作 | ZK catalog | ZK 复跑 | Raft before | Raft after | Raft after/ZK 复跑 |
|------|----------:|--------:|------------:|-----------:|-------------------:|
| `listDatabases` | 101 | 107 | 328 | **0.89** | 0.008× |
| `listDatabaseSummaries` | 131 | 134 | 330 | **2.28** | 0.017× |
| `getCurrentSchemaId` | 98 | 106 | 325 | **1.17** | 0.011× |
| `getLatestSchemas` | 199 | 222 | 331 | **3.24** | 0.015× |
| `listTables` | 101 | 107 | 329 | **1.39** | 0.013× |
| `getPartitions` | 99 | 112 | 331 | **1.57** | 0.014× |
| `getPartitionNumber` | 99 | 106 | 331 | **1.23** | 0.012× |
| `getPartitionRegistrations` | 198 | 219 | 327 | **1.59** | 0.007× |
| `getPartitionIdAndPaths` | 204 | 220 | 331 | **1.59** | 0.007× |
| `getPartitionNameAndIdsForTables` | 199 | 219 | 327 | **1.54** | 0.007× |
| `listAssignedTableIds` | 98 | 106 | 328 | **1.13** | 0.011× |
| `listAssignedPartitionIds` | 118 | 109 | 328 | **1.18** | 0.011× |
| `getSortedTabletServerList` | 98 | 105 | 326 | **1.18** | 0.011× |
| `listResourcesByType` | 139 | 107 | 335 | **0.91** | 0.008× |

#### Admin（`MetadataPathBenchmark`）

| 操作 | ZK catalog | ZK 复跑 | Raft before | Raft after | 备注 |
|------|----------:|--------:|------------:|-----------:|------|
| `listDatabases` | 245 | 181 | 133 | **70** | 本来 Raft 就快；scan 不是瓶颈 |
| `listTables` | 283 | 287 | 113 | **70** | 同上 |
| `getTableInfo`（warm，先跑 `createTable`） | 356 | 443 | 842 | **89** | 与 catalog 同形状：共享集群已有表 |
| `getTableInfo`（孤立冷跑，JVM 内第一条 Admin 读） | — | 429 | — | **104475** | 夹具 `max-wait=100ms`；**不是** prefix 回退 |
| `createTable`（对照，写路径未改） | 46581 | 46015 | 24713 | 27183 | 噪声内；本优化不应动写 |

**怎么读：** store list* 的 330µs 带消失了，Raft 回到 ~1µs 点读带。Admin `getTableInfo` 在 **warm** 下从 842µs 降到 89µs，并快于 ZK。不要把冷跑 104ms 写进「优化无效」。

---

## 附录：关键代码索引

| 主题 | 位置 |
|------|------|
| 实测表 | `zookeeper-removal-design.md` §8.4 |
| JMH | `fluss-jmh/src/test/java/org/apache/fluss/jmh/MetadataStoreBenchmark.java` 等 |
| Raft 写 | `RaftMetadataClient.append`、`RaftJournalWriter.append`、`FlussMetadataStateMachine.applyTransaction` |
| Ratis 属性 | `RaftMetadataServer.buildRaftProperties`（未设 force.sync / async-flush / unsafe-flush） |
| Ratis flush 默认 | Apache Ratis 2.4.1 `Log.FORCE_SYNC_NUM_DEFAULT=128`；`ASYNC_FLUSH_ENABLED_DEFAULT=false`；`UNSAFE_FLUSH_ENABLED_DEFAULT=false`；`SegmentedRaftLogWorker.shouldFlush` |
| ZK 写 | `ZooKeeperClient.registerTable` |
| ID | `RaftSequenceIDCounter` vs `ZkSequenceIDCounter` |
| 启动 wait | `FlussClusterExtension.waitUntilCoordinatorServerElected` |
| `getTableInfo` | `FlussAdmin.getTableInfo`、`MetadataManager.getTable`、`scanPrefixEntries` |
| stream max-wait | `ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT`（默认 500ms；IT/JMH 集群 100ms） |
| FetchMetadata 阻塞点 | `MetadataStreamRpcServer.fetchMetadata` → `MetadataStreamService.awaitNewData` on `fluss-netty-server-worker`（`netty.server.num-worker-threads` 默认 8） |
| Prefix iterator（已落地） | `MetadataKvStore.scanPrefixEntries`；`RaftMetadataStore` 委托；JMH before/after 见 **§8.6** |
| 实现指南 | 本文 **§8**（8.4 已落地，其余仍是 sketch） |
