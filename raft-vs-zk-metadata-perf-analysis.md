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
| **性质** | 分析报告。§8 是代码级实现指南；**8.4 / 8.3 / 8.2 已落地**；8.1 仍是 sketch。**§8.9 剩余优化空间的 #1–#6 已落地（§8.10）。§8.11–§8.12 的归因给出最终结论：单写 96.6% 是 fsync，其中一半来自 Ratis 内部的 commit-index metadata entry（Fluss 侧无法消除），gRPC 仅 0.6µs、apply 仅 3.7µs —— 单写路径已到底，剩余方向只有批量化与并发摊薄。优先级见 §9.0。** |
| **对照设计文档** | [`zookeeper-removal-design.md`](zookeeper-removal-design.md) **§8.4**（v3.17，2026-08-17 appendAll pipeline JMH） |
| **分支** | `feature/zookeeper-removal-raft` |
| **测量日期** | 2026-08-16 catalog；**2026-08-17** 复跑 `registerPartition` / Admin `createTable` / `metadataWrite` 对照 |
| **结论一句话** | 在 1-voter loopback 上，Raft **单条 durable 写**大约是 ZK TestingServer 的 **1.7–1.8×**（`registerTable` 4815µs vs 8582µs；2026-08-17 复跑 4733 / **8477**）。多 key DDL 在 8.2 `appendAll` 后不再串行 `.get()`：`registerPartition` Raft **8768µs**（原 16993）；Admin `createTable` **9309µs**（原 24713）。这不是「Raft 实现写坏了」，而是 **每条写都走完整 commit-apply 流水线 + 空队列立即 fsync + `.get()` 等到本地 apply**；读路径和 block 分配 ID 反而更快。**不能把这组数字外推到 3 节点跨 AZ 生产集群。** |

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

**读当前数字时：** `createTable` Raft 更快（**9309µs** vs ZK **40293µs**）**不能**用来否定 `registerTable` Raft 更慢（8.5ms vs 4.7ms）。前者被 tablet DDL 主导，后者才是共识写。

---

## 2. 哪些操作 Raft 更慢

判定：「明显更慢」= Raft/ZK ≳ **1.3×**，或绝对差 ≳ **1ms**（写）/ ≳ **200µs**（读）。单 key 写带是 §8.4 2026-08-16 catalog；`registerPartition` / Admin `createTable` 是 **2026-08-17** `appendAll` 复跑（§8.8）。

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
| `registerPartition` | **14888** | **8768** | **0.59×** | 8.2 `appendAll`：两 key 一次 apply wait（落地前 13973 / 16993 / 1.22×） |
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

`registerPartition` 在 8.2 落地前倍数只有 1.22×，是因为 ZK 这条本身就贵（父节点 create + 2-op transaction，~14ms），Raft 两次 8.5ms append 叠成 ~17ms。**2026-08-17 `appendAll` 后 Raft 8768µs**，落到单条 append 带，并快于本次 ZK 复跑 14888µs。语义上仍不是 ZK 那样的 multi-op 原子事务：两条独立 log entry，失败时 Ratis 可能已经 commit 了前缀。

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
| **`getTableInfo`** | **421**（warm） | **89**（warm） | **0.21×** | prefix iterator 后 Raft 更快。§8.3 异步停车 **没有**再缩短 warm 路径（仍 89µs）。孤立第一条 Admin 读仍叠夹具 `max-wait=100ms`（本次 **104749µs**） |

Admin 写里，只有「几乎等于一次 metadata store 写」的 database DDL 跟着共识变慢。`createTable` **9309µs** vs **40293µs**（8.2 `appendAll` 后）已经不是共识在主导。

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
| `createTable` | 40293 | **9309** | 全量 DDL。8.2 把 schema+assignment+table 收成一次 apply wait（catalog Raft 24713）。ZK 40293 vs catalog 46581 是本次复跑，不是 ZK 代码改动 |
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
| `getTableInfo` | 421 | 89 | warm；孤立冷跑仍 ~105ms，见 §2.4 / §8.7 |

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

**2026-08-16 晚更新：** prefix iterator 落地后，warm `getTableInfo` Raft **89µs** vs ZK **443µs**（当时复跑；原 catalog ZK 356µs）。store `getCurrentSchemaId` 从 325µs 降到 1.2µs，§3.4 里那笔 ~0.5ms 差消失了。

**2026-08-16 FetchMetadata 异步停车后再测：** warm 仍是 ZK **421µs** / Raft **89µs**（§8.4 现用这两列）。孤立把 `getTableInfo` 当 JVM 里第一条 Admin 读仍叠夹具 `max-wait=100ms`（**104749µs**，上次 104475µs）。异步停车 **不缩短** 这次 wait，只是不再占 Netty worker。

**诚实结论：** warm 路径上 `getTableInfo` 不是「Raft 更慢的读」。它和 `registerTable` 慢 3.8ms **从来不是同一个根因**。8.3 也不把它从 ~105ms 冷跑里救出来。

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

`createTable` **40293µs** vs **9309µs**：8.2 把 schema+assignment+table 收成一次 apply wait（落地前 Raft 24713µs）；其余是 tablet 建副本、ISR、RPC。ZK 模式 tablet 靠 ZK watch / 多次 znode；Raft 模式 metadata stream + 更少的父节点 create。`listPartitionInfos` 18.5ms vs 0.5ms 更极端：ZK 树走访 vs 本地结构。

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
3. **Raft 赢在点读、block ID、以及被 tablet DDL 主导的 `createTable`/`dropTable`。** 当前 Admin `createTable` Raft **9309µs** vs ZK **40293µs**。用建表数字说「Raft 更快」或用 store `registerTable` 说「Raft 不能用」，都是错的层。  
4. **`getTableInfo` 曾慢是读路径实现（全表 `entries()`），不是 fsync。** prefix iterator 落地后 warm Raft **89µs** &lt; ZK **421µs**。§8.3 异步停车后再测：warm **仍 89µs**（没有变快）；孤立冷跑 **仍 ~105ms**（`metadata.stream.max-wait=100ms`）。停车只是不占 Netty worker，不缩短 wait。  
5. **1-node JMH 不能验收 §8.4 的 3-voter NVMe 目标（写 p50 &lt;5ms）。** 1-voter 已经 8.6ms；3-voter 同 AZ 若 RTT+fsync 做得好，有可能回到 5ms 附近，**需要另测**；跨 AZ 由物理 RTT 决定。  
6. **8.2 / 8.3 / 8.4 已落地**（JMH 见 §8.6–§8.8）；写路径还剩 **8.1 `async-flush`**。明确禁止 `unsafe-flush`。任何一项都有正确性或运维代价。

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

> 在 2026-08-16 catalog、**2026-08-17** `appendAll` 复跑的 1-voter loopback JMH 上，Raft 单条 metadata 写约为 ZK TestingServer 的 1.7×（8.5ms vs 4.7ms）；点读和默认块分配 ID 明显更快；完整 `createTable` Raft **9309µs** vs ZK **40293µs**，`registerPartition` Raft **8768µs** vs ZK **14888µs**。这不能证明生产 3-voter 已经达标或未达标。

---

## 8. 代码级实现指南

本节把 §4 / §7.1 的推论落到 **具体类、方法和可编译的 Java 8 patch sketch**。**8.4、8.3、8.2 已落地**；8.1 仍是 sketch；**8.9 汇总 8.1–8.4 之外的剩余优化空间（全部未落地），执行顺序见 §9**。验证走现有 JMH（`MetadataStoreBenchmark` / `MetadataPathBenchmark`，`@Threads(1)` AverageTime µs/op），并标明哪一行会动、哪一行根本动不了。

约束与现有代码一致：源码 Java 8（禁止 `var` / `List.of` / `Stream.toList`）；shaded Ratis 包名 `org.apache.fluss.shaded.ratis.org.apache.ratis.*`；apply 线程规则见设计文档 **§5.6**。

落地顺序：**8.4 prefix iterator（S，已落地）→ 8.3 FetchMetadata 异步停车（M，已落地）→ 8.2 writer pipeline（M，已落地）→ 8.1 仅 `async-flush` 属性（S）**。不要先开 `unsafe-flush`。不要先加 `MULTI_OP` 记录类型（见 8.2.3）。

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
| **JMH** | `MetadataStoreBenchmark.metadataWrite` / `registerTable` / `updateTable` / `registerDatabase`：**不要预期** `@Threads(1)` 有显著下降。`MetadataPathBenchmark.createTable`：属性单独改 **几乎不动**（现 **9309µs**，8.2 已把 3 次 store append 收成一次 wait；`async-flush` 不改变 `@Threads(1)` `.get()`）。 |
| **生产风险** | `async-flush`：低–中（Ratis 支持的安全重叠；需 3-voter 看 commit 延迟与 flush 线程异常路径）。`unsafe-flush`：**不可接受**（静默丢元数据）。 |

现有 JMH 命令（不改 JMH 源码）：

```bash
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.metadataWrite
./mvnw -pl fluss-jmh test -Dtest=MetadataPathBenchmark.createTable
```

若只改了 `async-flush` 而 `metadataWrite` 几乎不变，这是 **预期**，不是实现失败。

---

### 8.2 一次 DDL 内 pipeline / 批量多条 metadata record

**已落地（2026-08-17）。** `RaftJournalWriter.appendAll` 对 N 条独立 log entry FIFO `append` 后 `allOf` 一次 apply wait。`RaftMetadataClient`：`registerPartitionAssignmentAndMetadata`（两 key）和 `registerTableSchemaAssignmentAndMetadata`（schema + 可选 assignment + table）。`MetadataManager.createTable` 先 `getTableIdAndIncrement()`（`ALLOCATE_ID_RANGE` 必须先 apply），再调 batched register。ZooKeeper 走接口 default：仍逐步 `registerFirstSchema` / `registerTableAssignment` / `registerTable`，没有 `appendAll`。无新 `Type.MULTI_OP`。失败时异常写明「prefix may already be committed」。JMH 见 **§8.8**。

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

`registerPartition` 同构：JMH 注明「assignment + metadata txn (2 keys)」。落地前 Raft **16993µs** ≈ 两次 8.5ms append；**落地后见 §8.8（8768µs）**。

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
| **JMH** | 落地后实测见 **§8.8**：`registerPartition` Raft **8768µs**（原 16993）；Admin `createTable` **9309µs**（原 24713，比「掉几个 ms」预期更深）；`metadataWrite` **8477µs**（单条，噪声内 vs catalog 8582）。`registerFirstSchema`+`registerTable` 各自单行 **不变**（仍是一条 append）。 |
| **生产风险** | 中。部分失败语义与今天 ZK 相同（schema 可能留下）；pipeline 后失败窗口更短但诊断要按「一批」来。Ratis 客户端对同 group 的有序 `async().send` 必须保持 FIFO（Ratis 默认 ordered stream）。IT：`RaftMetadataClientITCase` 建表后 `getTable` / `getSchemaById` / assignment 三件套仍可见。 |

```bash
./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.registerPartition
./mvnw -pl fluss-jmh test -Dtest=MetadataPathBenchmark.createTable
```

---

### 8.3 `FetchMetadata` 不得阻塞 Netty worker

**已落地（2026-08-16）。** `MetadataStreamRpcServer.fetchMetadata` 立刻返回 `fetchAsync` 的 future；caught-up long-poll 用 `ScheduledExecutorService` 超时 + waiter CAS；`onApplied` 只 `streamExecutor.execute(drainWaiters)`。`fetch()` 仍是 in-process `.get()` 包装。单测 `MetadataStreamServiceTest`；stream ITs 仍过。**已复跑** `getTableInfo`：warm 仍 89µs；孤立冷跑仍 ~105ms（stream 追上 / `max-wait`，不是 worker `wait()`）。本项不是 `getTableInfo` 的延迟修复。

#### 8.3.1 落地前调用链：long-poll 发生在 worker 上，不是 selector 上

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
| **JMH** | **现有 catalog 没有 `fetchMetadata` 行。** 不要用 `metadataWrite` 验收本项。已复跑 `MetadataPathBenchmark.getTableInfo`：warm **89µs**（与 prefix 后相同）；孤立冷跑 **104749µs**（wait 未缩短）。§3.4 主因曾是 `scanPrefixEntries`，**本项不是 getTableInfo 的延迟修复**。验收以 IT 为主：`MetadataStreamRpcITCase`、`MetadataStreamServiceITCase`、`MetadataStreamFeedITCase`（long-poll 被新 append 唤醒、超时返回空、leadership lost 立刻返回）。 |
| **生产风险** | 中。竞态：超时与 `onApplied` 双 complete；apply 误 complete RPC；waiter 泄漏；`notifyLeadershipLost` 必须 `complete` 所有 waiter 否则 tablet 会卡满 `maxWaitMs`。错误地在 apply 上读 log / 写响应 → 延迟抖动或 §5.6 死锁。落地后用 `Waiter.claimed` CAS + `CompletableFuture.complete` 单次完成；apply 只 enqueue；leadership lost 走 `close()`/`notifyLeadershipLost` 清 waiter。 |

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
| 8.1+8.2 writer | `appendAll` pipeline | **M** | `registerPartition` Raft **8768µs**（原 16993）；Admin `createTable` **9309µs**（原 24713）；`metadataWrite` **8477µs**（单条） | 中（部分失败窗口） | apply 上 `.get()`；未 apply 的 id 外泄 |
| 8.2 MultiOp 类型 | 新 `Type` + codec | **L** | 与 pipeline 同类，额外原子性 | 高（混版本 / stream） | 当 perf 第一刀 |
| 8.3 FetchMetadata async | future + scheduler | **M** | **无直接 JMH 行**；`getTableInfo` warm 仍 89µs，孤立仍 ~105ms | 中（双 complete / apply 误 complete） | 在 apply 线程 `complete` RPC 或扫 log |
| 8.4 prefix iterator | `KvStore.scanPrefixEntries` | **S** | 所有 ~330µs prefix 行；`getTableInfo` 主修复 | **低** | 继续用 `entries()` 做 list |
| **8.9 剩余空间** | 归因打点 / `appendAll` 推广+分块 / `disableWAL` / 本地直提交 / apply 线程瘦身 | S–M | 见 **§9.1** 逐项 | 低–中（逐项不同） | 未归因就调 flush 旋钮；无分块推广 `appendAll` |

**§8.9 的第 2/3/5/6 项已落地，实测见 §8.10；其余仍未落地。§9 给出执行顺序、验收命令与判定标准。**

**已落地：8.4 prefix iterator（§8.6）+ 8.3 FetchMetadata async（§8.7）+ 8.2 appendAll pipeline（§8.8）。** 8.4 救 store list* / warm `getTableInfo`；8.3 让 tablet long-poll 不再占满 8 个 Netty worker；8.2 让一次 DDL 内多 key 只等一次 apply（`registerPartition` / `createTable`）。写路径再考虑 `async-flush`（8.1）；**永远不要**为分数打开 `unsafe-flush`。

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

### 8.7 Second-cut：FetchMetadata async park（2026-08-16）

实现：`MetadataStreamService.fetchAsync` + `metadata-stream` 线程池 + `metadata-stream-timeout` scheduler。`Waiter.claimed`（`AtomicBoolean`）保证超时 / `onApplied` / leadership-lost 只完成一次；`doFetch` 与 RPC `complete` 只在 stream pool 上跑。`onApplied` 仅 `execute(drainWaiters)`。`MetadataStreamRpcServer.fetchMetadata` 立刻返回 future（`thenApply(toResponse)`）。同步 `fetch()` 留给 in-process subscriber / 测试。

**JMH（同机同 flags，2026-08-16 复跑）：** catalog 没有 `fetchMetadata` 行。复跑 `MetadataPathBenchmark`（全 class）+ 孤立 `getTableInfo` + 廉价 store 对照（`registerTable` / `getTable` / list*）。

| 操作 | ZK µs | Raft µs | vs 落地前 |
|------|------:|--------:|-----------|
| `getTableInfo` warm（共享集群） | **421** | **89** | Raft 与 prefix 后 89µs **相同**；ZK 原 catalog 356、prefix 复跑 443 |
| `getTableInfo` 孤立（JVM 内第一条 Admin 读） | 434 | **104749** | 仍 ≈ 夹具 `max-wait=100ms`（上次 104475）；**wait 时长未缩短** |
| store `registerTable` | 4734 | 8416 | vs catalog 4815 / 8582，噪声内 |
| store `getTable` | 110 | 1.08 | vs catalog 102 / 1.6，噪声内 |
| store `listDatabases` | 108 | 1.08 | vs §8.6 复跑 107 / 0.89，噪声内 |
| store `listTables` | 106 | 1.45 | vs §8.6 复跑 107 / 1.39，噪声内 |
| store `getCurrentSchemaId` | 106 | 1.18 | vs §8.6 复跑 106 / 1.17，噪声内 |
| store `getPartitions` | 106 | 1.57 | vs §8.6 复跑 112 / 1.57，不变 |
| store `listDatabaseSummaries` | 130 | 2.22 | vs §8.6 复跑 134 / 2.28，噪声内 |
| store `listAssignedTableIds` | 125 | 1.18 | vs §8.6 复跑 106 / 1.13，噪声内 |
| store `getSortedTabletServerList` | 104 | 1.16 | vs §8.6 复跑 105 / 1.18，噪声内 |
| store `listResourcesByType` | 106 | 1.09 | vs §8.6 复跑 107 / 0.91，噪声内 |

诚实结论：异步停车把 long-poll 从 Netty worker 挪到 metadata-stream 线程池，**不缩短** caught-up 订阅的 `maxWaitMs`，因此孤立冷 `getTableInfo` 仍可叠满 100ms。warm 89µs 是 prefix iterator 的结果，不是本刀。store 行不应、也没有被 FetchMetadata 停车打动。全 class Admin `list*` 会吃 `@Fork(0)` 里 `createTable` 残留表，**不要**拿那次 list 分数覆盖 §8.4 / §8.6。

验收：`MetadataStreamServiceTest`（超时空响应、数据先于超时、apply 线程不扫 log、无双 complete）；`MetadataStreamServiceITCase`、`MetadataStreamRpcITCase`、`MetadataStreamCacheApplierITCase`、`MetadataStreamFeedITCase`、`TabletServerMetadataStreamCacheApplierITCase`、`TabletServerRaftMetadataFeedITCase`。

---

### 8.8 Third-cut：appendAll pipeline（2026-08-17）

实现：`RaftJournalWriter.appendAll` 发出 N 条独立 log entry，然后一次 `allOf` 等到全部 local apply。`RaftMetadataClient.registerPartitionAssignmentAndMetadata` 两 key；`registerTableSchemaAssignmentAndMetadata` 在 id 已分配之后 pipeline schema + 可选 assignment + table。`MetadataManager.createTable` 先 `getTableIdAndIncrement()`。ZooKeeper 用接口 default，逐步写，无 `appendAll`。无新 `MetadataRecord.Type`。

**Flags：** warmup 2×1s，measurement 3×1s，`@Fork(0)`，1 thread，AverageTime µs/op。JAVA_HOME = TencentKona 11.0.23。只复跑会动的行 + `metadataWrite` 对照。

| 操作 | ZK catalog | ZK 复跑 | Raft before | Raft after | 说明 |
|------|----------:|--------:|------------:|-----------:|------|
| store `registerPartition` | 13973 | **14888** | 16993 | **8768** | 两 key → 一次 apply wait，落到单 append 带 |
| Admin `createTable` | 46581 | **40293** | 24713 | **9309** | schema+assignment+table 一次 wait；ZK 降幅是复跑噪声，不是 ZK 代码 |
| store `metadataWrite`（`registerTable`） | 4815 | **4733** | 8582 | **8477** | **单条 append，不应动**；噪声内 |

诚实结论：`registerPartition` 接近腰斩并快于 ZK transaction。Admin `createTable` 掉了约 15ms（三次串行 ~8.5ms store 写收成一次），仍含 tablet ISR/RPC，但本机这次 store 占比比 catalog 更大。**`metadataWrite` 没有变快。** `createPartition` Admin / `deletePartition` store 未复跑，不要把 8768 填进那些行。

验收：`RaftJournalWriterTest`、`RaftMetadataClientAppendAllTest`、`MetadataManagerCreateTableTest`、`RaftMetadataClientITCase`（含 batched create/partition + 既有 surface）。

---

### 8.9 剩余优化空间（8.1–8.4 之外；**全部未落地**）

**性质：** 本节是 §2 里「ZK 仍然更快」那些行的**剩余**杠杆清单，全部是 sketch，**没有任何一项被实现或测量**。凡涉及 Ratis 行为的断言都用 `javap` 对 `fluss-shaded-ratis-1.0-SNAPSHOT.jar`（Ratis 2.4.1）实测过常量与方法签名，见 8.9.1。

**核心判断：** §2.1 那条 **8.2–9.3ms** 的单 append 带是**唯一**根瓶颈。§2.2 的多写行、§2.4 的 database DDL、§2.5 的 1:1 ID、§3.3 的启动里那一次 leader 注册，全都是「N × 单条 append」。因此优化只有两类：**减少 append 条数**（8.9.2）或**压低单条 append 成本**（8.9.3），外加 apply 线程上的搭车开销（8.9.4）。

#### 8.9.1 归因缺口：3.8ms 差值目前没有分解数据

§4 列的五层根因是**推理**，不是测量。关键疑点：ZK `TestingServer` 同样每条 txn fsync（§4.2 已写明），所以 fsync 成本在两边大致相当，**Raft 多出的 ~3.8ms 里 fsync 占比未知**。剩余候选全部未被单独测量：

| 候选 | 量级假设 | 现有数据能否区分 |
|------|----------|------------------|
| Ratis gRPC loopback 编解码（`RaftClient.async().send`） | ? | **否** |
| log worker fsync（`SegmentedRaftLogWorker`） | ? | **否**（ZK 也 fsync，不是净增） |
| commit → apply 线程切换 | ? | **否** |
| `store.apply` RocksDB put + WAL | ? | **否** |
| `awaitApplied` waiter complete + 调用线程 unpark | ? | **否** |

**结论：在打点之前做 8.1 `async-flush` 是盲改。** §8.1.6 自己也写明「不要预期 `metadataWrite` 有显著下降」。建议先加分段计时（Ratis 自带 `RaftServerMetrics` / `raft.server.log.flushTime`，设计 §5.12 已规划），把 `send → reply → applied` 三段分开，再决定投哪一档。**这一项没有 JMH 分数收益，但后续所有取舍都依赖它。**

#### 8.9.2 减少 append 条数：`appendAll` 尚未推广到最差的几行

§8.2 只接了 `createTable` 和 `registerPartition`。**§2.2 里倍数最差的行根本没接**，仍是逐条 `append().get()`：

| JMH 行 | Raft/ZK | 现状 | 串行 append 条数 |
|--------|--------:|------|------------------|
| `deleteTableAssignment` | **2.64×**（全表最差） | 每 bucket 一条 `DELETE_LEADER_AND_ISR` + 1 条 assignment | 1 + #buckets |
| `deletePartitionAssignment` | 1.68× | 同构 | 1 + #buckets |
| `deleteTable` | 1.74× | 每 partition + 每 schema 各一条 | 1 + #partitions + #schemas |

```1496:1510:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public void deleteTableAssignment(long tableId) throws Exception {
        requireLeader();
        // ZK deleteTableAssignment uses deletingChildrenIfNeeded — cascade LeaderAndIsr for
        // non-partitioned table buckets so completeDeleteTable clears bucket state too.
        Optional<TableAssignment> assignment = getTableAssignment(tableId);
        if (assignment.isPresent()) {
            for (Integer bucketId : assignment.get().getBuckets()) {
                append(
                        MetadataRecord.Type.DELETE_LEADER_AND_ISR,
                        tableBucketBytes(new TableBucket(tableId, bucketId)),
                        EMPTY);
            }
        }
        append(MetadataRecord.Type.DELETE_TABLE_ASSIGNMENT, longToBytes(tableId), EMPTY);
    }
```

##### 比 JMH 更重要：ISR 批量路径也在逐条串行（**当前 catalog 完全没测**）

```1249:1256:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataClient.java
    public void batchUpdateLeaderAndIsr(
            Map<TableBucket, LeaderAndIsr> leaderAndIsrList, int expectedZkVersion)
            throws Exception {
        requireLeader();
        for (Map.Entry<TableBucket, LeaderAndIsr> entry : leaderAndIsrList.entrySet()) {
            updateLeaderAndIsr(entry.getKey(), entry.getValue(), expectedZkVersion);
        }
    }
```

`batchRegisterLeaderAndIsrForTablePartition` 同构。对照 ZK：

```533:548:fluss-server/src/main/java/org/apache/fluss/server/zk/ZooKeeperClient.java
    public void batchRegisterLeaderAndIsrForTablePartition(
            List<RegisterTableBucketLeadAndIsrInfo> registerList, int expectedZkVersion)
            throws Exception {
        if (registerList.isEmpty()) {
            return;
        }

        List<CuratorOp> ops = new ArrayList<>(registerList.size());
        // In transaction API, it is not allowed to use "creatingParentsIfNeeded()"
        // So we have to create parent dictionary in advance.
```

ZK 是**一次 Curator transaction**（N 个 op 一次提交）；Raft 是 **N × 8.5ms 串行**。一个 128-bucket 的表 ≈ **1.1 秒**。调用方全在故障转移 / ISR 收缩路径上：

| 调用方 | 位置 |
|--------|------|
| `TableBucketStateMachine.batchUpdateLeaderAndIsrWithFallback` | `TableBucketStateMachine.java:245` / `:275` |
| `TableBucketStateMachine`（partition 注册） | `TableBucketStateMachine.java:552` |
| `ReplicaStateMachine` | `ReplicaStateMachine.java:491` |
| `CoordinatorEventProcessor` | `CoordinatorEventProcessor.java:2035` |

**这是目前 Raft 模式最真实的性能风险，而 §8.4 catalog 里没有任何一行覆盖它**（`MetadataStoreBenchmark` 无 `batchUpdateLeaderAndIsr` / `batchRegisterLeaderAndIsrForTablePartition` 基准）。补这个基准的价值可能高于任何单条 append 优化。

##### 推广前的硬约束：现有 `appendAll` 无分块，会撞 Ratis 客户端窗口

`javap` 实测默认值（`fluss-shaded-ratis-1.0-SNAPSHOT.jar`）：

| Ratis 常量 | 实测值 |
|-----------|-------:|
| `RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT` | **100** |
| `RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT` | 4096 |
| `RaftServerConfigKeys.Log.QUEUE_ELEMENT_LIMIT_DEFAULT` | 4096 |

现有 `appendAll` 一次性把**全部** record `send` 出去，无上限：

```93:101:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftJournalWriter.java
    public CompletableFuture<Long> appendAll(List<MetadataRecord> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("records must not be empty");
        }
        List<CompletableFuture<Long>> futures =
                new ArrayList<CompletableFuture<Long>>(records.size());
        for (int i = 0; i < records.size(); i++) {
            futures.add(append(records.get(i)));
        }
```

今天最大调用方只有 3 条（schema + assignment + table），安全。**一旦用于 per-bucket（>100 buckets），会超出客户端 outstanding=100**，Ratis 抛 `ResourceUnavailableException` 或阻塞发送线程。

**推广必须同时加分块**（建议 chunk 64，逐块 `allOf().get()`），否则是把 JMH 分数换成生产故障。分块也会放大 8.9.4 的 C3 问题（`whenComplete` 跑在 apply 线程），**C3 应先修**。

语义上：分块后失败窗口从「一批」变成「若干批」，但 ZK 侧 `deletingChildrenIfNeeded` 本来也不是原子的，**不引入新的语义退化**。仍然不需要 `MetadataRecord.Type.MULTI_OP`（§8.2.3 的理由不变）。

#### 8.9.3 压低单条 append 成本（§2.2 里 create+delete 那 12 行的唯一杠杆）

`deleteDatabase` / `deleteRebalanceTask` / `deleteProducerOffsets` 这类行是**两个独立 API 调用**（JMH 的 delete 含前置 create），`appendAll` 救不了，只能降低单条绝对成本。

##### B1. RocksDB WAL 对恢复无贡献 —— 纯开销（建议优先）

```107:109:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RocksDBMetadataKvStore.java
        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        WriteOptions writeOptions = new WriteOptions();
```

默认 `WriteOptions` = **WAL 开、sync=false**。§3.1 已指出「apply 写 WAL 但不 fsync」，但漏了更强的结论：**这个 WAL 对恢复毫无贡献**。

恢复路径两条，都**不读** RocksDB 的持久化内容：

```265:280:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/FlussMetadataStateMachine.java
    private void load(@Nullable SingleFileSnapshotInfo snapshot) throws IOException {
        if (snapshot == null) {
            return; // fresh cluster, nothing to restore
        }
        File file = snapshot.getFile().getPath().toFile();
        if (!file.exists()) {
            throw new FileNotFoundException("Snapshot file not found: " + file);
        }
        List<byte[][]> entries = readEntries(file);
        synchronized (this) {
            store.restoreFromSnapshot(entries);
            TermIndex ti = snapshot.getTermIndex();
            lastAppliedIndex = ti.getIndex();
            setLastAppliedTermIndex(ti);
        }
    }
```

- **有 snapshot：** `restoreFromSnapshot` 第一步是 `kvStore.clear()` —— RocksDB 磁盘内容被**整体丢弃并覆盖**，再从 snapshot index+1 重放 log。
- **无 snapshot：** `lastAppliedIndex = -1`，从 log 起点重放全部；put/delete 幂等，残留被覆盖。

耐久性锚 100% 是 **Raft log fsync + snapshot 文件**（与 §8.1.5 禁止 `unsafe-flush` 的论证一致）。所以 WAL 只是每条 apply 多一次用户态 write + memtable 记账。

**改动：** `writeOptions.setDisableWAL(true)`，不需要新配置项。零正确性代价 —— 比 8.1 `async-flush`（要动 Ratis 耐久性时序）更该先做。省下的量级需实测。

**更激进的同向选项：** metadata store 体量小（snapshot 是单文件全量序列化，`snapshotEntries()` 直接返回 `List<byte[][]>`），KRaft 的对应设计就是**纯内存 image + snapshot**。`InMemoryMetadataKvStore`（`ConcurrentSkipListMap`，含 `scanPrefixEntries`）已是完整实现，今天只用于测试与 follower read view。做成生产可选后端（如 `coordinator.raft.metadata-store=memory|rocksdb`）可彻底消掉 apply 侧 I/O。**前置条件：先量真实 catalog 的堆占用**，否则超大 catalog 有 OOM 风险。

##### B2. 绕过 gRPC loopback：leader 本地直提交

`RaftJournalWriter.append` 走 `raftClient.async().send()` → gRPC，**即使 leader 就是本进程**（§4.1 已列，未给方案）。`javap` 实测 `RaftServer` 直接继承本地提交接口：

```text
RaftServer extends ... RaftClientProtocol, RaftClientAsynchronousProtocol, AdminProtocol ...
RaftClientAsynchronousProtocol:
  CompletableFuture<RaftClientReply> submitClientRequestAsync(RaftClientRequest) throws IOException
```

且 `RaftClientRequest.newBuilder()` 公开可用（`setClientId` / `setServerId` / `setGroupId` / `setCallId` / `setMessage` / `setType`，配 `RaftClientRequest.writeRequestType()`）。`RaftMetadataClient` 已持有 `ownedServer`，可 `getDivision(RAFT_GROUP_ID)` 判断本地是否 leader。

省掉：protobuf 编解码 + gRPC 两端线程切换 + 客户端滑动窗口记账。

**代价：** 自己维护单调 `callId` 与 `ClientId`；`NotLeaderException` 必须能回退到现有 `raftClient`（follower 或 leadership 转移时）；失去 Ratis 客户端的重试策略。

**这是唯一能同时改善所有 append 行、且不牺牲任何耐久性的手段。** 但幅度完全取决于 8.9.1 的归因结果 —— 若 gRPC 只占 3.8ms 里的一小部分，不值得这个复杂度。**必须先归因。**

##### B3. `async-flush`（= §8.1，重申结论）

`javap` 确认 API 与默认值存在：`Log.setAsyncFlushEnabled`，`ASYNC_FLUSH_ENABLED_DEFAULT=false`，`FORCE_SYNC_NUM_DEFAULT=128`，`UNSAFE_FLUSH_ENABLED_DEFAULT=false`；两者互斥（同开抛 `IllegalStateException`）。

§8.1.3 的判断成立：**`@Threads(1)` 串行 `.get()` 下几乎无效**。它只在并发写下有价值，而**现有 catalog 没有 `@Threads(N)` 写用例**，所以今天连验收手段都不存在。优先级应低于 B1，且必须配一个并发写 JMH 才能验收。§8.1.5 禁止 `unsafe-flush` 的理由不变，无需重新讨论。

#### 8.9.4 apply 线程上的搭车开销（都在写延迟关键路径内）

因为 `append` 要 `awaitApplied`（§4.3），**apply 循环里每一微秒都直接计入调用方写延迟**。三处可省：

**C1 — 每条写多一次 KvStore `get`，只为喂 watch 的 `oldValue`：**

```50:52:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/RaftMetadataStore.java
    public void apply(MetadataRecord record) {
        byte[] key = MetadataKeys.keyOf(record);
        byte[] oldValue = kvStore.get(key);
```

全仓只有 3 个 watcher（`RaftTableChangeWatcher` 3 个前缀、`RaftTabletServerChangeWatcher`、`RaftCoordinatorChangeWatcher`）。给 `MetadataWatchService` 加 `hasWatch(key)`，无匹配时跳过 `get` —— 绝大多数 record type 直接省掉一次 RocksDB 读。

**C2 — `fireWatch` 在 apply 线程把 key 转 hex 字符串，再对每个前缀做 `String.startsWith`：**

```61:70:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/MetadataWatchService.java
    public void fireWatch(byte[] key, byte[] oldValue, byte[] newValue) {
        String h = hex(key);
        for (Map.Entry<String, CopyOnWriteArrayList<WatchCallback>> e : watches.entrySet()) {
            if (h.startsWith(e.getKey())) {
                for (WatchCallback cb : e.getValue()) {
                    cb.onChange(key, oldValue, newValue);
                }
            }
        }
    }
```

每条 apply 一次 `StringBuilder` 分配 + 2×keyLen 字符写入 + 全 watch 表遍历。改成字节前缀比较、并按 3 字节 type header 分桶查 `HashMap`。纯 CPU、零风险。

**C3 — waiter 在持锁的 apply 线程上 complete（**这条是正确性问题，不只是性能**）：**

```200:206:fluss-server/src/main/java/org/apache/fluss/server/coordinator/raft/FlussMetadataStateMachine.java
    private synchronized void completeAppliedWaiters(long index) {
        NavigableMap<Long, CompletableFuture<Long>> done = appliedWaiters.headMap(index, true);
        for (CompletableFuture<Long> w : done.values()) {
            w.complete(index);
        }
        done.clear();
    }
```

调用方挂在这些 future 上的任何 `thenCompose` / `whenComplete` 都会在 **apply 线程 + 持有 state machine monitor** 时执行。

- `RaftMetadataClient.append` 只做 `.get()`（park/unpark），尚可。
- **但 `appendAll` 的 `allOf(...).whenComplete` 已经把「统计 applied 计数 + complete result」拽进了 apply 循环**（`RaftJournalWriter:107-129`）。这违反了 §5.6 以及 `setApplyListener` javadoc 自己定的规则 —— 对比 `MetadataStreamService.onApplied` 就老老实实只 `execute(drainWaiters)`。

**改动：** 锁内只收集 waiter 列表，出锁后再 complete；或统一交给一个 completion executor。**必须在 8.9.2 分块推广之前修**，否则 per-bucket 批量会显著放大 apply 循环上的搭车工作。

C1+C2+C3 各自只有几 µs~几十 µs，**动不了 8.5ms 的大头**，但零风险，且 C3 是正确性防线。

#### 8.9.5 启动路径（§3.3，194ms vs 99.3ms）

§3.3 的归因成立（first-election 窗口 + Ratis/RocksDB 初始化 + catch-up + 一次 leader 注册 append）。两个候选，**价值都低**：

1. **单 voter 时缩短 first-election。** `coordinator.raft.first-election.timeout` 默认 300ms（夹具 150ms，min = 一半）。voter set size == 1 时没有竞争者，理论上无需等待窗口。只改善单节点 / bootstrap 与测试夹具，**生产 3-voter 零收益**。
2. **leader 注册 append 合批 —— 实际不可行。** `registerCoordinatorLeader()`（`CoordinatorServer.java:433`）与 `createDefaultDatabase()`（`:492`）之间隔着 `synchronized (lock)` 块、`rpcClient` / `coordinatorChannelManager` / `autoPartitionManager` 构造、authorizer 启动、`coordinatorEventProcessor.startup()`。合成一次 `appendAll` 需要重排启动顺序，**风险远大于省一个 RTT**。不建议。

另：`javap` 确认 `RaftServerConfigKeys.LeaderElection.PRE_VOTE_DEFAULT = true`，`setPreVote` 可用。3-voter 选举时长可据此调，但 §8.4 已把选举验收交给 `RaftLeaderElectionTimingITCase`，**1-node SingleShot 启动 bench 不该用来验收选举**（§3.3 末尾已声明）。

---

### 8.10 Third-cut 落地：ISR 批量 + apply 路径瘦身（2026-08-23，同机同 flags）

落地了 §9.1 的 **#2 / #3 / #4 / #5 / #6**。**#1（归因打点）和 #7（本地直提交）仍未做**，所以本节不声称知道 8.5ms 里各成分占比。

#### 8.10.1 实现

| 项 | 改动 |
|----|------|
| **#2 C3** | `FlussMetadataStateMachine`：`applyEntry` / `notifyTermIndexUpdated` 锁内只 `drainAppliedWaiters` 收集 waiter，出锁后 `completeAll`；apply listener 也移出锁。 |
| **#3 B1** | `RocksDBMetadataKvStore`：`new WriteOptions().setDisableWAL(true)`。 |
| **#6 C1** | `MetadataWatchService.hasWatch(key)` 新增；`RaftMetadataStore.apply` 仅在有 watcher 匹配时才预读 `oldValue`。 |
| **#6 C2** | `MetadataWatchService` 由「hex 字符串 + `String.startsWith` 全表遍历」改为 copy-on-write `Registration[]` + 原始字节前缀比较，热路径零分配。 |
| **#5 A** | `RaftJournalWriter.appendAll` 改为分块 send-then-wait（`sendChunk` 递归，深度 `ceil(n/chunk)`）；`appendAll` 推广到 `deleteTableAssignment` / `deletePartitionAssignment` / `deleteTable` / `batchUpdateLeaderAndIsr` / `batchRegisterLeaderAndIsrForTablePartition`。 |
| **#5 配套** | 新增 `coordinator.raft.append-chunk-size`（默认 64）；`RaftMetadataServer.applyAppendPipelineProperties` 把 Ratis `Async.OUTSTANDING_REQUESTS_MAX` 设为 `max(100, 2 * chunk)`，使 chunk 增大不会静默撞上客户端在途上限。 |
| **#4 基准** | 新增 `MetadataStoreBenchmark.batchUpdateLeaderAndIsr`（100 buckets，跨默认 chunk 边界）。 |

`batchRegisterLeaderAndIsrForTablePartition` **故意不做基准**：ZK 实现用非幂等 `create`（含父节点 create），AverageTime 模式下反复调用会抛 `NodeExists`，测不出东西。

#### 8.10.2 新增基准：100-bucket ISR 批量

**Flags：** warmup 2×1s、measurement 3×1s、`@Fork(0)`、1 thread、AverageTime µs/op，JAVA_HOME = TencentKona 11.0.23（同 §8.6）。

| 配置 | ZK µs | Raft µs | Raft/ZK | chunk 数 |
|------|------:|--------:|--------:|---------:|
| `batchUpdateLeaderAndIsr`，chunk=64（默认） | **4456** | **18473** | **4.14×** | 2（64+36） |
| `batchUpdateLeaderAndIsr`，chunk=128 | 4456 | **9425** | **2.12×** | 1（100） |
| 单条 `updateLeaderAndIsr`（对照） | 4504 | 8315 | 1.85× | 1 |

**归因是算术闭合的，不是猜测：**

- 单条 Raft = 8315µs。chunk=64 时 18473 / 8315 = **2.22** ≈ 2 个 chunk。
- chunk=128 时 9425 / 8315 = **1.13** ≈ 1 个 chunk，且 9425 ≈ 单条 8315 + 少量 pipeline 开销。
- **结论：批量的成本 ≈ chunk 数 × 单条 append 成本。** pipeline 本身几乎不额外收费（100 条挤进一个 chunk 只比 1 条贵 13%）。

#### 8.10.3 落地前 vs 落地后

落地前 `batchUpdateLeaderAndIsr` 是逐条 `updateLeaderAndIsr(...)` 串行 `.get()`，即 100 × 单条：

| | 估算/实测 | 说明 |
|---|---:|---|
| 落地前（100 × 8315µs） | ≈ **832ms** | 未直接测（旧代码已替换），由单条实测线性外推 |
| 落地后 chunk=64 | **18.5ms** | ≈ **45×** 改善 |
| 落地后 chunk=128 | **9.4ms** | ≈ **88×** 改善 |

**这是本轮最重要的结果，而且它在 §8.4 的原 catalog 里完全不可见** —— 128-bucket 表的故障转移原本要 ~1.1 秒，现在是 ~10ms 量级。调用方全在关键路径：`TableBucketStateMachine:245`/`:275`/`:552`、`ReplicaStateMachine:491`、`CoordinatorEventProcessor:2035`。

#### 8.10.4 chunk 默认值为什么留在 64（而不是 128）

实测摆明 chunk=128 在这一行快一倍，但默认值仍是 64：

- **64 是延迟/在途请求的折中，不是最优点。** chunk 越大，单批在途 Raft 请求越多，与并发的单条 append 争抢同一个 `RaftClient` 窗口和 server 端写队列。
- 上限联动已经做了（`max(100, 2*chunk)`），所以**调大 chunk 是安全的配置动作**，不需要改代码：ISR 批量密集的部署可以设 `coordinator.raft.append-chunk-size=128`。
- 但把**默认**改到 128 等于让所有部署都吃更大的在途窗口，而收益只体现在 >64 条的批量上。**这个取舍需要 3-voter 实测（§7.2）后再定，1-voter loopback 不足以支撑改默认值。**

#### 8.10.5 未变的结论

- **单条 append 仍是 8315µs（Raft）vs 4504µs（ZK），1.85×。** 本轮完全没有触碰这个带 —— C1/C2/C3 是 µs 级、B1 省的是一次用户态 write，都远小于噪声。§7.1 的判断不变。
- `metadataWrite` 等单写行**不应该**因本轮改动而变化；若复跑发现变了，按 §9.3 当噪声或改错层处理。
- **仍然没有归因数据**（§8.9.1）。chunk 数 × 单条成本的闭合只说明「批量已经退化成单条问题」，没说明单条 8.5ms 里 fsync / gRPC / 线程切换各占多少。#7（本地直提交）的价值判断依旧悬空。

#### 8.10.6 验证

- 单测：`fluss-server` raft 包 **79 tests 全过**。新增/扩充：`MetadataWatchServiceTest`（6）、`RaftJournalWriterTest`（9，含分块 / 自定义 chunk / 跨块失败前缀计数）、`FlussMetadataStateMachineTest`（12，含 3 个「continuation 不得在锁内跑」的回归）、`RaftMetadataServerTest`（7，含 outstanding 上限联动）。
- IT（真实 Ratis + RocksDB）：`RaftMetadataClientITCase`、`RocksDBMetadataKvStoreITCase`、`MetadataStreamServiceITCase`、`MetadataStreamRpcITCase` 全过。

---

## 8.11 归因实测：8.5ms 里 96% 是 fsync，gRPC 只有 0.6µs（2026-08-23）

落地了 §9.1 的 **#1**（唯一的前置项）。**这一节推翻了 §4 和 §8.9 的若干推测，后续所有优先级都要按它重排。**

### 8.11.1 实现

- `AppendLatencyStats`（新类）：在 `RaftJournalWriter.append` 上打三个时间戳（send / reply / applied），累加 sum+count（无 reservoir，零分配），导出两段均值与 `sendToReplyShare()`。经 `RaftMetadataClient.appendLatencyStats()` 暴露。
- `MetadataJmhSupport.printAppendAttribution`：在 `@TearDown(Level.Trial)` 打印两段归因，并**反射**读取 Ratis 自带 `log_worker` / `server` 计时器（`flushTime` / `syncTime` / `appendEntryLatency` / `queueingDelay` / `enqueuedTime` / `clientWriteRequest`）。用反射是因为 Ratis 把 Dropwizard 重定位到 `ratis.thirdparty.com.codahale`，为一次性诊断不值得引入对重定位类的编译期依赖。

**没有自造 fsync 打点** —— Ratis 内置计时器已经覆盖，自己再测一遍只会引入偏差。

### 8.11.2 实测（单条 `updateLeaderAndIsr`，1-voter，flags 同 §8.6）

```
raft append attribution: appends=726 failures=0 total=8467.4us
    sendToReply=8463.6us(100.0%)  replyToApplied=3.7us(0.0%)
  ratis[...server...].clientWriteRequest:    count=726   mean=0.6us
  ratis[...log_worker].queueingDelay:        count=1453  mean=9.4us
  ratis[...log_worker].appendEntryLatency:   count=1452  mean=43.0us
  ratis[...log_worker].writelogExecutionTime:count=1452  mean=4124.4us
  ratis[...log_worker].flushTime:            count=1451  mean=4089.6us
  ratis[...log_worker].syncTime:             count=1451  mean=4059.2us
  ratis[...log_worker].enqueuedTime:         count=1264  mean=1706.0us
  ratis[...log_worker].startlogsegmentExecutionTime: count=1 mean=45512.6us
```

**注意 count 列：`flushTime` / `syncTime` / `appendEntryLatency` 的 count ≈ 1451 ≈ 2 × 726。每次 `append` 触发两次 fsync。**

预算表（每次 append，总 8467µs）：

| 成分 | 来源 | 每 append | 占比 |
|------|------|---------:|-----:|
| **fsync（2 × 4090µs）** | `flushTime` × 2 | **≈ 8179µs** | **≈ 96.6%** |
| log append CPU（2 × 43µs） | `appendEntryLatency` × 2 | ≈ 86µs | 1.0% |
| worker 队列等待（2 × 9.4µs） | `queueingDelay` × 2 | ≈ 19µs | 0.2% |
| **gRPC + Ratis 请求plumbing** | `clientWriteRequest` | **0.6µs** | **0.007%** |
| commit→apply + apply 循环 | 本节 `replyToApplied` | **3.7µs** | 0.04% |
| 残差 | — | ≈ 180µs | 2.1% |

`syncTime`(4059) ≈ `flushTime`(4090)，说明 flush 几乎全是 `FileChannel.force`；`writelogExecutionTime`(4124) ≈ flush + append，即 log worker 任务时间就是 fsync 时间。

### 8.11.3 被推翻的推测

| 出处 | 原推测 | 实测 | 结论 |
|------|--------|------|------|
| §4.1 / §8.9.3 B2 | gRPC loopback 编解码是 3.8ms 差值的候选大头 | **0.6µs（0.007%）** | **B2 彻底作废。** 即便把 gRPC 完全消除，单条 append 也只快 0.6µs。§9.1 的 #7 应删除，不是延后 |
| §4.2 | 「ZK 也 fsync，所以 fsync 不是净增」 | Raft **每次 append 两次 fsync**，每次 4.09ms | 部分错误：不是「fsync 更慢」，而是**次数翻倍** |
| §8.9.1 | 「剩余候选是 gRPC + 5~6 次线程切换 + RocksDB WAL + 唤醒」 | 这些合计 < 4%（含 3.7µs 的 commit→apply） | 全部是噪声级 |
| §8.9.4 C1/C2/C3 | apply 线程瘦身「几 µs~几十 µs」 | `replyToApplied` 总共只有 **3.7µs** | 判断正确但**上限极低**：整个 apply 段已经只有 3.7µs，再优化空间为零 |
| §8.9.3 B1 | `disableWAL` 应让所有 append 行同向下降 | 未观察到（§9.3 已记录） | 与本节一致：RocksDB 侧根本不在预算里 |

### 8.11.4 唯一有意义的方向：减少 fsync 次数

fsync 占 96.6%，且**每 append 两次**。这把优化空间收窄到一件事：**降低每次写的 fsync 次数或频率**。

1. **查清第二次 fsync 的来源（必须先做）。** `FORCE_SYNC_NUM_DEFAULT = 128`（`javap` 实测）不解释 2×。候选：commit-index metadata entry 单独 flush、`RaftServerConfigKeys.Log.StateMachineData` 相关同步、或 segment 预分配路径。若第二次 fsync 是 metadata entry 且可与数据 entry 合并/批量化，**单条 append 可望直接减半到 ~4.2ms，逼近 ZK 的 4.5ms**。这是目前唯一的高杠杆项。
2. **`async-flush`（§8.1 / #8）的定位需要上调。** 原判断是「串行 `.get()` 下几乎无效」——在 fsync 占 96.6% 的事实下，这个判断仍然成立（串行时无批可合），但它现在是**唯一**能摊薄 fsync 的机制，因此并发写基准的价值从「补充」上升为「必需」。
3. **`unsafe-flush` 依旧禁止**（§8.1.5）。fsync 占比高**不是**打开它的理由 —— 它的代价是静默丢元数据。
4. **对比 ZK 的正确口径。** ZK `TestingServer` 每 txn 一次 fsync ≈ 4.5ms；Raft 是两次 ≈ 8.2ms。**1.85× 几乎精确等于 2 倍 fsync 之比。** 这说明 Raft 与 ZK 的单写差距**不是**共识开销，而是 fsync 次数 —— 一个可能可修的实现细节，而非 §7.1 所说的「设计内正确性税」。§7.1 需要按此修订。

### 8.11.5 副产品：ISR 批量的解释完全闭合

§8.10.2 得出「批量成本 ≈ chunk 数 × 单条成本」。本节给出机制：单条成本 = 2 次 fsync，一个 chunk 内的 N 条并发 append 会被 log worker 合并成少量 fsync 批次，所以一个 chunk ≈ 一个 fsync 批次 ≈ 一条 append 的时间。这也解释了为什么 100 条挤进一个 chunk 只比 1 条贵 13%。

### 8.11.6 验证

- 单测：`AppendLatencyStatsTest`（7）新增，raft 包共 **86 tests 全过**。
- 打点开销：两次 `System.nanoTime()` + 三次原子加，相对 8.5ms 可忽略；实测 `updateLeaderAndIsr` 8151–8710µs，与打点前 8315µs 同噪声带内。
- 归因是**恒等闭合**的：`sendToReply + replyToApplied = total`（`AppendLatencyStatsTest.testSegmentsSumToTotal` 固化）。

---

## 8.12 第二次 fsync 的根因 + async-flush 落地（2026-08-23）

本节分两部分：**(A)** 确证 §8.11 发现的「每 append 两次 fsync」中第二次是什么、能不能消除（§9.0 #10）；**(B)** 据此落地唯一还有杠杆的优化 `async-flush`（§9.0 #8），并系统排查 #5 的覆盖面。

---

### A 部分：第二次 fsync 的根因

**结论：Fluss 侧不能消除，但可被批量/并发摊薄。**

#### 8.12.0 先确认 #5（appendAll 推广）已无遗漏

用 AWK 扫描 `RaftMetadataClient` 全部方法，找「含循环或多次 `append`」的候选（41 处命中）。逐一核对后：**写路径无遗漏**。

- 40 处是**读方法**（`listDatabases` / `getTables` / `bucketSnapshots` / …），循环只在内存 scan 结果上跑，不含 append。
- `deleteTable` / `batchUpdateLeaderAndIsr` / `batchRegisterLeaderAndIsrForTablePartition` 命中「有循环」但 `appends=0` —— 因为已改成往 `List<MetadataRecord>` 里塞、最后一次 `appendAll`（§8.10.1）。这正是期望形态。
- `updateMetadataVersion` 是 `appends=1 loop=1`：循环用于校验 voter 的 max supported version，只有一次 append。**正确，无需批量**。
- `tryRegisterProducerOffsets` 单次 append + `synchronized` 去重，正确。

**`appendAll` 的推广已完成，没有剩余的「N 次串行 append」写路径。**

### 8.12.1 决定性证据：entry 计数精确闭合

在 §8.11 的归因里补上 Ratis 的 entry 类型计数器，单条 `updateLeaderAndIsr` 一个 trial：

```
raft append attribution: appends=728 failures=0 total=8361.4us
    sendToReply=8357.8us(100.0%)  replyToApplied=3.7us(0.0%)
  stateMachineLogEntryCount: 728     ← 我们的数据 entry（= appends）
  metadataLogEntryCount:     727     ← Ratis 自己追加的 commit-index entry
  configLogEntryCount:         1     ← 启动时的配置 entry
  appendEntryCount:         1456     = 728 + 727 + 1  ✓
  flushCount:               1455     ≈ 每个 entry 一次 fsync  ✓
  flushTime: count=1455 mean=4056.2us    syncTime: count=1455 mean=4023.6us
```

**`metadataLogEntryCount` = `stateMachineLogEntryCount` − 1**：每次业务 append 精确对应一条额外 metadata entry。而 `flushCount ≈ appendEntryCount`，即**每个 entry 各自 fsync 一次**。8361µs ≈ 2 × 4056µs，闭合。

### 8.12.2 代码路径（`javap` 逐层确认）

```
LeaderStateImpl.updateCommit(LogEntryHeader[])
  ├─ raftLog.getLastCommittedIndex()      → commitIndex
  ├─ logMetadata(commitIndex)              ← 无条件调用，无分支、无去重
  │    └─ raftLog.appendMetadata(currentTerm, commitIndex)
  │         └─ RaftLogBase.appendMetadata → runner.runSequentially(...)
  │              └─ 与数据 entry 同一条 appendEntry → 同一 log worker → fsync
  └─ commitIndexChanged() → notifySenders()
```

`logMetadata` 的字节码只有 4 条指令（`getfield raftLog` / `getfield currentTerm` / `invokeinterface appendMetadata` / `notifySenders`），**没有任何条件判断**。commit index 每推进一次就写一条 metadata entry，且它走 `RaftLogBase.appendMetadata` → 同一个 `runner.runSequentially` → 同一个 log worker，因此享受同样的 fsync 语义。

这是 Ratis 持久化 commit index 的方式（重启后无需重放即可知道 commit 位置），**属于 Ratis 内部实现，Fluss 无法通过配置关闭**。`FORCE_SYNC_NUM_DEFAULT = 128` 与此无关 —— 那是「连续多少 entry 强制 sync 一次」的上限，不是下限。

### 8.12.3 为什么批量能摊薄它：批量场景对照

同一归因跑在 `batchUpdateLeaderAndIsr`（100 buckets/op，chunk=64）：

| 指标 | 单条场景 | 批量场景（100/op） | 变化 |
|------|--------:|------------------:|------|
| appends | 728 | 26520 | — |
| `appendEntryLatency` count | 1456（**2.00×** appends） | 27184（**1.025×** appends） | metadata entry 占比 100% → 2.5% |
| **`flushTime` count** | 1455（**2.00×** appends） | **1312（0.049×** appends） | **每次 fsync 覆盖 ~21 个 entry** |
| `flushTime` mean | 4056µs | 4123µs | **基本不变** |
| `appendEntryLatency` mean | 43.2µs | 2.1µs | 批量下摊薄 |
| `clientWriteRequest` mean | 1.1µs | 0.1µs | gRPC 依旧可忽略 |

两个关键事实：

1. **fsync 单次成本恒定 ≈ 4.1ms，与批大小无关**（4056 vs 4123µs）。这是磁盘特性，不是软件开销。
2. **批量把 fsync 次数从 2×appends 降到 0.05×appends**：log worker 把队列里积压的 entry 合并成一次 fsync，且 commit index 推进一次只产生一条 metadata entry（被 100 条 append 共享）。

**这解释了 §8.10.2 的全部现象**：一个 chunk ≈ 一到两个 fsync 批次 ≈ 一条单独 append 的时间，所以 100 条挤进一个 chunk 只比 1 条贵 13%；chunk=64 需要 2 个 chunk 所以近似翻倍。

### 8.12.4 #10 的结论：已诊断，不做

原设想是「消除第二次 fsync → 单条 8.5ms 降到 ~4.2ms，逼近 ZK 的 4.5ms」。证据表明**这条路在 Fluss 侧不可行**：

- metadata entry 由 `LeaderStateImpl.logMetadata` 无条件产生，**没有配置开关**，且承载 commit index 持久化 —— 去掉会损害 Ratis 自身的恢复语义。
- 要改只能改 Ratis（把 metadata entry 与后续数据 entry 合并 flush，或让它不单独触发 fsync）。这是**上游改动**，不属于本仓库范围，也不该为了 JMH 分数 fork Ratis。
- **真正的缓解手段是批量化，而它已经落地了**（§8.10）。

因此 **#10 从「最高优先级」调整为「已诊断、不做」**。取而代之的结论是：**单写路径已无 Fluss 侧可做的优化，剩下的全部是「让写变成批量写」。**

### 8.12.5 对 §7.1 / §8.11.4 的最终修订

§8.11.4 曾据「1.85× ≈ 2 倍 fsync 之比」推断单写差距是「可能可修的实现细节」。本节收紧这个判断：

- **机制已完全明确**：Raft 每写 2 次 fsync（数据 + commit-index metadata），ZK 每 txn 1 次。
- **但对 Fluss 不可修**（§8.12.4）。所以在**单条写**口径上，1.85× 应视为**采用 Ratis 的既定成本**，而非待办优化项。
- **有意义的口径转向两个**：(a) 批量写（已优化，§8.10）；(b) 3-voter 同 AZ 的 p50/p99（§7.2，未测 —— 届时网络 ack 会与 fsync 重叠，比值不会是简单的 2×）。

### 8.12.6 验证

- 诊断代码只加在 JMH 侧（`printAppendAttribution` 增加 counter dump），**未改动任何生产代码**。
- raft 包 **86 单测**不变、全过；`spotless:check` 通过。
- 数字可复现且恒等闭合：728 + 727 + 1 = 1456 ≈ `flushCount` 1455，不是统计估计。

---


---

### B 部分：async-flush 落地（并发写 1.99× 提速）

A 部分证明单条写在 Fluss 侧已到底，唯一剩下的杠杆是**让 fsync 被并发摊薄**。本部分落地 §9.0 #8 并实测验证。

#### 8.12.B1 新增配置与并发写基准

| 项 | 内容 |
|----|------|
| `coordinator.raft.async-flush.enabled`（默认 **false**） | 映射 Ratis `RaftServerConfigKeys.Log.setAsyncFlushEnabled`，在 `applyAppendPipelineProperties` 里设置 |
| `MetadataStoreBenchmark.concurrentUpdateLeaderAndIsr` | `@Threads(8)`，512 个预注册 bucket 轮转，线程间无写写冲突 |
| `-Dfluss.jmh.raft.async-flush=true` | JMH 侧覆盖开关，可 A/B 而不改 session cache key |

`@Threads(8)` 是**方法级覆盖**类上的 `@Threads(1)` —— 这是 §8.1.3 一直缺失的验收手段：此前所有写行都是串行的，`async-flush` 根本无从体现。

#### 8.12.B2 实测：A/B（1-voter，8 线程，其余 flags 同 §8.6）

| 指标 | async-flush **off**（默认） | async-flush **on** | 变化 |
|------|--------------------------:|------------------:|------|
| **`concurrentUpdateLeaderAndIsr`** | **8067.8µs** | **4055.1µs** | **1.99× 提速** |
| stdev | ±1551 | **±517** | 更稳 |
| `sendToReply` | 8127.1µs (100.0%) | 4045.6µs (100.0%) | 减半 |
| `replyToApplied` | 0.9µs | 0.6µs | 依旧可忽略 |
| **`flushTime` mean** | **4041.2µs** | **7.2µs** | **fsync 移出关键路径** |
| `flushCount` | 2518 | 12243 | 次数变多但每次几乎免费 |
| `flushCount / appends` | 0.45 | 1.15 | — |
| `metadataLogEntryCount / appends` | 0.23 | 0.18 | 并发本身已摊薄 metadata entry |

**两个独立的效果叠加：**

1. **并发本身就摊薄 fsync**（无需任何开关）：串行时 `flushCount/appends = 2.0`（§8.12.1），8 线程并发时降到 **0.45** —— log worker 把排队的 entry 合并了。这与 §8.12.3 的批量观察同源。
2. **async-flush 让 fsync 不再阻塞 ack**：`flushTime` mean 从 4041µs 塌到 **7.2µs**。注意 `flushCount` 反而**上升**到 12243（1.15×/append），但总延迟仍减半 —— 说明它不是靠"少 flush"，而是靠**不等 flush**。

#### 8.12.B3 为什么这不是 `unsafe-flush`（耐久性论证）

`async-flush` 与 §8.1.5 禁止的 `unsafe-flush` 是**不同语义**，这一点必须写清楚，否则很容易被后人当成同类而误开：

| | `async-flush`（本节采纳） | `unsafe-flush`（**永不采纳**） |
|---|---|---|
| ack 时机 | flush **完成之后** | flush **完成之前** |
| 崩溃后果 | 已 ack 的写不会丢 | **已 ack 的写可能静默丢失** |
| 作用机制 | flush 不阻塞其他 writer，可并行/合并 | 跳过等待 |
| Ratis 约束 | 两者互斥，同开抛 `IllegalStateException` | 同左 |

代码里已用注释锁死这个边界，并有两个测试守护：`testAsyncFlushDefaultsOffAndNeverEnablesUnsafeFlush`、`testAsyncFlushCanBeEnabledWithoutUnsafeFlush`（后者断言开 async 时 unsafe 仍为 false）。

#### 8.12.B4 默认值仍为 false（不是遗漏）

实测 1.99×，但默认值保持关闭，理由：

1. **只在并发写下有效。** 串行写没有可合并对象 —— §8.1.3 的原判断在此得到确认，本节没有推翻它，而是补齐了它缺失的另一半场景。
2. **1-voter loopback 不能代表生产。** 3-voter 下 follower ack 与 fsync 重叠，async-flush 的收益可能被网络掩盖，也可能仍显著 —— 未测（§7.2）。
3. **它改变了 flush 的时序行为**，虽不牺牲已 ack 写的耐久性，但值得由部署方显式开启。**元数据写密集且并发的部署可以设 `coordinator.raft.async-flush.enabled=true`。**

#### 8.12.B5 验证

- 生产侧改动：1 个新 `ConfigOption` + `applyAppendPipelineProperties` 里一行 setter。
- raft 包 **88 单测**全过（`RaftMetadataServerTest` 从 7 → 9，新增两个 flush 语义测试）；`spotless:check` 通过。
- A/B 可复现：`-Dfluss.jmh.raft.async-flush=true` 切换，两次运行的 stdev（±1551 / ±517）都远小于 4013µs 的差值。

---

## 8.13 3-voter 实测：async-flush 在真实 quorum 下 2.2×（2026-08-23）

补齐 §7.2 一直缺失的 3-voter 口径，并据此**定夺 §9.0 遗留的 async-flush 默认值问题**。结论与 1-voter 上的保守判断相反：**3-voter 下 async-flush 收益更大**。

### 8.13.1 夹具：必须 1→N 增长，不能一次性 bootstrap

`-Dfluss.jmh.raft.voters=3` 让 `StoreSession` 起 1 个 `RaftMetadataClient` + 2 个裸 `RaftMetadataServer` follower。两个坑都是实测踩出来的，**症状都是干净的 30s 静默超时，没有任何异常**：

1. **peer-id 方案必须一致。** `RaftMetadataClient` 用 `peerIdForAddress`（`host:port` → `host_port`）。follower 若用 `RaftPeerId.valueOf(address)`，双方对 peer 身份认知不一致，选举永不完成。为此把 `resolveGroupForVoters` 从 private 提为 public，两侧共用一套解析。
2. **必须 1→3 增长（`addVoter`）**，不能让 3 个 peer 都带完整 3-voter conf 启动 —— 后者每个 peer 都在等一个从未达成过配置共识的 quorum。`RaftVoterReconfigurationITCase` 的注释早已记录这个陷阱，但我第一次仍然踩了。

修好后 3-voter 起集群只要 **2.1s**。夹具本身由新增的 `RaftThreeVoterQuorumITCase` 守护（选举 + 真实 quorum 提交），否则将来回归时基准会静默测不出数而不是报错。

### 8.13.2 实测：单条写 A/B（3-voter，其余 flags 同 §8.6）

| 指标 | 1-voter（§8.11） | **3-voter, async-flush off** | **3-voter, async-flush on** |
|------|----------------:|----------------------------:|---------------------------:|
| `updateLeaderAndIsr` | 8312µs | **18109µs** | **8233µs** |
| stdev | ±243 | **±29564**（极不稳） | **±851** |
| `sendToReply` | 8358µs (100%) | 17943µs (100%) | 8225µs (100%) |
| `replyToApplied` | 3.7µs | 4.1µs | 1.8µs |
| **leader `flushTime` mean** | 4056µs | **8213µs** | **68µs** |
| follower `flushTime` mean | — | 9840 / 9811µs | 9.4 / 8.9µs |
| `flushCount / appends` | 2.0 | 2.0 | 2.0 |
| `clientWriteRequest` | 1.1µs | 0.6µs | — |

**关键发现：3-voter 让 fsync 变慢了一倍，而不是引入了网络开销。**

- `clientWriteRequest` 仍是 **0.6µs** —— 网络/gRPC 依然可忽略，§8.11.3 对 B2 的否定在 3-voter 下同样成立。
- leader `flushTime` 从 4056µs 涨到 **8213µs**，follower 涨到 ~9.8ms。三个 Ratis 节点在**同一块磁盘**上各自 fsync、互相争抢 —— 这是 fsync 串行化，不是共识开销。
- `flushCount/appends` 仍是 2.0（§8.12 的 commit-index metadata entry 在 3-voter 下不变）。

**async-flush 基本消除了这个问题**：leader `flushTime` 8213µs → **68µs**，总延迟 18109µs → **8233µs（2.20×）**，stdev 从 ±29564 收敛到 ±851。

### 8.13.3 修订 §8.12.B4：async-flush 的收益来源变了

§8.12.B4 曾以「只在并发写下有效」为由保持默认 false。3-voter 数据要求修订：

| 场景 | async-flush 收益 |
|------|-----------------|
| 1-voter 串行写 | 无（无可合并对象）—— §8.1.3 原判断成立 |
| 1-voter 并发写（8 线程） | **1.99×**（§8.12.B2） |
| **3-voter 串行写** | **2.20×** ← 新数据 |

**async-flush 在真实 quorum 下即使串行写也有 2.2×**，因为收益来源不同：不再是「合并多个 writer 的 fsync」，而是**让 leader 的 fsync 不阻塞它给 follower 发 AppendEntries、也不与 follower 的 fsync 串行排队**。

**但默认值仍保持 false**，理由收窄为一条：**三个节点共用一块磁盘是本基准的产物，不是生产形态**。生产 3-voter 分布在不同机器上，各自 fsync 不争抢同一磁盘，8213µs 不会出现。真实部署的收益应介于 1-voter（0×~1.99×，取决于并发度）与本节（2.20×）之间，需在目标硬件实测。

**明确建议**：**元数据写有并发、或多个 coordinator 共享存储的部署，应设 `coordinator.raft.async-flush.enabled=true`**（不牺牲已 ack 写的耐久性，§8.12.B3）。

### 8.13.4 副产品

`replyToApplied` 在三种配置下是 3.7 / 4.1 / **1.8µs**，始终 < 5µs。再次确认 §8.9.4（C1/C2/C3）那条路线的天花板：**apply 段从来不是瓶颈，与 voter 数无关**。

### 8.13.5 3-voter 批量写：chunk 128 优于 64，但本机数据被磁盘争抢污染

跑 `batchUpdateLeaderAndIsr`（100 buckets/op）的 chunk A/B，验证 §8.10.4 遗留的 chunk 默认值问题。

| 配置 | score | stdev | 逐次 iteration | leader `flushTime` |
|------|------:|------:|---------------|-------------------:|
| 3-voter, chunk=**64**, af=off | **44099µs** | ±40218 | 46016 / 44592 / … | 8793µs (count 1757) |
| 3-voter, chunk=**128**, af=off | **27515µs** | ±34967 | 29721 / 26261 / … | 9233µs (count 1706) |

**chunk=128 比 64 快 1.60×**（44099→27515µs），与 1-voter 上的 1.96×（§8.10.2）同向。机制一致：100 buckets 在 chunk=64 下要 2 个 chunk、在 128 下只要 1 个，而每个 chunk 至少一轮 fsync。

**但这两个数字都不能用来定默认值**，两条独立理由：

1. **stdev 大于均值的 80%+**（±40218 / ±34967）。JMH n=3 且三节点共用一块磁盘，fsync 排队使 iteration 间波动极大。**倍数关系可信（有 1-voter 同向数据 + chunk 数机制佐证），绝对值不可信。**
2. **`flushTime` 在两个配置下几乎相同**（8793 vs 9233µs，均 ~9ms），说明**单次 fsync 的成本由磁盘争抢主导，与 chunk 无关**。chunk 只改变 fsync 的**次数**，改变不了每次的代价 —— 而生产 3-voter 分布在不同机器上，单次 fsync 不会是 9ms。

**未完成的两格**：`async-flush=on` × {64, 128}。这两格本是用来把 fsync 移出关键路径、从而分离"chunk 效应"与"磁盘争抢"的对照组（若 af=on 下 128 仍优于 64，则 chunk 效应是真的 pipelining；若两者趋同，则此前的差异主要来自 fsync 次数）。**本机 shell 在连续跑完 4 组基准后不再可靠返回输出，这两格没跑出来**，因此上述"chunk 效应是真的"目前**只有 1-voter 数据（§8.10.2，stdev 仅 76–243µs）支撑，3-voter 侧未被独立确认**。

#### 结论：chunk 默认值保持 64

- 已知：chunk=128 在 1-voter（1.96×，数据干净）与 3-voter（1.60×，数据污染）下都更快，且**上限联动已做**（`max(100, 2*chunk)`，§8.10.1），调大是安全的配置动作。
- 但改**默认值**意味着所有部署都吃更大的在途窗口，而本机的 3-voter 环境（共用磁盘、fsync 9ms）**不能代表生产**。
- **建议**：ISR 批量密集的部署设 `coordinator.raft.append-chunk-size=128`，并在目标硬件上用 §9.2 的命令复核。

### 8.13.6 验证

- 新增 `RaftThreeVoterQuorumITCase`（1 test，2.1s）守护上述两个夹具陷阱。
- 生产侧改动仅一处：`resolveGroupForVoters` 由 private 提为 public（附 javadoc 说明为何需共用）。
- raft 包单测 + IT 全过；`spotless:check` 通过。
- A/B 可复现：`-Dfluss.jmh.raft.voters=3 [-Dfluss.jmh.raft.async-flush=true]`。**注意 off 那一轮 stdev 极大（±29564），单次运行不可信** —— 但 18109 vs 8233 的差值远超噪声，且有 `flushTime`（8213 vs 68µs）作为机制侧佐证。

---



### 9.0 当前进度（2026-08-23，§8.11 归因后重排）

> **§8.11 已把 #1 做完，并推翻了本节原有排序的前提。** 归因结果：单条 append 的 **96.6% 是 fsync（每次 append 两次，各 4.09ms）**，gRPC 只有 **0.6µs**，commit→apply 只有 **3.7µs**。因此 **#7 作废**，**#2/#3/#6 的性能上限已被证明极低**，唯一有杠杆的方向是**减少 fsync 次数**（§8.11.4）。

| # | 项 | 状态 | 归因后的评价 |
|---|----|------|--------------|
| 1 | 写路径分段归因 | **已落地**（§8.11） | **本轮最有价值的一步**：把优化面从 5 个候选收敛到 1 个 |
| 2 | C3 waiter 出锁 complete | **已落地**（§8.10.1） | 正确性修复有效；性能上限被证明只在 3.7µs 段内 |
| 3 | B1 `disableWAL` | **已落地**（§8.10.1） | 未观察到可测下降，与 §8.11 一致（RocksDB 不在预算里） |
| 4 | 补 ISR 批量基准 | **已落地**（§8.10.2） | 暴露了 45–88× 的真实风险 |
| 5 | A `appendAll` + 分块推广 | **已落地**，45–88×（§8.10.3） | 机制由 §8.11.5 解释闭合 |
| 6 | C1 / C2 apply 线程微优化 | **已落地**（§8.10.1） | 同 #2，上限极低 |
| 7 | ~~B2 本地直提交绕过 gRPC~~ | **作废** | gRPC = 0.6µs（0.007%）。**不要做** |
| 8 | B3 `async-flush` + 并发写 JMH | **已落地**（§8.12 B 部分） | **并发写 1.99× 提速**（8068→4055µs），`flushTime` 4041→7.2µs。默认 false，需显式开启 |
| 9 | D 单 voter 启动窗口 | **未做** | 价值仍低 |
| **10** | **查清第二次 fsync 的来源并尝试消除** | **已诊断 → 不做**（§8.12 A 部分） | 根因 = Ratis `LeaderStateImpl.logMetadata` 无条件写 commit-index metadata entry，**Fluss 侧无法关闭**；缓解手段是批量化（#5）与并发摊薄（#8），均已落地 |
| 新 | chunk 默认值 64→128 的取舍 | **已实测，保持 64**（§8.13.5） | 1-voter 1.96×（数据干净）、3-voter 1.60×（**数据被同磁盘 fsync 争抢污染**）。上限联动已做，调大安全；**建议 ISR 批量密集的部署设 128 并在目标硬件复核** |
| 新 | `async-flush` 默认值 false→true 的取舍 | **已实测，保持 false**（§8.13.3） | 1-voter 串行 0×、1-voter 并发 1.99×、**3-voter 串行 2.20×**。默认仍 false 的唯一理由是「三节点共用一块磁盘」是基准产物；**建议并发写或共享存储的部署显式开启** |
| 新 | 3-voter 口径（§7.2） | **已补齐**（§8.13） | 结论：网络仍可忽略（`clientWriteRequest` 0.6µs），瓶颈是同磁盘 fsync 争抢 |

**Fluss 侧的元数据写优化已做完，3-voter 口径也已补齐。**

已系统核验与实测：
- **覆盖面**：`appendAll` 无遗漏（§8.12.0）。
- **单条写归因到底**：96.6% 是 fsync，其中一半是 Ratis 内部 commit-index entry，不可消除（§8.11 / §8.12 A）。
- **三条摊薄路径全部落地并实测**：批量 45–88×（§8.10）、1-voter 并发 1.99×（§8.12 B）、3-voter 串行 2.20×（§8.13）。
- **B2（绕过 gRPC）在 1-voter 与 3-voter 下均被否定**：`clientWriteRequest` 恒为 0.6~1.1µs。

**剩余事项只有两类，都不是 1-voter 上能继续做的优化：**

1. **在节点不共享磁盘的环境复核两个默认值**（chunk 64→128、async-flush false→true）。两者在本机都实测为「更快但数据被磁盘争抢污染」（§8.13.3 / §8.13.5），已做成配置项，**建议密集部署显式开启并在目标硬件复核**。3-voter 夹具已就绪（§8.13.1）。
2. **#9（启动窗口）** 价值低，只影响单节点/测试夹具。

**一个已知的测量缺口**：3-voter 批量写的 `async-flush=on` × {chunk 64, 128} 两格未跑出（§8.13.5 末尾）。它们本用于分离「chunk 效应」与「磁盘争抢」；目前「chunk 效应为真」只有 1-voter 侧的干净数据支撑。**在不共享磁盘的环境上重跑时应优先补这两格。**

若还要继续压单写，只剩**上游改 Ratis**（合并 metadata entry 的 flush）—— 不属于本仓库范围。


### 9.1 原始排序（保留作记录；已被 §8.11 重排，见 §9.0）

按「风险 × 收益 ÷ 依赖」排。**1 和 2 是其余各项的前置条件。**

| # | 项 | 出处 | 工作量 | 现有 JMH 上预期 | 风险 | 状态 |
|---|----|------|--------|-----------------|------|------|
| 1 | 写路径分段归因（`send` / `reply` / `applied`） | 8.9.1 | S | **无分数变化**；解锁后续取舍 | 无 | 未做 |
| 2 | C3：waiter 出锁后 complete | 8.9.4 | S | **无分数变化**；修 §5.6 违反 | 低 | **已落地** |
| 3 | B1：`disableWAL`（可选：内存后端） | 8.9.3 | S | 所有单 append 行同向下降，幅度待测 | 低（已论证恢复不依赖 RocksDB 持久化） | **已落地**（未见可测下降） |
| 4 | 补基准：`batchUpdateLeaderAndIsr` / per-bucket 批量 | 8.9.2 | S | **新增行**，预期 Raft ≫ ZK | 无（只加 JMH） | **已落地** |
| 5 | A：`appendAll` **+ 分块** 推广到删除级联与 ISR 批量 | 8.9.2 | M | `deleteTableAssignment` 2.64× 应大幅回落；**真正收益在生产 ISR 路径** | 中（分块上限必做） | **已落地**（45–88×） |
| 6 | C1 / C2：apply 线程微优化 | 8.9.4 | S | 几 µs~几十 µs | 低 | **已落地** |
| 7 | B2：leader 本地直提交绕过 gRPC | 8.9.3 | M | 所有 append 行；幅度取决于 #1 | 中 | 未做（需 #1） |
| 8 | B3：`async-flush` + 新增并发写 JMH | 8.9.3 / 8.1 | S + M | 串行 JMH 无效；**并发才有** | 低–中 | 未做 |
| 9 | D：单 voter 启动窗口 | 8.9.5 | S | 仅 1-node 启动 | 低 | 未做 |
| — | `MULTI_OP` record type | §8.2.3 | L | 与 pipeline 同类 | 高（混版本 / stream） | 不做 |
| — | `unsafe-flush` | §8.1.5 | S | 分数好看 | **不可接受（静默丢元数据）** | 永不做 |

依赖关系：**#2 必须早于 #5**（分块放大 apply 线程搭车）；**#1 必须早于 #7 和 #8**（否则无法判断 gRPC / fsync 各占多少）；**#4 应早于 #5**（先有基准才能验收）。落地时按此顺序执行，实测见 §8.10。

### 9.2 验收命令（沿用现有 JMH，flags 同 §8.6–§8.8）

新增的 ISR 批量行（§8.10.2 实际用的命令，JMH 需 `test-compile` 生成 `BenchmarkList`）：

```bash
 mvn -o -pl fluss-jmh test-compile
 java -cp "fluss-jmh/target/test-classes:fluss-jmh/target/classes:$(cat /tmp/jmh_cp.txt)" \
   org.openjdk.jmh.Main -f 0 -wi 2 -w 1s -i 3 -r 1s -tu us \
   MetadataStoreBenchmark.batchUpdateLeaderAndIsr
```

覆盖 chunk 做 A/B（不改代码，见 §8.10.4）：

```bash
 java -Dfluss.jmh.raft.append-chunk-size=128 -cp "..." org.openjdk.jmh.Main \
   -f 0 -wi 2 -w 1s -i 3 -r 1s -tu us -p backend=RAFT \
   MetadataStoreBenchmark.batchUpdateLeaderAndIsr
```

**3-voter 复核（§8.13）** —— 定夺两个默认值必须在**节点不共享磁盘**的环境上跑，否则测到的是 fsync 争抢：

```bash
 # 单条写 A/B：async-flush 的收益
 java -Dfluss.jmh.raft.voters=3 [-Dfluss.jmh.raft.async-flush=true] -cp "..." \
   org.openjdk.jmh.Main -f 0 -wi 2 -w 1s -i 3 -r 1s -tu us -p backend=RAFT \
   'MetadataStoreBenchmark.updateLeaderAndIsr$'

 # 批量写 2x2 矩阵：chunk {64,128} x async-flush {false,true}
 # 注意 async-flush=true 的两格在 §8.13.5 中未跑出，应优先补齐 —— 它们是分离
 # "chunk 效应" 与 "磁盘争抢" 的对照组。
 java -Dfluss.jmh.raft.voters=3 -Dfluss.jmh.raft.append-chunk-size=128 \
   -Dfluss.jmh.raft.async-flush=true -cp "..." org.openjdk.jmh.Main \
   -f 0 -wi 2 -w 1s -i 3 -r 1s -tu us -p backend=RAFT \
   MetadataStoreBenchmark.batchUpdateLeaderAndIsr
```

3-voter 下务必核对输出里的 `# raft append attribution (voters=3)` 与每个节点的 `flushTime` mean：**若三个节点的 `flushTime` 都是毫秒级且彼此接近，说明它们在争抢同一块磁盘，该轮数据的绝对值不可用**（§8.13.2 / §8.13.5）。

其余会动的行 + 不应变化的对照：

```bash
 ./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.deleteTableAssignment
 ./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.deletePartitionAssignment
 ./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.deleteTable
 ./mvnw -pl fluss-jmh test -Dtest=MetadataStoreBenchmark.metadataWrite
```

复跑约定与 §8.6 一致：warmup 2×1s、measurement 3×1s、`@Fork(0)`、1 thread、AverageTime µs/op、`JAVA_HOME` = TencentKona 11.0.23，**只复跑会动的行 + `metadataWrite` 作为「不应变化」对照**。

### 9.3 判定标准（避免把噪声当收益）

| 项 | 应该看到 | 实际结果 / 若没看到说明什么 |
|----|----------|------------------------------|
| #1 归因 | 无分数变化；给出分段预算 | **fsync 96.6% / gRPC 0.6µs / apply 3.7µs**（§8.11.2）。作废 #7，收窄到 #10 |
| #3 B1 | 所有单 append 行同向小幅下降 | **未观察到可测下降**：单条 `updateLeaderAndIsr` 8315µs 与落地前 8.2–9.3ms 带同量级。§8.11 解释了原因 —— RocksDB 侧根本不在预算里 |
| #5 A | `deleteTableAssignment` 从 2.64× 明显回落；`metadataWrite` **不变** | ISR 批量 **45–88× 改善**（§8.10.3）；批量成本闭合为「chunk 数 × 单条」，机制见 §8.11.5 |
| ~~#7 B2~~ | 所有 append 行同向下降 | **作废**：`clientWriteRequest` 实测 **0.6µs** |
| #8 B3 | **串行不变**（预期），并发写 JMH 下改善 | 未做。串行不变是**预期**（§8.1.6）；但在 fsync 占 96.6% 的事实下，它已是唯一已知的摊薄机制 |
| #10 fsync 次数 | 单条 append 的 flush count 从 2× 降到 1× | **已诊断，不做**：根因是 Ratis 内部 commit-index metadata entry（§8.12.2），Fluss 侧无法关闭。批量下 flush count 已自然降到 0.049×（§8.12.3） |

n=3 的 JMH CI 很宽（§1.2）。**单行 <10% 的变化按噪声处理**，不写进 catalog。注意 §8.10.2 三行的 stdev 都很小（76–243µs），chunk 数导致的 2× 差异远超噪声，属于可信信号。§8.11 / §8.12 的归因不受 JMH CI 影响 —— 它们是恒等分解（两段之和 = 总和；728+727+1 = 1456 ≈ flushCount 1455），count 是整数关系而非统计估计。

### 9.4 结论修订（§8.11 / §8.12 之后）

1. **「1-voter 单写追平 ZK」不是有效目标 —— 理由已从含糊变具体。** 原措辞是「共识开销是设计内的正确性税」（§7.1，含糊）。实测机制：**Raft 每写 2 次 fsync（数据 entry + Ratis 的 commit-index metadata entry），ZK 每 txn 1 次；1.85× ≈ 2 倍 fsync 之比**（§8.11.2 / §8.12.1）。第二次 fsync 由 `LeaderStateImpl.logMetadata` 无条件产生，**Fluss 侧无法消除**（§8.12.2/§8.12.4）。因此这是**采用 Ratis 的既定成本**，不是待办优化项。
2. **仍然不要用 1-node JMH 声称「生产已对齐 ZK」。** §1.1 的不可外推表与 §6 的局限清单继续适用；3-voter 会引入真实网络 ack，与 fsync 重叠后比值不会是简单的 2×。
3. **`unsafe-flush` 依旧禁止。** fsync 占比高**不是**打开它的理由（§8.1.5）。
4. **单写已到 Fluss 侧的底。** 剩余方向只有两类：让写变成批量写（已落地，§8.10）；让 fsync 被并发摊薄（#8，机制已由 §8.12.3 证实）。

---

## 附录：关键代码索引

| 主题 | 位置 |
|------|------|
| 实测表 | `zookeeper-removal-design.md` §8.4 |
| JMH | `fluss-jmh/src/test/java/org/apache/fluss/jmh/MetadataStoreBenchmark.java` 等 |
| Raft 写 | `RaftMetadataClient.append` / `appendAll`、`RaftJournalWriter.append` / `appendAll`、`FlussMetadataStateMachine.applyTransaction` |
| Ratis 属性 | `RaftMetadataServer.buildRaftProperties`（未设 force.sync / async-flush / unsafe-flush） |
| Ratis flush 默认 | Apache Ratis 2.4.1 `Log.FORCE_SYNC_NUM_DEFAULT=128`；`ASYNC_FLUSH_ENABLED_DEFAULT=false`；`UNSAFE_FLUSH_ENABLED_DEFAULT=false`；`SegmentedRaftLogWorker.shouldFlush` |
| ZK 写 | `ZooKeeperClient.registerTable` |
| ID | `RaftSequenceIDCounter` vs `ZkSequenceIDCounter` |
| 启动 wait | `FlussClusterExtension.waitUntilCoordinatorServerElected` |
| `getTableInfo` | `FlussAdmin.getTableInfo`、`MetadataManager.getTable`、`scanPrefixEntries` |
| stream max-wait | `ConfigOptions.COORDINATOR_METADATA_STREAM_MAX_WAIT`（默认 500ms；IT/JMH 集群 100ms） |
| FetchMetadata 停车（已落地） | `MetadataStreamRpcServer.fetchMetadata` → `MetadataStreamService.fetchAsync`；timeout scheduler + metadata-stream pool；`onApplied` 只 enqueue（§8.7） |
| Prefix iterator（已落地） | `MetadataKvStore.scanPrefixEntries`；`RaftMetadataStore` 委托；JMH before/after 见 **§8.6** |
| **未接 `appendAll` 的多写路径（§8.9.2）** | `RaftMetadataClient.deleteTableAssignment` / `deletePartitionAssignment` / `deleteTable` / `batchUpdateLeaderAndIsr` / `batchRegisterLeaderAndIsrForTablePartition` |
| **ISR 批量调用方（§8.9.2，JMH 未覆盖）** | `TableBucketStateMachine:245`/`:275`/`:552`、`ReplicaStateMachine:491`、`CoordinatorEventProcessor:2035`；ZK 对照 `ZooKeeperClient.batchRegisterLeaderAndIsrForTablePartition`（一次 Curator transaction） |
| **Ratis 客户端窗口上限（§8.9.2）** | `RaftClientConfigKeys.Async.OUTSTANDING_REQUESTS_MAX_DEFAULT=100`；`Write.ELEMENT_LIMIT_DEFAULT=4096`；`Log.QUEUE_ELEMENT_LIMIT_DEFAULT=4096`（`javap` 实测） |
| **RocksDB WAL 与恢复路径（§8.9.3 B1）** | `RocksDBMetadataKvStore.open`（默认 `WriteOptions`）；`FlussMetadataStateMachine.load` → `RaftMetadataStore.restoreFromSnapshot`（先 `clear()`，不读 RocksDB 持久内容） |
| **本地直提交入口（§8.9.3 B2）** | `RaftServer extends RaftClientAsynchronousProtocol.submitClientRequestAsync`；`RaftClientRequest.newBuilder()` + `writeRequestType()`（`javap` 实测） |
| **apply 线程搭车开销（§8.9.4）** | `RaftMetadataStore.apply` 的 `oldValue` 预读（C1）；`MetadataWatchService.fireWatch` hex+全表遍历（C2）；`FlussMetadataStateMachine.completeAppliedWaiters` 持锁 complete + `RaftJournalWriter.appendAll` 的 `whenComplete`（C3，违反 §5.6） |
| **启动路径（§8.9.5）** | `CoordinatorServer.initCoordinatorLeader:433`（`registerCoordinatorLeader`）与 `:492`（`createDefaultDatabase`）之间隔着 `synchronized (lock)` 初始化块，无法合批；`LeaderElection.PRE_VOTE_DEFAULT=true` |
| 实现指南 | 本文 **§8**（8.2 / 8.3 / 8.4 已落地；8.1 仍是 sketch；**8.9 剩余空间全部未落地**） |
| **优先级路线图** | 本文 **§9**（排序 / 验收命令 / 判定标准 / 不可推翻的结论） |
