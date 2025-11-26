# TabletInvertedIndex 线程安全化设计文档

## 1. 背景
原来的 `TabletInvertedIndex` 通过 `QueryableReentrantReadWriteLock` 保护 `Long2ObjectOpenHashMap`，所有操作都共享一把粗粒度锁。随着 FE 并发任务增多（调度、ReportHandler、Txn Listener 等），该设计暴露出性能瓶颈和数据竞争风险：部分路径绕过锁直接访问 map，导致并发删除 tablet 时出现 NPE / `Preconditions` 异常。

## 2. 设计目标
- **功能目标**：提供线程安全的 `TabletInvertedIndex`，在多线程场景下正确维护 `tablet → meta/replica`、`backend → tablet` 的映射，并保证调用者拿到的是一致快照。
- **性能目标**：降低全局锁竞争，允许多线程同时读写索引；在 tablet/replica 频繁增删场景下仍能保持较高吞吐。

## 3. 用户接口说明
### 3.1 概念
- *TabletInvertedIndex*：FE 侧的倒排索引，负责从 tablet id 定位元信息 / replica，以及从 backend id 找到其承载的 tablet。
- *Replica list*：同一 tablet 在不同 BE 上的副本集合。

### 3.2 使用场景示例
- **调度/恢复**：`TabletScheduler` 在创建/修复 replica 完成后需要写 edit log，此时向索引获取 tablet meta 和 replica。
- **运维自检**：`ReportHandler` 对比 BE 上报与 FE 记录的 tablet 列表，找出差异并触发修复。
- **Binlog/MV**：`BinlogConsumeStateVO` 根据 tablet 元信息构造扫描范围。

### 3.3 对外接口
- **外部接口**：对于用户而言，此次改动透明；`SHOW TABLET` 等 SQL 仍可查询倒排索引状况。
- **内部接口（面向开发者）**：
  - `ConcurrentLong2ObjectHashMap`：线程安全 map，支持 `get/put/putIfAbsent/computeIfAbsent/remove/values/size/clear`；
  - `TabletInvertedIndex` 新增 `getOrCreateReplicaList/removeTabletFromBackend` 等工具方法，方便调用处维护一致性；
  - 读取相关接口均返回快照 `ArrayList`，调用者可直接遍历但不要尝试修改原集合。

### 3.4 异常处理
- 当 tablet/replica 在使用过程中被并发删除时，所有 API 会返回 `null` 或空列表，调用者需做好判空；`ReportHandler`、`BinlogConsumeStateVO`、`TabletScheduler` 等典型路径已内建日志提醒。

## 4. 技术内幕解析
### 4.1 影响模块
- `TabletInvertedIndex` 及其调用方（Alter、Txn Listener、Clone、Consistency、Proc、Show、RPC、ReportHandler、Binlog）。
### 4.2 与其他功能的关系
- 替换全局锁方案后，对外部接口无行为变化；内部依赖只需要按新对象的线程安全规则使用。
### 4.3 关键技术点
- 分段锁 + 表级锁的 `ConcurrentLong2ObjectHashMap`；
- `CopyOnWriteArrayList` 存储 replica / backend 列表；
- 调用侧的 `null` 判定与日志补充，确保并发删除时 graceful degrade。
### 4.4 调研
- 原方案：全局锁 + FastUtil map，简单但阻塞严重；
- 标准库 `ConcurrentHashMap`：不支持原生 long key，需要装箱，内存开销大；
- 选用自研 map 以兼顾 long key 高效读写与线程安全。

## 5. 设计折衷
1. **为什么选择该方案**：兼顾性能与内存；保留 FastUtil 的 long key 优势，同时避免大锁。
2. **替代方案缺陷**：
   - 继续使用全局锁：性能瓶颈无法解决；
   - 直接换成 `ConcurrentHashMap<Long, V>`：频繁装箱导致 GC 压力，且无法提供 `CopyOnWrite` 式副本列表。

## 6. 详细设计
1. **核心结构**：
   - `ConcurrentLong2ObjectHashMap`：segment 数为最近 2 的幂，段内 `Long2ObjectOpenHashMap` + `ReentrantReadWriteLock`，配合表级锁控制 resize；
   - `CopyOnWriteArrayList<Replica>` / `CopyOnWriteArrayList<Long>`：在写入前剔除旧元素，确保列表中无重复 backend/tablet。
2. **主要 API 流程**：
   - `addTablet`：putIfAbsent meta；
   - `deleteTablet`：remove meta → remove replica list → 对所有 backend 调用 `removeTabletFromBackend`；
   - `addReplica`：获取复制列表 → 去重 → append；同时在 backend 列表上 `addIfAbsent`；
   - `deleteReplica`：在 COW 列表中 removeIf，成功后调用 `removeTabletFromBackend`；
   - `getReplicasOnBackendByTabletIds`：对输入 tablet 列表调用 `getReplica`，若当前 backend 无任何 replica 且 backend 列表为空则返回 `null`。
3. **调用侧修复**：
   - `BinlogConsumeStateVO#toThrift`：meta 判空；
   - `TabletScheduler#finishCreateReplicaTask`：meta 判空 + 取消任务；
   - `ReportHandler#diffWithBackend`：tablet / replica 均判空并记录 info。

## 7. 稳定性设计
- **功能边界**：所有读取 API 允许 tablet/meta 缺失；并发删除只会返回 `null` 或空结果，调用者需容忍。
- **兼容性**：无元数据/协议变更；行为对外一致。
- **对其他功能影响**：由于 API 行为未变，旧逻辑无需调整；新增判空逻辑确保 ReportHandler 等任务不会因异常中断。

## 8. 可观测性设计
- 通过以下日志/指标辅助排查：
  - BinlogConsumeStateVO、TabletScheduler、ReportHandler 中新增的 warn/info 日志；
  - `MetricRepo` 中 `TabletCount`/`ReplicateCount` 指标，可监控索引规模；
  - 可在调试时查询 `SHOW TABLET`、`TabletSchedulerDetailProcDir` 等 proc 节点查看快照。

## 9. 缺点
- `CopyOnWrite` 在写放大场景会产生额外内存拷贝；
- `ConcurrentLong2ObjectHashMap` 仍需手动维护（相较于 JDK 自带集合），需要后续测试覆盖；
- 判空逻辑依赖调用者遵守约定，若新代码遗漏判空仍可能出现问题。

## 10. 未解决问题
- 目前未针对 `getReplicaCount()` 提供锁粒度更细的统计指标；
- 未加入自动化测试覆盖所有并发路径，仍需要长期运行验证；
- 未在 ReportHandler 等关键路径引入更细粒度的差异日志（后续可考虑）。

## 11. 测试要点
- **基础功能**：增删 tablet/replica、`SHOW TABLET`、ReportHandler diff、Binlog Scan。
- **并发场景**：同时执行 clone 任务 + ReportHandler + Binlog Consume，确认无 NPE / 数据错乱。
- **异常测试**：模拟 tablet 删除后 BE 仍上报、Replica 创建任务被取消等情况。

## 12. Roadmap
1. **阶段 1（已完成）**：替换数据结构、修复高风险调用点、补充文档；
2. **阶段 2（可选）**：为 `ConcurrentLong2ObjectHashMap` 增加单元测试 / benchmark，观察 CopyOnWrite 带来的 GC 影响；
3. **阶段 3（可选）**：在 ReportHandler / TabletScheduler 等热点路径增加可观测指标，进一步优化日志。

---

## 附录：调用点与并发性梳理
本文梳理了当前 FE 代码中对 `TabletInvertedIndex` 的所有生产级引用（测试代码不在列），说明各调用点的主要逻辑、目的与并发性评估，便于后续排查潜在问题。

### Alter / Schema Change / DDL 模块

| 文件 | 主要 API | 调用逻辑与目的 | 并发性说明 |
| --- | --- | --- | --- |
| `alter/MaterializedViewHandler` | `addTablet`、`deleteTablet`、`addReplica` | Rollup/MV 创建或失败回滚时同步倒排索引 | 运行在持有 db/table 锁的 DDL 线程内，仅与 Checkpoint 线程并发 |
| `alter/SchemaChangeJobV2`、`alter/SchemaChangeHandler` | `addTablet`、`deleteTablet`、`getTabletMeta` | Schema Change 各阶段维护索引；Replay 时补齐 tablet/replica | Job 执行受 Alter Handler 锁保护 |
| `alter/OlapTableRollupJobBuilder`、`alter/OlapTableAlterJobV2Builder` | `deleteTablet` | Builder 失败时清理 tablet | 顺序执行，安全 |
| `alter/LakeTableSchemaChangeJob` / `alter/LakeRollupJob` | `addTablet`、`deleteTablet`、`getTabletMeta` | Lake 表 schema/rollup 变更同步索引 | 调用时持有 db/table 锁 |
| `alter/SystemHandler` | `getTabletIdsByBackendId`、`getReplicasByTabletId`、`getTabletMeta` | 后台周期任务判断 BE 是否可 drop | 使用快照 list，允许略陈旧 |
| `alter/dynamictablet/DynamicTabletJob`、`SplitTabletJobFactory` | `getTabletMetaList`、`getTabletMeta` | 动态 Tablet 拆分/合并读取 meta | 读取快照，`NOT_EXIST_TABLET_META` 兜底 |

### Catalog / 存储管理

| 文件 | 主要 API | 调用逻辑与目的 | 并发性说明 |
| --- | --- | --- | --- |
| `catalog/OlapTable`、`catalog/MaterializedIndex` | `addTablet`、`deleteTablet` | 创建/删除 tablet | 持有表/分区锁 |
| `catalog/LocalTablet` | `addReplica`、`deleteReplica` | Tablet 层同步 replica | `LocalTablet` 内 `rwLock` 串行化 |
| `catalog/TempPartitions` | `deleteTablet` 等 | 临时分区切换同步索引 | DDL 线程执行 |
| `catalog/CatalogRecycleBin` | `deleteTablet`、`getTabletIdsByBackendId` | 回收站清理 | 全局 recycle bin 锁 |
| `catalog/TabletStatMgr` | `getTabletMeta`、`getTabletIdsByBackendId` | Tablet 统计 | `null` 判定 + 快照遍历 |
| `catalog/TabletInvertedIndex` | 本体 | —— | 采用线程安全容器 |

### 事务监听与 Txn 流程

| 文件 | 主要 API | 调用逻辑与目的 | 并发性说明 |
| --- | --- | --- | --- |
| `transaction/OlapTableTxnStateListener`、`transaction/LakeTableTxnStateListener` | `getReplica`、`deleteTablet` | Txn 提交/回滚同步 replica 状态 | Txn 状态机单线程 |

### Clone / 调度 / Rebalance

| 文件 | 主要 API | 目的 | 并发性说明 |
| --- | --- | --- | --- |
| `clone/TabletScheduler` | `getTabletMeta`、`getReplica` | 调度修复任务 | 已在 `finishCreateReplicaTask` 判空 |
| `clone/DiskAndTabletLoadReBalancer` | `getTabletMeta`、`getReplica`、`getTabletIdsByBackendId*` | 迁移/均衡策略 | 代码已有 `null` 检查 |
| `clone/ClusterLoadStatistic`、`clone/BackendLoadStatistic` | 引用 `TabletInvertedIndex` | 统计 | 读快照 |
| `alter/dynamictablet` 模块 | 同上 | —— | 同上 |

### Consistency / Leader / 系统后台任务

| 文件 | 主要 API | 目的 | 并发性说明 |
| --- | --- | --- | --- |
| `consistency/ConsistencyChecker`、`CheckConsistencyJob` | `getTabletMeta`、`getTabletIdsByBackendId`、`deleteTablet` | 检查 FE/BE 差异 | `null` 判定，删除使用线程安全 API |
| `leader/ReportHandler` | `getTabletMeta`、`getReplica`、`getTabletIdsByBackendId` | 处理 BE report | 现已对 tablet/replica 判空并跳过 |
| `leader/LeaderImpl` | `getTabletMeta`、`getReplica` | AgentTask 回调 | 多数路径判空后返回 |
| `scheduler/mv/BinlogConsumeStateVO` | `getTabletMeta` | 构造 Binlog 范围 | 已判空并记录 warn |
| `task/PublishVersionTask` | `getReplicasOnBackendByTabletIds` | Publish Version | 支持 `null` 表示 backend 没有 replica |
| `task/TabletMetadataUpdateAgentTaskFactory` | `getTabletMeta` | 元数据更新任务 | 判空警告 |
| `common/proc/TabletSchedulerDetailProcDir` 等 | `getReplicasByTabletId`、`getTabletNumByBackendId*` | Proc 展示 | 快照读取 |
| `system/SystemInfoService` | `getTabletIdsByBackendId` | 判断 BE 下线条件 | 快照读取 |
| `lake/vacuum/AutovacuumDaemon` | `getTabletMeta` | 清理 | 判空 |

### Catalog Service / RPC / 前端服务

| 文件 | 主要 API | 目的 | 并发性说明 |
| --- | --- | --- | --- |
| `service/FrontendServiceImpl` | `getTabletMeta` | RPC `getPartitionMeta` | `null` 过滤后返回 |
| `server/LocalMetastore` | `addTablet`、`deleteTablet`、`getTabletMeta` | DDL/DML 主流程 | 持有 db/table 锁 |
| `server/GlobalStateMgr` | 持有实例 | —— | 单例初始化 |
| `memory/MemoryUsageTracker`、`metric/MetricRepo` | 统计 | —— | 读快照 |

### Query / Planner / Proc

| 文件 | 主要 API | 目的 | 并发性说明 |
| --- | --- | --- | --- |
| `planner/BinlogScanNode` | `getTabletMeta` | 生成 Binlog Scan 范围 | Analyzer 已锁定表/分区 |
| `qe/ShowExecutor` | `getTabletMeta`、`getReplicasByTabletId` | `SHOW TABLET` | `null` 转为 `NOT_EXIST_VALUE` |
| `common/proc/*` | `getTabletIdsByBackendId*`、`getReplicasByTabletId` | 调试 | 快照读取 |

### Load / Backup / Export / 其他

| 文件 | 主要 API | 目的 | 并发性说明 |
| --- | --- | --- | --- |
| `backup/RestoreJob`、`lake/backup/LakeRestoreJob` | `addTablet`、`getTabletMeta` | Restore 完成后注册 tablet | Job 构建线程串行执行 |
| `load/ExportJob` | `getTabletMeta` | 构造导出计划 | `null` 跳过 |
| `load/OlapDeleteJob`、`load/PartitionUtils` | `deleteTablet` | Drop Partition / Delete | DDL 线程执行 |
| `task/TabletMetadataUpdateAgentTaskFactory` | 已述 | —— | 判空记录 |
