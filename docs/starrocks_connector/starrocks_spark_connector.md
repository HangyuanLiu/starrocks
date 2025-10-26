# StarRocks Spark Connector 数据访问实现总结

## 概述
StarRocks Connector for Apache Spark™（简称 Spark Connector）提供了 Spark DataSource 能力，将 StarRocks 表映射为 Spark SQL / DataFrame / RDD，可同时支持读取与写入。本说明聚焦「读取 StarRocks 数据」的实现原理、模块划分、代码组织以及开发测试要点。

Spark Connector 的核心思路是：
1. 在 Spark 侧通过 DataSource V2 接口接收用户配置；
2. 调用 StarRocks FE REST `_query_plan` 接口获取单表执行计划（`TQueryPlanInfo`）与 Tablet 列表；
3. 将 Tablet 映射为 Spark 的 InputPartition；
4. 在每个分区内，通过 Thrift RPC 调用 `TStarrocksExternalService`（BE 侧 Provider 能力）获取 Arrow RecordBatch，并转换为 Spark `InternalRow`。

写入能力（1.1.0 起支持）则复用 StarRocks 的 Stream Load / Routine Load，本篇一并列出模块，但重点描述读取流程。

## 模块与代码组织

| 模块层级 | 类/包（基于 `com.starrocks.connector.spark`） | 关键职责 |
| -------- | ----------------------------------------- | -------- |
| DataSource 接口 | `StarrocksSource`、`StarrocksTable` | 实现 DataSource V2（`TableProvider`、`SupportsRead/Write`），创建 Scan/Sink Builder |
| 参数解析 | `StarrocksOptions`、`StarrocksConfig`、`StarrocksSinkOptions` | 解析 DataFrameReader/DataFrameWriter 中的 `starrocks.*` 参数，提供默认值及校验 |
| REST 客户端 | `StarrocksRestService`、`StarrocksRestClient` | 调用 FE `_query_plan`、`/schema`、`/partition`、`/be_nodes`，支持 BasicAuth、重试、超时 |
| 元数据模型 | `StarrocksQueryPlan`, `StarrocksPartition` | 解析 `_query_plan` 返回的 JSON（Tablet 与路由信息），封装成 Spark InputPartition 需要的数据结构 |
| Pushdown 能力 | `StarrocksScanBuilder`（实现 `SupportsPushDownFilters/RequiredColumns` 等） | 将 Spark 谓词/列裁剪转为 REST 请求参数或 `_query_plan` 中的 select list |
| Scan / Partition | `StarrocksScan`, `StarrocksInputPartition`、`StarrocksPartitionReaderFactory` | 构建 Spark 执行计划，生成分区及 Reader |
| RPC 读取 | `StarrocksPartitionReader`、`StarrocksScanner` | 使用 Thrift `open_scanner/get_next/close_scanner` 调用 Provider 集群，返回 Arrow 数据；转换成 Spark `InternalRow` |
| Arrow 转换 | `ArrowUtils`、`StarrocksFieldConverter` | 将 Arrow Vector 转成 Spark Catalyst 类型，处理复杂类型和时区 |
| 写入（可选） | `StarrocksWriteBuilder`、`StarrocksDataWriter` | 打包数据并发送给 StarRocks Stream Load/Routine Load |
| 工具 & 保障 | `Retryer`、`ThriftClientPool`、`MetricsReporter` | 封装重试策略、连接池、统计指标 |

项目结构（简化）：
```
starrocks-connector-for-apache-spark/
 ├─ spark-source/              # 核心 DataSource 代码
 │   ├─ src/main/scala/com/starrocks/connector/spark/sql/...
 │   └─ src/main/java/com/starrocks/connector/spark/sql/...
 ├─ spark-sink/                # 写入相关模块
 ├─ common/                    # 通用常量、枚举、工具
 ├─ pom.xml / build.sbt        # 构建配置
 └─ docs/examples              # 示例程序
```

## 读取流程 (Spark SQL / DataFrame / RDD)
1. **配置阶段**  
   用户在 Spark 中设置：
   ```scala
   spark.read
     .format("starrocks")
     .option("starrocks.fe.http.url", "http://fe:8030")
     .option("starrocks.be.http.url", "http://be1:8040;http://be2:8040") // 写入时使用
     .option("starrocks.table.identifier", "database.table")
     .option("starrocks.user", "xxx")
     .option("starrocks.password", "yyy")
     .option("starrocks.columns", "c1,c2,c3") // 可选列裁剪
     .option("starrocks.filter.query", "c1 > 100") // 可选谓词
     .load()
   ```
   `StarrocksOptions` 完成参数解析与默认值填充，并校验必填项。

2. **DataSource 初始化**  
   `StarrocksSource` 返回 `StarrocksTable`，其 `newScanBuilder` 创建 `StarrocksScanBuilder`。
   - `StarrocksScanBuilder` 实现 `SupportsPushDownFilters`、`SupportsPushDownRequiredColumns` 等接口，将 Spark Filter/Projection 同步到内部配置。

3. **获取 `_query_plan`**  
   `StarrocksScanBuilder` 调用 `StarrocksRestService.getQueryPlan`，发送请求：
   ```json
   POST http://fe/api/{db}/{table}/_query_plan
   {
     "sql": "SELECT <cols> FROM <table> WHERE <pushdown_filters>"
   }
   ```
   FE 返回 JSON：
   ```json
   {
     "partitions": {
       "123456": {"routings": ["be1:9060", "be2:9060"], "version": 20, "schemaHash": 1234},
       ...
     },
     "opaqued_query_plan": "<base64 TQueryPlanInfo>",
     "status": 200
   }
   ```
   Connector 将其解析为 `StarrocksQueryPlan`（包含 Tablet→BE 映射、PlanFragment、输出列信息）。

4. **构建 Scan / Partition**  
   - `StarrocksScan`（实现 Spark `Scan` + `Batch`）在 `planInputPartitions` 中根据 Tablet 列表生成 `StarrocksInputPartition`，可按 Tablet 数量或并发限制切分。
   - `StarrocksInputPartition` 携带 Tablet ID、`opaqued_query_plan`、FE/BE 连接信息。
   - `createReaderFactory` 返回 `StarrocksPartitionReaderFactory`，实例化 `StarrocksPartitionReader`。

5. **执行读取**
   - `StarrocksPartitionReader` 调用 `StarrocksScanner`：
     1. `open_scanner`：提交 Tablet ID 列表、PlanFragment、所需列、batch size 等，获取 `context_id` 与 `selected_columns`。
     2. `get_next`：循环拉取 Arrow RecordBatch，针对 `starrocks.batch.size` 控制批尺寸；将二进制解码为 Arrow Vector。
     3. Arrow → Spark：`ArrowUtils` 将 Arrow Vector 映射到 Spark Catalyst 类型，处理时间、decimal、复杂类型等。
     4. EOS：当 `eos=true` 时结束，调用 `close_scanner`。
   - Reader 将 `InternalRow` 返回给 Spark，引擎完成进一步计算。

6. **任务结束与资源回收**  
   - 成功或异常时都会调用 `close()` 释放 Thrift 连接。
   - 支持失败重试（按 Tablet/Scanner 维度），必要时回退到其他 BE。

## 写入流程概览（1.1.0+）
1. `StarrocksTable` 同时实现 `SupportsWrite`，`newWriteBuilder` → `StarrocksWriteBuilder`。
2. 通过 `StarrocksSinkOptions` 解析目标表、Stream Load 相关参数。
3. Writer 将 DataFrame 分区数据转换为 CSV/JSON/Parquet（取决于配置），调用 StarRocks Stream Load 或 Routine Load。
4. 写入完成后可选择性地提交/回滚（幂等控制）。

## 关键参数（读取）

| 参数 | 说明 |
| ---- | ---- |
| `starrocks.table.identifier` | 必填，格式 `<db>.<table>` |
| `starrocks.fe.http.url` | 必填，FE HTTP 地址，支持多个（分号分隔） |
| `starrocks.user/password` | 必填，认证信息 |
| `starrocks.columns` | 可选，逗号分隔的列列表；若缺省则读取所有列 |
| `starrocks.filter.query` | 可选，按 SQL 语法构造的谓词字符串，由 FE 解析 |
| `starrocks.request.retries` / `starrocks.request.retry.interval.ms` | REST/RPC 重试次数及间隔 |
| `starrocks.exec.mem.limit` | 针对单次 Scanner 的内存限制（传给 Provider） |
| `starrocks.batch.size` | 每次 `get_next` 返回的最大行数 |
| `starrocks.desensitize` | 控制日志脱敏 |

写入相关参数包括 `starrocks.write.properties.*`、`starrocks.sink.buffer-size` 等，此处略。

## 设计思路与原理
- **Plan 下推**：通过 REST `_query_plan` 将 Spark 的列裁剪、谓词尽可能推送到 StarRocks，减少网络传输。
- **Tablet 并行**：以 Tablet 为粒度构建 Spark 分区，实现 StarRocks 与 Spark 之间的并行流水线，充分利用 BE 端的 Tablet 副本与 Spark Executor 的并发能力。
- **Arrow 数据格式**：选择 Arrow 作为传输格式，减少序列化开销，并可快速转换为 Spark Catalyst。
- **可靠性**：REST/RPC 均内置重试策略；`StarrocksScanner` 支持切换到其他副本；Spark 任务失败可自动重试。
- **安全性**：支持 BasicAuth；可在配置中启用 HTTPS；敏感信息在日志中脱敏。
- **双向能力**：通过 `SupportsWrite` 扩展写入路径，使用 Stream Load/Routine Load，保持读写一致性。

## 开发与扩展计划建议
| 阶段 | 内容 | 验证 |
| ---- | ---- | ---- |
| 1. 架构梳理 | 整理 DataSource V2 + REST/RPC 基础骨架，完成文档与代码注释 | 单元测试、自定义示例 |
| 2. 功能增强 | 支持更多谓词/列类型推下、统计信息缓存、自动并发控制 | 集成测试、TPC-H 场景 |
| 3. 性能优化 | Arrow 批量调优、异步预取、向量化转换优化 | 压测、Profile |
| 4. 安全与运维 | TLS/Kerberos、凭据轮转、监控指标（RPC 延迟、吞吐）、诊断工具 | 运维脚本、监控接入 |
| 5. 生态整合 | 与 StarRocks Catalog（Consumer 角色）联合测试，确保模式兼容 | 端到端联调 |

## 测试与验证要点
- **单元测试**：选项解析、REST 响应解析、Spark Filter/Projection pushdown。
- **集成测试**：本地或 PseudoCluster 搭建 StarRocks，验证 DataFrame 读取结果、容错与性能。
- **异常注入**：模拟 REST 失败、Thrift 断连、Tablet 不存在、认证失败，确认重试与错误信息。
- **性能压测**：在不同规模下评估 TPS/QPS、延迟、资源占用；比较不同 batch size 与并发设置。
- **写入路径**：验证 Stream Load 成功、错误反馈、幂等控制。

## 与 Provider/Consumer 角色的关系
- Spark Connector 在「Consumer」侧扮演外部查询引擎，通过 REST + Thrift 调用 StarRocks Provider 能力；其读取流程与本文档的 External Catalog Consumer 基本一致。
- 当 StarRocks 自身扮演 Consumer 时，可复用大量逻辑（属性解析、REST `_query_plan`、Thrift 调度、Arrow 转换）；Spark Connector 提供了成熟的工程实践参考。

## 资料与参考
- [`starrocks-connector-for-apache-spark` GitHub 仓库](https://github.com/StarRocks/starrocks-connector-for-apache-spark)
- [读取 StarRocks 的官方文档](https://docs.starrocks.io/docs/unloading/Spark_connector/)
- [Spark DataSource V2 官方文档](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)
