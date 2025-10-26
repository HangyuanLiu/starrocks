# StarRocks Catalog 方案一（BE RPC / Connector 模式）

## 背景与动因
- 当前 StarRocks 集群常按业务域拆分部署，公共表和业务表分布在不同实例，联邦分析需要跨集群访问。
- 既有 JDBC Catalog 缺乏 Tablet 粒度切片与谓词下推，查询依赖外部 BE 返回完整结果集，无法满足秒级 SLA。
- 将外部表同步到单一集群不仅增加同步链路成本，也难以保证一致性。Spark/Trino 等生态的 StarRocks Connector 已经验证了“客户端通过 BE RPC 获取数据”的模式，可作为快速落地方案。

## 目标
### 功能目标
- 新增 `type = "starrocks"` External Catalog，通过 `_query_plan` + BE Thrift RPC 访问外部 StarRocks 集群数据。
- 支持 `shared_nothing` 与 `shared_data` 外部部署，兼容未来直读对象存储的扩展。

### 性能目标
- 借助 Tablet 切片、谓词下推、批量 RPC，实现 ≥1 GB/s 的扫描吞吐。
- 控制跨集群 RPC 并发与批大小，保证延迟稳定在秒级范围内。

## 用户接口
### 创建 Catalog
```sql
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES (
    "type" = "starrocks",
    "starrocks.run_mode" = "shared_nothing",
    "starrocks.fetch.mode" = "rpc",
    "starrocks.fe.http.url" = "...",
    "starrocks.fe.jdbc.url" = "...",
    "starrocks.user" = "...",
    "starrocks.password" = "...",
    "starrocks.be.rpc.endpoints" = "be1:9060,be2:9060",
    "starrocks.request.retries" = "3",
    "starrocks.request.connect.timeout.ms" = "10000",
    "starrocks.request.read.timeout.ms" = "30000",
    "starrocks.request.query.timeout.s" = "3600",
    "starrocks.batch.size" = "4096",
    "starrocks.exec.mem.limit" = "2147483648",
    "starrocks.enable_data_cache" = "true",
    StorageCredentialParams
);
```

| 参数 | 是否必须 | 默认值 | 说明 |
| ---- | -------- | ------ | ---- |
| `starrocks.run_mode` | 是 | — | 外部集群运行模式：`shared_nothing` / `shared_data`。决定是否需要对象存储参数。 |
| `starrocks.fetch.mode` | 否 | `rpc` | 数据获取方式，表示通过 BE Thrift 接口拉取数据。 |
| `starrocks.fe.http.url` | 是 | — | 外部 FE HTTP 地址，用于调用 `_query_plan`。 |
| `starrocks.fe.jdbc.url` | 是 | — | 外部 FE MySQL 地址，用于 JDBC 元数据拉取。 |
| `starrocks.user` / `starrocks.password` | 是 | — | REST/JDBC 认证信息，支持敏感字段脱敏。 |
| `starrocks.be.rpc.endpoints` | 是 | — | 外部 BE/CN Thrift 地址列表，可配权重或优先级。 |
| `starrocks.request.retries` | 否 | `3` | 单次 RPC 失败后的重试次数（含切换节点）。 |
| `starrocks.request.connect.timeout.ms` | 否 | `10000` | Thrift 连接超时。 |
| `starrocks.request.read.timeout.ms` | 否 | `30000` | Thrift 读取超时。 |
| `starrocks.request.query.timeout.s` | 否 | `3600` | 透传给外部 BE 的查询超时。 |
| `starrocks.batch.size` | 否 | `4096` | 每次拉取的最大行数或 Arrow batch 大小。 |
| `starrocks.exec.mem.limit` | 否 | `2147483648` | 单查询内存限制（默认 2 GB）。 |
| `starrocks.enable_data_cache` | 否 | `true` | 是否启用 Data Cache，命中后避免重复 RPC。 |
| `StorageCredentialParams` | 否 | — | 当外部集群为 `shared_data` 时预留对象存储参数，后续可无缝切换直读。 |

## 架构设计
### 总体流程
1. **Catalog 注册**：`CatalogMgr` 通过 `ConnectorMgr` 加载 `StarRocksConnector`，持久化 Catalog 属性并注册到全局元数据。
2. **元数据同步**：`StarRocksConnector` 使用 JDBC 获取库/表/列信息，通过 REST `_query_plan` 获取 Tablet 列表、外部执行计划。
3. **计划生成**：优化器为外部表生成 `Logical/PhysicalStarRocksScanOperator`，`PlanFragmentBuilder` 构造 `StarRocksScanNode`，封装 `_query_plan`、Tablet 元数据和 RPC 参数。
4. **执行阶段**：BE/CN Pipeline 创建 `StarRocksScanOperator`，通过 Thrift RPC 与外部 BE 建立 Scanner，周期性调用 `get_next` 拉取数据。
5. **数据缓存**：若开启 Data Cache，则在获取数据后写入缓存，后续命中直接返回。

### 元数据同步（FE）
- **Connector 框架复用**：在 `ConnectorType`/`ConnectorFactory` 中注册 `STARROCKS` 类型，复用 `ConnectorMgr` 生命周期管理。
- **JDBC 元数据**：参考 `JDBCConnector`，通过 `StarRocksConnectorMetadata` 调用外部 FE MySQL 接口刷新库表结构，缓存于 `StarRocksMetadataCache`。
- **REST `_query_plan`**：调用 `/api/{db}/{table}/_query_plan`，解析 `opaqued_query_plan` 包含的 `TQueryPlanInfo`，提取 Tablet 列表、Fragment、Output Schema 等信息。
- **Tablet 缓存**：将 Tablet -> BE 映射、分区信息、版本号缓存于 FE，可支持增量刷新与 TTL。
- **刷新机制**：`REFRESH EXTERNAL STARROCKS CATALOG <db>.<table>` 清理缓存并强制拉新 `_query_plan`；支持错误回退与告警。

### 查询计划生成
- **Analyzer 扩展**：`CatalogAnalyzer` 识别 `STARROCKS` Catalog，校验参数、权限，构建外部表对象。
- **逻辑/物理算子**：新增 `LogicalStarRocksScanOperator`（记录列裁剪、谓词等）与 `PhysicalStarRocksScanOperator`（携带 Tablet Range、RPC 参数）。
- **Plan 构建**：`PlanFragmentBuilder` 调用 `StarRocksScanNode`，封装 `_query_plan`、Tablet 分片，生成 `StarRocksScanRange`。
- **Explain 输出**：`StarRocksScanNode` 在 Explain 中展示外部集群、Tablet 数量、BE 节点、启用 Data Cache 情况。
- **统计信息**：在 `StatisticsCalculator` 中增加逻辑，缺省使用外部表估算或向外部拉取统计（可后续扩展）。

### 执行链路（CN/BE）
- **Thrift 扩展**：在 `PlanNodes.thrift`/`BackendService.thrift` 中增加 `TStarRocksScanNode`/`TStarRocksScanRange` 字段（包含 `_query_plan`、Tablet 列表、RPC 参数、Data Cache 配置）。
- **Scan Node/Operator**：
  - FE 序列化 `StarRocksScanNode`，写入 Thrift。
  - BE 新增 `StarRocksScanNode`（计划层）和 `StarRocksScanOperator`（Pipeline 执行层），负责管理 Scanner 生命周期。
- **RPC 调用**：
  - 使用 `ThriftConnectionPool` 与 `TStarrocksExternalService` 建立连接。
  - `open_scanner` 传入 `_query_plan`、Tablet、投影列、谓词，获取 `scanner_id`。
  - `get_next` 按 `batch.size` 获取数据，转换为 `Chunk`。
  - `close_scanner` 释放资源，处理异常重试。
- **重试与故障转移**：支持对单 Tablet 切换到下一个可用 BE，或按照策略退避/熔断。
- **Data Cache**：在 `StarRocksScanOperator` 中，根据配置调用 Data Cache 接口，维护命中/未命中指标，与 Profile/监控对接。

### 稳定性设计
- **并发控制**：限制单查询/全局外部 RPC 并发度，防止压垮外部集群；可通过 BE 配置或 Catalog 参数控制。
- **超时和重试**：对连接、读、查询设置独立超时与重试，可配置退避策略；超过阈值触发熔断。
- **资源隔离**：结合 `starrocks.exec.mem.limit`、BE 线程池限制、Data Cache 配额确保不会影响本地任务。
- **错误处理**：分类处理网络、认证、权限、查询异常，提供可读错误信息和 JSON Trace。

### 可观测性
- **日志**：Connector 记录 `_query_plan` 调用、外部 BE 地址；BE 记录每次 RPC 请求、重试、切换节点。
- **Profile**：增加 `ExternalRpcTime`、`ExternalRpcRetry`、`ExternalBytesRead`、`DataCacheHitRatio` 等指标。
- **监控**：在指标体系中增加外部 RPC QPS、平均/最大延迟、失败率；支持按外部 BE、Catalog drill down。
- **告警**：对连续失败、熔断、认证错误提供告警。

## 测试策略
- **单元测试**
  - FE：`StarRocksCatalogPropertiesTest`、`StarRocksConnectorMetadataTest`、`StarRocksMetadataCacheTest`。
  - 优化器：`StarRocksScanRuleTest`，验证谓词推送、列裁剪。
  - BE：`StarRocksScanOperatorTest`，Mock 外部 RPC 验证生命周期。
- **集成测试**
  - PseudoCluster：启动两套 FE/BE，模拟外部集群，验证端到端查询、刷新、重试。
  - MinIO（可选）模拟 shared_data，验证对象存储字段兼容性。
- **性能测试**
  - TPC-H/TPC-DS，比较 JDBC Catalog vs RPC 模式的性能指标。
  - 压测不同并发、批量大小，观察外部集群负载。
- **异常测试**
  - 模拟外部 BE 宕机、网络抖动、认证失败。
  - 验证重试、熔断、告警。

## Roadmap（示例）
| 阶段 | 工作项 | 时间 |
| ---- | ------ | ---- |
| Metadata | Connector 注册、属性解析、REST/JDBC 拉取 | 7.14 – 7.25 |
| Plan | Logical/Physical Scan、`StarRocksScanNode`、Scan Range | 7.25 – 8.05 |
| RPC 执行 | Thrift 扩展、Scan Operator、Data Cache 对接 | 7.25 – 8.20 |
| 测试 | UT/IT/压力/故障注入、监控脚本 | 8.10 – 9.01 |
| 交付 | 文档、告警、运维脚本、灰度方案 | 9.01 – 9.15 |

## 模块拆解与实现计划

| 模块 | 主要职责 | 拆分任务 | 关键修改文件 |
| ---- | -------- | -------- | ------------ |
| FE Connector 层 | Catalog 生命周期、属性解析、REST/JDBC 元数据、Tablet 缓存 | 类型注册 → 属性解析 → REST `_query_plan` → MetadataCache → 刷新与权限 | `ConnectorType.java`、`ConnectorFactory.java`、`com/starrocks/connector/starrocks/*`（新增）、`RestClient.java`、`GlobalStateMgr.java` |
| 查询规划层 | 解析外部表、构建物理计划、生成 Scan Range | Analyzer 支持 → Logical/Physical Operator → `StarRocksScanNode` → Explain/统计 | `CatalogAnalyzer.java`、`LogicalStarRocksScanOperator.java`、`PhysicalStarRocksScanOperator.java`、`PlanFragmentBuilder.java`、`StarRocksScanNode.java`、`StatisticsCalculator.java`、`PlanNodes.thrift` |
| RPC 执行层 | Thrift 字段扩展、Scan Operator、RPC 生命周期、Chunk 转换 | Thrift 生成 → FE 序列化 → BE `StarRocksScanNode` → `StarRocksScanOperator` → `ThriftConnectionPool` 扩展 | `gensrc/thrift/PlanNodes.thrift`、`gensrc/thrift/BackendService.thrift`、`be/src/plan/starrocks_scan_node.*`、`be/src/exec/pipeline/scan/starrocks_scan_operator.*`、`be/src/runtime/thrift_connection_pool.*` |
| Data Cache 与监控 | Data Cache 集成、Profile/监控指标、熔断策略 | 缓存写入 → 命中统计 → Profile 输出 → 监控脚本 | `be/src/cache/*`、`be/src/exec/pipeline/scan/*`、`be/src/runtime/profile.cpp`、`tools/profile`、监控配置 |
| 配置与安全 | 多 BE 配置、熔断、权限和安全管理 | 参数校验 → BE 权重/优先级 → 凭据管理 → TLS/Kerberos（可选） | `StarRocksCatalogProperties.java`、`PropertyAnalyzer.java`、`CloudConfiguration.java`、配置文档 |
| 测试与运维 | UT/IT/压测/故障注入、文档与脚本 | FE/BE UT → PseudoCluster → 压测脚本 → 故障注入 → 运维指引 | `fe/tests`、`be/tests`、`fe/pseudocluster`、`tools/benchmark`、`docs/zh|en/data_source/catalog/starrocks_catalog_rpc.md` |

### 实施步骤（迭代计划）
| 阶段 | 范围 | 关键修改文件 | 验证方式 |
| ---- | ---- | ------------ | -------- |
| 1. Connector 骨架 | 注册 Catalog、属性解析、REST/JDBC mock | `ConnectorType.java`、`ConnectorFactory.java`、`StarRocksCatalogProperties.java`、`StarRocksConnector.java` | FE UT：属性解析、`SHOW CATALOGS` |
| 2. 元数据与缓存 | 打通 JDBC/REST、缓存 `_query_plan` | `StarRocksConnectorMetadata.java`、`StarRocksMetadataCache.java`、`RestClient.java` | FE UT + PseudoCluster：`SHOW TABLES`、`REFRESH` |
| 3. 计划生成 | Logical/Physical Operator、`StarRocksScanNode` | `PlanFragmentBuilder.java`、`Logical/PhysicalStarRocksScanOperator.java`、`StarRocksScanNode.java` | FE UT：Explain/Plan、列裁剪 |
| 4. RPC 执行链路 | Thrift 扩展、BE Scan Operator、RPC 客户端 | `PlanNodes.thrift`、`BackendService.thrift`、`be/src/plan/`、`be/src/exec/pipeline/scan/`、`thrift_connection_pool.cpp` | BE UT、端到端查询 |
| 5. Data Cache & 监控 | 缓存写入、指标、熔断策略 | `be/src/exec/pipeline/scan/starrocks_*`、`be/src/cache`、监控脚本 | Profile/监控验证 |
| 6. 配置与测试交付 | 凭据管理、压测、故障注入、文档 | `PropertyAnalyzer.java`、`CloudConfiguration.java`、`docs/*`、测试脚本 | 压测报告、运维文档 |

完成每个阶段后可形成可评审的 PR，降低集成风险。

## 风险与权衡
- **外部资源消耗**：RPC 模式依赖外部 BE，需与目标集群协商资源配额；可通过并发控制与查询限流缓解。
- **网络链路**：跨集群网络延迟与稳定性直接影响查询表现，需要在配置中引入重试、熔断、备用节点。
- **安全性**：Thrift 连接需考虑认证（账号/密码、Kerberos）与 TLS；Catalog 属性、日志中需要脱敏。
- **性能上限**：相比对象存储直读，受外部 BE CPU/内存限制，峰值吞吐受制于外部集群；作为快速落地方案可接受。
- **扩展性**：既能与直读方案共存，也可作为 fallback 模式，在对象存储不可达或权限受限时使用。

## 适用场景
- 目标集群主要为存算一体，暂未部署对象存储直读能力。
- 项目需快速交付，想复用 Spark/Trino Connector 确认过的模式。
- 运维可接受外部 BE 的额外负载，并能协作配置 RPC 凭据与监控。

## 后续扩展
- 支持 `starrocks.fetch.mode` 在 `rpc`/`object_store` 间切换，或根据运行情况自动降级。
- 引入查询配额与熔断策略，按用户或 Catalog 维度控制 RPC 并发。
- 向外部集群获取统计信息，优化成本估计。
- 支持列级加密、压缩传输、行列过滤能力同步，与外部 BE 功能演进保持一致。
