# StarRocks 作为查询引擎消费外部 StarRocks Catalog 的实现说明

## 角色定位与使用场景
- **角色**：当前集群作为“查询引擎（Consumer）”，通过 External Catalog 访问其他 StarRocks 集群的数据，支持跨集群 Join/分析。
- **入口**：用户在当前集群创建 `type = "starrocks"` 的 External Catalog；查询阶段可选择两种取数模式：
  - `fetch.mode = "rpc"`：调用外部 BE 的 `TStarrocksExternalService`（Vendor 兼容模式）。
  - `fetch.mode = "object_store"`：直读外部集群的对象存储 Segment（高性能模式）。
- **目标**：提供统一的元数据管理、计划生成与执行链路，对上层 SQL 无感知。

## 总体架构概览
```
        当前集群（Consumer）                                    外部 StarRocks 集群（Provider）
┌──────────────────────────────────────┐            ┌─────────────────────────────────┐
│ FE: ConnectorMgr / CatalogMgr        │            │ FE: TableQueryPlanAction        │
│  └─ StarRocksConnector               │ REST _plan │  └─ 单节点计划 + Tablet 列表     │
│      └─ RestClient / JDBC            ├───────────►│                                 │
│                                      │            └─────────────────────────────────┘
│ Optimizer: Logical/Physical Scan     │
│  └─ StarRocksScanNode                │            ┌─────────────────────────────────┐
│                                      │  RPC/FS    │ BE: TStarrocksExternalService    │
│ BE/CN Pipeline                       ├───────────►│  └─ FragmentMgr / TabletReader   │
│  └─ StarRocksScanOperator            │            └─────────────────────────────────┘
│      └─ fetch.mode=rpc ⇒ RPC Reader  │
│      └─ fetch.mode=object_store ⇒ format-sdk Reader │
└──────────────────────────────────────┘
```

## 模块与代码组织关系
| 层级 | 模块/类 | 关键职责 | 主要文件 |
| ---- | ------- | -------- | -------- |
| FE Connector | `StarRocksConnector`、`StarRocksConnectorMetadata` | Catalog 注册、属性解析、JDBC/REST 元数据同步、缓存刷新 | `fe/fe-core/src/main/java/com/starrocks/connector/starrocks/*`（新增） |
| REST 客户端 | `StarRocksRestClient` | 封装 `_query_plan` 等 REST 调用，处理超时/重试 | `format-sdk/src/main/java/com/starrocks/format/rest/RestClient.java` |
| 元数据缓存 | `StarRocksMetadataCache` | 缓存库表、分区、Tablet、版本信息 | 同上 |
| 优化器 | `LogicalStarRocksScanOperator` / `PhysicalStarRocksScanOperator` | 承载列裁剪、谓词推送、Scan Range 信息 | `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical|physical` |
| Plan 构建 | `StarRocksScanNode` | 将 `_query_plan`、Tablet 元数据、取数参数写入 Thrift PlanNode | `fe/fe-core/src/main/java/com/starrocks/planner/StarRocksScanNode.java` |
| 执行层（RPC 模式） | `StarRocksScanOperator` + RPC Scanner | 通过 Thrift RPC (open/get_next/close) 获取数据，转换为 Chunk | `be/src/exec/pipeline/scan/starrocks_scan_operator.*`、`be/src/runtime/thrift_connection_pool.*` |
| 执行层（直读模式） | `StarRocksScanOperator` + `StarRocksFormatReader` | 解析 `_query_plan`，直接读对象存储 Segment，配合 Data Cache | `format-sdk/src/main/cpp/format/starrocks_format_reader.cpp`、`be/src/storage/lake/tablet_reader.cpp` |
| 配置与凭据 | `StarRocksCatalogProperties`、`CloudConfiguration` | 解析 `fetch.mode`、RPC/对象存储参数、Storage Volume 映射 | `fe/fe-core/src/main/java/com/starrocks/connector/starrocks/StarRocksCatalogProperties.java`、`CloudConfiguration.java` |

### 与 Spark Connector 的实现关系
- StarRocks 内部 Consumer 的 RPC 模式与 Spark Connector 的读取链路完全一致：均复用 FE `_query_plan`、BE `open_scanner/get_next/close_scanner`、Arrow 数据格式以及 Tablet 粒度分片。因此在实现上可以参考 Connector 的 `StarrocksScanBuilder`、`StarrocksPartitionReader` 等模块。
- 直读模式可视作 Spark Connector 的「未来形态」扩展：在 Connector 中也正在尝试通过 `_query_plan` 获得对象存储路径并直接读取 Segment；StarRocks Consumer 需要确保 `_query_plan` 中包含足够的存储信息（version、schemaHash、path），以便双方共享协议。
- 参数对齐：Catalog 属性与 Spark Connector 的 Reader Options (`starrocks.columns`、`starrocks.filter.query`、`starrocks.batch.size` 等) 保持相同语义，便于迁移与故障切换。

## 功能实现原理
### 1. 元数据与 `_query_plan` 获取
1. 解析 Catalog 属性（`StarRocksCatalogProperties`）：
   - 基础信息：外部 FE HTTP/JDBC 地址、认证信息；
   - 取数模式：`fetch.mode`（默认 `object_store`）；
   - RPC 参数或对象存储凭据。
2. 通过 JDBC (`StarRocksConnectorMetadata`) 拉取库、表、列、分区等元数据；
3. 通过 REST `_query_plan` 获取单表计划、Tablet 列表与 `TQueryPlanInfo`；
4. 将 Tablet → 版本/SchemaHash、对象存储路径（直读模式）等信息写入缓存。

### 2. 查询计划生成
1. `CatalogAnalyzer` 识别 `STARROCKS` Catalog，并创建外部表对象；
2. `LogicalStarRocksScanOperator` 记录列裁剪、谓词信息；
3. `PhysicalStarRocksScanOperator` 在 Memo 中携带 Tablet 分片、取数参数；
4. `PlanFragmentBuilder` 生成 `StarRocksScanNode`，根据 `fetch.mode`：
   - `rpc`：构造 `TStarRocksScanRange`，包含 `_query_plan`、Tablet、RPC 参数；
   - `object_store`：构造 `THdfsScanNode`/`TLakeScanNode`，携带对象存储路径、凭据；
5. `Explain` 输出外部 Catalog、表、Tablet 数、取数模式、Data Cache 状态。

### 3. 执行层取数
#### RPC 模式
1. `StarRocksScanOperator` 通过 `ThriftConnectionPool` 调用外部集群的 `open_scanner`；
2. 获取 `context_id` 后循环调用 `get_next`，接收 Arrow RecordBatch 并转换为 `Chunk`；
3. 支持重试、offset 校验、Data Cache 写入；
4. 查询结束或错误时调用 `close_scanner`，释放外部资源。

#### 对象存储直读模式
1. `StarRocksScanOperator` 根据 Tablet 分片与 `_query_plan` 构造 `TabletReaderParams`；
2. `StarRocksFormatReader`（format-sdk）解析 `_query_plan` 输出表达式与谓词，将对象存储路径传给 `TabletReader`；
3. `TabletReader` 按 Segment 读取对象存储文件，支持物理/逻辑切分和 Data Cache；
4. 结果转换为 `Chunk` 并返回上层。

## 开发计划与步骤
| 阶段 | 范围 | 关键修改文件 | 验证方式 |
| ---- | ---- | ------------ | -------- |
| 1. Connector 框架 | 注册 Catalog、属性解析、REST/JDBC mock | `ConnectorType.java`、`ConnectorFactory.java`、`StarRocksCatalogProperties.java`、`StarRocksConnector.java` | FE UT：属性解析、`SHOW CATALOGS` |
| 2. 元数据 & 缓存 | 实现 `_query_plan` 获取、Tablet 缓存、`REFRESH` | `StarRocksConnectorMetadata.java`、`StarRocksMetadataCache.java`、`RestClient.java` | FE UT + PseudoCluster：`SHOW TABLES`、`REFRESH` |
| 3. 计划生成 | Logical/Physical Operator、`StarRocksScanNode`、Explain | `PlanFragmentBuilder.java`、`Logical/PhysicalStarRocksScanOperator.java`、`StarRocksScanNode.java` | Plan/Explain UT，谓词/列裁剪验证 |
| 4. 执行链路（RPC） | Thrift 扩展、BE Scan Operator、RPC 客户端 | `PlanNodes.thrift`、`BackendService.thrift`、`be/src/exec/pipeline/scan/starrocks_*` | BE UT、端到端查询、与 Provider 集群联调 |
| 5. 执行链路（直读） | format-sdk Reader、`TabletReader` 参数、Data Cache | `starrocks_format_reader.cpp`、`tablet_reader.cpp`、`StarRocksScanOperator` | MinIO/S3 测试、性能压测 |
| 6. 配置与运维 | Storage Volume 映射、凭据、安全、监控 | `StorageVolumeMgr.java`、`CloudConfiguration.java`、`PropertyAnalyzer.java`、`docs/*` | 压测、故障注入、文档交付 |

## 设计原则与考量
- **模式兼容**：以 `fetch.mode` 切换 RPC 与直读，实现平滑迁移；当对象存储不可达时可降级至 RPC。
- **单表计划**：与 Provider 保持一致，使用单节点计划，Fragment 内部由当前集群调度。
- **Data Cache 与列裁剪**：两种模式均支持 Data Cache；列裁剪、谓词推送在 FE 阶段完成，减少外部网络传输。
- **安全性**：REST/JDBC 凭据统一托管，支持密文存储；对象存储凭据可继承 Storage Volume，避免重复配置。
- **容错**：RPC 模式提供多 BE 列表、重试、熔断；直读模式提供对象存储重试、回退策略。
- **扩展性**：模块与 Provider 设计解耦，可扩展到多数据源或多租户环境。

## 测试策略
- **功能**：创建/刷新 Catalog、基础 SELECT、Join/聚合、视图/物化视图引用。
- **性能**：TPC-H/TPC-DS 压测，分别测量 RPC 与直读模式的吞吐与延迟。
- **异常**：外部 BE 宕机、网络抖动、凭据错误、对象存储限流；验证重试与错误信息。
- **缓存**：验证 Data Cache 命中率、缓存一致性、刷新后缓存废弃。
- **安全**：检查敏感信息脱敏、权限控制（SELECT 权限、Catalog 级授权）。

## 运维与监控建议
- 关键指标：外部 RPC QPS、延迟、失败率；对象存储读耗时；Data Cache 命中率；Catalog 刷新耗时。
- 告警：REST `_query_plan` 连续失败、RPC 熔断、对象存储重试过多、缓存回填失败。
- 运维工具：Profile 分析脚本、Catalog 状态查看、缓存清理、凭据轮转脚本。
- 文档：提供配置模板（含敏感信息占位）、FAQ（常见错误码、网络/权限问题）。

## 总结
通过上述模块化设计，StarRocks 能够在同一套 External Catalog 机制下支持两种取数模式，为跨集群查询提供统一体验。后续可继续增强：统计信息同步、权限联动、成本模型优化、更加智能的模式选择（根据网络/负载自动切换），进一步完善“Provider + Consumer”双角色协同的生态闭环。
