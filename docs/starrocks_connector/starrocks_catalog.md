# StarRocks Catalog 直读方案

## 背景与动因
- 多个业务集群部署了 StarRocks，公共表与各业务表散落在不同集群，需要跨集群 Join 与 OLAP 查询。
- JDBC Catalog 方案需要通过外部 BE 逐行返回结果，既无法 Tablet 切片，也缺乏谓词下推，导致延迟与带宽占用均不满足 ≤5s 的 SLA。
- 现有兜底手段通常是把外部表同步到自助分析集群，带来的问题是重复存储、同步链路复杂以及数据一致性风险。

## 目标
### 功能目标
- 新增 `type = "starrocks"` 的 External Catalog，直接读取外部 StarRocks 集群对象存储中的 Segment 文件，避免经过外部 BE。
- 兼容外部集群的 `shared_data`（存算分离）与 `shared_nothing`（存算一体）部署；存算分离场景是核心设计对象。

### 性能目标
- 通过 Tablet 粒度并行与对象存储批量读取，使 Scan 吞吐达到 1 GB/s 级别（持续基准验证）。
- Query Profile 能揭示对象存储读取耗时、Data Cache 命中、重试情况，便于定位瓶颈。

## 用户接口
### 创建 Catalog
```sql
CREATE EXTERNAL CATALOG <catalog_name>
[COMMENT <comment>]
PROPERTIES (
    "type" = "starrocks",
    "starrocks.run_mode" = "shared_data",
    "starrocks.fetch.mode" = "object_store",
    "starrocks.fe.http.url" = "...",
    "starrocks.fe.jdbc.url" = "...",
    "starrocks.user" = "...",
    "starrocks.password" = "...",
    -- 其他可选参数
    StorageCredentialParams
);
```

| 参数 | 是否必须 | 默认值 | 说明 |
| ---- | -------- | ------ | ---- |
| `starrocks.run_mode` | 是 | — | 外部集群运行模式：`shared_data` 或 `shared_nothing`。 |
| `starrocks.fetch.mode` | 否 | `object_store` | 数据获取方式，仅支持对象存储直读。 |
| `starrocks.fe.http.url` | 是 | — | 外部 FE HTTP 入口，供 REST 接口调用。 |
| `starrocks.fe.jdbc.url` | 是 | — | 外部 FE MySQL 地址，供 JDBC 拉取元数据。 |
| `starrocks.user` / `starrocks.password` | 是 | — | 访问外部 FE 的凭据。 |
| `starrocks.request.retries` | 否 | `3` | REST / 对象存储请求的重试次数。 |
| `starrocks.request.connect.timeout.ms` | 否 | `30000` | REST 连接超时。 |
| `starrocks.request.read.timeout.ms` | 否 | `30000` | REST 读超时。 |
| `starrocks.exec.mem.limit` | 否 | `2147483648` | 单查询内存限制（默认 2 GB）。 |
| `starrocks.enable_data_cache` | 否 | `true` | 是否启用 Data Cache，命中后不会访问对象存储。 |
| `storage.volume` | 否 | 默认存储卷 | 指定使用的 Storage Volume，决定对象存储凭据。 |
| `StorageCredentialParams` | 否 | — | 补充或覆盖 `fs.*` 形式的对象存储配置。 |

## 架构设计
### 总体流程
1. `CatalogMgr` 通过 `ConnectorMgr` 初始化 `StarRocksConnector`，注册 Catalog 并持久化元数据。
2. `StarRocksConnector` 结合 JDBC 与 REST 接口同步库、表、分区、Tablet、对象存储路径等信息。
3. 优化器识别 `type=starrocks` 的表，构造 `StarRocksScanNode` 并生成 Tablet 级 Scan Range，同时准备列裁剪与谓词推送。
4. CN/BE 在执行期使用 `format-sdk` 的 `StarRocksFormatReader` 读取对象存储上的 Segment 文件，直接生成数据块给下游算子。

### 元数据同步（FE）
- **框架复用**：在 `com.starrocks.connector.ConnectorType` 中新增 `STARROCKS`，由 `ConnectorFactory` 负责实例化。
- **JDBC**：沿用 JDBC 通道读取库、表、列定义，缓存在 `ConnectorTblMetaInfoMgr` 中。
- **REST**：通过 `format-sdk/src/main/java/com/starrocks/format/rest/RestClient.java` 调用以下接口：
  - `/api/v2/catalogs/{catalog}/databases/{db}/tables/{table}/schema`：获取列信息、分布键等。
  - `/api/v2/catalogs/{catalog}/databases/{db}/tables/{table}/partition`：获取分区、Tablet 列表、`storagePath`、`visibleVersion`。
  - `/api/{db}/{table}/_query_plan`：在执行前获取 `opaqued_query_plan`，用于推送过滤条件。
- **存储卷映射**：REST 返回的 `storagePath` 表示对象存储的根目录，连接器需要与本地 `StorageVolumeMgr` (fe/fe-core/src/main/java/com/starrocks/server/StorageVolumeMgr.java) 匹配；若 Catalog 属性中指定了 `storage.volume`，则优先使用对应凭据。
- **刷新**：`REFRESH EXTERNAL STARROCKS CATALOG <db>.<table>` 触发重新拉取 schema 与 partition；若需要读取未发布版本，可传 `temporary=true` 参数取得即时版本。

### 查询计划生成
- **Analyzer 扩展**：`com.starrocks.sql.analyzer.CatalogAnalyzer` 在解析时识别 `STARROCKS` 类型，构建外部表元数据对象。
- **逻辑/物理算子**：新增 `LogicalStarRocksScanOperator` / `PhysicalStarRocksScanOperator`，借鉴 `LogicalDeltaLakeScanOperator` 的列裁剪与谓词分类。
- **ScanNode**：在 `PlanFragmentBuilder` 中生成 `StarRocksScanNode`，实现方式参考 `fe/fe-core/src/main/java/com/starrocks/planner/DeltaLakeScanNode.java`：
  - 将 Tablet 元数据转成 `RemoteFileScanRange`。
  - 将过滤条件、最小/最大值、分区条件注入 `HDFSScanNodePredicates`。
  - 携带 Data Cache 选项与统计信息。

### 执行路径（CN/BE）
- **格式读取器**：`StarRocksScanNode` 在 pipeline 中实例化 `StarRocksFormatReader`（`format-sdk/src/main/cpp/format/starrocks_format_reader.cpp`），其内部包装 `storage/lake/tablet_reader.cpp`：
  - `FixedLocationProvider` 负责解析 `storagePath` 与 Tablet 版本。
  - `FileSystem::Create` 根据 `fs.*` 参数构造 S3/HDFS/OSS/GCS 客户端。
  - `_query_plan` 解出的 `TQueryPlanInfo` 提供谓词表达式和输出列。
- **切片与并发**：`TabletReader` 支持物理/逻辑拆分（`lake/tablet_reader.cpp` 中的 `_need_split` & `_could_split_physically`），可按行数将大 Tablet 拆分多个 Scan Range。
- **Data Cache**：通过 `TabletReaderParams.lake_io_opts.fill_data_cache` 决定是否写入 Data Cache；缓存文件结构与本地 Lake 表一致，指标位于 `be/src/cache`。
- **错误处理**：对象存储读失败时，根据 `fs.s3a.retry.*` 或 Catalog 的超时配置重试；必要时将错误上抛，便于 FE 调度其他 Tablet。

## 稳定性设计
- **分批读取**：`TabletReader` 逐 Rowset/Segment 拉取数据，并配合 `config::lake_tablet_rows_splitted_ratio` 控制拆分，避免单次拉取过多数据。
- **多级超时**：REST、对象存储与查询全局分别受 `starrocks.request.*`、`fs.*` 和 Session `query_timeout` 控制，及时熔断慢操作。
- **重试策略**：`RestClient` 与 `FileSystem` 均内置重试逻辑；当重试耗尽时快速报错，防止阻塞 pipeline。
- **资源隔离**：`starrocks.exec.mem.limit` 限制单查询内存；Data Cache 监控缓存使用量，防止外部抖动影响本地节点。

## 可观测性
- **日志**：Connector 记录 REST 调用、对象存储路径、重试次数；执行端记录 Tablet 读取耗时与错误。
- **Profile**：保留 `RowStarRocksRead`、`DataCacheReadBytes`、`ScanRange` 等指标；可新增对象存储带宽、重试统计。
- **全局监控**：`SHOW BACKENDS` / `SHOW COMPUTE NODES` 中的 `DataCacheMetrics` 可确认缓存状态；建议在监控系统新增对象存储 4xx/5xx 计数与耗时。

## 测试策略
- **功能**：创建/刷新 Catalog、基本查询、跨库 Join、Data Cache 命中校验。
- **性能**：TPC-H/TPC-DS 在外部集群压测，关注吞吐与延迟；对比 JDBC Catalog 基准。
- **异常**：模拟对象存储限速、凭据失效、REST 超时、版本落后，验证重试与错误上报。
- **回归**：FE/BE 单元测试覆盖元数据解析与 Scan Node；Pseudo Cluster 或 Mini Cluster 验证端到端流程。

## Roadmap（示例）
| 阶段 | 工作项 | 时间 |
| ---- | ------ | ---- |
| Metadata | `StarRocksConnector`、REST schema/partition 接入 | 7.14 – 7.25 |
| Plan | `StarRocksScanNode`、Tablet Range 生成、谓词推送 | 7.25 – 8.05 |
| Scan | `StarRocksFormatReader` 集成、Data Cache 打通 | 7.25 – 8.15 |
| 测试 | 功能/性能/异常自测 & 调优 | 8.15 – 9.01 |
| 后续 | 统计信息采集、权限治理 | 2025 H2+ |

## 模块拆解与实现计划

| 模块 | 主要职责 | 拆分任务 | 关键依赖 |
| ---- | -------- | -------- | -------- |
| FE Connector 层 | Catalog 生命周期、REST/JDBC 元数据同步、缓存与刷新 | 类型注册 → 属性解析 → REST 访问层 → 元数据缓存 → 刷新与 ACL | `ConnectorMgr`、`RestClient`、`StorageVolumeMgr` |
| 查询规划层 | 逻辑/物理算子、Scan Range 生成、谓词/列裁剪 | Analyzer 支持 → Logical/Physical Operator → PlanFragmentBuilder 集成 → Explain/统计 | 优化器框架、`HDFSScanNodePredicates` |
| 执行层 | Pipeline Scan Operator、format-sdk 集成、Data Cache | Thrift 结构更新 → FE PlanNode 序列化 → BE Scan Operator → format-sdk 扩展 → Data Cache 接入 | `format-sdk`、`storage/lake/tablet_reader.cpp`、BE Pipeline |
| 配置与凭据 | 多云对象存储配置、Storage Volume 映射、敏感信息处理 | Catalog 属性→CloudConfiguration → Volume 选择 → 凭据刷新 → 异常回退 | `StorageVolumeMgr`、`CloudConfiguration` |
| 测试与工具 | UT/IT/e2e/压测脚本、监控与文档 | FE/BE UT → PseudoCluster IT → 压测 & 故障注入 → Profile/监控脚本 → 用户指南 | CI 平台、监控系统、压测工具 |

### FE Connector 层
- **代码改动**
  - `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java`：新增 `STARROCKS` 枚举，加入 `SUPPORT_TYPE_SET`。
  - `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorFactory.java`：增加 `createStarRocksConnector()` 分支，装配 REST/JDBC 配置。
  - 新增包 `com.starrocks.connector.starrocks`：
    - `StarRocksConnector.java`：实现 `CatalogConnector`，封装 `open/close`、持久化、日志。
    - `StarRocksConnectorMetadata.java`：实现 `ConnectorMetadata`，组合 JDBC 元数据与 REST Tablet 信息。
    - `StarRocksCatalogProperties.java`：解析 Catalog 属性、构造 REST/JDBC/FS 参数（含超时、重试、fetch.mode 校验）。
    - `StarRocksMetadataCache.java`：分层缓存库/表/分区/Tablet 及版本，支持 `refreshTable`。
    - `StarRocksRestClient.java`：基于 `format-sdk` 的 `RestClient`，补充 endpoint failover、认证、`temporary=true` 获取。
  - `fe/fe-core/src/main/java/com/starrocks/server/GlobalStateMgr.java`：在 `refreshExternalTable` 中调用新 Connector 的刷新接口。
  - `format-sdk/src/main/java/com/starrocks/format/rest/RestClient.java`：增补连接/读超时参数化、BasicAuth、retry 策略。
- **实施步骤**
  1. 类型注册与属性解析。
  2. REST Client 改造，验证 `_query_plan` 与 `/partition` 调用。
  3. Metadata 层实现（含缓存、刷新、错误处理）。
  4. 与 `ConnectorMgr` 集成，新增 Catalog 时持久化属性。
  5. 补充 `REFRESH`、`SHOW CREATE CATALOG` 的安全展示。

### 查询规划层
- **代码改动**
  - `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/CatalogAnalyzer.java`：识别 `STARROCKS`，校验权限与属性。
  - 新增 `LogicalStarRocksScanOperator` / `PhysicalStarRocksScanOperator`（位于 `sql/optimizer/operator` 包）。
  - `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalScanOperator.java`：如需通用方法扩展（列映射、统计）。
  - `fe/fe-core/src/main/java/com/starrocks/sql/plan/PlanFragmentBuilder.java`：在 `PlanBuilder` 中增加 `visitPhysicalStarRocksScan`。
  - 新增 `StarRocksScanNode`（参考 `DeltaLakeScanNode`），位于 `fe/fe-core/src/main/java/com/starrocks/planner/`。
  - 更新 `StatisticsCalculator` 与相关规则（`PushDownPredicateRule`, `PruneScanColumnRule` 等）支持新算子。
  - 若需要 Thrift 字段：修改 `gensrc/thrift/PlanNodes.thrift` 的 `TLakeScanNode` 增加对象存储路径、凭据字段。
- **实施步骤**
  1. Analyzer 支持，确保 SQL 语义正确解析。
  2. 实现 Logical/Physical Operator，定义必要的属性（Tablet 列表、Data Cache 配置）。
  3. `PlanFragmentBuilder` 集成，生成 `StarRocksScanNode` 与 `ScanRange`。
  4. `Explain` 输出与统计支持。
  5. 衔接 Connector 缓存返回的 Tablet 信息，验证 Plan 正确性。

### 执行层
- **代码改动**
  - FE：`StarRocksScanNode` 序列化 Data Cache、FS 参数至 Thrift。
  - Thrift：扩展 `TPlanNode`/`TLakeScanNode` / `TInternalScanRange` 以携带 `storage_path`、`opaqued_query_plan`、FS 配置。
  - BE：
    - 新增 `be/src/exec/pipeline/scan/starrocks_scan_operator.{h,cpp}`：继承通用 Scan Operator，封装 format-sdk Reader。
    - 新增 `be/src/exec/pipeline/scan/starrocks_scan_task.{h,cpp}`：描述 Tablet、Range、分片任务。
    - 新增 `be/src/plan/starrocks_scan_node.{h,cpp}`：解析 Thrift 节点，创建 Operator。
    - 修改 `be/src/storage/lake/tablet_reader.cpp`：允许注入 `opaqued_query_plan`、FS Handler、Data Cache 控制。
    - `be/src/runtime/exec_env.cpp` / `be/src/runtime/runtime_state.cpp`：若需要初始化 format-sdk 组件或配置查询级参数。
  - `format-sdk/src/main/cpp/format/starrocks_format_reader.cpp`：添加 FS 参数映射、Data Cache 控制、错误处理改进。
- **实施步骤**
  1. 定义 Thrift 扩展，生成代码。
  2. 实现 FE `StarRocksScanNode` 序列化。
  3. 开发 BE Scan Operator + Task，调通 format-sdk Reader。
  4. 与 Data Cache 结合，补充相应指标。
  5. 压测与容错测试（限流、FS 超时、重试）。

### 配置与凭据
- **代码改动**
  - `fe/fe-core/src/main/java/com/starrocks/server/StorageVolumeMgr.java`：增加根据 Catalog 属性选择 Volume/凭据的接口。
  - `fe/fe-core/src/main/java/com/starrocks/credential/CloudConfiguration.java`：扩展字段（path-style、region、token）。
  - `fe/fe-core/src/main/java/com/starrocks/common/util/PropertyAnalyzer.java`：新增 Catalog 参数解析和校验。
  - `be/src/common/config.cpp`：新增 BE 级别默认配置（对象存储重试次数、连接池大小）。
  - 更新文档：提示如何配置凭据与安全策略。
- **实施步骤**
  1. 定义 Catalog 属性 → CloudConfiguration 的映射规则。
  2. 支持 Storage Volume 默认值与覆盖策略。
  3. 实现凭据刷新机制（可选，视场景决定）。
  4. 加入敏感信息脱敏和审计日志。

### 测试与工具
- **测试计划**
  - FE UT：`StarRocksConnectorTest`、`StarRocksCatalogPropertiesTest`（模拟 REST/JDBC 返回）。
  - 优化器 UT：`StarRocksScanRuleTest` 验证列裁剪、谓词推送。
  - BE UT：`StarRocksScanOperatorTest`、`StarRocksFormatReaderTest`（Mock FS）。
  - PseudoCluster IT：部署 MinIO/对象存储模拟外部集群，验证查询结果与刷新。
  - 性能测试：TPC-H/TPC-DS 脚本，比较 JDBC Catalog 与直读方案。
  - 故障注入：模拟对象存储 5xx、凭据失效、网络抖动，观察重试与错误。
- **工具与文档**
  - Profile 分析脚本（新增 `tools/profile/profile_starrocks_catalog.py`）。
  - 监控指标指南（对象存储耗时、重试、Data Cache）。
  - 用户文档：`docs/zh/data_source/catalog/starrocks_catalog.md`、`docs/en/...` 提供配置示例、FAQ。

执行顺序建议：
1. **Connector 层**：完成类型注册、属性解析、REST/JDBC 数据访问与缓存，产出稳定的 Tablet/版本接口。
2. **查询规划层**：基于 Connector 提供的数据生成正确的物理计划，确保谓词/列裁剪生效。
3. **执行层**：实现 Pipeline Scan Operator，与 format-sdk、Data Cache 集成，保证查询路径通畅。
4. **配置与凭据**：完善多云配置、凭据管理、异常处理，确保运维可用。
5. **测试与工具**：串起单元/集成/性能/故障测试，补齐监控与文档。

各模块可以并行推进，但需要注意：执行层依赖规划层生成的 Scan Range 数据结构；规划层需要在 Connector 的 Tablet 元数据接口稳定后上线；测试与工具应在主流程可运行后逐步补齐压测与观测能力。

### 迭代实施路线图
| 阶段 | 范围 | 关键修改文件 | 验证方式 |
| ---- | ---- | ------------ | -------- |
| 第 1 阶段：Connector 骨架 | 完成 Catalog 类型注册、属性解析、REST 客户端封装，`StarRocksConnector` 返回 mock 数据 | `ConnectorType.java`、`ConnectorFactory.java`、`com/starrocks/connector/starrocks/*`（新）、`RestClient.java` | FE UT：属性/配置解析，`SHOW CATALOGS` 验证注册 |
| 第 2 阶段：元数据 & 缓存 | 打通 JDBC + REST，缓存库/表/Tablet，支持 `REFRESH` | `StarRocksConnectorMetadata.java`、`StarRocksMetadataCache.java`、`GlobalStateMgr.java` | FE UT + PseudoCluster：`SHOW TABLES`、`REFRESH` 行为 |
| 第 3 阶段：优化器/Plan | 新增 Logical/Physical Scan、`StarRocksScanNode`，生成 Scan Range | `LogicalStarRocksScanOperator.java`、`PhysicalStarRocksScanOperator.java`、`PlanFragmentBuilder.java`、`StarRocksScanNode.java`、`PlanNodes.thrift` | FE UT：Plan/Explain；与第 2 阶段联调 |
| 第 4 阶段：执行链路 | FE PlanNode 序列化，BE Scan Operator、format-sdk 扩展、Data Cache 对接 | `be/src/plan/starrocks_scan_node*`、`be/src/exec/pipeline/scan/starrocks_*`、`tablet_reader.cpp`、`starrocks_format_reader.cpp` | BE UT、端到端小集群、MinIO 验证 |
| 第 5 阶段：配置/凭据 & 测试工具 | Storage Volume 映射、凭据刷新、配置项、全面测试 & 文档 | `StorageVolumeMgr.java`、`CloudConfiguration.java`、`PropertyAnalyzer.java`、`docs/*`、测试脚本 | 全链路 IT、压测、故障注入、文档交付 |

每个阶段结束后都可以形成一次可评审的提交，便于渐进合并与风险控制。

## 与代码的对应关系
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorType.java`：注册 `STARROCKS` 类型。
- `fe/fe-core/src/main/java/com/starrocks/connector/ConnectorMgr.java`：Catalog 生命周期管理。
- `format-sdk/src/main/java/com/starrocks/format/rest/RestClient.java`：REST 元数据访问。
- `format-sdk/src/main/cpp/format/starrocks_format_reader.cpp`：对象存储直读核心逻辑。
- `be/src/storage/lake/tablet_reader.cpp`：Tablet 数据解析与切片实现。
- `be/src/cache/*`、`gensrc/thrift/PlanNodes.thrift`：Data Cache 与 Scan Node Thrift 结构。

## 未决问题
- **统计信息**：当前无外部表统计，后续需要构建采集/推导机制以优化优化器成本估计。
- **权限模型**：需明确外部 Catalog 的鉴权策略，与本地 RBAC、Row Policy 的耦合方式仍待设计。
- **多租户与限流**：跨集群查询需结合资源组或管控面配置对对象存储带宽/QPS 做配额管理。
- **最终一致性**：外部集群版本可见性与对象存储的最终一致性差异，需要提供刷新/重试策略与排障指引。
