# StarRocks 作为数据提供方的实现说明

## 角色定位与使用场景
- **角色**：当外部查询引擎（Spark、Flink、Trino、另一套 StarRocks 等）需要访问本集群中的表时，本集群承担“数据提供方（Provider）”角色，通过 Thrift RPC 提供 Tablet 级数据扫描能力。
- **入口**：外部查询引擎通过 REST `_query_plan` 获取执行计划，再通过 `TStarrocksExternalService` 的 `open_scanner/get_next/close_scanner` 接口拉取数据。
- **典型应用**：StarRocks Spark/Flink Connector、Trino-Connector，以及即将接入的 StarRocks Catalog（消费方）在 `fetch.mode=rpc` 时所依赖的服务端能力。

## 总体架构概览
```
外部客户端                            StarRocks Provider 集群
REST 调用 (_query_plan)  ───►  FE TableQueryPlanAction  ──►  单节点执行计划 + Tablet 切片
                                         │
                                         ▼
                               BE FragmentMgr/ExternalService
Thrift open/get_next/close     ◄── TStarrocksExternalService  ──►  Pipeline 执行、ResultQueueMgr
                                         │
                                         ▼
                                  Tablet / Data Cache / 存储
```

### 架构要点
1. **FE 提供 `_query_plan` 接口**（`com.starrocks.http.rest.TableQueryPlanAction`）：接收 SQL，生成单节点计划与 Tablet 分布信息，返回 base64 编码的 `TQueryPlanInfo`。
2. **BE 暴露 `TStarrocksExternalService`**（`gensrc/thrift/StarrocksExternalService.thrift` + `be/src/service/backend_base.cpp`）：维护 Scanner 生命周期，调用 FragmentMgr 执行外部计划。
3. **FragmentMgr 执行外部 PlanFragment**（`be/src/runtime/fragment_mgr.cpp`）：解码 plan，构造执行参数，触发 Pipeline，结果由 `ResultQueueMgr` 拉取。
4. **ExternalScanContextMgr**（`be/src/runtime/external_scan_context_mgr.cpp`）：管理扫描上下文、超时回收与取消。
5. **数据返回格式**：`get_next` 返回 Arrow RecordBatch 序列化后的二进制，便于 Connector 转换。

## 模块与代码组织关系
| 层级 | 模块/类 | 关键职责 | 主要文件 |
| ---- | ------- | -------- | -------- |
| FE REST 接口 | `TableQueryPlanAction` | 解析 REST 请求、校验权限、规划单节点执行计划、返回 Tablet 路由与 `opaqued_query_plan` | `fe/fe-core/src/main/java/com/starrocks/http/rest/TableQueryPlanAction.java` |
| FE 规划器 | `StatementPlanner` + `ExecPlan` | 在 `SingleNodeExecPlan` 模式下生成一条包含 MemorySink 的 Fragment | `fe/fe-core/src/main/java/com/starrocks/sql/StatementPlanner.java` |
| BE 服务 | `BackendServiceBase::open/get_next/close` | 与外部客户端交互，创建扫描上下文，调用 FragmentMgr，封装返回值 | `be/src/service/backend_base.cpp` |
| 上下文管理 | `ExternalScanContextMgr` | 维护 ScanContext、超时回收、取消 Fragment、释放结果队列 | `be/src/runtime/external_scan_context_mgr.*` |
| 执行调度 | `FragmentMgr::exec_external_plan_fragment` | 解码 `_query_plan`，生成执行参数，准备 Pipeline Fragment，调度执行 | `be/src/runtime/fragment_mgr.cpp` |
| 结果通道 | `ResultQueueMgr` | 存储 Pipeline 输出（Arrow RecordBatch），供 `get_next` 拉取 | `be/src/runtime/result_queue_mgr.*` |
| 序列化 | `StarrocksExternalService.thrift` | 定义 RPC 接口与参数、返回值结构 | `gensrc/thrift/StarrocksExternalService.thrift` |

模块之间的调用流程如下：
1. REST `_query_plan` → `TableQueryPlanAction.handleQuery` → 生成 `TQueryPlanInfo`（Fragment + Tablet）。
2. 外部调用 `open_scanner` → `BackendServiceBase::open_scanner` → `FragmentMgr::exec_external_plan_fragment`。
3. Fragment 执行后将数据写入 `ResultQueueMgr`，并注册到 `ExternalScanContextMgr`。
4. 外部循环调用 `get_next` 拉取数据，直到 `eos=true`，最后 `close_scanner` 释放资源。

### 与 Spark Connector 的交互细节
- **列裁剪/谓词推送**：Spark Connector 在 `_query_plan` 请求中会携带 `starrocks.columns`、`starrocks.filter.query` 等配置，`TableQueryPlanAction` 需保证输出列与 `_query_plan` 中的 `selected_columns` 对齐。
- **Arrow 转换契约**：Spark 端使用 Arrow Vector → Catalyst 的映射，Provider 侧在 `FragmentMgr::exec_external_plan_fragment` 中调用 `serialize_record_batch` 返回 Arrow RecordBatch；字段顺序与类型必须与 `selected_columns` 保持一致。
- **重试语义**：Spark Connector 会在 Scanner 失败时调用 `close_scanner` 并重建；`ExternalScanContextMgr` 要正确处理重复 `close`、offset 校验失败等场景。
- **batch size 控制**：Connector 将 `starrocks.batch.size` 传入 `TScanOpenParams.batch_size`；Provider 侧需校验范围并在 Pipeline 执行中使用相同批大小，以避免 Arrow 过大或过小。
- **性能指标**：Spark 端依赖 Profile/日志定位性能问题，因而 Provider 端的 Profile (`enable_profile_for_external_plan`) 应包含 RPC 时间、行数等信息。

## 执行流程详解
1. **请求解析**：`TableQueryPlanAction` 验证 DB/Table 与 SQL 一致性，只允许单表、无聚合/排序/子查询。
2. **计划生成**：
   - `ConnectContext` 设置 `SingleNodeExecPlan=true`，强制生成单节点 Fragment。
   - `PlanFragment` 的 sink 被改写成 `TMemoryScratchSink`，以便将结果导入 `ResultQueueMgr`。
   - 解析 `TScanRangeLocations`，构造 Tablet → 主机列表映射及 `TTabletVersionInfo`。
3. **Plan 序列化**：构造 `TQueryPlanInfo`，包含：
   - `plan_fragment`：单节点执行计划；
   - `desc_tbl`：Tuple/Slot 描述；
   - `output_exprs`：输出列；
   - `tablet_info`：Tablet 版本等元信息；
   - `query_id`：唯一查询 ID。
4. **RPC 生命周期**：
   - `open_scanner`：创建 `ScanContext`，分配 `fragment_instance_id`，调用 `FragmentMgr` 执行计划。
   - `get_next`：根据 `context_id` 拉取 RecordBatch，维护 offset、一致性，返回 `rows` + `eos`。
   - `close_scanner`：取消 Fragment/ResultQueue，释放上下文；GC 线程也会定期清理超时上下文。
5. **Fragment 执行**：
   - `exec_external_plan_fragment` 负责反序列化 `_query_plan`，构造 `TExecPlanFragmentParams` 与 `TPlanFragmentExecParams`；
   - 仅创建一个实例，设置 `TQueryType::EXTERNAL`、禁用 page cache；
   - `FragmentExecutor` 使用 Pipeline 模式处理 Tablet 切片，将结果写入 `ResultQueueMgr`。

## 设计原理与思路
- **单节点执行**：外部 Connector 只需要获取 Tablet 数据，无需复杂分布式调度，因此 FE 在规划阶段开启 `SingleNodeExecPlan`，保证只产生一个 Scan + MemorySink。
- **可控的结果通道**：结果通过 `ResultQueueMgr` 缓存为 Arrow RecordBatch，避免频繁上下文切换，也便于 Connector 直接消费。
- **上下文与 GC**：`ExternalScanContextMgr` 维护 `context_id → ScanContext`，并以 `keep_alive_min` 控制生命周期，防止客户端异常退出导致资源泄漏。
- **重试与一致性**：`get_next` 参数包含 `offset`，服务端校验 offset 防止客户端重复请求导致数据重复；异常时返回具体错误码。
- **安全与审计**：`TableQueryPlanAction` 通过 `Authorizer.checkTableAction` 校验 SELECT 权限，并对请求 SQL/用户信息记录日志。

## 开发计划与步骤（增强或复用）
| 阶段 | 目标 | 主要工作 | 验证方式 |
| ---- | ---- | -------- | -------- |
| 1. 架构梳理与抽象 | 将现有能力整理为 Provider SDK | 梳理 REST/RPC 接口，补充文档与统一配置入口 | 阅读代码 + 单元测试 |
| 2. 可配置化增强 | 支持多租户配额、限流、TLS | 新增配置项、接入 `AuthManager`、支持 TLS Thrift | 单元 + 集成测试 |
| 3. 性能与监控 | 提供细粒度指标、压测优化 | 增加 Profile/Metrics、优化批量大小、添加重试策略 | 压测、Profile 分析 |
| 4. 兼容直读 | 与对象存储直读模式协同 | 在 `_query_plan` 中返回 `storagePath` 等信息，为直读模式提供数据 | 外部客户端联调 |

## 代码改动与扩展建议
- **抽取配置**：统一处理 `starrocks.be.rpc.*` 配置，支持动态刷新。
- **熔断与限流**：在 `ExternalScanContextMgr` 或 RPC 层记录并限制活跃 Scanner 数。
- **安全增强**：支持 OAuth/JWT 方式传递凭据；RPC 层可扩展为 TLS/Kerberos。
- **错误分类**：完善 `Status` → Thrift 错误码映射，便于外部识别重试条件。

## 测试与验证要点
- REST `_query_plan` 正常/异常返回（权限不足、SQL 非法、非单表等）。
- RPC 生命周期：正常流、提前 close、客户端异常中断、offset 不一致。
- 多 Tablet 并发扫描、元数据缓存失效后的刷新。
- GC 场景：`keep_alive_min` 超时回收是否会取消 Fragment 并释放 ResultQueue。
- Arrow 序列化兼容性：检查字段顺序、类型映射。

## 运维与监控
- 关注指标：`active_scan_context_count`、RPC 延迟、ResultQueue 长度。
- 日志：`TableQueryPlanAction`、`BackendServiceBase` 对异常均有日志输出，可结合 QueryID 排查。
- 配置：`scan_context_gc_interval_min` 控制 GC；`enable_profile_for_external_plan` 可开启 Profile。
- 故障处置：若发现持续失败，可人工触发 `close_scanner` 或在外部 Connector 层回退。

通过以上机制，StarRocks 能够稳定地向外部系统提供 Tablet 级别的数据扫描接口，为各类 Connector 和 Catalog 功能提供基础设施。
