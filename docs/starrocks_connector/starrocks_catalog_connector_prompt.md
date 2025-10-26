# StarRocks Catalog（Connector 模式）迭代提示

## 背景

正在实现 `type = "starrocks"` 的 External Catalog，通过 FE/BE connector 模式访问外部 StarRocks 集群。当前代码已完成 FE/BE 框架和 RPC 调用链的核心功能。

## 已完成子任务

### 1. Thrift 与 FE 规划链路 ✅
   - `PlanNodes.thrift` 扩展 `CONNECTOR_SCAN_NODE`，增加 `tuple_id`、库表名、`opaqued_query_plan` 与 `properties`。
   - FE 侧 `StarRocksScanNode`、优化器、`PlanFragmentBuilder` 均支持新节点，并生成带 hosts 与回退 endpoint 的 `TInternalScanRange`。
   - `StarRocksExternalTable` / `StarRocksMetadataCache` 缓存 `_query_plan`、tablet 路由与执行属性。

### 2. BE Connector 框架 ✅
   - `ConnectorManager` 注册 `StarRocksConnector`，`ConnectorScanNode` 能创建该数据源。
   - `StarRocksConnector` / `StarRocksDataSourceProvider` 解析 FE 下发属性、端点、超时配置。
   - `StarRocksDataSource` 能解码 `_query_plan`、提取 tablet/host 并构造 `TScanOpenParams`。

### 3. RPC 调用链打通 ✅ **[2024-10-28 完成]**

**实现文件：**
- `be/src/connector/starrocks_connector.h` (新增 30 行)
- `be/src/connector/starrocks_connector.cpp` (新增 293 行)

**已实现功能：**

#### 3.1 Thrift 客户端管理
- ✅ 创建并管理 `TStarrocksExternalServiceClient`
- ✅ 配置连接超时、读写超时 (`connect_timeout_ms`, `read_timeout_ms`)
- ✅ 支持多 BE endpoint 配置和解析
- ✅ 实现连接失败时的自动重试和节点切换逻辑
- ✅ 延迟连接策略（第一次 `get_next()` 时才建立连接）

#### 3.2 Scanner 生命周期管理
- ✅ `call_open_scanner()`: 调用外部 BE 的 `open_scanner` RPC，获取并管理 `context_id`
- ✅ `call_get_next()`: 循环拉取数据批次，管理 offset 确保数据一致性
- ✅ `call_close_scanner()`: 释放远程扫描上下文和资源
- ✅ 完善异常处理，支持 Thrift RPC 异常捕获和节点切换

#### 3.3 数据转换
- ✅ `deserialize_arrow_batch()`: 使用 Arrow IPC Stream Reader 反序列化外部返回的数据
- ✅ `convert_arrow_to_chunk()`: 将 Arrow RecordBatch 转换为 StarRocks Chunk
- ✅ 完整支持类型映射和 nullable 列处理
- ✅ 使用零拷贝反序列化（`arrow::Buffer::Wrap`）

#### 3.4 统计信息收集
- ✅ 行数统计 (`_num_rows_read`)
- ✅ 字节数统计 (`_num_bytes_read`)
- ✅ offset 管理确保数据一致性

#### 3.5 容错机制
- ✅ 多 BE 节点故障转移 (`_current_host_index`)
- ✅ RPC 调用失败时的重试策略
- ✅ 资源清理和防泄漏（RAII 风格）
- ✅ 详细的错误日志和状态码检查

**实现细节参考：**
- 实现总结: `docs/starrocks_connector/rpc_implementation_summary.md`
- 关键代码: `docs/starrocks_connector/rpc_key_implementation.md`
- 完成报告: `docs/starrocks_connector/rpc_completion_report.md`

## 待完成子任务

### 1. 数据转换与统计增强 ⏳ **[优先级：高]**

**已完成：**
- ✅ Arrow → Chunk 基础转换
- ✅ 行数、字节数统计

**待完成：**
- ⏳ 补齐 Profile 指标
  - [ ] CPU 时间统计 (`cpu_time_spent()`)
  - [ ] RPC 调用次数和延迟
  - [ ] Data Cache 命中率（如果启用）
  - [ ] 扫描耗时分解（网络时间、转换时间等）

- ⏳ LIMIT 支持
  - [ ] 在 `build_open_params()` 中传递 LIMIT 参数
  - [ ] 在 `get_next()` 中提前终止数据拉取
  - [ ] 测试 LIMIT 正确性

- ⏳ 谓词和 Runtime Filter 集成
  - [ ] 将谓词条件传递到 `TScanOpenParams`
  - [ ] Runtime Filter 下推到外部 BE
  - [ ] 测试谓词下推效果

**下一步计划：**
1. 首先实现 Profile 指标收集，便于性能分析
2. 实现 LIMIT 优化，提升小数据量查询性能
3. 逐步添加谓词下推支持

### 2. PlanFragment 构造完善 ⏳ **[优先级：中]**

**当前状态：**
- ✅ 基础的 `TScanOpenParams` 构造
- ✅ Tablet ID 和基本元数据传递

**待完成：**
- ⏳ 精准扫描范围构造
  - [ ] 解析 `_query_plan` 中的 fragment 信息
  - [ ] 构造精确的 tablet 版本信息
  - [ ] 支持分区裁剪信息传递

- ⏳ Schema 一致性校验
  - [ ] FE 下发的 tuple descriptor 与外部表 schema 对比
  - [ ] 类型兼容性检查
  - [ ] 列顺序和名称映射验证

- ⏳ Thrift 结构扩展（如需要）
  - [ ] 评估是否需要新的 Thrift 字段
  - [ ] 向后兼容性设计

**下一步计划：**
1. 分析 `_query_plan` 内容，确定需要传递的信息
2. 实现 schema 校验逻辑
3. 根据测试结果决定是否扩展 Thrift 结构

### 3. 测试与验证 ⏳ **[优先级：高]**

**待完成：**

#### 3.1 单元测试
- [ ] `StarRocksDataSource` 基础测试
  - [ ] `create_thrift_client()` 节点切换逻辑
  - [ ] `deserialize_arrow_batch()` 各种 Arrow 类型
  - [ ] `convert_arrow_to_chunk()` 类型转换
  - [ ] 异常处理路径测试

- [ ] Mock RPC 测试
  - [ ] Mock `TStarrocksExternalServiceClient`
  - [ ] 测试 open/get_next/close 生命周期
  - [ ] 测试 offset 管理逻辑
  - [ ] 测试 EOS 处理

#### 3.2 集成测试
- [ ] Pseudo Cluster 测试
  - [ ] 启动两套 FE/BE 模拟外部集群
  - [ ] 端到端查询验证
  - [ ] 多 tablet 并行扫描
  - [ ] 大数据量测试

- [ ] 异常场景测试
  - [ ] 外部 BE 宕机
  - [ ] 网络中断和超时
  - [ ] 认证失败
  - [ ] 版本不匹配

#### 3.3 编译验证
- [ ] `./build.sh be` 编译通过
- [ ] 无编译警告
- [ ] 链接正确

#### 3.4 性能测试
- [ ] TPC-H/TPC-DS 查询性能
- [ ] 与 JDBC Catalog 性能对比
- [ ] 不同批量大小 (`batch_size`) 的性能影响
- [ ] 并发查询压力测试

**下一步计划：**
1. **优先级 1**: 编译验证和基础单元测试
2. **优先级 2**: Mock RPC 单元测试
3. **优先级 3**: Pseudo Cluster 集成测试
4. **优先级 4**: 性能测试和异常场景测试

### 4. 功能增强（可选） 🔮 **[优先级：低]**

**后续可考虑：**
- [ ] 连接池复用（避免每次查询重建连接）
- [ ] 压缩传输支持
- [ ] 列级加密支持
- [ ] 更细粒度的权限控制
- [ ] 查询取消传播到外部 BE
- [ ] Data Cache 集成优化
- [ ] 动态调整 batch size

## 当前工作重点

**阶段目标：** 完成测试与验证，确保 RPC 调用链稳定可靠

**建议优先级：**
1. **编译验证** - 确保代码可以编译通过
2. **基础单元测试** - 验证核心逻辑正确性
3. **Profile 指标** - 便于后续性能调优
4. **集成测试** - 端到端验证功能完整性
5. **LIMIT 优化** - 提升常见场景性能

## 版本历史

- **2024-10-28**: 完成 RPC 调用链打通（子任务 3）
  - 实现 Thrift 客户端管理
  - 实现 Scanner 生命周期管理
  - 实现 Arrow 数据转换
  - 实现故障转移和重试机制

## 参考文档

- **总体设计**: `starrocks_catalog_connector_mode.md`
- **Provider 侧**: `starrocks_catalog_provider.md`
- **Consumer 侧**: `starrocks_catalog_consumer.md`
- **RPC 实现总结**: `rpc_implementation_summary.md`
- **关键代码片段**: `rpc_key_implementation.md`
- **完成报告**: `rpc_completion_report.md`

将本提示用于后续迭代，可快速同步当前进度与剩余工作。
