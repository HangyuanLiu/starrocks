# StarRocks Connector RPC 调用链实现总结

## 已完成的工作

### 1. 头文件扩展 (starrocks_connector.h)
- 添加了Arrow RecordBatch的前向声明
- 引入了ThriftClient和TStarrocksExternalServiceClient
- 扩展了StarRocksDataSource类，增加了：
  - RPC状态管理字段（_scanner_opened, _eos, _context_id等）
  - 统计信息字段（_num_rows_read, _num_bytes_read）
  - Thrift客户端管理（_thrift_client, _current_host_index）
  - 新的私有方法用于RPC调用和数据转换

### 2. RPC调用实现 (starrocks_connector.cpp)

#### 2.1 Thrift客户端管理
- **create_thrift_client()**: 
  - 解析BE endpoints或使用scan range中的hosts
  - 创建ThriftClient并设置超时参数
  - 实现了重试和节点切换逻辑
  - 支持connection/read/send timeout配置

#### 2.2 Scanner生命周期管理
- **call_open_scanner()**:
  - 调用外部BE的open_scanner RPC
  - 获取并保存context_id
  - 处理异常并支持节点切换
  
- **call_get_next()**:
  - 循环调用get_next拉取数据
  - 管理offset确保数据一致性
  - 处理eos标志
  - 反序列化Arrow数据并转换为Chunk
  
- **call_close_scanner()**:
  - 释放远程扫描上下文
  - 清理context_id
  - 处理异常情况

#### 2.3 数据转换
- **deserialize_arrow_batch()**:
  - 使用Arrow IPC StreamReader反序列化
  - 从二进制数据重建RecordBatch
  
- **convert_arrow_to_chunk()**:
  - 遍历每个列进行类型转换
  - 使用get_arrow_converter获取转换函数
  - 处理nullable列
  - 构建StarRocks Chunk对象

#### 2.4 执行流程优化
- **get_next()**:
  - 延迟创建Thrift客户端
  - 延迟调用open_scanner
  - 支持EOS状态检查
  
- **close()**:
  - 清理RPC连接
  - 释放所有资源
  - 处理异常情况

### 3. 新增的依赖项
- Arrow相关：arrow::Buffer, arrow::io::BufferReader, arrow::ipc::RecordBatchStreamReader
- StarRocks转换：ArrowConvertContext, ConvertFuncTree, get_arrow_converter
- 列操作：ColumnHelper, NullableColumn, Filter
- 字符串工具：strings::Substitute

### 4. 错误处理和重试机制
- 连接失败时自动切换到下一个BE节点
- RPC调用失败时提供详细错误信息
- 支持配置化的重试次数
- offset不匹配检测

### 5. 性能优化
- 使用Arrow零拷贝反序列化
- 预先分配列空间
- 批量数据拉取
- 统计信息收集（行数、字节数）

## 关键设计点

1. **渐进式连接**: open()只准备参数，真正的RPC连接在第一次get_next()时建立
2. **故障转移**: 支持多个BE endpoint，失败时自动切换
3. **状态管理**: 使用多个bool标志管理不同阶段的状态
4. **资源清理**: 在close()中确保远程资源正确释放
5. **Arrow兼容**: 完全兼容外部BE返回的Arrow IPC Stream格式

## 下一步工作建议

1. **测试验证**:
   - 单元测试各个RPC方法
   - 集成测试端到端查询
   - 异常场景测试（网络中断、BE宕机等）

2. **性能优化**:
   - Profile指标收集
   - 连接池复用
   - 批量大小调优

3. **功能完善**:
   - Runtime filter支持
   - 谓词下推
   - LIMIT优化
   - 压缩传输

4. **监控和日志**:
   - 详细的RPC调用日志
   - 性能指标上报
   - 异常告警
