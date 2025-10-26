# 关键实现代码片段

## 1. RPC客户端创建和连接（支持故障转移）

```cpp
Status StarRocksDataSource::create_thrift_client() {
    const auto& ctx = _provider->execution_context();
    
    // 1. 准备BE endpoint列表
    if (_scan_range_ctx.hosts.empty()) {
        // 从配置中解析BE endpoints
        for (const auto& endpoint_str : ctx.be_rpc_endpoints) {
            size_t colon_pos = endpoint_str.find(':');
            TNetworkAddress addr;
            addr.hostname = endpoint_str.substr(0, colon_pos);
            addr.port = std::stoi(endpoint_str.substr(colon_pos + 1));
            _scan_range_ctx.hosts.push_back(addr);
        }
    }
    
    // 2. 选择当前索引对应的host
    if (_current_host_index >= _scan_range_ctx.hosts.size()) {
        return Status::InternalError("All BE hosts failed for StarRocks connector");
    }
    
    const auto& host = _scan_range_ctx.hosts[_current_host_index];
    
    // 3. 创建ThriftClient并配置超时
    _thrift_client = std::make_unique<ThriftClient<TStarrocksExternalServiceClient>>(
        host.hostname, host.port);
    
    _thrift_client->set_conn_timeout(ctx.connect_timeout_ms);
    _thrift_client->set_recv_timeout(ctx.read_timeout_ms);
    _thrift_client->set_send_timeout(ctx.read_timeout_ms);
    
    // 4. 连接失败时递增索引，尝试下一个BE
    Status st = _thrift_client->open_with_retry(ctx.request_retries, 100);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to connect to StarRocks BE " << host.hostname << ":" << host.port;
        _current_host_index++;  // 切换到下一个BE
        _thrift_client.reset();
        
        // 5. 支持重试机制（递归重试下一个节点）
        if (_current_host_index < _scan_range_ctx.hosts.size()) {
            return create_thrift_client();  // 递归重试
        }
        return st;
    }
    
    LOG(INFO) << "Connected to StarRocks BE " << host.hostname << ":" << host.port;
    return Status::OK();
}
```

## 2. 打开Scanner

```cpp
Status StarRocksDataSource::call_open_scanner() {
    if (!_thrift_client || _thrift_client->iface() == nullptr) {
        return Status::InternalError("Thrift client not initialized");
    }
    
    TScanOpenResult result;
    try {
        // 调用外部BE的open_scanner RPC
        _thrift_client->iface()->open_scanner(result, _open_params);
    } catch (apache::thrift::TException& e) {
        // 异常时尝试切换BE节点
        std::string err_msg = strings::Substitute("Failed to call open_scanner: $0", e.what());
        LOG(WARNING) << err_msg;
        
        _current_host_index++;
        _thrift_client.reset();
        if (_current_host_index < _scan_range_ctx.hosts.size()) {
            RETURN_IF_ERROR(create_thrift_client());
            return call_open_scanner();  // 重试
        }
        return Status::ThriftRpcError(err_msg);
    }
    
    // 检查返回状态
    if (result.status.status_code != TStatusCode::OK) {
        std::string err_msg = "open_scanner failed";
        if (!result.status.error_msgs.empty()) {
            err_msg = result.status.error_msgs[0];
        }
        return Status::InternalError(err_msg);
    }
    
    // 保存context_id用于后续调用
    if (!result.__isset.context_id || result.context_id.empty()) {
        return Status::InternalError("open_scanner returned empty context_id");
    }
    
    _context_id = result.context_id;
    _offset = 0;
    
    LOG(INFO) << "Successfully opened StarRocks scanner, context_id=" << _context_id;
    return Status::OK();
}
```

## 3. 获取下一批数据

```cpp
Status StarRocksDataSource::call_get_next(ChunkPtr* chunk) {
    if (!_thrift_client || _thrift_client->iface() == nullptr) {
        return Status::InternalError("Thrift client not initialized");
    }
    
    // 构造请求参数
    TScanNextBatchParams params;
    params.__set_context_id(_context_id);
    params.__set_offset(_offset);  // 发送offset确保一致性
    
    TScanBatchResult result;
    try {
        // 调用get_next RPC
        _thrift_client->iface()->get_next(result, params);
    } catch (apache::thrift::TException& e) {
        std::string err_msg = strings::Substitute("Failed to call get_next: $0", e.what());
        LOG(WARNING) << err_msg;
        return Status::ThriftRpcError(err_msg);
    }
    
    // 检查返回状态
    if (result.status.status_code != TStatusCode::OK) {
        std::string err_msg = "get_next failed";
        if (!result.status.error_msgs.empty()) {
            err_msg = result.status.error_msgs[0];
        }
        return Status::InternalError(err_msg);
    }
    
    // 检查是否结束
    if (result.__isset.eos && result.eos) {
        _eos = true;
        return Status::EndOfFile("StarRocks connector scan finished");
    }
    
    // 处理空数据
    if (!result.__isset.rows || result.rows.empty()) {
        *chunk = std::make_shared<Chunk>();
        return Status::OK();
    }
    
    // Arrow反序列化
    std::shared_ptr<arrow::RecordBatch> arrow_batch;
    RETURN_IF_ERROR(deserialize_arrow_batch(result.rows, &arrow_batch));
    
    // 转换为Chunk
    RETURN_IF_ERROR(convert_arrow_to_chunk(*arrow_batch, chunk));
    
    // 更新offset和统计信息
    _offset += arrow_batch->num_rows();
    _num_rows_read += arrow_batch->num_rows();
    _num_bytes_read += result.rows.size();
    
    return Status::OK();
}
```

## 4. Arrow反序列化

```cpp
Status StarRocksDataSource::deserialize_arrow_batch(
    const std::string& serialized_data,
    std::shared_ptr<arrow::RecordBatch>* batch) {
    
    // 1. 包装为Arrow Buffer（零拷贝）
    auto buffer = arrow::Buffer::Wrap(serialized_data.data(), serialized_data.size());
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    
    // 2. 创建IPC Stream Reader
    auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
    if (!reader_res.ok()) {
        return Status::InternalError(
            strings::Substitute("Failed to create Arrow RecordBatch reader: $0", 
                              reader_res.status().ToString()));
    }
    
    auto reader = reader_res.ValueOrDie();
    
    // 3. 读取RecordBatch
    auto read_res = reader->ReadNext();
    if (!read_res.ok()) {
        return Status::InternalError(
            strings::Substitute("Failed to read Arrow RecordBatch: $0", 
                              read_res.status().ToString()));
    }
    
    *batch = read_res.ValueOrDie();
    if (*batch == nullptr) {
        return Status::InternalError("Deserialized Arrow RecordBatch is null");
    }
    
    return Status::OK();
}
```

## 5. Arrow到Chunk转换

```cpp
Status StarRocksDataSource::convert_arrow_to_chunk(
    const arrow::RecordBatch& batch, ChunkPtr* chunk) {
    
    auto new_chunk = std::make_shared<Chunk>();
    
    if (batch.num_rows() == 0) {
        *chunk = new_chunk;
        return Status::OK();
    }
    
    const auto& slots = _tuple_desc->slots();
    if (batch.num_columns() != slots.size()) {
        return Status::InternalError(
            strings::Substitute("Arrow RecordBatch column count ($0) does not match tuple descriptor ($1)",
                              batch.num_columns(), slots.size()));
    }
    
    // 遍历每一列进行转换
    for (size_t i = 0; i < slots.size(); ++i) {
        const auto* slot = slots[i];
        const auto& arrow_array = batch.column(i);
        
        // 1. 创建StarRocks列
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(batch.num_rows());
        
        // 2. 准备转换上下文
        ArrowConvertContext conv_ctx;
        conv_ctx.state = nullptr;
        conv_ctx.current_slot = const_cast<SlotDescriptor*>(slot);
        
        Filter chunk_filter(batch.num_rows(), 1);
        
        // 3. 获取类型转换器
        ArrowTypeId arrow_type = static_cast<ArrowTypeId>(arrow_array->type_id());
        auto conv_func = get_arrow_converter(
            arrow_type, slot->type().type, slot->is_nullable(), false);
        
        if (conv_func == nullptr) {
            return Status::NotSupported(
                strings::Substitute("Unsupported Arrow type conversion: $0 -> $1",
                                  arrow_array->type()->ToString(), slot->type().debug_string()));
        }
        
        // 4. 处理NULL列
        ConvertFuncTree conv_tree(conv_func);
        uint8_t* null_data = nullptr;
        std::vector<uint8_t> null_buffer;
        if (slot->is_nullable()) {
            null_buffer.resize(batch.num_rows());
            null_data = null_buffer.data();
            fill_null_column(arrow_array.get(), 0, batch.num_rows(),
                down_cast<NullableColumn*>(column.get())->null_column().get(), 0);
        }
        
        // 5. 执行转换
        RETURN_IF_ERROR(conv_func(arrow_array.get(), 0, batch.num_rows(), 
            column.get(), 0, null_data, &chunk_filter, &conv_ctx, &conv_tree));
        
        new_chunk->append_column(column, slot->id());
    }
    
    *chunk = new_chunk;
    return Status::OK();
}
```

## 6. 执行流程控制

```cpp
Status StarRocksDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    // 延迟初始化
    if (!_opened) {
        RETURN_IF_ERROR(open(state));
    }
    
    // 检查是否已结束
    if (_eos) {
        return Status::EndOfFile("StarRocks connector scan finished");
    }
    
    // 延迟建立连接和打开scanner
    if (!_scanner_opened) {
        RETURN_IF_ERROR(create_thrift_client());  // 建立连接
        RETURN_IF_ERROR(call_open_scanner());     // 打开scanner
        _scanner_opened = true;
    }
    
    // 拉取数据
    return call_get_next(chunk);
}

void StarRocksDataSource::close(RuntimeState* state) {
    // 释放远程资源
    if (_scanner_opened && !_context_id.empty()) {
        Status st = call_close_scanner();
        if (!st.ok()) {
            LOG(WARNING) << "Failed to close StarRocks scanner: " << st;
        }
    }
    
    // 关闭连接
    _thrift_client.reset();
    _scanner_opened = false;
    _eos = false;
}
```

## 关键设计特点

1. **延迟连接**: RPC连接在第一次get_next时才建立，避免无用连接
2. **自动重试**: 连接和RPC调用失败时自动切换BE节点
3. **状态一致性**: 通过offset确保数据不丢失、不重复
4. **资源管理**: 确保异常情况下也能正确释放资源
5. **类型安全**: 完整的Arrow类型到StarRocks类型转换
6. **零拷贝**: Arrow反序列化使用零拷贝Buffer包装
7. **错误处理**: 详细的错误信息和日志记录
