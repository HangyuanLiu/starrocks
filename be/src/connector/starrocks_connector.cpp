// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "connector/starrocks_connector.h"

#include <cctype>
#include <cstdlib>
#include <string_view>

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "exec/arrow_to_starrocks_converter.h"
#include "exec/arrow_type_traits.h"
#include "exec/connector_scan_node.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "common/status.h"
#include "util/thrift_util.h"
#include "util/url_coding.h"

namespace starrocks::connector {

namespace {

std::string trim_copy(std::string_view value) {
    size_t start = 0;
    size_t end = value.size();
    while (start < end && std::isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return std::string(value.substr(start, end - start));
}

std::vector<std::string> parse_endpoints(const std::map<std::string, std::string>& properties) {
    std::vector<std::string> endpoints;
    auto it = properties.find("be_rpc_endpoints");
    if (it == properties.end()) {
        return endpoints;
    }
    std::string_view view(it->second);
    size_t start = 0;
    while (start <= view.size()) {
        size_t pos = view.find_first_of(",;", start);
        size_t length = (pos == std::string_view::npos) ? (view.size() - start) : (pos - start);
        std::string token = trim_copy(view.substr(start, length));
        if (!token.empty()) {
            endpoints.push_back(std::move(token));
        }
        if (pos == std::string_view::npos) {
            break;
        }
        start = pos + 1;
    }
    return endpoints;
}

int parse_int_property(const std::map<std::string, std::string>& properties, const std::string& key, int default_value) {
    auto it = properties.find(key);
    if (it == properties.end()) {
        return default_value;
    }
    try {
        return std::stoi(it->second);
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Invalid integer value '" << it->second << "' for property '" << key
                     << "' in starrocks connector scan node, using default " << default_value << ": " << ex.what();
        return default_value;
    }
}

} // namespace

DataSourceProviderPtr StarRocksConnector::create_data_source_provider(ConnectorScanNode* scan_node,
                                                                      const TPlanNode& plan_node) const {
    return std::make_unique<StarRocksDataSourceProvider>(scan_node, plan_node);
}

// ================================================================

StarRocksDataSourceProvider::StarRocksDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node)
        : _scan_node(scan_node), _scan_node_thrift(plan_node.connector_scan_node) {
    if (_scan_node_thrift.__isset.connector_name) {
        _execution_context.connector_name = _scan_node_thrift.connector_name;
    }
    if (_scan_node_thrift.__isset.db_name) {
        _execution_context.db_name = _scan_node_thrift.db_name;
    }
    if (_scan_node_thrift.__isset.table_name) {
        _execution_context.table_name = _scan_node_thrift.table_name;
    }
    if (_scan_node_thrift.__isset.opaqued_query_plan) {
        _execution_context.opaqued_query_plan = _scan_node_thrift.opaqued_query_plan;
    }
    if (_scan_node_thrift.__isset.properties) {
        _execution_context.properties = _scan_node_thrift.properties;
    }
    _execution_context.be_rpc_endpoints = parse_endpoints(_execution_context.properties);
    _execution_context.connect_timeout_ms =
            parse_int_property(_execution_context.properties, "connect_timeout_ms",
                               _execution_context.connect_timeout_ms);
    _execution_context.read_timeout_ms =
            parse_int_property(_execution_context.properties, "read_timeout_ms", _execution_context.read_timeout_ms);
    _execution_context.request_retries =
            parse_int_property(_execution_context.properties, "request_retries", _execution_context.request_retries);
}

DataSourcePtr StarRocksDataSourceProvider::create_data_source(const TScanRange& scan_range) {
    return std::make_unique<StarRocksDataSource>(this, scan_range);
}

const TupleDescriptor* StarRocksDataSourceProvider::tuple_descriptor(RuntimeState* state) const {
    if (_scan_node_thrift.__isset.tuple_id) {
        return state->desc_tbl().get_tuple_descriptor(_scan_node_thrift.tuple_id);
    }
    const auto& tuple_descs = _scan_node->row_desc().tuple_descriptors();
    if (!tuple_descs.empty()) {
        return tuple_descs[0];
    }
    return nullptr;
}

// ================================================================

StarRocksDataSource::StarRocksDataSource(const StarRocksDataSourceProvider* provider, const TScanRange& scan_range)
        : _provider(provider), _scan_range(&scan_range) {}

Status StarRocksDataSource::open(RuntimeState* state) {
    if (_opened) {
        return Status::OK();
    }
    _tuple_desc = const_cast<TupleDescriptor*>(_provider->tuple_descriptor(state));
    if (_tuple_desc == nullptr) {
        return Status::InternalError("tuple descriptor is null for starrocks connector");
    }
    RETURN_IF_ERROR(init_scan_range_context());
    RETURN_IF_ERROR(build_open_params(state));
    _opened = true;
    return Status::OK();
}

Status StarRocksDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (!_opened) {
        RETURN_IF_ERROR(open(state));
    }
    
    if (_eos) {
        return Status::EndOfFile("StarRocks connector scan finished");
    }
    
    if (!_scanner_opened) {
        RETURN_IF_ERROR(create_thrift_client());
        RETURN_IF_ERROR(call_open_scanner());
        _scanner_opened = true;
    }
    
    return call_get_next(chunk);
}

void StarRocksDataSource::close(RuntimeState* state) {
    if (_scanner_opened && !_context_id.empty()) {
        Status st = call_close_scanner();
        if (!st.ok()) {
            LOG(WARNING) << "Failed to close StarRocks scanner: " << st;
        }
    }
    _thrift_client.reset();
    _scanner_opened = false;
    _eos = false;
}

Status StarRocksDataSource::init_scan_range_context() {
    if (_scan_range_initialized) {
        return Status::OK();
    }
    if (_scan_range == nullptr || !_scan_range->__isset.internal_scan_range) {
        return Status::InvalidArgument("StarRocks scan range missing internal_scan_range");
    }
    const auto& internal = _scan_range->internal_scan_range;
    _scan_range_ctx.tablet_id = internal.tablet_id;
    _scan_range_ctx.db_name = internal.db_name;
    _scan_range_ctx.table_name = internal.table_name;
    _scan_range_ctx.hosts.assign(internal.hosts.begin(), internal.hosts.end());

    auto parse_long = [](const std::string& value, int64_t default_value, const char* field) -> int64_t {
        if (value.empty()) {
            return default_value;
        }
        try {
            return std::stoll(value);
        } catch (const std::exception& ex) {
            LOG(WARNING) << "Failed to parse '" << field << "' value '" << value
                         << "' for StarRocks scan range: " << ex.what();
            return default_value;
        }
    };

    auto parse_int = [](const std::string& value, int32_t default_value, const char* field) -> int32_t {
        if (value.empty()) {
            return default_value;
        }
        try {
            return std::stoi(value);
        } catch (const std::exception& ex) {
            LOG(WARNING) << "Failed to parse '" << field << "' value '" << value
                         << "' for StarRocks scan range: " << ex.what();
            return default_value;
        }
    };

    _scan_range_ctx.version = parse_long(internal.version, _scan_range_ctx.version, "version");
    _scan_range_ctx.schema_hash = parse_int(internal.schema_hash, _scan_range_ctx.schema_hash, "schema_hash");

    _scan_range_initialized = true;
    return Status::OK();
}

Status StarRocksDataSource::ensure_query_plan_parsed() {
    if (_query_plan_initialized) {
        return Status::OK();
    }
    const auto& ctx = _provider->execution_context();
    if (ctx.opaqued_query_plan.empty()) {
        return Status::InvalidArgument("StarRocks connector missing opaqued_query_plan");
    }
    std::string decoded_plan;
    if (!base64_decode(ctx.opaqued_query_plan, &decoded_plan)) {
        return Status::InvalidArgument("Failed to decode StarRocks opaqued_query_plan");
    }
    const uint8_t* buffer = reinterpret_cast<const uint8_t*>(decoded_plan.data());
    uint32_t length = static_cast<uint32_t>(decoded_plan.size());
    Status st = deserialize_thrift_msg(buffer, &length, TProtocolType::BINARY, &_query_plan_info);
    if (!st.ok()) {
        return Status::InternalError("Failed to deserialize StarRocks query plan: " + st.to_string());
    }
    _query_plan_initialized = true;
    return Status::OK();
}

Status StarRocksDataSource::build_open_params(RuntimeState* state) {
    if (_open_params_initialized) {
        return Status::OK();
    }
    RETURN_IF_ERROR(init_scan_range_context());
    RETURN_IF_ERROR(ensure_query_plan_parsed());

    const auto& ctx = _provider->execution_context();
    std::string database = !_scan_range_ctx.db_name.empty() ? _scan_range_ctx.db_name : ctx.db_name;
    std::string table = !_scan_range_ctx.table_name.empty() ? _scan_range_ctx.table_name : ctx.table_name;
    if (database.empty() || table.empty()) {
        return Status::InvalidArgument("StarRocks connector missing database/table metadata");
    }

    TScanOpenParams params;
    params.__set_cluster(ctx.connector_name.empty() ? "starrocks" : ctx.connector_name);
    params.__set_database(database);
    params.__set_table(table);
    params.tablet_ids.push_back(_scan_range_ctx.tablet_id);
    params.__set_opaqued_query_plan(ctx.opaqued_query_plan);
    params.__set_batch_size(state->chunk_size());
    if (_read_limit >= 0) {
        params.__set_limit(_read_limit);
    }
    if (!ctx.user().empty()) {
        params.__set_user(ctx.user());
    }
    if (!ctx.password().empty()) {
        params.__set_passwd(ctx.password());
    }
    if (!ctx.properties.empty()) {
        params.__set_properties(ctx.properties);
    }
    auto mem_limit = state->query_mem_tracker_ptr() != nullptr ? state->query_mem_tracker_ptr()->limit() : -1;
    if (mem_limit > 0) {
        params.__set_mem_limit(mem_limit);
    }
    if (state->query_options().__isset.query_timeout) {
        params.__set_query_timeout(state->query_options().query_timeout);
    }

    _open_params = std::move(params);
    _open_params_initialized = true;
    return Status::OK();
}

Status StarRocksDataSource::create_thrift_client() {
    const auto& ctx = _provider->execution_context();
    
    if (_scan_range_ctx.hosts.empty()) {
        if (ctx.be_rpc_endpoints.empty()) {
            return Status::InvalidArgument("No BE endpoints available for StarRocks connector");
        }
        
        for (const auto& endpoint_str : ctx.be_rpc_endpoints) {
            size_t colon_pos = endpoint_str.find(':');
            if (colon_pos == std::string::npos) {
                LOG(WARNING) << "Invalid BE endpoint format: " << endpoint_str;
                continue;
            }
            TNetworkAddress addr;
            addr.hostname = endpoint_str.substr(0, colon_pos);
            try {
                addr.port = std::stoi(endpoint_str.substr(colon_pos + 1));
            } catch (const std::exception& e) {
                LOG(WARNING) << "Invalid port in BE endpoint: " << endpoint_str;
                continue;
            }
            _scan_range_ctx.hosts.push_back(addr);
        }
        
        if (_scan_range_ctx.hosts.empty()) {
            return Status::InvalidArgument("No valid BE endpoints after parsing");
        }
    }
    
    if (_current_host_index >= _scan_range_ctx.hosts.size()) {
        return Status::InternalError("All BE hosts failed for StarRocks connector");
    }
    
    const auto& host = _scan_range_ctx.hosts[_current_host_index];
    _thrift_client = std::make_unique<ThriftClient<TStarrocksExternalServiceClient>>(host.hostname, host.port);
    
    _thrift_client->set_conn_timeout(ctx.connect_timeout_ms);
    _thrift_client->set_recv_timeout(ctx.read_timeout_ms);
    _thrift_client->set_send_timeout(ctx.read_timeout_ms);
    
    Status st = _thrift_client->open_with_retry(ctx.request_retries, 100);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to connect to StarRocks BE " << host.hostname << ":" << host.port 
                     << ", error: " << st;
        _current_host_index++;
        _thrift_client.reset();
        
        if (_current_host_index < _scan_range_ctx.hosts.size()) {
            return create_thrift_client();
        }
        return st;
    }
    
    LOG(INFO) << "Connected to StarRocks BE " << host.hostname << ":" << host.port;
    return Status::OK();
}

Status StarRocksDataSource::call_open_scanner() {
    if (!_thrift_client || _thrift_client->iface() == nullptr) {
        return Status::InternalError("Thrift client not initialized");
    }
    
    TScanOpenResult result;
    try {
        _thrift_client->iface()->open_scanner(result, _open_params);
    } catch (apache::thrift::TException& e) {
        std::string err_msg = strings::Substitute("Failed to call open_scanner: $0", e.what());
        LOG(WARNING) << err_msg;
        
        _current_host_index++;
        _thrift_client.reset();
        if (_current_host_index < _scan_range_ctx.hosts.size()) {
            RETURN_IF_ERROR(create_thrift_client());
            return call_open_scanner();
        }
        return Status::ThriftRpcError(err_msg);
    }
    
    if (result.status.status_code != TStatusCode::OK) {
        std::string err_msg = "open_scanner failed";
        if (!result.status.error_msgs.empty()) {
            err_msg = result.status.error_msgs[0];
        }
        return Status::InternalError(err_msg);
    }
    
    if (!result.__isset.context_id || result.context_id.empty()) {
        return Status::InternalError("open_scanner returned empty context_id");
    }
    
    _context_id = result.context_id;
    _offset = 0;
    
    LOG(INFO) << "Successfully opened StarRocks scanner, context_id=" << _context_id;
    return Status::OK();
}

Status StarRocksDataSource::call_get_next(ChunkPtr* chunk) {
    if (!_thrift_client || _thrift_client->iface() == nullptr) {
        return Status::InternalError("Thrift client not initialized");
    }
    
    TScanNextBatchParams params;
    params.__set_context_id(_context_id);
    params.__set_offset(_offset);
    
    TScanBatchResult result;
    try {
        _thrift_client->iface()->get_next(result, params);
    } catch (apache::thrift::TException& e) {
        std::string err_msg = strings::Substitute("Failed to call get_next: $0", e.what());
        LOG(WARNING) << err_msg;
        return Status::ThriftRpcError(err_msg);
    }
    
    if (result.status.status_code != TStatusCode::OK) {
        std::string err_msg = "get_next failed";
        if (!result.status.error_msgs.empty()) {
            err_msg = result.status.error_msgs[0];
        }
        return Status::InternalError(err_msg);
    }
    
    if (result.__isset.eos && result.eos) {
        _eos = true;
        return Status::EndOfFile("StarRocks connector scan finished");
    }
    
    if (!result.__isset.rows || result.rows.empty()) {
        *chunk = std::make_shared<Chunk>();
        return Status::OK();
    }
    
    std::shared_ptr<arrow::RecordBatch> arrow_batch;
    RETURN_IF_ERROR(deserialize_arrow_batch(result.rows, &arrow_batch));
    
    RETURN_IF_ERROR(convert_arrow_to_chunk(*arrow_batch, chunk));
    
    _offset += arrow_batch->num_rows();
    _num_rows_read += arrow_batch->num_rows();
    _num_bytes_read += result.rows.size();
    
    return Status::OK();
}

Status StarRocksDataSource::call_close_scanner() {
    if (!_thrift_client || _thrift_client->iface() == nullptr || _context_id.empty()) {
        return Status::OK();
    }
    
    TScanCloseParams params;
    params.__set_context_id(_context_id);
    
    TScanCloseResult result;
    try {
        _thrift_client->iface()->close_scanner(result, params);
    } catch (apache::thrift::TException& e) {
        LOG(WARNING) << "Failed to call close_scanner: " << e.what();
        return Status::ThriftRpcError(e.what());
    }
    
    if (result.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "close_scanner returned error status";
        if (!result.status.error_msgs.empty()) {
            LOG(WARNING) << "Error: " << result.status.error_msgs[0];
        }
    }
    
    _context_id.clear();
    return Status::OK();
}

Status StarRocksDataSource::deserialize_arrow_batch(const std::string& serialized_data,
                                                     std::shared_ptr<arrow::RecordBatch>* batch) {
    auto buffer = arrow::Buffer::Wrap(serialized_data.data(), serialized_data.size());
    auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    
    auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
    if (!reader_res.ok()) {
        return Status::InternalError(
            strings::Substitute("Failed to create Arrow RecordBatch reader: $0", reader_res.status().ToString()));
    }
    
    auto reader = reader_res.ValueOrDie();
    auto read_res = reader->ReadNext();
    if (!read_res.ok()) {
        return Status::InternalError(
            strings::Substitute("Failed to read Arrow RecordBatch: $0", read_res.status().ToString()));
    }
    
    *batch = read_res.ValueOrDie().batch;
    if (*batch == nullptr) {
        return Status::InternalError("Deserialized Arrow RecordBatch is null");
    }
    
    return Status::OK();
}

Status StarRocksDataSource::convert_arrow_to_chunk(const arrow::RecordBatch& batch, ChunkPtr* chunk) {
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
    
    for (size_t i = 0; i < slots.size(); ++i) {
        const auto* slot = slots[i];
        const auto& arrow_array = batch.column(i);
        
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        column->reserve(batch.num_rows());
        
        ArrowConvertContext conv_ctx;
        conv_ctx.state = nullptr;
        conv_ctx.current_slot = const_cast<SlotDescriptor*>(slot);
        
        Filter chunk_filter(batch.num_rows(), 1);
        
        ArrowTypeId arrow_type = static_cast<ArrowTypeId>(arrow_array->type_id());
        auto conv_func = get_arrow_converter(arrow_type, slot->type().type, slot->is_nullable(), false);
        
        if (conv_func == nullptr) {
            return Status::NotSupported(
                strings::Substitute("Unsupported Arrow type conversion: $0 -> $1",
                                  arrow_array->type()->ToString(), slot->type().debug_string()));
        }
        
        ConvertFuncTree conv_tree(conv_func);
        uint8_t* null_data = nullptr;
        std::vector<uint8_t> null_buffer;
        if (slot->is_nullable()) {
            null_buffer.resize(batch.num_rows());
            null_data = null_buffer.data();
            fill_null_column(arrow_array.get(), 0, batch.num_rows(),
                           down_cast<NullableColumn*>(column.get())->null_column().get(), 0);
        }
        
        RETURN_IF_ERROR(conv_func(arrow_array.get(), 0, batch.num_rows(), column.get(), 0,
                                 null_data, &chunk_filter, &conv_ctx, &conv_tree));
        
        new_chunk->append_column(column, slot->id());
    }
    
    *chunk = new_chunk;
    return Status::OK();
}

} // namespace starrocks::connector
