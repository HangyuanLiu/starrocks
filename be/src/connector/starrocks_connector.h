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

#pragma once

#include <map>
#include <vector>

#include <optional>

#include "connector/connector.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "gen_cpp/StarrocksExternalService_types.h"
#include "common/statusor.h"
#include "util/thrift_client.h"
#include "gen_cpp/TStarrocksExternalService.h"

namespace arrow {
class RecordBatch;
}

namespace starrocks {

class ConnectorScanNode;
class TupleDescriptor;

namespace connector {

struct StarRocksExecutionContext {
    std::string connector_name;
    std::string db_name;
    std::string table_name;
    std::string opaqued_query_plan;
    std::map<std::string, std::string> properties;
    std::vector<std::string> be_rpc_endpoints;
    int32_t connect_timeout_ms = 10000;
    int32_t read_timeout_ms = 30000;
    int32_t request_retries = 3;

    std::string property_or_empty(const std::string& key) const {
        auto it = properties.find(key);
        if (it == properties.end()) {
            return "";
        }
        return it->second;
    }

    std::string user() const { return property_or_empty("user"); }
    std::string password() const { return property_or_empty("password"); }
    std::string fetch_mode() const { return property_or_empty("fetch_mode"); }
};

class StarRocksConnector final : public Connector {
public:
    StarRocksConnector() = default;
    ~StarRocksConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::STARROCKS; }
};

class StarRocksDataSourceProvider final : public DataSourceProvider {
public:
    StarRocksDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);

    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

    const StarRocksExecutionContext& execution_context() const { return _execution_context; }

private:
    ConnectorScanNode* _scan_node;
    TConnectorScanNode _scan_node_thrift;
    StarRocksExecutionContext _execution_context;
};

class StarRocksDataSource final : public DataSource {
public:
    StarRocksDataSource(const StarRocksDataSourceProvider* provider, const TScanRange& scan_range);
    ~StarRocksDataSource() override = default;

    std::string name() const override { return "StarRocksDataSource"; }

    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;
    void close(RuntimeState* state) override;

    int64_t raw_rows_read() const override { return _num_rows_read; }
    int64_t num_rows_read() const override { return _num_rows_read; }
    int64_t num_bytes_read() const override { return _num_bytes_read; }
    int64_t cpu_time_spent() const override { return 0; }

private:
    const StarRocksDataSourceProvider* _provider;
    const TScanRange* _scan_range;
    bool _opened = false;
    bool _scanner_opened = false;
    bool _eos = false;
    const TupleDescriptor* _tuple_desc = nullptr;

    struct ScanRangeContext {
        int64_t tablet_id = 0;
        int64_t version = -1;
        int32_t schema_hash = 0;
        std::string db_name;
        std::string table_name;
        std::vector<TNetworkAddress> hosts;
    };

    Status init_scan_range_context();
    Status build_open_params(RuntimeState* state);
    Status ensure_query_plan_parsed();
    
    Status create_thrift_client();
    Status call_open_scanner();
    Status call_get_next(ChunkPtr* chunk);
    Status call_close_scanner();
    Status deserialize_arrow_batch(const std::string& serialized_data, std::shared_ptr<arrow::RecordBatch>* batch);
    Status convert_arrow_to_chunk(const arrow::RecordBatch& batch, ChunkPtr* chunk);

    ScanRangeContext _scan_range_ctx;
    bool _scan_range_initialized = false;
    bool _open_params_initialized = false;
    TScanOpenParams _open_params;
    bool _query_plan_initialized = false;
    TQueryPlanInfo _query_plan_info;
    
    std::unique_ptr<ThriftClient<TStarrocksExternalServiceClient>> _thrift_client;
    std::string _context_id;
    int64_t _offset = 0;
    int64_t _num_rows_read = 0;
    int64_t _num_bytes_read = 0;
    size_t _current_host_index = 0;
};

} // namespace connector
} // namespace starrocks
