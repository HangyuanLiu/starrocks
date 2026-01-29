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
#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "connector/connector.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "gen_cpp/QueryPlanExtra_types.h"

namespace starrocks {
class TupleDescriptor;
class SlotDescriptor;
namespace lake {
class TabletManager;
} // namespace lake
} // namespace starrocks

namespace starrocks::connector {

struct StarRocksExecutionContext;
class StarRocksDataSourceProvider;

/**
 * StarRocks Lake DataSource for object_store mode.
 * 
 * This DataSource directly reads tablet data from object storage using lake::TabletReader,
 * bypassing the need for RPC to Provider BE nodes. It provides better performance and
 * reduces Provider cluster load when reading shared-data (cloud-native) tables.
 */
class StarRocksLakeDataSource final : public DataSource {
public:
    StarRocksLakeDataSource(const StarRocksDataSourceProvider* provider, const TScanRange& scan_range);
    ~StarRocksLakeDataSource() override = default;

    std::string name() const override { return "StarRocksLakeDataSource"; }

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
    bool _eos = false;
    const TupleDescriptor* _tuple_desc = nullptr;

    struct ScanRangeContext {
        int64_t tablet_id = 0;
        int64_t version = -1;
        int32_t schema_hash = 0;
        std::string db_name;
        std::string table_name;
    };

    Status init_scan_range_context();
    Status init_lake_reader(RuntimeState* state);
    Status parse_tablet_root_path(std::string* storage_path);
    Status build_cloud_configuration(TCloudConfiguration* cloud_conf);

    ScanRangeContext _scan_range_ctx;
    bool _scan_range_initialized = false;
    
    // Lake reader components
    std::shared_ptr<lake::TabletManager> _lake_tablet_manager;
    std::shared_ptr<ChunkIterator> _prj_iter;
    std::shared_ptr<FileSystem> _fs_with_credentials;  // FileSystem with object storage credentials
    TCloudConfiguration _cloud_conf;
    std::vector<SlotDescriptor*> _materialized_slots;
    bool _lake_reader_opened = false;
    
    // Metrics
    int64_t _num_rows_read = 0;
    int64_t _num_bytes_read = 0;
};

} // namespace starrocks::connector
