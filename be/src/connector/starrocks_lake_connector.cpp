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

#include "connector/starrocks_lake_connector.h"

#include <string>
#include <string_view>

#include "column/chunk.h"
#include "common/logging.h"
#include "connector/starrocks_connector.h"
#include "exec/connector_scan_node.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/projection_iterator.h"
#include "util/json.h"
#include "velocypack/vpack.h"

namespace starrocks::connector {

StarRocksLakeDataSource::StarRocksLakeDataSource(const StarRocksDataSourceProvider* provider,
                                                 const TScanRange& scan_range)
        : _provider(provider), _scan_range(&scan_range) {}

Status StarRocksLakeDataSource::open(RuntimeState* state) {
    if (_opened) {
        return Status::OK();
    }
    
    _tuple_desc = const_cast<TupleDescriptor*>(_provider->tuple_descriptor(state));
    if (_tuple_desc == nullptr) {
        return Status::InternalError("tuple descriptor is null for starrocks lake connector");
    }
    
    RETURN_IF_ERROR(init_scan_range_context());
    RETURN_IF_ERROR(init_lake_reader(state));
    
    _opened = true;
    return Status::OK();
}

Status StarRocksLakeDataSource::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if (!_opened) {
        RETURN_IF_ERROR(open(state));
    }
    
    if (_eos) {
        return Status::EndOfFile("StarRocks lake connector scan finished");
    }
    
    if (!_lake_reader_opened || !_prj_iter) {
        return Status::InternalError("Lake reader not opened");
    }
    
    RETURN_IF_ERROR(state->check_mem_limit("read chunk from lake storage"));
    
    ASSIGN_OR_RETURN(auto chunk_ptr, 
                     ChunkHelper::new_chunk_pooled_checked(_prj_iter->output_schema(), state->chunk_size()));
    chunk->reset(chunk_ptr);
    
    Status status = _prj_iter->get_next(chunk_ptr);
    
    if (!status.ok()) {
        if (status.is_end_of_file()) {
            _eos = true;
        }
        return status;
    }
    
    // Update metrics
    _num_rows_read += chunk_ptr->num_rows();
    _num_bytes_read += chunk_ptr->bytes_usage();
    
    return Status::OK();
}

void StarRocksLakeDataSource::close(RuntimeState* state) {
    if (_prj_iter) {
        _prj_iter->close();
        _prj_iter.reset();
    }
    _lake_tablet_manager.reset();
    _lake_reader_opened = false;
    _eos = false;
}

Status StarRocksLakeDataSource::init_scan_range_context() {
    if (_scan_range_initialized) {
        return Status::OK();
    }
    
    if (_scan_range == nullptr || !_scan_range->__isset.internal_scan_range) {
        return Status::InvalidArgument("StarRocks lake scan range missing internal_scan_range");
    }
    
    const auto& internal = _scan_range->internal_scan_range;
    _scan_range_ctx.tablet_id = internal.tablet_id;
    _scan_range_ctx.db_name = internal.db_name;
    _scan_range_ctx.table_name = internal.table_name;

    auto parse_long = [](const std::string& value, int64_t default_value, const char* field) -> int64_t {
        if (value.empty()) {
            return default_value;
        }
        try {
            return std::stoll(value);
        } catch (const std::exception& ex) {
            LOG(WARNING) << "Failed to parse '" << field << "' value '" << value
                         << "' for StarRocks lake scan range: " << ex.what();
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
                         << "' for StarRocks lake scan range: " << ex.what();
            return default_value;
        }
    };

    _scan_range_ctx.version = parse_long(internal.version, _scan_range_ctx.version, "version");
    _scan_range_ctx.schema_hash = parse_int(internal.schema_hash, _scan_range_ctx.schema_hash, "schema_hash");

    _scan_range_initialized = true;
    return Status::OK();
}

Status StarRocksLakeDataSource::parse_tablet_root_path(std::string* storage_path) {
    const auto& ctx = _provider->execution_context();
    
    // Parse tablet_root_paths mapping from properties
    auto it = ctx.properties.find("tablet_root_paths");
    if (it == ctx.properties.end()) {
        return Status::NotFound("tablet_root_paths not found in properties for object_store mode");
    }
    
    std::string tablet_root_paths_json = it->second;
    
    // Parse JSON mapping to extract storage path for this tablet
    try {
        auto json_value_or = JsonValue::parse_json_or_string(Slice(tablet_root_paths_json));
        if (!json_value_or.ok()) {
            return Status::InvalidArgument("Failed to parse tablet_root_paths JSON");
        }
        
        auto json_slice = json_value_or.value().to_vslice();
        if (!json_slice.isObject()) {
            return Status::InvalidArgument("tablet_root_paths is not a JSON object");
        }
        
        std::string tablet_id_key = std::to_string(_scan_range_ctx.tablet_id);
        vpack::Slice path_slice = json_slice.get(tablet_id_key);
        
        if (path_slice.isNone() || !path_slice.isString()) {
            return Status::NotFound(strings::Substitute(
                "Storage path for tablet $0 not found in tablet_root_paths", _scan_range_ctx.tablet_id));
        }
        
        *storage_path = path_slice.copyString();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(strings::Substitute(
            "Failed to parse tablet storage path: $0", e.what()));
    }

    if (storage_path->empty()) {
        return Status::InvalidArgument(strings::Substitute(
            "Empty storage path for tablet $0", _scan_range_ctx.tablet_id));
    }

    return Status::OK();
}

Status StarRocksLakeDataSource::init_lake_reader(RuntimeState* state) {
    // Parse storage path for this tablet
    std::string storage_path;
    RETURN_IF_ERROR(parse_tablet_root_path(&storage_path));

    // Create FixedLocationProvider with the storage root
    auto location_provider = std::make_shared<lake::FixedLocationProvider>(storage_path);
    
    // Create local TabletManager with zero cache (ephemeral usage for external scan)
    _lake_tablet_manager = std::make_shared<lake::TabletManager>(location_provider, 0);
    
    // Get VersionedTablet (not just Tablet)
    ASSIGN_OR_RETURN(auto versioned_tablet, 
                     _lake_tablet_manager->get_tablet(_scan_range_ctx.tablet_id, _scan_range_ctx.version));
    
    // Get tablet schema (returns shared_ptr directly, not StatusOr)
    auto tablet_schema = versioned_tablet.get_schema();
    
    // Build scanner columns from tuple descriptor (columns to read)
    std::vector<uint32_t> scanner_columns;
    for (const auto* slot : _tuple_desc->slots()) {
        if (slot->is_materialized()) {
            int32_t index = tablet_schema->field_index(slot->col_name());
            if (index < 0) {
                return Status::InternalError(strings::Substitute(
                    "Column '$0' not found in tablet schema", slot->col_name()));
            }
            scanner_columns.push_back(index);
        }
    }
    
    if (scanner_columns.empty()) {
        return Status::InternalError("No materialized slots for lake reader");
    }
    
    // Build schema for reading
    starrocks::Schema reader_schema = ChunkHelper::convert_schema(tablet_schema, scanner_columns);
    
    // Create reader using versioned_tablet.new_reader
    // new_reader returns StatusOr<unique_ptr<TabletReader>>
    // We need to convert unique_ptr to shared_ptr and assign to _prj_iter (shared_ptr<ChunkIterator>)
    ASSIGN_OR_RETURN(auto reader_ptr, versioned_tablet.new_reader(std::move(reader_schema)));
    
    // Convert unique_ptr to shared_ptr and cast to ChunkIterator base class
    // TabletReader inherits from ChunkIterator, so this is safe
    std::shared_ptr<lake::TabletReader> reader = std::move(reader_ptr);
    _prj_iter = reader;
    
    // Prepare params for reader
    TabletReaderParams params;
    params.reader_type = READER_QUERY;
    params.chunk_size = state->chunk_size();
    params.is_pipeline = true;
    params.skip_aggregation = true;  // External connector: no aggregation
    params.profile = nullptr;  // Simplified: no detailed profiling
    params.runtime_state = state;
    params.use_page_cache = false;  // Simplified: no page cache for external read
    
    // Initialize encoded schema and output schema
    // Use empty maps since we don't have global dicts or unused columns for external connector
    ColumnIdToGlobalDictMap empty_dict_map;
    std::unordered_set<uint32_t> empty_unused_cols;
    
    RETURN_IF_ERROR(_prj_iter->init_encoded_schema(empty_dict_map));
    RETURN_IF_ERROR(_prj_iter->init_output_schema(empty_unused_cols));
    
    // Prepare and open the reader
    RETURN_IF_ERROR(reader->prepare());
    RETURN_IF_ERROR(reader->open(params));
    
    _lake_reader_opened = true;
    
    LOG(INFO) << "Lake reader initialized for tablet " << _scan_range_ctx.tablet_id 
              << " version " << _scan_range_ctx.version << " at " << storage_path;
    
    return Status::OK();
}

} // namespace starrocks::connector
