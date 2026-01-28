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

#include <algorithm>
#include <array>
#include <cctype>
#include <string>
#include <string_view>

#include "column/chunk.h"
#include "common/logging.h"
#include "connector/starrocks_connector.h"
#include "exec/connector_scan_node.h"
#include "fs/fs.h"
#include "gen_cpp/CloudConfiguration_types.h"
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
    LOG(INFO) << "tablet_root_paths JSON: " << tablet_root_paths_json;
    
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
        LOG(INFO) << "Looking for tablet_id: " << tablet_id_key << " in tablet_root_paths";
        
        vpack::Slice path_slice = json_slice.get(tablet_id_key);
        
        if (path_slice.isNone() || !path_slice.isString()) {
            return Status::NotFound(strings::Substitute(
                "Storage path for tablet $0 not found in tablet_root_paths", _scan_range_ctx.tablet_id));
        }
        
        *storage_path = path_slice.copyString();
        LOG(INFO) << "Extracted storage_path for tablet " << tablet_id_key << ": " << *storage_path;
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

Status StarRocksLakeDataSource::build_cloud_configuration(TCloudConfiguration* cloud_conf) {
    const auto& ctx = _provider->execution_context();
    
    // Extract cloud storage properties and build cloud_properties map
    // Support multiple prefixes: fs.*, aws.s3.*, aliyun.oss.*
    std::map<std::string, std::string> cloud_properties;
    bool has_cloud_properties = false;
    bool has_s3_compatible_properties = false;

    auto is_true_value = [](const std::string& value) -> bool {
        if (value.empty()) {
            return false;
        }
        std::string lower = value;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return lower == "true" || lower == "1";
    };

    auto prefer_virtual_host_style = [](const std::string& endpoint) -> bool {
        if (endpoint.empty()) {
            return false;
        }
        std::string lower = endpoint;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        std::string_view view = lower;
        auto scheme_pos = view.find("://");
        if (scheme_pos != std::string_view::npos) {
            view.remove_prefix(scheme_pos + 3);
        }
        auto slash_pos = view.find('/');
        if (slash_pos != std::string_view::npos) {
            view = view.substr(0, slash_pos);
        }

        static const std::array<std::string_view, 8> kVirtualHostDomains = {
                ".amazonaws.com", ".aliyuncs.com", ".myhuaweicloud.com", ".myqcloud.com",
                ".volces.com", ".ivolces.com", ".ksyuncs.com", "storage.googleapis.com"};
        for (const auto& suffix : kVirtualHostDomains) {
            if (view.size() >= suffix.size() &&
                view.compare(view.size() - suffix.size(), suffix.size(), suffix) == 0) {
                return true;
            }
        }
        return false;
    };
    
    LOG(INFO) << "=== Extracting cloud storage properties from execution context ===";
    LOG(INFO) << "Total properties in context: " << ctx.properties.size();
    
    for (const auto& [key, value] : ctx.properties) {
        // Check if it's a cloud storage property
        bool is_fs_prop = key.find("fs.") == 0;
        bool is_aws_prop = key.find("aws.s3.") == 0;
        bool is_aliyun_prop = key.find("aliyun.oss.") == 0;
        
        if (is_fs_prop || is_aws_prop || is_aliyun_prop) {
            has_cloud_properties = true;
            // Print all cloud storage properties in plain text for debugging
            LOG(INFO) << "  " << key << " = " << value;
            
            if (is_fs_prop) {
                // Transform fs.oss.* to aws.s3.* and aliyun.oss.* for compatibility
                if (key == "fs.oss.accessKeyId" || key == "fs.oss.access_key") {
                    cloud_properties["aws.s3.access_key"] = value;
                    cloud_properties["aliyun.oss.access_key"] = value;
                    has_s3_compatible_properties = true;
                } else if (key == "fs.oss.accessKeySecret" || key == "fs.oss.secret_key") {
                    cloud_properties["aws.s3.secret_key"] = value;
                    cloud_properties["aliyun.oss.secret_key"] = value;
                    has_s3_compatible_properties = true;
                } else if (key == "fs.oss.endpoint") {
                    cloud_properties["aws.s3.endpoint"] = value;
                    cloud_properties["aliyun.oss.endpoint"] = value;
                    has_s3_compatible_properties = true;
                } else if (key.find("fs.s3.") == 0) {
                    // Transform fs.s3.* to aws.s3.*
                    std::string aws_key = "aws." + key.substr(3); // Remove "fs." prefix
                    cloud_properties[aws_key] = value;
                    has_s3_compatible_properties = true;
                } else {
                    // Keep other fs.* properties as-is
                    cloud_properties[key] = value;
                }
            } else if (is_aws_prop) {
                // aws.s3.* properties: transform aws.s3.accessKeyId/accessKeySecret to standard keys
                if (key == "aws.s3.accessKeyId") {
                    cloud_properties["aws.s3.access_key"] = value;
                } else if (key == "aws.s3.accessKeySecret") {
                    cloud_properties["aws.s3.secret_key"] = value;
                } else {
                    // Keep other aws.s3.* properties as-is
                    cloud_properties[key] = value;
                }
                has_s3_compatible_properties = true;
            } else if (is_aliyun_prop) {
                // aliyun.oss.* properties: already in correct format, keep as-is
                cloud_properties[key] = value;
                if (key == "aliyun.oss.access_key" &&
                    cloud_properties.find("aws.s3.access_key") == cloud_properties.end()) {
                    cloud_properties["aws.s3.access_key"] = value;
                } else if (key == "aliyun.oss.secret_key" &&
                           cloud_properties.find("aws.s3.secret_key") == cloud_properties.end()) {
                    cloud_properties["aws.s3.secret_key"] = value;
                } else if (key == "aliyun.oss.endpoint" &&
                           cloud_properties.find("aws.s3.endpoint") == cloud_properties.end()) {
                    cloud_properties["aws.s3.endpoint"] = value;
                }
                has_s3_compatible_properties = true;
            }
        }
    }
    
    // Also set default S3-compatible settings for OSS
    if (has_cloud_properties) {
        // Auto-detect SSL setting from endpoint
        auto endpoint_it = cloud_properties.find("aws.s3.endpoint");
        if (endpoint_it != cloud_properties.end()) {
            const std::string& endpoint = endpoint_it->second;
            if (endpoint.find("http://") == 0) {
                // HTTP endpoint, disable SSL
                cloud_properties["aws.s3.enable_ssl"] = "false";
                LOG(INFO) << "  Auto-detected HTTP endpoint, setting: aws.s3.enable_ssl = false";
            } else if (endpoint.find("https://") == 0) {
                // HTTPS endpoint, enable SSL
                cloud_properties["aws.s3.enable_ssl"] = "true";
                LOG(INFO) << "  Auto-detected HTTPS endpoint, setting: aws.s3.enable_ssl = true";
            }
        }

        bool prefer_virtual_host = false;
        if (endpoint_it != cloud_properties.end()) {
            prefer_virtual_host = prefer_virtual_host_style(endpoint_it->second);
        }

        auto path_style_it = cloud_properties.find("aws.s3.enable_path_style_access");
        if (prefer_virtual_host) {
            if (path_style_it == cloud_properties.end() || is_true_value(path_style_it->second)) {
                cloud_properties["aws.s3.enable_path_style_access"] = "false";
                LOG(INFO) << "  Endpoint prefers virtual host style, setting: aws.s3.enable_path_style_access = false";
            }
        } else if (path_style_it == cloud_properties.end()) {
            // Default to path style for compatibility with most S3-compatible providers.
            cloud_properties["aws.s3.enable_path_style_access"] = "true";
            LOG(INFO) << "  Setting default: aws.s3.enable_path_style_access = true";
        }
    }
    
    if (!has_cloud_properties) {
        LOG(WARNING) << "No cloud storage properties (fs.*, aws.s3.*, aliyun.oss.*) found for object_store mode. "
                     << "Object storage access may fail without proper credentials.";
        return Status::OK();  // Not an error, but will likely fail later
    }
    
    if (has_s3_compatible_properties) {
        cloud_conf->__set_cloud_type(TCloudType::AWS);
    }

    // Set cloud_properties in TCloudConfiguration
    cloud_conf->__set_cloud_properties(cloud_properties);
    cloud_conf->__isset.cloud_properties = true;
    
    LOG(INFO) << "Built CloudConfiguration with " << cloud_properties.size() 
              << " properties (including transformed keys) for object storage access";
    
    // Log final property keys for verification
    LOG(INFO) << "Final cloud_properties keys:";
    for (const auto& [k, v] : cloud_properties) {
        bool is_secret = (k.find("secret") != std::string::npos || k.find("key") != std::string::npos);
        if (is_secret && !v.empty()) {
            LOG(INFO) << "    " << k << " = " << v.substr(0, 4) << "***";
        } else {
            LOG(INFO) << "    " << k << " = " << v;
        }
    }
    
    return Status::OK();
}

Status StarRocksLakeDataSource::init_lake_reader(RuntimeState* state) {
    // Parse storage path for this tablet
    std::string storage_path;
    RETURN_IF_ERROR(parse_tablet_root_path(&storage_path));

    // Build cloud configuration from fs.* properties
    TCloudConfiguration cloud_conf;
    RETURN_IF_ERROR(build_cloud_configuration(&cloud_conf));
    
    // Create FileSystem with credentials for accessing object storage
    if (cloud_conf.__isset.cloud_properties && !cloud_conf.cloud_properties.empty()) {
        FSOptions fs_options(&cloud_conf);
        auto fs_result = FileSystem::CreateUniqueFromString(storage_path, fs_options);
        if (!fs_result.ok()) {
            return Status::InternalError(strings::Substitute(
                "Failed to create FileSystem for storage path '$0' with credentials: $1. "
                "Please verify fs.* properties (fs.oss.accessKeyId, fs.oss.accessKeySecret, fs.oss.endpoint, etc.) "
                "are correctly configured in the catalog.",
                storage_path, fs_result.status().message()));
        }
        _fs_with_credentials = std::move(fs_result).value();
        LOG(INFO) << "Successfully created FileSystem with configured credentials for " << storage_path;
    } else {
        LOG(WARNING) << "No cloud credentials provided, will use default FileSystem (may fail for private storage)";
    }

    // Create FixedLocationProvider with the storage root
    auto location_provider = std::make_shared<lake::FixedLocationProvider>(storage_path);

    const std::string tablet_meta_location =
            location_provider->tablet_metadata_location(_scan_range_ctx.tablet_id, _scan_range_ctx.version);
    const std::string bundle_meta_location =
            location_provider->bundle_tablet_metadata_location(_scan_range_ctx.tablet_id, _scan_range_ctx.version);
    LOG(INFO) << "Resolved tablet metadata location: " << tablet_meta_location;
    LOG(INFO) << "Resolved bundle tablet metadata location: " << bundle_meta_location;
    
    // Create local TabletManager with zero cache (ephemeral usage for external scan)
    _lake_tablet_manager = std::make_shared<lake::TabletManager>(location_provider, 0);
    
    // Resolve tablet metadata using the manager's detection logic (bundle vs per-tablet).
    // This ensures we only read the correct metadata format for the tablet.
    ASSIGN_OR_RETURN(auto tablet_metadata,
                     _lake_tablet_manager->get_single_tablet_metadata(_scan_range_ctx.tablet_id,
                                                               _scan_range_ctx.version, true, 0,
                                                               _fs_with_credentials));
    
    // Manually construct VersionedTablet with the metadata
    lake::VersionedTablet versioned_tablet(_lake_tablet_manager.get(), std::move(tablet_metadata));
    LOG(INFO) << "111";
    // Get tablet schema (returns shared_ptr directly, not StatusOr)
    auto tablet_schema = versioned_tablet.get_schema();
    LOG(INFO) << "22";
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
    
    // CRITICAL: Pass FileSystem with credentials via LakeIOOptions
    // This ensures all data file reads use the correct object storage credentials
    if (_fs_with_credentials) {
        params.lake_io_opts.fs = _fs_with_credentials;
        params.lake_io_opts.location_provider = location_provider;
        LOG(INFO) << "Passing FileSystem with credentials to TabletReader via lake_io_opts";
    }
    
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
