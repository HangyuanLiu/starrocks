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

#include "exec/starrocks_table_sink.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/datum_convert.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "exprs/expr.h"
#include "fmt/format.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/protobuf_file.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/json.h"
#include "velocypack/Iterator.h"
#include "velocypack/vpack.h"

namespace starrocks {

namespace {

const std::string kDefaultPartitionValue = "__STARROCKS_DEFAULT_PARTITION__";

bool is_unsupported_object_storage_property(const std::string& key) {
    static const std::array<std::string, 7> kUnsupportedPrefixes = {
            "fs.s3a.", "fs.s3n.", "fs.s3.", "fs.oss.", "fs.cos.", "fs.obs.", "aliyun.oss."};
    for (const auto& prefix : kUnsupportedPrefixes) {
        if (key.rfind(prefix, 0) == 0) {
            return true;
        }
    }
    return false;
}

std::string join_keys(const std::vector<std::string>& keys) {
    std::string result;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i > 0) {
            result.append(", ");
        }
        result.append(keys[i]);
    }
    return result;
}

std::string to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

bool is_true_value(const std::string& value) {
    if (value.empty()) {
        return false;
    }
    std::string lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return lower == "true" || lower == "1";
}

bool prefer_virtual_host_style(const std::string& endpoint) {
    if (endpoint.empty()) {
        return false;
    }
    std::string lower = endpoint;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
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
}

} // namespace

StarRocksTableSink::StarRocksTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs, Status* status,
                                       RuntimeState* state)
        : _pool(pool), _t_output_exprs(t_exprs) {
    if (!t_exprs.empty()) {
        *status = Expr::create_expr_trees(_pool, t_exprs, &_output_expr_ctxs, state);
    }
}

StarRocksTableSink::~StarRocksTableSink() = default;

StarRocksTableSink::TabletWriterContext::~TabletWriterContext() = default;

Status StarRocksTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    DCHECK(thrift_sink.__isset.starrocks_table_sink);
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));

    _t_sink = thrift_sink.starrocks_table_sink;
    _txn_id = _t_sink.txn_id;
    if (_t_sink.__isset.label) {
        _label = _t_sink.label;
    }
    if (_t_sink.__isset.db_name) {
        _db_name = _t_sink.db_name;
    }
    if (_t_sink.__isset.table_name) {
        _table_name = _t_sink.table_name;
    }
    _tuple_id = _t_sink.tuple_id;

    if (_t_sink.__isset.properties) {
        std::vector<std::string> unsupported_keys;
        std::map<std::string, std::string> cloud_properties;
        bool has_cloud_properties = false;
        for (const auto& entry : _t_sink.properties) {
            if (is_unsupported_object_storage_property(entry.first)) {
                unsupported_keys.push_back(entry.first);
                continue;
            }
            if (entry.first.rfind("aws.s3.", 0) != 0) {
                continue;
            }
            has_cloud_properties = true;
            if (entry.first == "aws.s3.accessKeyId") {
                cloud_properties["aws.s3.access_key"] = entry.second;
            } else if (entry.first == "aws.s3.accessKeySecret") {
                cloud_properties["aws.s3.secret_key"] = entry.second;
            } else {
                cloud_properties[entry.first] = entry.second;
            }
        }

        if (has_cloud_properties) {
            auto endpoint_it = cloud_properties.find("aws.s3.endpoint");
            auto ssl_it = cloud_properties.find("aws.s3.enable_ssl");
            if (endpoint_it != cloud_properties.end() && ssl_it == cloud_properties.end()) {
                const std::string& endpoint = endpoint_it->second;
                if (endpoint.find("http://") == 0) {
                    cloud_properties["aws.s3.enable_ssl"] = "false";
                } else if (endpoint.find("https://") == 0) {
                    cloud_properties["aws.s3.enable_ssl"] = "true";
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
                }
            } else if (path_style_it == cloud_properties.end()) {
                cloud_properties["aws.s3.enable_path_style_access"] = "true";
            }

            _cloud_conf.__set_cloud_type(TCloudType::AWS);
            _cloud_conf.__set_cloud_properties(cloud_properties);
            _cloud_conf.__isset.cloud_properties = true;

            LOG(INFO) << "StarRocksTableSink built CloudConfiguration with " << cloud_properties.size()
                      << " aws.s3 properties";
            for (const auto& [k, v] : cloud_properties) {
                bool is_secret = (k.find("secret") != std::string::npos || k.find("key") != std::string::npos);
                if (is_secret && !v.empty()) {
                    LOG(INFO) << "    " << k << " = " << v.substr(0, 4) << "***";
                } else {
                    LOG(INFO) << "    " << k << " = " << v;
                }
            }
        } else {
            LOG(WARNING) << "StarRocksTableSink did not receive aws.s3.* properties; "
                         << "object storage access may fail";
        }

        if (!unsupported_keys.empty()) {
            return Status::InvalidArgument(fmt::format(
                    "Unsupported object storage properties detected: [{}]. "
                    "Only aws.s3.* properties are supported for StarRocks external writes.",
                    join_keys(unsupported_keys)));
        }
    }

    RETURN_IF_ERROR(_parse_tablet_root_paths());
    return _parse_tablet_versions();
}

Status StarRocksTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    if (_profile == nullptr) {
        _profile = state->obj_pool()->add(new RuntimeProfile("StarRocksTableSink"));
    }
    _profile->add_info_string("TxnID", fmt::format("{}", _txn_id));

    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));

    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_output_tuple_desc == nullptr) {
        return Status::InternalError("unknown destination tuple descriptor");
    }

    if (!_output_expr_ctxs.empty()) {
        if (_output_expr_ctxs.size() != _output_tuple_desc->slots().size()) {
            return Status::InternalError("number of exprs is not same with slots");
        }
        for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
            if (!is_type_compatible(_output_expr_ctxs[i]->root()->type().type,
                                    _output_tuple_desc->slots()[i]->type().type)) {
                auto msg = fmt::format(
                        "type of exprs is not match slot's, expr_type={}, slot_type={}, slot_name={}",
                        _output_expr_ctxs[i]->root()->type().type, _output_tuple_desc->slots()[i]->type().type,
                        _output_tuple_desc->slots()[i]->col_name());
                return Status::InternalError(msg);
            }
        }
    }

    std::unordered_map<std::string, SlotDescriptor*> slot_map;
    for (auto* slot : _output_tuple_desc->slots()) {
        if (slot == nullptr || slot->col_name().empty()) {
            continue;
        }
        slot_map.emplace(to_lower(slot->col_name()), slot);
    }

    if (_t_sink.__isset.partition_column_names) {
        for (const auto& column_name : _t_sink.partition_column_names) {
            auto it = slot_map.find(to_lower(column_name));
            if (it == slot_map.end()) {
                return Status::InternalError(fmt::format("partition column '{}' not found in sink tuple", column_name));
            }
            _partition_slots.emplace_back(it->second);
            _partition_types.emplace_back(it->second->type());
        }
    }

    if (_t_sink.__isset.distribution_column_names) {
        for (const auto& column_name : _t_sink.distribution_column_names) {
            auto it = slot_map.find(to_lower(column_name));
            if (it == slot_map.end()) {
                return Status::InternalError(
                        fmt::format("distribution column '{}' not found in sink tuple", column_name));
            }
            _distribution_slots.emplace_back(it->second);
        }
    }

    _partition_key_columns.clear();
    _partition_key_columns.resize(_partition_slots.size());
    _partition_key_pool = std::make_unique<MemPool>();

    return _init_partitions();
}

Status StarRocksTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    _open_done = true;
    return Status::OK();
}

Status StarRocksTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    if (chunk == nullptr || chunk->num_rows() == 0) {
        return Status::OK();
    }

    if (!_output_expr_ctxs.empty()) {
        _output_chunk = std::make_unique<Chunk>();
        for (size_t i = 0; i < _output_expr_ctxs.size(); ++i) {
            ASSIGN_OR_RETURN(ColumnPtr tmp, _output_expr_ctxs[i]->evaluate(chunk));
            MutableColumnPtr output_column = nullptr;
            if (tmp->only_null()) {
                output_column = ColumnHelper::create_column(_output_tuple_desc->slots()[i]->type(), true);
                output_column->append_nulls(chunk->num_rows());
            } else {
                output_column = ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(tmp));
            }
            _output_chunk->append_column(std::move(output_column), _output_tuple_desc->slots()[i]->id());
        }
        chunk = _output_chunk.get();
    } else {
        chunk->reset_slot_id_to_index();
        for (size_t i = 0; i < _output_tuple_desc->slots().size(); ++i) {
            chunk->set_slot_id_to_index(_output_tuple_desc->slots()[i]->id(), i);
        }
    }

    chunk->unpack_and_duplicate_const_columns();

    std::vector<uint32_t> hashes;
    _compute_hashes(chunk, &hashes);

    std::vector<PartitionInfo*> partitions;
    RETURN_IF_ERROR(_find_partitions(chunk, hashes, &partitions));

    std::unordered_map<int64_t, std::vector<uint32_t>> tablet_rows;
    std::unordered_map<int64_t, PartitionInfo*> tablet_partitions;
    std::unordered_map<int64_t, int64_t> tablet_backends;

    const size_t num_rows = chunk->num_rows();
    tablet_rows.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        auto* partition = partitions[i];
        if (partition == nullptr) {
            return Status::InternalError("failed to resolve partition for input row");
        }
        if (partition->tablets.empty()) {
            return Status::InternalError("partition has no tablets to write");
        }
        size_t tablet_index = hashes[i] % partition->tablets.size();
        const auto& tablet = partition->tablets[tablet_index];
        tablet_rows[tablet.tablet_id].push_back(static_cast<uint32_t>(i));
        tablet_partitions.emplace(tablet.tablet_id, partition);
        tablet_backends.emplace(tablet.tablet_id, tablet.backend_id);
    }

    for (auto& entry : tablet_rows) {
        int64_t tablet_id = entry.first;
        auto& indexes = entry.second;
        if (indexes.empty()) {
            continue;
        }
        auto tablet_chunk = chunk->clone_empty_with_slot(indexes.size());
        tablet_chunk->append_selective(*chunk, indexes.data(), 0, indexes.size());

        auto part_it = tablet_partitions.find(tablet_id);
        auto backend_it = tablet_backends.find(tablet_id);
        if (part_it == tablet_partitions.end() || backend_it == tablet_backends.end()) {
            return Status::InternalError("missing partition metadata for tablet writer");
        }

        ASSIGN_OR_RETURN(auto writer_ctx, _get_or_create_writer(tablet_id, part_it->second, backend_it->second));
        RETURN_IF_ERROR(writer_ctx->writer->write(*tablet_chunk));
    }

    return Status::OK();
}

Status StarRocksTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    _close_done = true;

    Expr::close(_output_expr_ctxs, state);

    if (!exec_status.ok()) {
        for (auto& entry : _tablet_writers) {
            if (entry.second && entry.second->writer) {
                entry.second->writer->close();
            }
        }
        return exec_status;
    }

    for (auto& entry : _tablet_writers) {
        RETURN_IF_ERROR(_finish_writer(entry.second.get()));
        _add_commit_info(entry.second.get());
    }

    if (_tablet_writers.empty()) {
        _add_dummy_commit_info();
    }

    return Status::OK();
}

Status StarRocksTableSink::try_open(RuntimeState* state) {
    if (_open_done) {
        return Status::OK();
    }
    return open(state);
}

bool StarRocksTableSink::is_open_done() {
    return _open_done;
}

Status StarRocksTableSink::open_wait() {
    return Status::OK();
}

bool StarRocksTableSink::is_full() {
    return false;
}

Status StarRocksTableSink::send_chunk_nonblocking(RuntimeState* state, Chunk* chunk) {
    return send_chunk(state, chunk);
}

Status StarRocksTableSink::try_close(RuntimeState* state) {
    _close_done = true;
    return Status::OK();
}

Status StarRocksTableSink::close_wait(RuntimeState* state, Status close_status) {
    return close(state, close_status);
}

bool StarRocksTableSink::is_close_done() {
    return _close_done;
}

void StarRocksTableSink::set_profile(RuntimeProfile* profile) {
    if (_profile != nullptr) {
        LOG(WARNING) << "StarRocksTableSink profile is set duplicated";
        return;
    }
    _profile = profile;
}

Status StarRocksTableSink::_init_partitions() {
    if (_t_sink.partitions.empty()) {
        return Status::InvalidArgument("starrocks table sink partitions are empty");
    }

    _partitions.clear();
    _partition_map.clear();

    bool is_unpartitioned = !_t_sink.__isset.partition_type ||
                            _t_sink.partition_type == TStarRocksPartitionType::UNPARTITIONED ||
                            _partition_slots.empty();

    for (const auto& t_part : _t_sink.partitions) {
        auto partition = std::make_unique<PartitionInfo>();
        if (t_part.__isset.id) {
            partition->id = t_part.id;
        }
        if (t_part.__isset.bucket_num) {
            partition->bucket_num = t_part.bucket_num;
        }
        if (t_part.__isset.distribution_type) {
            partition->distribution_type = t_part.distribution_type;
        }
        if (t_part.__isset.storage_path) {
            partition->storage_path = t_part.storage_path;
        }
        if (t_part.__isset.is_min_partition) {
            partition->is_min_partition = t_part.is_min_partition;
        }
        if (t_part.__isset.is_max_partition) {
            partition->is_max_partition = t_part.is_max_partition;
        }
        if (t_part.__isset.tablets) {
            partition->tablets.reserve(t_part.tablets.size());
            for (const auto& t_tablet : t_part.tablets) {
                if (!t_tablet.__isset.tablet_id || !t_tablet.__isset.backend_id) {
                    return Status::InternalError("tablet id or backend id is missing");
                }
                TabletInfo tablet_info;
                tablet_info.tablet_id = t_tablet.tablet_id;
                tablet_info.backend_id = t_tablet.backend_id;
                partition->tablets.emplace_back(std::move(tablet_info));
            }
        }

        if (is_unpartitioned) {
            partition->is_min_partition = true;
            partition->is_max_partition = true;
            partition->start_key.columns = nullptr;
            partition->end_key.columns = nullptr;
            _partition_map[&partition->end_key].push_back(partition.get());
        } else if (_t_sink.partition_type == TStarRocksPartitionType::RANGE) {
            bool min_key = partition->is_min_partition;
            bool max_key = partition->is_max_partition;
            if (min_key) {
                partition->start_key.columns = nullptr;
            } else if (t_part.__isset.start_keys) {
                RETURN_IF_ERROR(_append_partition_key(t_part.start_keys, &partition->start_key, false));
            } else {
                return Status::InternalError("range partition missing start keys");
            }
            if (max_key) {
                partition->end_key.columns = nullptr;
            } else if (t_part.__isset.end_keys) {
                RETURN_IF_ERROR(_append_partition_key(t_part.end_keys, &partition->end_key, false));
            } else {
                return Status::InternalError("range partition missing end keys");
            }
            _partition_map[&partition->end_key].push_back(partition.get());
        } else if (_t_sink.partition_type == TStarRocksPartitionType::LIST) {
            if (!t_part.__isset.in_keys || t_part.in_keys.empty()) {
                return Status::InternalError("list partition missing in-keys");
            }
            partition->in_keys.resize(t_part.in_keys.size());
            for (size_t i = 0; i < t_part.in_keys.size(); ++i) {
                RETURN_IF_ERROR(_append_partition_key(t_part.in_keys[i], &partition->in_keys[i], false));
                _partition_map[&partition->in_keys[i]].push_back(partition.get());
            }
        } else {
            return Status::InternalError("unsupported partition type");
        }

        _partitions.emplace_back(std::move(partition));
    }

    return Status::OK();
}

Status StarRocksTableSink::_append_partition_key(const std::vector<std::string>& values, ChunkRow* key,
                                                 bool is_infinite) {
    if (is_infinite) {
        key->columns = nullptr;
        key->index = 0;
        return Status::OK();
    }

    if (values.size() != _partition_types.size()) {
        return Status::InternalError(fmt::format("partition key size {} not match column size {}", values.size(),
                                                 _partition_types.size()));
    }

    for (size_t i = 0; i < values.size(); ++i) {
        const auto& type_desc = _partition_types[i];
        bool is_nullable = _partition_slots[i]->is_nullable();
        if (_partition_key_columns[i] == nullptr) {
            _partition_key_columns[i] = ColumnHelper::create_column(type_desc, is_nullable);
        }

        Datum datum;
        if (is_nullable && values[i] == kDefaultPartitionValue) {
            datum.set_null();
        } else {
            auto type_info = get_type_info(type_desc);
            RETURN_IF_ERROR(datum_from_string(type_info.get(), &datum, values[i], _partition_key_pool.get()));
        }
        _partition_key_columns[i]->append_datum(datum);
    }

    key->columns = &_partition_key_columns;
    key->index = _partition_key_columns[0]->size() - 1;
    return Status::OK();
}

Status StarRocksTableSink::_find_partitions(Chunk* chunk, const std::vector<uint32_t>& hashes,
                                            std::vector<PartitionInfo*>* partitions) {
    const size_t num_rows = chunk->num_rows();
    partitions->assign(num_rows, nullptr);

    if (_partition_slots.empty()) {
        if (_partition_map.empty()) {
            return Status::InternalError("no partitions available for starrocks sink");
        }
        auto& part_list = _partition_map.begin()->second;
        for (size_t i = 0; i < num_rows; ++i) {
            (*partitions)[i] = part_list[hashes[i] % part_list.size()];
        }
        return Status::OK();
    }

    MutableColumns partition_columns(_partition_slots.size());
    for (size_t i = 0; i < _partition_slots.size(); ++i) {
        partition_columns[i] = chunk->get_column_by_slot_id(_partition_slots[i]->id())->as_mutable_ptr();
    }

    ChunkRow row(&partition_columns, 0);

    if (_t_sink.partition_type == TStarRocksPartitionType::LIST) {
        for (size_t i = 0; i < num_rows; ++i) {
            row.index = static_cast<uint32_t>(i);
            auto it = _partition_map.find(&row);
            if (it == _partition_map.end()) {
                return Status::InternalError("failed to match list partition for row");
            }
            auto* part = it->second[hashes[i] % it->second.size()];
            if (!_part_contains(part, &row)) {
                return Status::InternalError("row is outside list partition bounds");
            }
            (*partitions)[i] = part;
        }
        return Status::OK();
    }

    if (_t_sink.partition_type == TStarRocksPartitionType::RANGE) {
        for (size_t i = 0; i < num_rows; ++i) {
            row.index = static_cast<uint32_t>(i);
            auto it = _partition_map.upper_bound(&row);
            if (it == _partition_map.end()) {
                return Status::InternalError("failed to match range partition for row");
            }
            auto* part = it->second[hashes[i] % it->second.size()];
            if (!_part_contains(part, &row)) {
                return Status::InternalError("row is outside range partition bounds");
            }
            (*partitions)[i] = part;
        }
        return Status::OK();
    }

    return Status::InternalError("unsupported partition type");
}

void StarRocksTableSink::_compute_hashes(const Chunk* chunk, std::vector<uint32_t>* hashes) {
    const size_t num_rows = chunk->num_rows();
    hashes->assign(num_rows, 0);

    if (!_distribution_slots.empty()) {
        for (auto* slot : _distribution_slots) {
            const Column* column = chunk->get_column_by_slot_id(slot->id()).get();
            column->crc32_hash(&(*hashes)[0], 0, num_rows);
        }
    } else {
        uint32_t seed = _rand.Next();
        for (size_t i = 0; i < num_rows; ++i) {
            (*hashes)[i] = seed++;
        }
    }
}

bool StarRocksTableSink::_part_contains(PartitionInfo* part, ChunkRow* key) const {
    if (part->start_key.columns == nullptr) {
        return true;
    }
    return !PartionKeyComparator()(key, &part->start_key);
}

Status StarRocksTableSink::_parse_tablet_root_paths() {
    if (!_t_sink.__isset.properties) {
        return Status::OK();
    }

    auto it = _t_sink.properties.find("tablet_root_paths");
    if (it == _t_sink.properties.end()) {
        return Status::OK();
    }

    auto json_value_or = JsonValue::parse_json_or_string(Slice(it->second));
    if (!json_value_or.ok()) {
        return Status::InvalidArgument("failed to parse tablet_root_paths JSON");
    }

    auto json_slice = json_value_or.value().to_vslice();
    if (!json_slice.isObject()) {
        return Status::InvalidArgument("tablet_root_paths is not a JSON object");
    }

    for (auto it : vpack::ObjectIterator(json_slice)) {
        if (!it.key.isString() || !it.value.isString()) {
            return Status::InvalidArgument("tablet_root_paths contains non-string key/value");
        }
        std::string key = it.key.copyString();
        std::string value = it.value.copyString();
        if (value.empty()) {
            continue;
        }
        int64_t tablet_id = 0;
        try {
            tablet_id = std::stoll(key);
        } catch (const std::exception& e) {
            return Status::InvalidArgument(fmt::format("invalid tablet id in tablet_root_paths: {}", key));
        }
        _tablet_root_paths.emplace(tablet_id, std::move(value));
    }

    return Status::OK();
}

Status StarRocksTableSink::_parse_tablet_versions() {
    if (!_t_sink.__isset.properties) {
        return Status::OK();
    }

    auto it = _t_sink.properties.find("tablet_versions");
    if (it == _t_sink.properties.end()) {
        return Status::OK();
    }

    auto json_value_or = JsonValue::parse_json_or_string(Slice(it->second));
    if (!json_value_or.ok()) {
        return Status::InvalidArgument("failed to parse tablet_versions JSON");
    }

    auto json_slice = json_value_or.value().to_vslice();
    if (!json_slice.isObject()) {
        return Status::InvalidArgument("tablet_versions is not a JSON object");
    }

    for (auto item : vpack::ObjectIterator(json_slice)) {
        if (!item.key.isString()) {
            return Status::InvalidArgument("tablet_versions contains non-string key");
        }
        std::string key = item.key.copyString();
        int64_t tablet_id = 0;
        try {
            tablet_id = std::stoll(key);
        } catch (const std::exception& e) {
            return Status::InvalidArgument(fmt::format("invalid tablet id in tablet_versions: {}", key));
        }

        int64_t version = 0;
        if (item.value.isInt() || item.value.isSmallInt()) {
            version = item.value.getIntUnchecked();
        } else if (item.value.isUInt()) {
            version = static_cast<int64_t>(item.value.getUIntUnchecked());
        } else if (item.value.isString()) {
            try {
                version = std::stoll(item.value.copyString());
            } catch (const std::exception& e) {
                return Status::InvalidArgument(fmt::format("invalid tablet version in tablet_versions: {}", key));
            }
        } else {
            return Status::InvalidArgument("tablet_versions contains non-number value");
        }

        if (version > 0) {
            _tablet_versions.emplace(tablet_id, version);
        }
    }

    return Status::OK();
}

StatusOr<StarRocksTableSink::TabletWriterContext*> StarRocksTableSink::_get_or_create_writer(
        int64_t tablet_id, PartitionInfo* partition, int64_t backend_id) {
    auto it = _tablet_writers.find(tablet_id);
    if (it != _tablet_writers.end()) {
        return it->second.get();
    }

    std::string root_path = _resolve_tablet_root_path(tablet_id, partition);
    if (root_path.empty()) {
        return Status::NotFound(fmt::format("no root path for tablet {}", tablet_id));
    }

    ASSIGN_OR_RETURN(auto fs, _create_fs(root_path));

    auto location_provider = std::make_shared<lake::FixedLocationProvider>(root_path);
    auto tablet_manager = std::make_shared<lake::TabletManager>(location_provider, 0);

    TabletMetadataPtr tablet_metadata;
    auto version_it = _tablet_versions.find(tablet_id);
    if (version_it != _tablet_versions.end() && version_it->second > 0) {
        auto meta_or = tablet_manager->get_single_tablet_metadata(tablet_id, version_it->second, true, 0, fs);
        if (!meta_or.ok()) {
            LOG(WARNING) << "Failed to load tablet metadata by version for tablet " << tablet_id
                         << " version " << version_it->second << ": " << meta_or.status();
        } else {
            tablet_metadata = std::move(meta_or).value();
        }
    }

    if (tablet_metadata == nullptr) {
        std::vector<std::string> objects;
        std::string prefix = fmt::format("{:016X}_", tablet_id);
        std::string metadata_root = location_provider->metadata_root_location(tablet_id);
        auto scan_cb = [&](std::string_view name) {
            if (name.size() >= prefix.size() && name.compare(0, prefix.size(), prefix) == 0) {
                objects.emplace_back(lake::join_path(metadata_root, name));
            }
            return true;
        };

        RETURN_IF_ERROR(fs->iterate_dir(metadata_root, scan_cb));
        if (objects.empty()) {
            return Status::NotFound(fmt::format("tablet {} metadata not found", tablet_id));
        }
        std::sort(objects.begin(), objects.end());
        std::string metadata_location = objects.back();

        ASSIGN_OR_RETURN(tablet_metadata, tablet_manager->get_tablet_metadata(metadata_location, true, 0, fs));
    }
    auto tablet_schema = std::make_shared<TabletSchema>(tablet_metadata->schema());
    auto tablet = std::make_unique<lake::Tablet>(tablet_manager.get(), tablet_id, location_provider, tablet_schema);

    ASSIGN_OR_RETURN(auto writer, tablet->new_writer(lake::WriterType::kHorizontal, _txn_id));
    writer->set_fs(fs);
    writer->set_location_provider(location_provider);
    RETURN_IF_ERROR(writer->open());

    auto writer_ctx = std::make_unique<TabletWriterContext>();
    writer_ctx->tablet_id = tablet_id;
    writer_ctx->backend_id = backend_id;
    writer_ctx->root_path = root_path;
    writer_ctx->fs = fs;
    writer_ctx->location_provider = std::move(location_provider);
    writer_ctx->tablet_manager = std::move(tablet_manager);
    writer_ctx->tablet = std::move(tablet);
    writer_ctx->writer = std::move(writer);

    auto* writer_ctx_ptr = writer_ctx.get();
    _tablet_writers.emplace(tablet_id, std::move(writer_ctx));
    return writer_ctx_ptr;
}

StatusOr<std::shared_ptr<FileSystem>> StarRocksTableSink::_create_fs(const std::string& root_path) const {
    if (_cloud_conf.__isset.cloud_properties && !_cloud_conf.cloud_properties.empty()) {
        return FileSystem::Create(root_path, FSOptions(&_cloud_conf));
    }
    return FileSystem::Create(root_path, FSOptions());
}

Status StarRocksTableSink::_finish_writer(TabletWriterContext* writer_ctx) {
    if (writer_ctx == nullptr || writer_ctx->writer == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(writer_ctx->writer->finish());
    RETURN_IF_ERROR(_write_txn_log(writer_ctx));
    writer_ctx->writer->close();
    return Status::OK();
}

Status StarRocksTableSink::_write_txn_log(TabletWriterContext* writer_ctx) {
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(writer_ctx->tablet_id);
    txn_log->set_txn_id(_txn_id);

    auto* op_write = txn_log->mutable_op_write();
    for (const auto& segment : writer_ctx->writer->segments()) {
        op_write->mutable_rowset()->add_segments(segment.path);
        op_write->mutable_rowset()->add_segment_size(segment.size.value());
        auto* segment_meta = op_write->mutable_rowset()->add_segment_metas();
        segment.sort_key_min.to_proto(segment_meta->mutable_sort_key_min());
        segment.sort_key_max.to_proto(segment_meta->mutable_sort_key_max());
        segment_meta->set_num_rows(segment.num_rows);
    }
    for (const auto& del : writer_ctx->writer->dels()) {
        op_write->add_dels(del.path);
    }
    op_write->mutable_rowset()->set_num_rows(writer_ctx->writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer_ctx->writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);

    auto txn_log_path = writer_ctx->location_provider->txn_log_location(writer_ctx->tablet_id, _txn_id);
    ProtobufFile file(txn_log_path, writer_ctx->fs);
    return file.save(*txn_log);
}

std::string StarRocksTableSink::_resolve_tablet_root_path(int64_t tablet_id, PartitionInfo* partition) const {
    auto it = _tablet_root_paths.find(tablet_id);
    if (it != _tablet_root_paths.end()) {
        return it->second;
    }
    if (partition != nullptr && !partition->storage_path.empty()) {
        return partition->storage_path;
    }
    return {};
}

void StarRocksTableSink::_add_commit_info(const TabletWriterContext* writer_ctx) {
    if (_runtime_state == nullptr || writer_ctx == nullptr) {
        return;
    }
    TSinkCommitInfo commit_info;
    TTabletCommitInfo tablet_commit_info;
    tablet_commit_info.tabletId = writer_ctx->tablet_id;
    tablet_commit_info.backendId = writer_ctx->backend_id;
    commit_info.__set_starrocks_tablet_commit_info(tablet_commit_info);
    if (!_label.empty()) {
        commit_info.__set_starrocks_label(_label);
    }
    if (_txn_id != 0) {
        commit_info.__set_starrocks_txn_id(_txn_id);
    }
    _runtime_state->add_sink_commit_info(commit_info);
}

void StarRocksTableSink::_add_dummy_commit_info() {
    if (_runtime_state == nullptr) {
        return;
    }
    TSinkCommitInfo commit_info;
    if (!_label.empty()) {
        commit_info.__set_starrocks_label(_label);
    }
    if (_txn_id != 0) {
        commit_info.__set_starrocks_txn_id(_txn_id);
    }
    _runtime_state->add_sink_commit_info(commit_info);
}

} // namespace starrocks
