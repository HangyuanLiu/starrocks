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
#include <unordered_map>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/async_data_sink.h"
#include "exec/tablet_info.h"
#include "gen_cpp/CloudConfiguration_types.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/descriptors.h"
#include "util/random.h"

namespace starrocks {

class ExprContext;
class FileSystem;
class MemPool;
class RuntimeProfile;
class RuntimeState;

namespace lake {
class LocationProvider;
class Tablet;
class TabletManager;
class TabletWriter;
} // namespace lake

class StarRocksTableSink final : public AsyncDataSink {
public:
    StarRocksTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs, Status* status, RuntimeState* state);

    ~StarRocksTableSink() override;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status send_chunk(RuntimeState* state, Chunk* chunk) override;
    Status close(RuntimeState* state, Status exec_status) override;

    Status try_open(RuntimeState* state) override;
    bool is_open_done() override;
    Status open_wait() override;
    bool is_full() override;
    Status send_chunk_nonblocking(RuntimeState* state, Chunk* chunk) override;
    Status try_close(RuntimeState* state) override;
    Status close_wait(RuntimeState* state, Status close_status) override;
    bool is_close_done() override;
    void set_profile(RuntimeProfile* profile) override;

    RuntimeProfile* profile() override { return _profile; }

private:
    struct TabletInfo {
        int64_t tablet_id = 0;
        int64_t backend_id = 0;
    };

    struct PartitionInfo {
        int64_t id = 0;
        bool is_min_partition = false;
        bool is_max_partition = false;
        int32_t bucket_num = 0;
        std::string distribution_type;
        std::string storage_path;
        std::vector<TabletInfo> tablets;
        ChunkRow start_key;
        ChunkRow end_key;
        std::vector<ChunkRow> in_keys;
    };

    struct TabletWriterContext {
        int64_t tablet_id = 0;
        int64_t backend_id = 0;
        std::string root_path;
        std::shared_ptr<FileSystem> fs;
        std::shared_ptr<lake::LocationProvider> location_provider;
        std::shared_ptr<lake::TabletManager> tablet_manager;
        std::unique_ptr<lake::Tablet> tablet;
        std::unique_ptr<lake::TabletWriter> writer;

        ~TabletWriterContext();
    };

    Status _init_partitions();
    Status _append_partition_key(const std::vector<std::string>& values, ChunkRow* key, bool is_infinite);
    Status _find_partitions(Chunk* chunk, const std::vector<uint32_t>& hashes,
                            std::vector<PartitionInfo*>* partitions);
    void _compute_hashes(const Chunk* chunk, std::vector<uint32_t>* hashes);
    bool _part_contains(PartitionInfo* part, ChunkRow* key) const;
    Status _parse_tablet_root_paths();
    Status _parse_tablet_versions();

    StatusOr<TabletWriterContext*> _get_or_create_writer(int64_t tablet_id, PartitionInfo* partition,
                                                         int64_t backend_id);
    StatusOr<std::shared_ptr<FileSystem>> _create_fs(const std::string& root_path) const;
    Status _finish_writer(TabletWriterContext* writer_ctx);
    Status _write_txn_log(TabletWriterContext* writer_ctx);
    std::string _resolve_tablet_root_path(int64_t tablet_id, PartitionInfo* partition) const;
    void _add_commit_info(const TabletWriterContext* writer_ctx);
    void _add_dummy_commit_info();

private:
    ObjectPool* _pool;
    const std::vector<TExpr>& _t_output_exprs;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<Chunk> _output_chunk;
    RuntimeProfile* _profile = nullptr;

    TStarRocksTableSink _t_sink;
    TupleId _tuple_id = 0;
    TupleDescriptor* _output_tuple_desc = nullptr;

    std::vector<SlotDescriptor*> _partition_slots;
    std::vector<SlotDescriptor*> _distribution_slots;
    MutableColumns _partition_key_columns;
    std::vector<TypeDescriptor> _partition_types;

    std::vector<std::unique_ptr<PartitionInfo>> _partitions;
    std::map<ChunkRow*, std::vector<PartitionInfo*>, PartionKeyComparator> _partition_map;

    std::unordered_map<int64_t, std::unique_ptr<TabletWriterContext>> _tablet_writers;
    std::unordered_map<int64_t, std::string> _tablet_root_paths;
    std::unordered_map<int64_t, int64_t> _tablet_versions;
    TCloudConfiguration _cloud_conf;

    int64_t _txn_id = 0;
    std::string _label;
    std::string _db_name;
    std::string _table_name;

    bool _open_done = false;
    bool _close_done = false;

    Random _rand{static_cast<uint32_t>(time(nullptr))};
    std::unique_ptr<MemPool> _partition_key_pool;
};

} // namespace starrocks
