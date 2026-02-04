// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common/status.h"
#include "storage/lake/compaction_scheduler.h"
#include "storage/lake/lake_persistent_index_parallel_compact_mgr.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/metacache.h"
#include "storage/lake/replication_txn_manager.h"
#include "storage/lake/rowset.h"
#include "storage/lake/rowset_update_state.h"
#include "storage/lake/schema_change.h"
#include "storage/lake/table_schema_service.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/tablet_write_log_manager.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/lake/update_manager.h"
#include "storage/options.h"

namespace starrocks::lake {

TabletManager::TabletManager(std::shared_ptr<LocationProvider> location_provider, UpdateManager* update_mgr,
                             int64_t /*cache_capacity*/)
        : _location_provider(std::move(location_provider)), _update_mgr(update_mgr) {}

TabletManager::TabletManager(std::shared_ptr<LocationProvider> location_provider, int64_t /*cache_capacity*/)
        : _location_provider(std::move(location_provider)) {}

Metacache::~Metacache() = default;


StatusOr<SegmentPtr> TabletManager::load_segment(const FileInfo& /*segment_info*/, int /*segment_id*/,
                                                 size_t* /*footer_size_hint*/, const LakeIOOptions& /*lake_io_opts*/,
                                                 bool /*fill_metadata_cache*/, TabletSchemaPtr /*tablet_schema*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

StatusOr<SegmentPtr> TabletManager::load_segment(const FileInfo& /*segment_info*/, int /*segment_id*/,
                                                 const LakeIOOptions& /*lake_io_opts*/, bool /*fill_metadata_cache*/,
                                                 TabletSchemaPtr /*tablet_schema*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

void TabletManager::prune_metacache() {}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(int64_t /*tablet_id*/, int64_t /*version*/,
                                                               bool /*fill_cache*/, int64_t /*expected_gtid*/,
                                                               const std::shared_ptr<FileSystem>& /*fs*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

StatusOr<TabletMetadataPtr> TabletManager::get_tablet_metadata(const std::string& /*path*/, bool /*fill_cache*/,
                                                               int64_t /*expected_gtid*/,
                                                               const std::shared_ptr<FileSystem>& /*fs*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

StatusOr<TabletMetadataPtr> TabletManager::get_single_tablet_metadata(int64_t /*tablet_id*/, int64_t /*version*/,
                                                                      bool /*fill_cache*/, int64_t /*expected_gtid*/,
                                                                      const std::shared_ptr<FileSystem>& /*fs*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

StatusOr<TabletMetadataPtr> TabletManager::get_single_tablet_metadata(int64_t /*tablet_id*/, int64_t /*version*/,
                                                                      const CacheOptions& /*cache_opts*/,
                                                                      int64_t /*expected_gtid*/,
                                                                      const std::shared_ptr<FileSystem>& /*fs*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

Status TabletManager::put_combined_txn_log(const CombinedTxnLogPB& /*log*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

void TabletManager::update_metacache_limit(size_t /*limit*/) {}

void TabletManager::update_segment_cache_size(std::string_view /*key*/, size_t /*mem_cost*/, intptr_t /*segment_addr_hint*/) {}

StatusOr<TabletAndRowsets> TabletManager::capture_tablet_and_rowsets(int64_t /*tablet_id*/, int64_t /*from_version*/,
                                                                     int64_t /*to_version*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

void TabletManager::clean_in_writing_data_size() {}

StatusOr<std::unique_ptr<TabletWriter>> Tablet::new_writer(WriterType /*type*/, int64_t /*txn_id*/,
                                                           uint32_t /*max_rows_per_segment*/,
                                                           ThreadPool* /*flush_pool*/, bool /*is_compaction*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

StatusOr<TabletMetadataPtr> Tablet::get_metadata(int64_t /*version*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

const std::shared_ptr<const TabletSchema> Tablet::tablet_schema() const {
    return nullptr;
}

size_t Tablet::num_rows() const {
    return 0;
}

Status TabletReader::parse_seek_range(const TabletSchema& /*schema*/,
                                      TabletReaderParams::RangeStartOperation /*start_op*/,
                                      TabletReaderParams::RangeEndOperation /*end_op*/,
                                      const std::vector<OlapTuple>& /*start_keys*/,
                                      const std::vector<OlapTuple>& /*end_keys*/, std::vector<SeekRange>* /*ranges*/,
                                      MemPool* /*pool*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

UpdateManager::~UpdateManager() = default;

CompactionScheduler::~CompactionScheduler() = default;

void CompactionScheduler::update_compact_threads(int /*thread_num*/) {}

CompactionState::~CompactionState() = default;

LakePrimaryIndex::~LakePrimaryIndex() = default;

RowsetUpdateState::~RowsetUpdateState() = default;

Status SchemaChangeHandler::process_alter_tablet(const TAlterTabletReqV2& /*request*/) {
    return Status::NotSupported("Lake schema change is disabled on macOS");
}

Status SchemaChangeHandler::process_update_tablet_meta(const TUpdateTabletMetaInfoReq& /*request*/) {
    return Status::NotSupported("Lake schema change is disabled on macOS");
}

Status ReplicationTxnManager::remote_snapshot(const TRemoteSnapshotRequest& /*request*/,
                                              TSnapshotInfo* /*src_snapshot_info*/) {
    return Status::NotSupported("Lake replication snapshot is disabled on macOS");
}

Status ReplicationTxnManager::replicate_snapshot(const TReplicateSnapshotRequest& /*request*/) {
    return Status::NotSupported("Lake replication snapshot is disabled on macOS");
}

void SegmentPKIterator::close() {
    _pk_column_chunk.reset();
    _iter.reset();
    _begin_rowid_offsets.clear();
    _current_rows = 0;
    _memory_usage = 0;
    _status = Status::NotSupported("Lake rowset update is disabled on macOS");
}

// TabletWriteLogManager stubs
TabletWriteLogManager::TabletWriteLogManager() = default;

TabletWriteLogManager* TabletWriteLogManager::instance() {
    static TabletWriteLogManager instance;
    return &instance;
}

std::vector<TabletWriteLogEntry> TabletWriteLogManager::get_logs(int64_t /*table_id*/, int64_t /*partition_id*/,
                                                                 int64_t /*tablet_id*/, int64_t /*log_type*/,
                                                                 int64_t /*start_finish_time*/,
                                                                 int64_t /*end_finish_time*/) const {
    return {};
}

// LakePersistentIndexParallelCompactMgr stubs
LakePersistentIndexParallelCompactMgr::~LakePersistentIndexParallelCompactMgr() = default;

void LakePersistentIndexParallelCompactMgr::shutdown() {}

Status LakePersistentIndexParallelCompactMgr::update_max_threads(int /*max_threads*/) {
    return Status::NotSupported("Lake storage is disabled on macOS");
}

} // namespace starrocks::lake
