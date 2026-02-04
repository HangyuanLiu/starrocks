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

#include "exec/file_scanner/parquet_scanner.h"

namespace starrocks {

ParquetScanner::ParquetScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                               ScannerCounter* counter, bool schema_only)
        : FileScanner(state, profile, scan_range.params, counter, schema_only),
          _scan_range(scan_range),
          _next_file(0),
          _curr_file_reader(nullptr),
          _scanner_eof(true),
          _max_chunk_size(0),
          _batch_start_idx(0),
          _chunk_start_idx(0) {
    _file_format_str = "parquet";
}

ParquetScanner::~ParquetScanner() = default;

Status ParquetScanner::open() {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

StatusOr<ChunkPtr> ParquetScanner::get_next() {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

Status ParquetScanner::get_schema(std::vector<SlotDescriptor>* /*schema*/) {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

void ParquetScanner::close() {
    FileScanner::close();
}

Status ParquetScanner::convert_array_to_column(ConvertFuncTree* /*func*/, size_t /*num_elements*/,
                                               const arrow::Array* /*array*/, Column* /*column*/,
                                               size_t /*batch_start_idx*/, size_t /*column_start_idx*/,
                                               Filter* /*chunk_filter*/, ArrowConvertContext* /*conv_ctx*/) {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

Status ParquetScanner::new_column(const arrow::DataType* /*arrow_type*/, const SlotDescriptor* /*slot_desc*/,
                                  MutableColumnPtr* /*column*/, ConvertFuncTree* /*conv_func*/, Expr** /*expr*/,
                                  ObjectPool& /*pool*/, bool /*strict_mode*/) {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

Status ParquetScanner::build_dest(const arrow::DataType* /*arrow_type*/, const TypeDescriptor* /*type_desc*/,
                                  bool /*is_nullable*/, TypeDescriptor* /*raw_type_desc*/,
                                  ConvertFuncTree* /*conv_func*/, bool& /*need_cast*/, bool /*strict_mode*/) {
    return Status::NotSupported("ParquetScanner is not supported in macOS format-lib build");
}

} // namespace starrocks

