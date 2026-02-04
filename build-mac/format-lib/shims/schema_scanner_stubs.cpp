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

#include "exec/schema_scanner/schema_be_cloud_native_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"

namespace starrocks {

SchemaBeDataCacheMetricsScanner::SchemaBeDataCacheMetricsScanner() : SchemaScanner(nullptr, 0) {}

Status SchemaBeDataCacheMetricsScanner::start(RuntimeState* /*state*/) {
    return Status::NotSupported("DataCache metrics scanner is not supported in macOS format-lib build");
}

Status SchemaBeDataCacheMetricsScanner::get_next(ChunkPtr* /*chunk*/, bool* eos) {
    if (eos != nullptr) {
        *eos = true;
    }
    return Status::OK();
}

SchemaBeCloudNativeCompactionsScanner::SchemaBeCloudNativeCompactionsScanner() : SchemaScanner(nullptr, 0) {}

SchemaBeCloudNativeCompactionsScanner::~SchemaBeCloudNativeCompactionsScanner() = default;

Status SchemaBeCloudNativeCompactionsScanner::start(RuntimeState* /*state*/) {
    return Status::NotSupported("Cloud native compactions scanner is not supported in macOS format-lib build");
}

Status SchemaBeCloudNativeCompactionsScanner::get_next(ChunkPtr* /*chunk*/, bool* eos) {
    if (eos != nullptr) {
        *eos = true;
    }
    return Status::OK();
}

} // namespace starrocks
