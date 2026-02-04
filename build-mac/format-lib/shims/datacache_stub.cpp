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

#include "cache/datacache.h"

#include "common/statusor.h"
#include "storage/options.h"

namespace starrocks {

DataCache* DataCache::GetInstance() {
    static DataCache instance;
    return &instance;
}

// format-lib doesn't rely on StarRocks' data cache. Keep it disabled but linkable.
Status DataCache::init(const std::vector<StorePath>& /*store_paths*/) {
    return Status::OK();
}

void DataCache::destroy() {}

void DataCache::try_release_resource_before_core_dump() {}

bool DataCache::page_cache_available() const {
    return false;
}

StatusOr<int64_t> DataCache::get_datacache_limit() {
    return 0;
}

int64_t DataCache::check_datacache_limit(int64_t datacache_limit) {
    return datacache_limit;
}

bool DataCache::adjust_mem_capacity(int64_t /*delta*/, size_t /*min_capacity*/) {
    return false;
}

size_t DataCache::get_mem_capacity() const {
    return 0;
}

} // namespace starrocks
