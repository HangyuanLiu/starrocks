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

#include "cache/mem_cache/page_cache.h"

namespace starrocks {

std::atomic<size_t> StoragePageCacheMetrics::returned_page_handle_count{};
std::atomic<size_t> StoragePageCacheMetrics::released_page_handle_count{};

void StoragePageCache::init_metrics() {}

bool StoragePageCache::lookup(const std::string& /*key*/, PageCacheHandle* /*handle*/) {
    return false;
}

Status StoragePageCache::insert(const std::string& /*key*/, std::vector<uint8_t>* /*data*/,
                                const MemCacheWriteOptions& /*opts*/, PageCacheHandle* /*handle*/) {
    return Status::OK();
}

Status StoragePageCache::insert(const std::string& /*key*/, void* /*data*/, int64_t /*size*/, MemCacheDeleter /*deleter*/,
                                const MemCacheWriteOptions& /*opts*/, PageCacheHandle* /*handle*/) {
    return Status::OK();
}

void StoragePageCache::set_capacity(size_t /*capacity*/) {}

size_t StoragePageCache::get_capacity() const {
    return 0;
}

uint64_t StoragePageCache::get_lookup_count() const {
    return 0;
}

uint64_t StoragePageCache::get_hit_count() const {
    return 0;
}

uint64_t StoragePageCache::get_insert_count() const {
    return 0;
}

uint64_t StoragePageCache::get_insert_evict_count() const {
    return 0;
}

uint64_t StoragePageCache::get_release_evict_count() const {
    return 0;
}

bool StoragePageCache::adjust_capacity(int64_t /*delta*/, size_t /*min_capacity*/) {
    return false;
}

void StoragePageCache::prune() {}

size_t StoragePageCache::get_pinned_count() const {
    return 0;
}

} // namespace starrocks

