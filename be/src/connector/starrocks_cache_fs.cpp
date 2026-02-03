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

#include "connector/starrocks_cache_fs.h"

#include <algorithm>

#include "common/config.h"
#include "common/statusor.h"
#include "io/cache_input_stream.h"
#include "io/cache_select_input_stream.hpp"
#include "io/shared_buffered_input_stream.h"

namespace starrocks::connector {

namespace {

bool ends_with(std::string_view value, std::string_view suffix) {
    if (suffix.size() > value.size()) {
        return false;
    }
    return value.substr(value.size() - suffix.size()) == suffix;
}

} // namespace

StarRocksCacheFileSystem::StarRocksCacheFileSystem(std::shared_ptr<FileSystem> fs, DataCacheOptions options)
        : _fs(std::move(fs)), _options(options) {}

StatusOr<std::unique_ptr<SequentialFile>> StarRocksCacheFileSystem::new_sequential_file(
        const SequentialFileOptions& opts, const std::string& fname) {
    return _fs->new_sequential_file(opts, fname);
}

StatusOr<std::unique_ptr<RandomAccessFile>> StarRocksCacheFileSystem::new_random_access_file(
        const RandomAccessFileOptions& opts, const std::string& fname) {
    ASSIGN_OR_RETURN(auto raw_file, _fs->new_random_access_file(opts, fname));
    return _wrap_with_cache(opts, std::move(raw_file), fname, nullptr);
}

StatusOr<std::unique_ptr<RandomAccessFile>> StarRocksCacheFileSystem::new_random_access_file(
        const RandomAccessFileOptions& opts, const FileInfo& file_info) {
    ASSIGN_OR_RETURN(auto raw_file, _fs->new_random_access_file(opts, file_info));
    return _wrap_with_cache(opts, std::move(raw_file), file_info.path, &file_info);
}

StatusOr<std::unique_ptr<WritableFile>> StarRocksCacheFileSystem::new_writable_file(const std::string& fname) {
    return _fs->new_writable_file(fname);
}

StatusOr<std::unique_ptr<WritableFile>> StarRocksCacheFileSystem::new_writable_file(const WritableFileOptions& opts,
                                                                                    const std::string& fname) {
    return _fs->new_writable_file(opts, fname);
}

StatusOr<std::unique_ptr<RandomAccessFile>> StarRocksCacheFileSystem::_wrap_with_cache(
        const RandomAccessFileOptions& opts, std::unique_ptr<RandomAccessFile> raw_file, const std::string& path,
        const FileInfo* file_info) {
    if (!_options.enable_datacache || !_should_cache_path(path)) {
        return raw_file;
    }

    uint64_t file_size = 0;
    if (file_info != nullptr && file_info->size.has_value()) {
        file_size = file_info->size.value();
    } else {
        auto size_or = raw_file->stream()->get_size();
        if (!size_or.ok()) {
            return raw_file;
        }
        file_size = size_or.value();
    }

    int64_t modification_time = _options.modification_time;
    if (modification_time <= 0) {
        auto mod_or = _fs->get_file_modified_time(path);
        if (mod_or.ok()) {
            modification_time = static_cast<int64_t>(mod_or.value());
        }
    }

    const std::string& filename = raw_file->filename();
    std::shared_ptr<io::SeekableInputStream> input_stream = raw_file->stream();
    auto shared_stream = std::make_shared<io::SharedBufferedInputStream>(input_stream, filename, file_size);
    const io::SharedBufferedInputStream::CoalesceOptions shared_options = {
            .max_dist_size = config::io_coalesce_read_max_distance_size,
            .max_buffer_size = config::io_coalesce_read_max_buffer_size};
    shared_stream->set_coalesce_options(shared_options);

    std::unique_ptr<io::CacheInputStream> cache_stream;
    if (_options.enable_cache_select) {
        cache_stream =
                std::make_unique<io::CacheSelectInputStream>(shared_stream, filename, file_size, modification_time);
    } else {
        cache_stream = std::make_unique<io::CacheInputStream>(shared_stream, filename, file_size, modification_time);
        cache_stream->set_enable_populate_cache(_options.enable_populate_datacache);
        cache_stream->set_enable_async_populate_mode(_options.enable_datacache_async_populate_mode);
        cache_stream->set_enable_cache_io_adaptor(_options.enable_datacache_io_adaptor);
        cache_stream->set_enable_block_buffer(config::datacache_block_buffer_enable);
    }
    cache_stream->set_priority(_options.datacache_priority);
    cache_stream->set_ttl_seconds(_options.datacache_ttl_seconds);
    shared_stream->set_align_size(cache_stream->get_align_size());

    auto wrapped = RandomAccessFile::from(std::move(cache_stream), filename, raw_file->is_cache_hit(),
                                          opts.encryption_info);
    wrapped->set_size(file_size);
    return wrapped;
}

bool StarRocksCacheFileSystem::_should_cache_path(std::string_view path) const {
    if (path.find("/data/") != std::string_view::npos) {
        return true;
    }
    return ends_with(path, ".dat");
}

} // namespace starrocks::connector
