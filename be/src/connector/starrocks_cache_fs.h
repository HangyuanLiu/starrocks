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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "cache/cache_options.h"
#include "fs/fs.h"

namespace starrocks::connector {

class StarRocksCacheFileSystem final : public FileSystem {
public:
    StarRocksCacheFileSystem(std::shared_ptr<FileSystem> fs, DataCacheOptions options);

    Type type() const override { return _fs->type(); }

    StatusOr<std::unique_ptr<SequentialFile>> new_sequential_file(const SequentialFileOptions& opts,
                                                                  const std::string& fname) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const std::string& fname) override;

    StatusOr<std::unique_ptr<RandomAccessFile>> new_random_access_file(const RandomAccessFileOptions& opts,
                                                                       const FileInfo& file_info) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const std::string& fname) override;

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& opts,
                                                              const std::string& fname) override;

    Status path_exists(const std::string& fname) override { return _fs->path_exists(fname); }

    Status get_children(const std::string& dir, std::vector<std::string>* result) override {
        return _fs->get_children(dir, result);
    }

    Status iterate_dir(const std::string& dir, const std::function<bool(std::string_view)>& cb) override {
        return _fs->iterate_dir(dir, cb);
    }

    Status iterate_dir2(const std::string& dir, const std::function<bool(DirEntry)>& cb) override {
        return _fs->iterate_dir2(dir, cb);
    }

    Status delete_file(const std::string& fname) override { return _fs->delete_file(fname); }

    Status create_dir(const std::string& dirname) override { return _fs->create_dir(dirname); }

    Status create_dir_if_missing(const std::string& dirname, bool* created) override {
        return _fs->create_dir_if_missing(dirname, created);
    }

    Status create_dir_recursive(const std::string& dirname) override { return _fs->create_dir_recursive(dirname); }

    Status delete_dir(const std::string& dirname) override { return _fs->delete_dir(dirname); }

    Status delete_dir_recursive(const std::string& dirname) override { return _fs->delete_dir_recursive(dirname); }

    Status sync_dir(const std::string& dirname) override { return _fs->sync_dir(dirname); }

    StatusOr<bool> is_directory(const std::string& path) override { return _fs->is_directory(path); }

    Status canonicalize(const std::string& path, std::string* result) override {
        return _fs->canonicalize(path, result);
    }

    StatusOr<uint64_t> get_file_size(const std::string& fname) override { return _fs->get_file_size(fname); }

    StatusOr<uint64_t> get_file_modified_time(const std::string& fname) override {
        return _fs->get_file_modified_time(fname);
    }

    Status rename_file(const std::string& src, const std::string& target) override {
        return _fs->rename_file(src, target);
    }

    Status link_file(const std::string& old_path, const std::string& new_path) override {
        return _fs->link_file(old_path, new_path);
    }

    StatusOr<SpaceInfo> space(const std::string& path) override { return _fs->space(path); }

    Status drop_local_cache(const std::string& path) override { return _fs->drop_local_cache(path); }

    Status delete_files(std::span<const std::string> paths) override { return _fs->delete_files(paths); }

private:
    StatusOr<std::unique_ptr<RandomAccessFile>> _wrap_with_cache(const RandomAccessFileOptions& opts,
                                                                 std::unique_ptr<RandomAccessFile> raw_file,
                                                                 const std::string& path,
                                                                 const FileInfo* file_info);

    bool _should_cache_path(std::string_view path) const;

    std::shared_ptr<FileSystem> _fs;
    DataCacheOptions _options;
};

} // namespace starrocks::connector
