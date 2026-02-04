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

#include "runtime/file_result_writer.h"

namespace starrocks {

FileResultWriter::FileResultWriter(const ResultFileOptions* file_option, const std::vector<ExprContext*>& output_expr_ctxs,
                                   RuntimeProfile* parent_profile)
        : _file_opts(file_option), _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {}

FileResultWriter::~FileResultWriter() = default;

Status FileResultWriter::init(RuntimeState* /*state*/) {
    return Status::NotSupported("FileResultWriter is not supported in macOS format-lib build");
}

Status FileResultWriter::open(RuntimeState* /*state*/) {
    return Status::NotSupported("FileResultWriter is not supported in macOS format-lib build");
}

Status FileResultWriter::append_chunk(Chunk* /*chunk*/) {
    return Status::NotSupported("FileResultWriter is not supported in macOS format-lib build");
}

Status FileResultWriter::close() {
    return Status::OK();
}

} // namespace starrocks

