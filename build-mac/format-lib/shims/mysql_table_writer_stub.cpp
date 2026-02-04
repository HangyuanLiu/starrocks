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

#include "runtime/mysql_table_writer.h"

namespace starrocks {

std::string MysqlConnInfo::debug_string() const {
    return fmt::format("MysqlConnInfo(host={}, port={}, user={}, db={})", host, port, user, db);
}

MysqlTableWriter::MysqlTableWriter(const std::vector<ExprContext*>& output_exprs, int chunk_size)
        : _output_expr_ctxs(output_exprs), _mysql_conn(nullptr), _chunk_size(chunk_size) {}

MysqlTableWriter::~MysqlTableWriter() = default;

Status MysqlTableWriter::open(const MysqlConnInfo& /*conn_info*/, const std::string& /*tbl*/) {
    return Status::NotSupported("MysqlTableWriter is not supported in macOS format-lib build");
}

Status MysqlTableWriter::append(Chunk* /*chunk*/) {
    return Status::NotSupported("MysqlTableWriter is not supported in macOS format-lib build");
}

} // namespace starrocks

