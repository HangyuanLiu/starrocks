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

#include <cstdint>
#include <string>

#include "common/status.h"
#include "exprs/function_context.h"
#include "udf/java/java_function_fwd.h"

namespace starrocks {

const AggregateFunction* getJavaUDAFFunction(bool /*input_nullable*/) {
    return nullptr;
}

const AggregateFunction* getJavaWindowFunction() {
    return nullptr;
}

const TableFunction* getJavaUDTFFunction() {
    return nullptr;
}

Status init_udaf_context(int64_t /*fid*/, const std::string& /*url*/, const std::string& /*checksum*/,
                         const std::string& /*symbol*/, FunctionContext* /*context*/) {
    return Status::NotSupported("Java UDAF not available on macOS");
}

} // namespace starrocks

