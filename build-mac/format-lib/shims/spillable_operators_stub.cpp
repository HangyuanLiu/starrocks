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

#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"
#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"
#include "exec/pipeline/sort/spillable_partition_sort_sink_operator.h"

namespace starrocks::pipeline {

Status SpillableHashJoinBuildOperatorFactory::prepare(RuntimeState* /*state*/) {
    return Status::NotSupported("Spill is not supported in macOS format-lib build");
}

void SpillableHashJoinBuildOperatorFactory::close(RuntimeState* /*state*/) {}

OperatorPtr SpillableHashJoinBuildOperatorFactory::create(int32_t /*degree_of_parallelism*/, int32_t /*driver_sequence*/) {
    return nullptr;
}

Status SpillableHashJoinProbeOperatorFactory::prepare(RuntimeState* /*state*/) {
    return Status::NotSupported("Spill is not supported in macOS format-lib build");
}

OperatorPtr SpillableHashJoinProbeOperatorFactory::create(int32_t /*degree_of_parallelism*/, int32_t /*driver_sequence*/) {
    return nullptr;
}

OperatorPtr SpillablePartitionSortSinkOperatorFactory::create(int32_t /*degree_of_parallelism*/, int32_t /*driver_sequence*/) {
    return nullptr;
}

Status SpillablePartitionSortSinkOperatorFactory::prepare(RuntimeState* /*state*/) {
    return Status::NotSupported("Spill is not supported in macOS format-lib build");
}

void SpillablePartitionSortSinkOperatorFactory::close(RuntimeState* /*state*/) {}

} // namespace starrocks::pipeline

