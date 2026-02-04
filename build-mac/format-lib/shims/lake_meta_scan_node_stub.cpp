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

#include "exec/lake_meta_scan_node.h"

namespace starrocks {

LakeMetaScanNode::LakeMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : MetaScanNode(pool, tnode, descs) {
    _name = "lake_meta_scan_stub";
}

Status LakeMetaScanNode::open(RuntimeState* /*state*/) {
    return Status::NotSupported("LakeMetaScanNode is not supported in macOS format-lib build");
}

Status LakeMetaScanNode::get_next(RuntimeState* /*state*/, ChunkPtr* /*chunk*/, bool* eos) {
    if (eos != nullptr) {
        *eos = true;
    }
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> LakeMetaScanNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* /*context*/) {
    return {};
}

} // namespace starrocks

