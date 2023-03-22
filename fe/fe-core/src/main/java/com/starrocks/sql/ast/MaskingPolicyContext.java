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

package com.starrocks.sql.ast;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class MaskingPolicyContext implements ParseNode {
    @SerializedName(value = "policyId")
    Long policyId;

    @SerializedName(value = "usingColumns")
    List<String> usingColumns;

    PolicyName policyName;
    NodePosition pos;

    public MaskingPolicyContext(PolicyName policyName, List<String> usingColumns, NodePosition pos) {
        this.policyName = policyName;
        this.usingColumns = usingColumns;
        this.pos = pos;
    }

    public MaskingPolicyContext(Long policyId, List<String> usingColumns, NodePosition pos) {
        this.policyId = policyId;
        this.usingColumns = usingColumns;
        this.pos = pos;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public List<String> getUsingColumns() {
        return usingColumns;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
