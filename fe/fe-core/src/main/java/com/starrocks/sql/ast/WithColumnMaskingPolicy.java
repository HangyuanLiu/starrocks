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

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class WithColumnMaskingPolicy implements ParseNode {
    private final PolicyName policyName;
    private final List<String> usingColumns;
    private final NodePosition pos;

    public WithColumnMaskingPolicy(PolicyName policyName, List<String> usingColumns, NodePosition pos) {
        this.policyName = policyName;
        this.usingColumns = usingColumns;
        this.pos = pos;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getUsingColumns() {
        return usingColumns;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
