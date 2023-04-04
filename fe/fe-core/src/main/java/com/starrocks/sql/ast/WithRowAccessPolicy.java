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

public class WithRowAccessPolicy implements ParseNode {
    private final PolicyName policyName;
    private final List<String> onColumns;
    private final NodePosition pos;

    public WithRowAccessPolicy(PolicyName policyName, List<String> onColumns, NodePosition pos) {
        this.policyName = policyName;
        this.onColumns = onColumns;
        this.pos = pos;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getOnColumns() {
        return onColumns;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
