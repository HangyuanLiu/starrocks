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

import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

public class ApplyRowAccessPolicyClause extends AlterTableClause {
    private final PolicyName policyName;
    private final RowAccessPolicyContext rowAccessPolicyContext;

    public ApplyRowAccessPolicyClause(PolicyName policyName, RowAccessPolicyContext rowAccessPolicyContext,
                                      NodePosition nodePosition) {
        super(AlterOpType.APPLY_ROW_ACCESS_POLICY, nodePosition);
        this.policyName = policyName;
        this.rowAccessPolicyContext = rowAccessPolicyContext;
    }

    public ApplyRowAccessPolicyClause(PolicyName policyName, NodePosition pos) {
        super(AlterOpType.REVOKE_ROW_ACCESS_POLICY, pos);
        this.policyName = policyName;
        this.rowAccessPolicyContext = null;
    }

    public ApplyRowAccessPolicyClause(NodePosition pos) {
        super(AlterOpType.REVOKE_ALL_ROW_ACCESS_POLICY, pos);
        this.policyName = null;
        this.rowAccessPolicyContext = null;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public RowAccessPolicyContext getRowAccessPolicyContext() {
        return rowAccessPolicyContext;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitApplyRowAccessPolicyClause(this, context);
    }
}
