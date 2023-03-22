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

public class ApplyMaskingPolicyClause extends AlterTableClause {
    private final PolicyName policyName;
    private final String maskingColumn;
    private final MaskingPolicyContext maskingPolicyContext;

    public ApplyMaskingPolicyClause(PolicyName policyName, String maskingColumn, MaskingPolicyContext maskingPolicyContext,
                                    NodePosition nodePosition) {
        super(AlterOpType.APPLY_COLUMN_MASKING_POLICY, nodePosition);
        this.policyName = policyName;
        this.maskingColumn = maskingColumn;
        this.maskingPolicyContext = maskingPolicyContext;
    }

    public ApplyMaskingPolicyClause(String maskingColumn, NodePosition nodePosition) {
        super(AlterOpType.REVOKE_COLUMN_MASKING_POLICY, nodePosition);
        this.policyName = null;
        this.maskingColumn = maskingColumn;
        this.maskingPolicyContext = null;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public String getMaskingColumn() {
        return maskingColumn;
    }

    public MaskingPolicyContext getMaskingPolicyContext() {
        return maskingPolicyContext;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitApplyMaskingPolicyClause(this, context);
    }
}