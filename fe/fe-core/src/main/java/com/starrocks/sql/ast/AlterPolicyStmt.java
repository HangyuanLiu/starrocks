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

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

public class AlterPolicyStmt extends DdlStmt {

    private final PolicyType policyType;
    private final boolean ifExists;
    private final PolicyName policyName;
    private final PolicyRenameObject policyRenameObject;
    private final PolicySetBody policySetBodyObject;

    public AlterPolicyStmt(PolicyType policyType, PolicyName policyName, boolean ifExists, PolicyRenameObject policyRenameObject,
                           NodePosition pos) {
        super(pos);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
        this.policyRenameObject = policyRenameObject;
        this.policySetBodyObject = null;
    }

    public AlterPolicyStmt(PolicyType policyType, PolicyName policyName, boolean ifExists, PolicySetBody policySetBodyObject,
                           NodePosition pos) {
        super(pos);
        this.policyType = policyType;
        this.policyName = policyName;
        this.ifExists = ifExists;
        this.policySetBodyObject = policySetBodyObject;
        this.policyRenameObject = null;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public PolicyRenameObject getPolicyRenameObject() {
        return policyRenameObject;
    }

    public PolicySetBody getPolicySetBodyObject() {
        return policySetBodyObject;
    }

    public static class PolicySetBody {
        private final Expr policyBody;

        public PolicySetBody(Expr policyBody) {
            this.policyBody = policyBody;
        }

        public Expr getPolicyBody() {
            return policyBody;
        }
    }

    public static class PolicyRenameObject {
        private final String newPolicyName;

        public PolicyRenameObject(String newPolicyName) {
            this.newPolicyName = newPolicyName;
        }

        public String getNewPolicyName() {
            return newPolicyName;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterPolicyStatement(this, context);
    }
}


