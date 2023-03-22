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
import com.starrocks.analysis.TypeDef;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class CreateMaskingPolicyStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final PolicyName policyName;
    private final List<String> argNames;
    private final List<TypeDef> argTypeDefs;
    private final TypeDef returnType;
    private final Expr expression;

    public CreateMaskingPolicyStmt(PolicyName policyName, boolean ifNotExists,
                                   List<String> argNames, List<TypeDef> argTypeDefs, TypeDef returnType,
                                   Expr expression, NodePosition pos) {
        super(pos);
        this.policyName = policyName;
        this.ifNotExists = ifNotExists;
        this.argNames = argNames;
        this.argTypeDefs = argTypeDefs;
        this.returnType = returnType;
        this.expression = expression;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<TypeDef> getArgTypeDefs() {
        return argTypeDefs;
    }

    public TypeDef getReturnType() {
        return returnType;
    }

    public Expr getExpression() {
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateMaskingPolicyStatement(this, context);
    }
}
