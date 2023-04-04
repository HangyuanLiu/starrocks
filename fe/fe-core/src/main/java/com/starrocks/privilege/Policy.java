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

package com.starrocks.privilege;

import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.PolicyType;

import java.util.List;

public class Policy {
    private final PolicyType policyType;
    private final Long policyId;
    private String name;
    private final DbPEntryObject dbPEntryObject;
    private final List<String> argNames;
    private final List<Type> argTypes;
    private final Type retType;
    private Expr policyExpression;
    private String comment;

    public Policy(PolicyType policyType, Long policyId,
                  String policyName, DbPEntryObject dbPEntryObject,
                  List<String> argNames, List<Type> argTypes, Type retType,
                  Expr policyExpression, String comment) {
        this.policyType = policyType;
        this.policyId = policyId;

        this.name = policyName;
        this.dbPEntryObject = dbPEntryObject;

        this.argNames = argNames;
        this.argTypes = argTypes;
        this.retType = retType;
        this.policyExpression = policyExpression;
        this.comment = comment;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DbPEntryObject getDbPEntryObject() {
        return dbPEntryObject;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Type getRetType() {
        return retType;
    }

    public Expr getPolicyExpression() {
        return policyExpression;
    }

    public void setPolicyExpression(Expr policyExpression) {
        this.policyExpression = policyExpression;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
