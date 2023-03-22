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

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class Policy implements Writable {

    @SerializedName(value = "id")
    private final Long policyId;

    @SerializedName(value = "name")
    private PolicyName policyName;

    @SerializedName(value = "dbPEntryObject")
    private DbPEntryObject dbPEntryObject;

    @SerializedName(value = "argNames")
    private List<String> argNames;

    @SerializedName(value = "argTypes")
    private List<Type> argTypes;

    @SerializedName(value = "retType")
    private Type retType;

    @SerializedName(value = "p")
    private String policyExpressionSQL;
    private Expr policyExpression;

    public Policy(Long policyId, List<String> argNames, List<Type> argTypes, Type retType, String policyExpressionSQL) {
        this.policyId = policyId;
        this.argNames = argNames;
        this.argTypes = argTypes;
        this.retType = retType;
        this.policyExpressionSQL = policyExpressionSQL;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public void setPolicyName(PolicyName policyName) {
        this.policyName = policyName;
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

    public String getPolicyExpressionSQL() {
        return policyExpressionSQL;
    }

    public void setPolicyExpression(Expr policyExpression) {
        this.policyExpression = policyExpression;
    }

    public Expr getPolicyExpression() {
        return policyExpression;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        policyExpressionSQL = AstToSQLBuilder.toSQL(policyExpression);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Policy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        Policy c = GsonUtils.GSON.fromJson(json, Policy.class);

        Expr expression = SqlParser.parseSqlToExpr(c.getPolicyExpressionSQL(), SqlModeHelper.MODE_DEFAULT);
        c.setPolicyExpression(expression);

        return c;
    }
}
