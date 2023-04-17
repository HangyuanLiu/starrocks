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

package ee.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.privilege.DbPEntryObject;
import com.starrocks.privilege.Policy;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.PolicyType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CreatePolicyInfo implements Writable {
    @SerializedName(value = "type")
    private PolicyType policyType;

    @SerializedName(value = "id")
    private final Long policyId;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "db")
    private DbPEntryObject dbPEntryObject;

    @SerializedName(value = "argNames")
    private List<String> argNames;

    @SerializedName(value = "argTypes")
    private List<Type> argTypes;

    @SerializedName(value = "retType")
    private Type retType;

    @SerializedName(value = "p")
    private String policyExpressionSQL;

    @SerializedName(value = "sqlMode")
    private long sqlMode = 0L;

    @SerializedName(value = "comment")
    private String comment;

    public CreatePolicyInfo(Policy policy) {
        this.policyType = policy.getPolicyType();
        this.policyId = policy.getPolicyId();
        this.name = policy.getName();
        this.dbPEntryObject = policy.getDbPEntryObject();
        this.argNames = policy.getArgNames();
        this.argTypes = policy.getArgTypes();
        this.retType = policy.getRetType();
        this.policyExpressionSQL = AstToSQLBuilder.toSQL(policy.getPolicyExpression());
        this.comment = policy.getComment();
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
        return GlobalStateMgr.getSqlParser().parseSqlToExpr(policyExpressionSQL, sqlMode);
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreatePolicyInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreatePolicyInfo.class);
    }
}
