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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

public class ShowPolicyStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;
    private final PolicyType policyType;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("name", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("catalog", ScalarType.createVarchar(100)));
        builder.addColumn(new Column("database", ScalarType.createVarchar(100)));
        META_DATA = builder.build();
    }

    public ShowPolicyStmt(PolicyType policyType, NodePosition pos) {
        super(pos);
        this.policyType = policyType;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowPolicyStatement(this, context);
    }
}
