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

package com.starrocks.sql.ast.feedback;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.NodePosition;

public class AddPlanAdvisorStmt extends PlanAdvisorStmt {

    private final QueryStatement queryStmt;

    public AddPlanAdvisorStmt(NodePosition pos, QueryStatement queryStmt) {
        super(pos);
        this.queryStmt = queryStmt;
    }

    public QueryStatement getQueryStmt() {
        return queryStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddPlanAdvisorStatement(this, context);
    }

}