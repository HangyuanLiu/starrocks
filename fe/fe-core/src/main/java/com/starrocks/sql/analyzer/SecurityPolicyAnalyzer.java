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
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.TypeDef;
import com.starrocks.privilege.Policy;
import com.starrocks.privilege.SecurityPolicyManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterPolicyStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaskingPolicyStmt;
import com.starrocks.sql.ast.CreateRowAccessPolicyStmt;
import com.starrocks.sql.ast.DropPolicyStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;

import java.util.Collections;
import java.util.List;

public class SecurityPolicyAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new SecurityPolicyAnalyzerVisitor().analyze(statement, session);
    }

    static class SecurityPolicyAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        SecurityPolicyManager securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateMaskingPolicyStatement(CreateMaskingPolicyStmt statement, ConnectContext session) {
            //create masking policy policy_1 as (var int) returns int -> var + 1
            //select var + 1 from values(cast(null as int)) as (var)


            SelectList selectList = new SelectList(Lists.newArrayList(
                    new SelectListItem(statement.getExpression(), null)), false);
            List<Expr> row = Lists.newArrayList();
            for (TypeDef typeDef : statement.getArgTypeDefs()) {
                row.add(NullLiteral.create(typeDef.getType()));
                //row.add(new CastExpr(typeDef, new NullLiteral()));
            }
            List<List<Expr>> rows = Collections.singletonList(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, statement.getArgNames());

            SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, null, null, null);
            QueryStatement queryStatement = new QueryStatement(selectRelation);
            Analyzer.analyze(queryStatement, session);

            Expr result = queryStatement.getQueryRelation().getOutputExpression().get(0);
            if (!result.getType().equals(statement.getReturnType().getType())) {
                throw new SemanticException("");
            }

            return null;
        }

        @Override
        public Void visitCreateRowAccessPolicyStatement(CreateRowAccessPolicyStmt statement, ConnectContext session) {
            return null;
        }

        @Override
        public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext session) {
            return null;
        }

        @Override
        public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext session) {
            Policy policy = securityPolicyManager.getPolicyByName(stmt.getPolicyName());
            if (policy != null) {
                stmt.setPolicyId(policy.getPolicyId());
            } else if (!stmt.isIfExists()) {
                throw new SemanticException("");
            }
            return null;
        }
    }
}
