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

package ee.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.TypeDef;
import com.starrocks.privilege.Policy;
import com.starrocks.privilege.SecurityPolicyManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.PolicyType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import ee.starrocks.sql.ast.AlterPolicyStmt;
import ee.starrocks.sql.ast.AstVisitorEE;
import ee.starrocks.sql.ast.CreatePolicyStmt;
import ee.starrocks.sql.ast.DropPolicyStmt;
import ee.starrocks.sql.ast.ShowCreatePolicyStmt;
import ee.starrocks.sql.ast.ShowPolicyStmt;

import java.util.Collections;
import java.util.List;

public class SecurityPolicyAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new SecurityPolicyAnalyzerVisitor().analyze(statement, session);
    }

    static class SecurityPolicyAnalyzerVisitor extends AstVisitorEE<Void, ConnectContext> {
        SecurityPolicyManager securityPolicyManager = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext session) {
            PolicyName policyName = statement.getPolicyName();
            normalizationTableName(session, policyName);

            SelectList selectList;
            if (statement.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(statement.getExpression(), null)), false);
            } else {
                selectList = new SelectList(Lists.newArrayList(
                        new SelectListItem(null)), false);
            }
            List<Expr> row = Lists.newArrayList();
            for (TypeDef typeDef : statement.getArgTypeDefs()) {
                row.add(NullLiteral.create(typeDef.getType()));
            }
            List<List<Expr>> rows = Collections.singletonList(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, statement.getArgNames());

            Expr predicate = null;
            if (statement.getPolicyType().equals(PolicyType.ROW_ACCESS)) {
                predicate = statement.getExpression();
            }

            SelectRelation selectRelation = new SelectRelation(selectList, valuesRelation, predicate, null, null);
            QueryStatement queryStatement = new QueryStatement(selectRelation);
            GlobalStateMgr.getAnalyzer().analyze(queryStatement, session);

            if (statement.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
                Expr result = queryStatement.getQueryRelation().getOutputExpression().get(0);
                if (!result.getType().equals(statement.getReturnType().getType())) {
                    throw new SemanticException("");
                }
            }

            return null;
        }

        @Override
        public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationTableName(session, policyName);

            Policy policy = securityPolicyManager.getPolicyByName(stmt.getPolicyType(), policyName);
            if (policy != null) {
                stmt.setPolicyId(policy.getPolicyId());
            } else if (!stmt.isIfExists()) {
                throw new SemanticException("");
            }
            return null;
        }

        @Override
        public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationTableName(session, policyName);
            return null;
        }

        @Override
        public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
            if (Strings.isNullOrEmpty(statement.getCatalog())) {
                if (Strings.isNullOrEmpty(context.getCurrentCatalog())) {
                    throw new SemanticException("No catalog selected");
                }
                statement.setCatalog(context.getCurrentCatalog());
            }
            if (Strings.isNullOrEmpty(statement.getDbName())) {
                if (Strings.isNullOrEmpty(context.getDatabase())) {
                    throw new SemanticException("No database selected");
                }
                statement.setDbName(context.getDatabase());
            }
            return null;
        }

        @Override
        public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt stmt, ConnectContext session) {
            PolicyName policyName = stmt.getPolicyName();
            normalizationTableName(session, policyName);
            return null;
        }

        public static void normalizationTableName(ConnectContext connectContext, PolicyName policyName) {
            if (Strings.isNullOrEmpty(policyName.getCatalog())) {
                if (Strings.isNullOrEmpty(connectContext.getCurrentCatalog())) {
                    throw new SemanticException("No catalog selected");
                }
                policyName.setCatalog(connectContext.getCurrentCatalog());
            }
            if (Strings.isNullOrEmpty(policyName.getDbName())) {
                if (Strings.isNullOrEmpty(connectContext.getDatabase())) {
                    throw new SemanticException("No database selected");
                }
                policyName.setDbName(connectContext.getDatabase());
            }

            if (Strings.isNullOrEmpty(policyName.getName())) {
                throw new SemanticException("Policy name is null");
            }
        }
    }
}
