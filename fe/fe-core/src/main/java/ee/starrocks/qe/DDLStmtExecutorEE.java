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

package ee.starrocks.qe;

import com.starrocks.analysis.ParseNode;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.ast.StatementBase;
import ee.starrocks.privilege.SecurityPolicyManagerEE;
import ee.starrocks.sql.ast.AlterPolicyStmt;
import ee.starrocks.sql.ast.AstVisitorEE;
import ee.starrocks.sql.ast.CreatePolicyStmt;
import ee.starrocks.sql.ast.DropPolicyStmt;
import ee.starrocks.sql.ast.EnterpriseStatement;

import java.io.IOException;

public class DDLStmtExecutorEE extends DDLStmtExecutor {

    @Override
    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws Exception {
        try {
            if (stmt instanceof EnterpriseStatement) {
                return stmt.accept(StmtExecutorVisitorEE.getInstance(), context);
            } else {
                return super.execute(stmt, context);
            }
        } catch (RuntimeException re) {
            if (re.getCause() instanceof DdlException) {
                throw (DdlException) re.getCause();
            } else if (re.getCause() instanceof IOException) {
                throw (IOException) re.getCause();
            } else if (re.getCause() != null) {
                throw new DdlException(re.getCause().getMessage());
            } else {
                throw re;
            }
        }
    }

    protected static class StmtExecutorVisitorEE extends AstVisitorEE<ShowResultSet, ConnectContext> {

        private static final StmtExecutorVisitorEE INSTANCE = new StmtExecutorVisitorEE();

        public static StmtExecutorVisitorEE getInstance() {
            return INSTANCE;
        }

        @Override
        public ShowResultSet visitNode(ParseNode node, ConnectContext context) {
            throw new RuntimeException(new DdlException("unsupported statement: " + node.toSql()));
        }

        @Override
        public ShowResultSet visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                SecurityPolicyManagerEE securityPolicyManagerEE =
                        (SecurityPolicyManagerEE) context.getGlobalStateMgr().getSecurityPolicyManager();
                securityPolicyManagerEE.createMaskingPolicy(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                SecurityPolicyManagerEE securityPolicyManagerEE =
                        (SecurityPolicyManagerEE) context.getGlobalStateMgr().getSecurityPolicyManager();
                securityPolicyManagerEE.dropPolicy(stmt);
            });
            return null;
        }

        @Override
        public ShowResultSet visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                SecurityPolicyManagerEE securityPolicyManagerEE =
                        (SecurityPolicyManagerEE) context.getGlobalStateMgr().getSecurityPolicyManager();
                securityPolicyManagerEE.alterPolicy(stmt);
            });
            return null;
        }
    }
}
