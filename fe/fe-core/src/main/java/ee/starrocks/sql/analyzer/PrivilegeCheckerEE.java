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

import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.PrivilegeCheckerV2;
import com.starrocks.sql.ast.StatementBase;
import ee.starrocks.sql.ast.AlterPolicyStmt;
import ee.starrocks.sql.ast.AstVisitorEE;
import ee.starrocks.sql.ast.CreatePolicyStmt;
import ee.starrocks.sql.ast.DropPolicyStmt;
import ee.starrocks.sql.ast.EnterpriseStatement;
import ee.starrocks.sql.ast.ShowCreatePolicyStmt;
import ee.starrocks.sql.ast.ShowPolicyStmt;

public class PrivilegeCheckerEE extends PrivilegeCheckerV2 {
    public void check(StatementBase statement, ConnectContext context) {
        if (statement instanceof EnterpriseStatement) {
            new PrivilegeCheckerVisitor().check(statement, context);
        } else {
            super.check(statement, context);
        }
    }

    // ---------------------------------------- Security Policy Statement ---------------------------------------------------
    private static class PrivilegeCheckerVisitor extends AstVisitorEE<Void, ConnectContext> {

        public void check(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreatePolicyStatement(CreatePolicyStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkDbAction(context,
                    statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                    PrivilegeType.CREATE_POLICY)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE POLICY");
            }
            return null;
        }

        @Override
        public Void visitDropPolicyStatement(DropPolicyStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkPolicyAction(context, statement.getPolicyType(), statement.getPolicyName().getCatalog(),
                    statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.DROP)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "DROP");
            }
            return null;
        }

        @Override
        public Void visitAlterPolicyStatement(AlterPolicyStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkPolicyAction(context, statement.getPolicyType(), statement.getPolicyName().getCatalog(),
                    statement.getPolicyName().getDbName(), statement.getPolicyName().getName(), PrivilegeType.ALTER)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER");
            }
            return null;
        }

        @Override
        public Void visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
            return visitShowStatement(statement, context);
        }

        @Override
        public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
            if (!PrivilegeActions.checkAnyActionOnPolicy(context, statement.getPolicyType(),
                    statement.getPolicyName().getCatalog(), statement.getPolicyName().getDbName(),
                    statement.getPolicyName().getName())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ANY");
            }
            return null;
        }
    }
}
