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
package ee.starrocks.sql;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.StatementBase;
import ee.starrocks.sql.analyzer.SecurityPolicyAnalyzer;
import ee.starrocks.sql.ast.AlterPolicyStmt;
import ee.starrocks.sql.ast.AstVisitorEE;
import ee.starrocks.sql.ast.CreatePolicyStmt;
import ee.starrocks.sql.ast.DropPolicyStmt;
import ee.starrocks.sql.ast.EnterpriseStatement;
import ee.starrocks.sql.ast.ShowCreatePolicyStmt;
import ee.starrocks.sql.ast.ShowPolicyApplyStmt;
import ee.starrocks.sql.ast.ShowPolicyStmt;

public class AnalyzerEE extends Analyzer {

    @Override
    public void analyze(StatementBase statement, ConnectContext session) {
        if (statement instanceof EnterpriseStatement) {
            new AnalyzerVisitorEE().analyze(statement, session);
        } else {
            super.analyze(statement, session);
        }
    }

    protected static class AnalyzerVisitorEE extends AstVisitorEE<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        // ---------------------------------------- Security Policy Statement ---------------------------------------------------

        @Override
        public Void visitCreatePolicyStatement(CreatePolicyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitDropPolicyStatement(DropPolicyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitAlterPolicyStatement(AlterPolicyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitShowPolicyStatement(ShowPolicyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitShowCreatePolicyStatement(ShowCreatePolicyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }

        @Override
        public Void visitShowPolicyApplyStatement(ShowPolicyApplyStmt stmt, ConnectContext context) {
            SecurityPolicyAnalyzer.analyze(stmt, context);
            return null;
        }
    }
}
