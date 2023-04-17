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
package ee.starrocks.sql.ast;

import com.starrocks.sql.ast.AstVisitor;

public class AstVisitorEE<R, C> extends AstVisitor<R, C> {
    // ---------------------------------------- Security Policy Statement ---------------------------------------------------

    public R visitCreatePolicyStatement(CreatePolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitAlterPolicyStatement(AlterPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitDropPolicyStatement(DropPolicyStmt statement, C context) {
        return visitDDLStatement(statement, context);
    }

    public R visitShowPolicyStatement(ShowPolicyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }

    public R visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, C context) {
        return visitStatement(statement, context);
    }

    public R visitShowPolicyApplyStatement(ShowPolicyApplyStmt statement, C context) {
        return visitShowStatement(statement, context);
    }
}
