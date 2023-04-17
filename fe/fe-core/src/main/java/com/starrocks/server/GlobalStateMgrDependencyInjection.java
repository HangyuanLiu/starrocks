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
package com.starrocks.server;

import com.starrocks.privilege.SecurityPolicyManager;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PrivilegeCheckerV2;
import com.starrocks.sql.parser.SqlParser;
import ee.starrocks.qe.DDLStmtExecutorEE;

import javax.inject.Inject;

public class GlobalStateMgrDependencyInjection {
    private final SqlParser sqlParser;
    private final Analyzer analyzer;
    private final PrivilegeCheckerV2 privilegeChecker;
    private final DDLStmtExecutorEE ddlStmtExecutorEE;
    private final SecurityPolicyManager securityPolicyManager;

    @Inject
    public GlobalStateMgrDependencyInjection(
            SqlParser sqlParser,
            Analyzer analyzer,
            PrivilegeCheckerV2 privilegeChecker,
            DDLStmtExecutorEE ddlStmtExecutorEE,
            SecurityPolicyManager securityPolicyManager) {
        this.sqlParser = sqlParser;
        this.analyzer = analyzer;
        this.privilegeChecker = privilegeChecker;
        this.ddlStmtExecutorEE = ddlStmtExecutorEE;
        this.securityPolicyManager = securityPolicyManager;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public PrivilegeCheckerV2 getPrivilegeChecker() {
        return privilegeChecker;
    }

    public DDLStmtExecutorEE getDdlStmtExecutorEE() {
        return ddlStmtExecutorEE;
    }

    public SecurityPolicyManager getSecurityPolicyManager() {
        return securityPolicyManager;
    }
}
