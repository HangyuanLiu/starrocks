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

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.starrocks.privilege.SecurityPolicyManager;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.PrivilegeCheckerV2;
import com.starrocks.sql.parser.AstBuilder;
import ee.starrocks.privilege.SecurityPolicyManagerEE;
import ee.starrocks.qe.DDLStmtExecutorEE;
import ee.starrocks.sql.AnalyzerEE;
import ee.starrocks.sql.analyzer.PrivilegeCheckerEE;
import ee.starrocks.sql.parser.AstBuilderEE;

public class ServerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AstBuilder.class).to(AstBuilderEE.class).in(Scopes.SINGLETON);
        bind(Analyzer.class).to(AnalyzerEE.class).in(Scopes.SINGLETON);
        bind(PrivilegeCheckerV2.class).to(PrivilegeCheckerEE.class).in(Scopes.SINGLETON);
        bind(DDLStmtExecutor.class).to(DDLStmtExecutorEE.class).in(Scopes.SINGLETON);
        bind(SecurityPolicyManager.class).to(SecurityPolicyManagerEE.class).in(Scopes.SINGLETON);
    }

    /*
    @Override
    protected void configure() {
        bind(AstBuilder.class).to(AstBuilderEE.class).in(Scopes.SINGLETON);
        bind(SecurityPolicyManager.class).to(SecurityPolicyManagerEE.class).in(Scopes.SINGLETON);
    }
     */
}
