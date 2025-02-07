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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;

import java.util.Map;

public class SecurityIntegrationStatementAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext context) {
        new SecurityIntegrationStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class SecurityIntegrationStatementAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
            return null;
        }

        private void validateIntegerProp(Map<String, String> propertyMap, String key, int min, int max)
                throws SemanticException {
            if (propertyMap.containsKey(key)) {
                String val = propertyMap.get(key);
                try {
                    int intVal = Integer.parseInt(val);
                    if (intVal < min || intVal > max) {
                        throw new NumberFormatException("current value of '" +
                                key + "' is invalid, value: " + intVal +
                                ", should be in range [" + min + ", " + max + "]");
                    }
                } catch (NumberFormatException e) {
                    throw new SemanticException("invalid '" +
                            key + "' property value: " + val + ", error: " + e.getMessage(), e);
                }
            }
        }

        private void validateBooleanProp(Map<String, String> propertyMap, String key) throws SemanticException {
            if (propertyMap.containsKey(key)) {
                String val = propertyMap.get(key);
                if (!val.equalsIgnoreCase("true") && !val.equalsIgnoreCase("false")) {
                    throw new SemanticException("invalid '" +
                            key + "' property value, expected 'true' or 'false'");
                }
            }

        }

        @Override
        public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement,
                                                           ConnectContext context) {
            return null;
        }

        @Override
        public Void visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement,
                                                          ConnectContext context) {
            return null;
        }

        @Override
        public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                                ConnectContext context) {
            return null;
        }
    }

}
