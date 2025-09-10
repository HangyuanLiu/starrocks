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

package com.starrocks.authentication;

import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.VariableExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class VariableHandler {
    private static final Logger LOG = LogManager.getLogger(VariableHandler.class);

    // We can not make sure the set variables are all valid. Even if some variables are invalid, we should let user continue
    // to execute SQL.
    public static void updateByUserProperty(ConnectContext context, UserProperty userProperty) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        SessionVariable sessionVariable = context.getSessionVariable();

        try {
            // set session variables
            Map<String, String> sessionVariables = userProperty.getSessionVariables();
            for (Map.Entry<String, String> entry : sessionVariables.entrySet()) {
                String currentValue = globalStateMgr.getVariableMgr().getValue(
                        sessionVariable, new VariableExpr(entry.getKey()));
                if (!currentValue.equalsIgnoreCase(
                        globalStateMgr.getVariableMgr().getDefaultValue(entry.getKey()))) {
                    // If the current session variable is not default value, we should respect it.
                    continue;
                }
                SystemVariable variable = new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue()));
                modifySystemVariable(context, sessionVariable, variable, true);
            }

            // set catalog and database
            boolean dbHasBeenSetByUser = !context.getCurrentCatalog().equals(
                    globalStateMgr.getVariableMgr().getDefaultValue(SessionVariable.CATALOG))
                    || !context.getDatabase().isEmpty();
            if (!dbHasBeenSetByUser) {
                String catalog = userProperty.getCatalog();
                String database = userProperty.getDatabase();
                if (catalog.equals(UserProperty.CATALOG_DEFAULT_VALUE)) {
                    if (!database.equals(UserProperty.DATABASE_DEFAULT_VALUE)) {
                        context.changeCatalogDb(userProperty.getCatalogDbName());
                    }
                } else {
                    if (database.equals(UserProperty.DATABASE_DEFAULT_VALUE)) {
                        context.changeCatalog(catalog);
                    } else {
                        context.changeCatalogDb(userProperty.getCatalogDbName());
                    }
                    SystemVariable variable = new SystemVariable(SessionVariable.CATALOG, new StringLiteral(catalog));
                    modifySystemVariable(context, sessionVariable, variable, true);
                }
            }
        } catch (Exception e) {
            LOG.warn("set session env failed: ", e);
            // In handshake, we will send error message to client. But it seems that client will ignore it.

            //getState().setOk(0L, 0, String.format("set session variables from user property failed: %s", e.getMessage()));
        }
    }

    public static void modifySystemVariable(ConnectContext context, SessionVariable sessionVariable, SystemVariable setVar, boolean onlySetSessionVar) throws DdlException {
        VariableMgr variableMgr = GlobalStateMgr.getCurrentState().getVariableMgr();

        variableMgr.setSystemVariable(sessionVariable, setVar, onlySetSessionVar);
        if (!SetType.GLOBAL.equals(setVar.getType()) && variableMgr.shouldForwardToLeader(setVar.getVariable())) {
            context.addModifiedSessionVariables(setVar);
        }
    }
}
