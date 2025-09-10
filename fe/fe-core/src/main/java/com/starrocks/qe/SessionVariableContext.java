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

package com.starrocks.qe;

import com.starrocks.authentication.UserProperty;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.VariableExpr;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class SessionVariableContext {
    private static final Logger LOG = LogManager.getLogger(SessionVariableContext.class);
    private final Supplier<SessionVariable> sessionVariableSupplier;
    private SessionVariable sessionVariable;
    private final Map<String, SystemVariable> modifiedSessionVariables = new HashMap<>();
    private Map<String, UserVariable> userVariables;
    private Map<String, UserVariable> userVariablesCopyInWrite;

    public SessionVariableContext(Supplier<SessionVariable> sessionVariableSupplier) {
        this.sessionVariableSupplier = sessionVariableSupplier;
        this.sessionVariable = sessionVariableSupplier.get();
        this.userVariables = new ConcurrentHashMap<>();
    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    // Reset session variable to default values provided by VariableMgr and clear modified set
    public void resetSessionVariable() {
        this.sessionVariable = sessionVariableSupplier.get();
        modifiedSessionVariables.clear();
    }

    public Map<String, UserVariable> getUserVariables() {
        return userVariables;
    }

    public Map<String, UserVariable> getUserVariablesCopyInWrite() {
        return userVariablesCopyInWrite;
    }

    public UserVariable getUserVariable(String variable) {
        return userVariables.get(variable);
    }

    public UserVariable getUserVariableCopyInWrite(String variable) {
        if (userVariablesCopyInWrite == null) {
            return null;
        }
        return userVariablesCopyInWrite.get(variable);
    }

    public void modifyUserVariable(UserVariable userVariable) {
        if (userVariables.size() > 1024 && !userVariables.containsKey(userVariable.getVariable())) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        userVariables.put(userVariable.getVariable(), userVariable);
    }

    public void modifyUserVariableCopyInWrite(UserVariable userVariable) {
        if (userVariablesCopyInWrite != null) {
            if (userVariablesCopyInWrite.size() > 1024) {
                throw new SemanticException("User variable exceeds the maximum limit of 1024");
            }
            userVariablesCopyInWrite.put(userVariable.getVariable(), userVariable);
        }
    }

    public void modifyUserVariables(Map<String, UserVariable> userVarCopyInWrite) {
        if (userVarCopyInWrite.size() > 1024) {
            throw new SemanticException("User variable exceeds the maximum limit of 1024");
        }
        this.userVariables = userVarCopyInWrite;
    }

    // Instead of modifying the live map, use a copy to ensure atomicity/isolation of updates
    public void modifyUserVariablesCopyInWrite(Map<String, UserVariable> userVariables) {
        this.userVariablesCopyInWrite = userVariables;
    }

    public void resetUserVariableCopyInWrite() {
        userVariablesCopyInWrite = null;
    }

    public void addModifiedSessionVariables(SystemVariable systemVariable) {
        modifiedSessionVariables.put(systemVariable.getVariable(), systemVariable);
    }

    public SetStmt getModifiedSessionVariables() {
        List<SetListItem> sessionVariables = new ArrayList<>();
        if (MapUtils.isNotEmpty(modifiedSessionVariables)) {
            sessionVariables.addAll(modifiedSessionVariables.values());
        }
        if (MapUtils.isNotEmpty(userVariables)) {
            for (UserVariable userVariable : userVariables.values()) {
                if (!userVariable.isFromHint()) {
                    sessionVariables.add(userVariable);
                }
            }
        }

        if (sessionVariables.isEmpty()) {
            return null;
        } else {
            return new SetStmt(sessionVariables);
        }
    }

    // We can not make sure the set variables are all valid. Even if some variables are invalid,
    // we should let user continue to execute SQL.
    // We can not make sure the set variables are all valid. Even if some variables are invalid,
    // we should let user continue to execute SQL during handshake.
    public void updateByUserProperty(ConnectContext context, UserProperty userProperty) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

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
                modifySystemVariable(context, variable, true);
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
                    modifySystemVariable(context, variable, true);
                }
            }
        } catch (Exception e) {
            LOG.warn("set session env failed: ", e);
        }
    }

    // Apply a system variable change to this session and record it for forwarding if needed
    public void modifySystemVariable(ConnectContext context, SystemVariable setVar, boolean onlySetSessionVar)
            throws DdlException {
        VariableMgr variableMgr = GlobalStateMgr.getCurrentState().getVariableMgr();
        variableMgr.setSystemVariable(sessionVariable, setVar, onlySetSessionVar);
        if (!SetType.GLOBAL.equals(setVar.getType()) && variableMgr.shouldForwardToLeader(setVar.getVariable())) {
            context.addModifiedSessionVariables(setVar);
        }
    }
}
