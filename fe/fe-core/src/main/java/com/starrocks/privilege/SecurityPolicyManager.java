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

package com.starrocks.privilege;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AlterPolicyInfo;
import com.starrocks.persist.CreatePolicyInfo;
import com.starrocks.persist.DropPolicyInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterPolicyStmt;
import com.starrocks.sql.ast.CreatePolicyStmt;
import com.starrocks.sql.ast.DropPolicyStmt;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.PolicyType;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.ast.WithRowAccessPolicy;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SecurityPolicyManager {
    @SerializedName(value = "idToPolicy")
    private Map<Long, Policy> idToPolicy;
    private final Map<DbPEntryObject, Map<String, Policy>> nameToMaskingPolicy;
    private final Map<DbPEntryObject, Map<String, Policy>> nameToRowAccessPolicy;

    private final ReentrantReadWriteLock policyLock;

    @SerializedName(value = "policyContextMap")
    private final ConcurrentMap<TablePEntryObject, PolicyContext> policyContextMap;

    public SecurityPolicyManager() {
        idToPolicy = new HashMap<>();
        nameToMaskingPolicy = new HashMap<>();
        nameToRowAccessPolicy = new HashMap<>();
        policyLock = new ReentrantReadWriteLock();

        policyContextMap = new ConcurrentHashMap<>();
    }

    private void policyReadLock() {
        policyLock.readLock().lock();
    }

    private void policyReadUnlock() {
        policyLock.readLock().unlock();
    }

    private void policyWriteLock() {
        policyLock.writeLock().lock();
    }

    private void policyWriteUnLock() {
        policyLock.writeLock().unlock();
    }

    public boolean hasTableAppliedPolicy(TablePEntryObject tablePEntryObject) {
        return policyContextMap.containsKey(tablePEntryObject);
    }

    public ConcurrentMap<TablePEntryObject, PolicyContext> getPolicyContextMap() {
        return policyContextMap;
    }

    public PolicyContext getTableAppliedPolicyInfo(TablePEntryObject tableId) {
        return policyContextMap.get(tableId);
    }

    public Policy getPolicyById(Long policyId) {
        return idToPolicy.get(policyId);
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName) {
        policyReadLock();
        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(policyName.getCatalog(), policyName.getDbName()));
            if (policyType.equals(PolicyType.COLUMN_MASKING)) {
                Map<String, Policy> policies = nameToMaskingPolicy.get(dbPEntryObject);
                if (policies == null) {
                    return null;
                } else {
                    return policies.get(policyName.getName());
                }
            } else {
                Map<String, Policy> policies = nameToRowAccessPolicy.get(dbPEntryObject);
                if (policies == null) {
                    return null;
                } else {
                    return policies.get(policyName.getName());
                }
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            policyReadUnlock();
        }
    }

    public Map<String, Policy> getNameToPolicy(String catalog, String dbName, PolicyType policyType) {
        policyReadLock();
        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(catalog, dbName));
            if (policyType.equals(PolicyType.COLUMN_MASKING)) {
                return nameToMaskingPolicy.get(dbPEntryObject);
            } else {
                return nameToRowAccessPolicy.get(dbPEntryObject);
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            policyReadUnlock();
        }
    }

    public boolean isPolicyHasApplied(PolicyType policyType, Long policyId) {
        return policyContextMap.values().stream().anyMatch(policyContext -> policyContext.hasApplyPolicy(policyType, policyId));
    }

    public void createMaskingPolicy(CreatePolicyStmt stmt) throws DdlException {
        long policyId = GlobalStateMgr.getCurrentState().getNextId();
        String policyName = stmt.getPolicyName().getName();

        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (stmt.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        policyWriteLock();
        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName()));

            Policy policy = new Policy(stmt.getPolicyType(),
                    policyId, policyName,
                    dbPEntryObject,
                    stmt.getArgNames(),
                    stmt.getArgTypeDefs().stream().map(TypeDef::getType).collect(Collectors.toList()),
                    stmt.getReturnType().getType(),
                    stmt.getExpression(),
                    stmt.getComment());

            if (nameToPolicy.containsKey(dbPEntryObject)) {
                Map<String, Policy> polices = nameToPolicy.get(dbPEntryObject);

                if (polices.containsKey(policyName)) {
                    Policy p = polices.get(policyName);
                    if (stmt.isReplaceIfExists()) {
                        doDropPolicyUnlock(stmt.getPolicyType(), dbPEntryObject, policyName, p.getPolicyId(), false);
                    } else if (!stmt.isIfNotExists()) {
                        throw new DdlException("Policy " + policyName + " has exist");
                    }
                }
                polices.put(stmt.getPolicyName().getName(), policy);
            } else {
                Map<String, Policy> polices = new HashMap<>();
                polices.put(stmt.getPolicyName().getName(), policy);
                nameToPolicy.put(dbPEntryObject, polices);
            }

            idToPolicy.put(policyId, policy);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateMaskingPolicy(policy);
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayCreatePolicy(CreatePolicyInfo createPolicyInfo) {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (createPolicyInfo.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        policyWriteLock();
        try {
            DbPEntryObject dbPEntryObject = createPolicyInfo.getDbPEntryObject();

            Policy policy = new Policy(
                    createPolicyInfo.getPolicyType(),
                    createPolicyInfo.getPolicyId(),
                    createPolicyInfo.getName(),
                    createPolicyInfo.getDbPEntryObject(),
                    createPolicyInfo.getArgNames(),
                    createPolicyInfo.getArgTypes(),
                    createPolicyInfo.getRetType(),
                    createPolicyInfo.getPolicyExpression(),
                    createPolicyInfo.getComment());

            idToPolicy.put(policy.getPolicyId(), policy);

            if (nameToPolicy.containsKey(dbPEntryObject)) {
                Map<String, Policy> polices = nameToPolicy.get(dbPEntryObject);

                if (polices.containsKey(createPolicyInfo.getName())) {
                    Policy p = polices.get(createPolicyInfo.getName());
                    idToPolicy.remove(p.getPolicyId());
                }

                polices.put(createPolicyInfo.getName(), policy);
            } else {
                Map<String, Policy> polices = new HashMap<>();
                polices.put(createPolicyInfo.getName(), policy);
                nameToPolicy.put(dbPEntryObject, polices);
            }

        } finally {
            policyWriteUnLock();
        }
    }

    public void dropPolicy(DropPolicyStmt stmt) throws DdlException {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (stmt.getPolicyType().equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        PolicyName policyName = stmt.getPolicyName();
        Long policyId = stmt.getPolicyId();

        policyWriteLock();
        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName()));

            Map<String, Policy> policies = nameToPolicy.get(dbPEntryObject);
            Policy policy = policies.get(stmt.getPolicyName().getName());

            if (policy == null || !policyId.equals(policy.getPolicyId())) {
                if (stmt.isIfExists()) {
                    return;
                } else {
                    throw new DdlException("Can't find policy " + policyName);
                }
            }

            doDropPolicyUnlock(policy.getPolicyType(), dbPEntryObject, stmt.getPolicyName().getName(), policy.getPolicyId(),
                    stmt.isForce());

            GlobalStateMgr.getCurrentState().getEditLog().logDropPolicy(policyName, dbPEntryObject, policy);
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayDropPolicy(DropPolicyInfo dropPolicyInfo) throws DdlException {
        policyWriteLock();
        try {
            doDropPolicyUnlock(dropPolicyInfo.getPolicyType(), dropPolicyInfo.getDb(), dropPolicyInfo.getName(),
                    dropPolicyInfo.getPolicyId(), true);
        } finally {
            policyWriteUnLock();
        }
    }

    private void doDropPolicyUnlock(PolicyType policyType, DbPEntryObject dbPEntryObject, String policyName, Long policyId,
                                    boolean force) throws DdlException {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        if (isPolicyHasApplied(policyType, policyId)) {
            if (!force) {
                throw new DdlException("Can't drop policy which has be apply");
            }
        }

        Map<String, Policy> polices = nameToPolicy.get(dbPEntryObject);
        polices.remove(policyName);
        idToPolicy.remove(policyId);
    }

    public void alterPolicy(AlterPolicyStmt stmt) {
        policyWriteLock();
        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(stmt.getPolicyName().getCatalog(), stmt.getPolicyName().getDbName()));

            if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicySetBody) {
                AlterPolicyStmt.PolicySetBody policySetBody = (AlterPolicyStmt.PolicySetBody) stmt.getAlterPolicyClause();
                doAlterPolicySetBodyUnlocked(stmt.getPolicyType(), dbPEntryObject, stmt.getPolicyName().getName(),
                        policySetBody.getPolicyBody());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicySetBody(stmt.getPolicyName(), dbPEntryObject,
                        AstToSQLBuilder.toSQL(policySetBody.getPolicyBody()));
            } else if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicySetComment) {
                AlterPolicyStmt.PolicySetComment policySetComment =
                        (AlterPolicyStmt.PolicySetComment) stmt.getAlterPolicyClause();
                doAlterPolicySetCommentUnlocked(stmt.getPolicyType(), dbPEntryObject, stmt.getPolicyName().getName(),
                        policySetComment.getComment());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicySetComment(stmt.getPolicyName(), dbPEntryObject,
                        policySetComment.getComment());
            } else if (stmt.getAlterPolicyClause() instanceof AlterPolicyStmt.PolicyRename) {
                AlterPolicyStmt.PolicyRename policyRename = (AlterPolicyStmt.PolicyRename) stmt.getAlterPolicyClause();
                doAlterPolicyRenameUnlocked(stmt.getPolicyType(), dbPEntryObject, stmt.getPolicyName().getName(),
                        policyRename.getNewPolicyName());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicyRename(stmt.getPolicyName(), dbPEntryObject,
                        policyRename.getNewPolicyName());
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayAlterPolicy(AlterPolicyInfo alterPolicyInfo) {
        policyWriteLock();
        try {
            if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyInfo.PolicySetBodyInfo) {
                AlterPolicyInfo.PolicySetBodyInfo policySetBodyObject =
                        (AlterPolicyInfo.PolicySetBodyInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                Expr expression = SqlParser.parseSqlToExpr(policySetBodyObject.getPolicyBody(), SqlModeHelper.MODE_DEFAULT);
                doAlterPolicySetBodyUnlocked(alterPolicyInfo.getPolicyType(), alterPolicyInfo.getDbPEntryObject(),
                        alterPolicyInfo.getPolicyName(), expression);
            } else if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyInfo.PolicySetCommentInfo) {
                AlterPolicyInfo.PolicySetCommentInfo setCommentInfo =
                        (AlterPolicyInfo.PolicySetCommentInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                doAlterPolicySetCommentUnlocked(alterPolicyInfo.getPolicyType(),
                        alterPolicyInfo.getDbPEntryObject(), alterPolicyInfo.getPolicyName(),
                        setCommentInfo.getComment());
            } else if (alterPolicyInfo.getAlterPolicyClauseInfo() instanceof AlterPolicyInfo.PolicyRenameInfo) {
                AlterPolicyInfo.PolicyRenameInfo policyRenameObject =
                        (AlterPolicyInfo.PolicyRenameInfo) alterPolicyInfo.getAlterPolicyClauseInfo();
                doAlterPolicyRenameUnlocked(alterPolicyInfo.getPolicyType(), alterPolicyInfo.getDbPEntryObject(),
                        alterPolicyInfo.getPolicyName(), policyRenameObject.getNewPolicyName());
            }
        } finally {
            policyWriteUnLock();
        }
    }

    private void doAlterPolicySetBodyUnlocked(PolicyType policyType, DbPEntryObject dbPEntryObject, String policyName,
                                              Expr policyBody) {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        Map<String, Policy> policies = nameToPolicy.get(dbPEntryObject);
        Policy policy = policies.get(policyName);
        policy.setPolicyExpression(policyBody);
    }

    private void doAlterPolicySetCommentUnlocked(PolicyType policyType, DbPEntryObject dbPEntryObject, String policyName,
                                                 String comment) {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        Map<String, Policy> policies = nameToPolicy.get(dbPEntryObject);
        Policy policy = policies.get(policyName);
        policy.setComment(comment);
    }

    private void doAlterPolicyRenameUnlocked(PolicyType policyType, DbPEntryObject dbPEntryObject, String policyName,
                                             String newName) {
        Map<DbPEntryObject, Map<String, Policy>> nameToPolicy;
        if (policyType.equals(PolicyType.COLUMN_MASKING)) {
            nameToPolicy = nameToMaskingPolicy;
        } else {
            nameToPolicy = nameToRowAccessPolicy;
        }

        Map<String, Policy> policies = nameToPolicy.get(dbPEntryObject);
        Policy policy = policies.get(policyName);
        policy.setName(newName);

        policies.remove(policyName);
        policies.put(newName, policy);
    }

    public void removeInvalidObject() {
        policyReadLock();
        try {
            nameToMaskingPolicy.entrySet().removeIf(entry -> !entry.getKey().validate(GlobalStateMgr.getCurrentState()));
            nameToRowAccessPolicy.entrySet().removeIf(entry -> !entry.getKey().validate(GlobalStateMgr.getCurrentState()));

            Iterator<Map.Entry<TablePEntryObject, PolicyContext>> iterator = policyContextMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TablePEntryObject, PolicyContext> entry = iterator.next();
                if (!entry.getKey().validate(GlobalStateMgr.getCurrentState())) {
                    iterator.remove();
                } else {
                    PolicyContext policyContext = entry.getValue();
                    Map<String, ColumnMaskingPolicyContext> m = policyContext.getMaskingPolicyApply();
                    for (ColumnMaskingPolicyContext context : m.values()) {
                        if (!idToPolicy.containsKey(context.getPolicyId())) {
                            policyContext.revokeMaskingPolicy(context.getPolicyId());
                        }
                    }

                    List<RowAccessPolicyContext> r = policyContext.getRowAccessPolicyApply();
                    for (RowAccessPolicyContext context : r) {
                        if (!idToPolicy.containsKey(context.getPolicyId())) {
                            policyContext.revokeRowAccessPolicy(context.getPolicyId());
                        }
                    }
                }
            }
        } finally {
            policyReadUnlock();
        }
    }

    public void save(DataOutputStream dos) throws IOException {
        try {
            int cnt = idToPolicy.size() + policyContextMap.size();
            SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SecurityPolicyManager.class.getName(), cnt);

            writer.writeJson(idToPolicy.size());
            for (Map.Entry<Long, Policy> entry : idToPolicy.entrySet()) {
                writer.writeJson(entry.getKey());
                writer.writeJson(new CreatePolicyInfo(entry.getValue()));
            }

            writer.writeJson(policyContextMap.size());
            for (Map.Entry<TablePEntryObject, PolicyContext> entry : policyContextMap.entrySet()) {
                writer.writeJson(entry.getKey());
                writer.writeJson(entry.getValue());
            }

            writer.close();
        } catch (SRMetaBlockException e) {
            throw new IOException("failed to save SecurityPolicyManager", e);
        }
    }

    public static SecurityPolicyManager load(DataInputStream dis) throws IOException, DdlException {
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, SecurityPolicyManager.class.getName());

        try {
            SecurityPolicyManager securityPolicyManager = new SecurityPolicyManager();
            int policySize = (int) reader.readJson(int.class);
            for (int i = 0; i < policySize; ++i) {
                Long policyId = (Long) reader.readJson(Long.class);
                CreatePolicyInfo createPolicyInfo = (CreatePolicyInfo) reader.readJson(CreatePolicyInfo.class);

                Policy policy = new Policy(createPolicyInfo.getPolicyType(), createPolicyInfo.getPolicyId(),
                        createPolicyInfo.getName(), createPolicyInfo.getDbPEntryObject(),
                        createPolicyInfo.getArgNames(), createPolicyInfo.getArgTypes(),
                        createPolicyInfo.getRetType(), createPolicyInfo.getPolicyExpression(), createPolicyInfo.getComment());
                securityPolicyManager.idToPolicy.put(policyId, policy);
            }

            int policyContextSize = (int) reader.readJson(int.class);
            for (int i = 0; i < policyContextSize; ++i) {
                TablePEntryObject tablePEntryObject = (TablePEntryObject) reader.readJson(TablePEntryObject.class);
                PolicyContext policyContext = (PolicyContext) reader.readJson(PolicyContext.class);
                securityPolicyManager.policyContextMap.put(tablePEntryObject, policyContext);
            }

            return securityPolicyManager;
        } catch (SRMetaBlockException | SRMetaBlockEOFException e) {
            throw new DdlException("failed to load SecurityPolicyManager!", e);
        } finally {
            try {
                reader.close();
            } catch (SRMetaBlockException e) {
                throw new DdlException("failed to load SecurityPolicyManager!", e);
            }
        }
    }

    public void applyMaskingPolicyContext(TableName tableName, String columnName, WithColumnMaskingPolicy withColumnMaskingPolicy)
            throws DdlException {

        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        try {
            PolicyName policyName = withColumnMaskingPolicy.getPolicyName();
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(policyName.getCatalog(), policyName.getDbName()));
            Map<String, Policy> policies = nameToMaskingPolicy.get(dbPEntryObject);
            Policy policy = policies.get(policyName.getName());

            if (policyContextMap.containsKey(tablePEntryObject)) {
                PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
                tableAppliedPolicyInfo.applyMaskingPolicy(columnName,
                        new ColumnMaskingPolicyContext(policy.getPolicyId(), withColumnMaskingPolicy.getUsingColumns()));
            } else {
                PolicyContext tableAppliedPolicyInfo = new PolicyContext();
                tableAppliedPolicyInfo.applyMaskingPolicy(columnName,
                        new ColumnMaskingPolicyContext(policy.getPolicyId(), withColumnMaskingPolicy.getUsingColumns()));
                policyContextMap.put(tablePEntryObject, tableAppliedPolicyInfo);
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    public void revokeMaskingPolicyContext(String catalog, String dbName, String tblName,
                                           String columnName) throws DdlException {
        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(
                    GlobalStateMgr.getCurrentState(), Lists.newArrayList(catalog, dbName, tblName));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        if (policyContextMap.containsKey(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
            tableAppliedPolicyInfo.revokeMaskingPolicy(columnName);
        }
    }

    public void applyRowAccessPolicyContext(TableName tableName, WithRowAccessPolicy withRowAccessPolicy)
            throws DdlException {
        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        try {
            PolicyName policyName = withRowAccessPolicy.getPolicyName();
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(policyName.getCatalog(), policyName.getDbName()));
            Map<String, Policy> policies = nameToRowAccessPolicy.get(dbPEntryObject);
            Policy policy = policies.get(policyName.getName());

            if (policyContextMap.containsKey(tablePEntryObject)) {
                PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
                tableAppliedPolicyInfo.addRowAccessPolicy(
                        new RowAccessPolicyContext(policy.getPolicyId(), withRowAccessPolicy.getOnColumns()));
            } else {
                PolicyContext tableAppliedPolicyInfo = new PolicyContext();
                tableAppliedPolicyInfo.addRowAccessPolicy(
                        new RowAccessPolicyContext(policy.getPolicyId(), withRowAccessPolicy.getOnColumns()));
                policyContextMap.put(tablePEntryObject, tableAppliedPolicyInfo);
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    public void revokeRowAccessPolicyContext(String catalog, String dbName, String tblName,
                                             PolicyName policyName) throws DdlException {
        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(
                    GlobalStateMgr.getCurrentState(), Lists.newArrayList(catalog, dbName, tblName));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        try {
            DbPEntryObject dbPEntryObject = DbPEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(policyName.getCatalog(), policyName.getDbName()));
            Map<String, Policy> policies = nameToRowAccessPolicy.get(dbPEntryObject);
            Policy policy = policies.get(policyName.getName());

            if (policyContextMap.containsKey(tablePEntryObject)) {
                PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
                tableAppliedPolicyInfo.revokeRowAccessPolicy(policy.getPolicyId());
            }
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    public void revokeALLRowAccessPolicyContext(String catalog, String dbName, String tblName) throws DdlException {
        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(
                    GlobalStateMgr.getCurrentState(), Lists.newArrayList(catalog, dbName, tblName));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        if (policyContextMap.containsKey(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
            tableAppliedPolicyInfo.clearRowAccessPolicy();
        }
    }
}
