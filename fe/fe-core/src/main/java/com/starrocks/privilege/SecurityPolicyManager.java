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
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterPolicyStmt;
import com.starrocks.sql.ast.CreateMaskingPolicyStmt;
import com.starrocks.sql.ast.CreateRowAccessPolicyStmt;
import com.starrocks.sql.ast.DropPolicyStmt;
import com.starrocks.sql.ast.MaskingPolicyContext;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.PolicyType;
import com.starrocks.sql.ast.RowAccessPolicyContext;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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
    private final Map<PolicyName, Policy> nameToPolicy;
    private final ReentrantReadWriteLock policyLock;

    @SerializedName(value = "policyContextMap")
    private final ConcurrentMap<TablePEntryObject, PolicyContext> policyContextMap;

    public SecurityPolicyManager() {
        idToPolicy = new HashMap<>();
        nameToPolicy = new HashMap<>();
        policyContextMap = new ConcurrentHashMap<>();
        policyLock = new ReentrantReadWriteLock();
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

    public PolicyContext getTableAppliedPolicyInfo(TablePEntryObject tableId) {
        return policyContextMap.get(tableId);
    }

    public Policy getPolicyById(Long policyId) {
        return idToPolicy.get(policyId);
    }

    public Policy getPolicyByName(PolicyName policyName) {
        policyReadLock();
        try {
            return nameToPolicy.get(policyName);
        } finally {
            policyReadUnlock();
        }
    }

    //FIXME:
    public Map<PolicyName, Policy> getNameToPolicy() {
        return nameToPolicy;
    }

    public void createMaskingPolicy(CreateMaskingPolicyStmt stmt) {
        long policyId = GlobalStateMgr.getCurrentState().getNextId();

        policyWriteLock();
        try {
            MaskingPolicy maskingPolicy = new MaskingPolicy(
                    policyId,
                    stmt.getArgNames(),
                    stmt.getArgTypeDefs().stream().map(TypeDef::getType).collect(Collectors.toList()),
                    stmt.getReturnType().getType(),
                    AstToSQLBuilder.toSQL(stmt.getExpression()));
            maskingPolicy.setPolicyExpression(stmt.getExpression());

            idToPolicy.put(policyId, maskingPolicy);
            nameToPolicy.put(stmt.getPolicyName(), maskingPolicy);

            GlobalStateMgr.getCurrentState().getEditLog().logCreateMaskingPolicy(maskingPolicy);
        } finally {
            policyWriteUnLock();
        }
    }

    public void createRowAccessPolicy(CreateRowAccessPolicyStmt stmt) {
        long policyId = GlobalStateMgr.getCurrentState().getNextId();

        policyWriteLock();
        try {
            RowAccessPolicy rowAccessPolicy = new RowAccessPolicy(
                    policyId,
                    stmt.getArgNames(),
                    stmt.getArgTypeDefs().stream().map(TypeDef::getType).collect(Collectors.toList()),
                    stmt.getReturnType().getType(),
                    AstToSQLBuilder.toSQL(stmt.getExpression()));
            rowAccessPolicy.setPolicyExpression(stmt.getExpression());

            idToPolicy.put(policyId, rowAccessPolicy);
            nameToPolicy.put(stmt.getPolicyName(), rowAccessPolicy);

            GlobalStateMgr.getCurrentState().getEditLog().logCreateRowAccessPolicy(rowAccessPolicy);
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayCreatePolicy(Policy policy) {
        policyWriteLock();
        try {
            MaskingPolicy maskingPolicy = (MaskingPolicy) policy;
            idToPolicy.put(maskingPolicy.getPolicyId(), maskingPolicy);
            nameToPolicy.put(policy.getPolicyName(), policy);
        } finally {
            policyWriteUnLock();
        }
    }

    public void dropPolicy(DropPolicyStmt stmt) {
        PolicyName policyName = stmt.getPolicyName();
        Long policyId = stmt.getPolicyId();

        policyWriteLock();
        try {
            Policy policy = nameToPolicy.get(policyName);
            if (policy == null) {
                if (stmt.isIfExists()) {
                    return;
                } else {
                    throw new SemanticException("");
                }
            }

            if (!policyId.equals(policy.getPolicyId())) {
                throw new SemanticException("");
            }

            if (policy instanceof MaskingPolicy) {
                doDropPolicyUnlock(PolicyType.COLUMN_MASKING, policy.getPolicyName(), policy.getPolicyId());
            } else {
                doDropPolicyUnlock(PolicyType.ROW_ACCESS, policy.getPolicyName(), policy.getPolicyId());
            }

            GlobalStateMgr.getCurrentState().getEditLog().logDropPolicy(policy);
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayDropPolicy(Policy policy) {
        policyWriteLock();
        try {
            if (policy instanceof MaskingPolicy) {
                doDropPolicyUnlock(PolicyType.COLUMN_MASKING, policy.getPolicyName(), policy.getPolicyId());
            } else {
                doDropPolicyUnlock(PolicyType.ROW_ACCESS, policy.getPolicyName(), policy.getPolicyId());
            }
        } finally {
            policyWriteUnLock();
        }
    }

    private void doDropPolicyUnlock(PolicyType policyType, PolicyName policyName, Long policyId) {
        nameToPolicy.remove(policyName);
        idToPolicy.remove(policyId);

        for (Map.Entry<TablePEntryObject, PolicyContext> m : policyContextMap.entrySet()) {
            PolicyContext policyContext = m.getValue();
            if (policyType.equals(PolicyType.COLUMN_MASKING)) {
                policyContext.dropMaskingPolicyContext(policyId);
            } else {
                policyContext.dropRowAccessPolicyContext(policyId);
            }
        }
    }

    public void alterPolicy(AlterPolicyStmt stmt) {
        policyWriteLock();
        try {
            if (stmt.getPolicyRenameObject() != null) {
                AlterPolicyStmt.PolicyRenameObject policyRenameObject = stmt.getPolicyRenameObject();
                doAlterPolicyRenameUnlocked(stmt.getPolicyName(), policyRenameObject.getNewPolicyName());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicyRename(stmt.getPolicyName(),
                        policyRenameObject.getNewPolicyName());
            } else if (stmt.getPolicySetBodyObject() != null) {
                AlterPolicyStmt.PolicySetBody policySetBody = stmt.getPolicySetBodyObject();
                doAlterPolicySetBodyUnlocked(stmt.getPolicyName(), policySetBody.getPolicyBody());
                GlobalStateMgr.getCurrentState().getEditLog().logAlterPolicySetBody(stmt.getPolicyName(),
                        AstToSQLBuilder.toSQL(policySetBody.getPolicyBody()));
            }
        } finally {
            policyWriteUnLock();
        }
    }

    public void replayAlterPolicy(AlterPolicyInfo alterPolicyInfo) {
        policyWriteLock();
        try {
            if (alterPolicyInfo.getPolicyRenameObject() != null) {
                AlterPolicyInfo.PolicyRenameObject policyRenameObject = alterPolicyInfo.getPolicyRenameObject();
                doAlterPolicyRenameUnlocked(alterPolicyInfo.getPolicyName(), policyRenameObject.getNewPolicyName());
            } else if (alterPolicyInfo.getPolicySetBodyObject() != null) {
                AlterPolicyInfo.PolicySetBodyObject policySetBodyObject = alterPolicyInfo.getPolicySetBodyObject();
                Expr expression = SqlParser.parseSqlToExpr(policySetBodyObject.getPolicyBody(), SqlModeHelper.MODE_DEFAULT);
                doAlterPolicySetBodyUnlocked(alterPolicyInfo.getPolicyName(), expression);
            }
        } finally {
            policyWriteUnLock();
        }
    }

    private void doAlterPolicyRenameUnlocked(PolicyName policyName, String newName) {
        Policy policy = nameToPolicy.get(policyName);
        PolicyName newPolicyName = new PolicyName(policyName.getCatalog(), policyName.getDbName(),
                newName, NodePosition.ZERO);

        policy.setPolicyName(newPolicyName);
        nameToPolicy.remove(policyName);
        nameToPolicy.put(newPolicyName, policy);
    }

    private void doAlterPolicySetBodyUnlocked(PolicyName policyName, Expr policyBody) {
        Policy policy = nameToPolicy.get(policyName);
        policy.setPolicyExpression(policyBody);
    }

    public void removeInvalidObject() {
        policyReadLock();
        try {
            Iterator<Map.Entry<PolicyName, Policy>> iterator = nameToPolicy.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<PolicyName, Policy> entry = iterator.next();
                Policy policy = entry.getValue();
                if (!policy.getDbPEntryObject().validate(GlobalStateMgr.getCurrentState())) {
                    iterator.remove();
                }
            }

            Iterator<Map.Entry<TablePEntryObject, PolicyContext>> contextIterator = policyContextMap.entrySet().iterator();
            while (contextIterator.hasNext()) {
                Map.Entry<TablePEntryObject, PolicyContext> entry = contextIterator.next();
                if (!entry.getKey().validate(GlobalStateMgr.getCurrentState())) {
                    iterator.remove();
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
                writer.writeJson(entry.getValue());
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

    public static SecurityPolicyManager load(DataInputStream dis)
            throws IOException, DdlException {
        SRMetaBlockReader reader = new SRMetaBlockReader(dis, SecurityPolicyManager.class.getName());

        try {
            SecurityPolicyManager securityPolicyManager = new SecurityPolicyManager();
            int policySize = (int) reader.readJson(int.class);
            for (int i = 0; i < policySize; ++i) {
                Long policyId = (Long) reader.readJson(Long.class);
                Policy policy = (Policy) reader.readJson(Policy.class);
                String serializedExpression = policy.getPolicyExpressionSQL();
                Expr expression = SqlParser.parseSqlToExpr(serializedExpression, SqlModeHelper.MODE_DEFAULT);
                policy.setPolicyExpression(expression);
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

    public static class PolicyContext {
        @SerializedName(value = "MaskingPolicyContext")
        private final Map<String, MaskingPolicyContext> maskingPolicyApply;

        @SerializedName(value = "RowAccessPolicyContext")
        private final List<RowAccessPolicyContext> rowAccessPolicyApply;

        private final ReentrantReadWriteLock contextLock;

        public PolicyContext() {
            rowAccessPolicyApply = new ArrayList<>();
            maskingPolicyApply = new HashMap<>();
            contextLock = new ReentrantReadWriteLock();
        }

        public void addMaskingPolicy(String maskingColumn, MaskingPolicyContext maskingPolicyContext) {
            maskingPolicyApply.put(maskingColumn, maskingPolicyContext);
        }

        public void dropMaskingPolicy(String maskingColumn) {
            maskingPolicyApply.remove(maskingColumn);
        }

        public Map<String, MaskingPolicyContext> getMaskingPolicyApply() {
            return maskingPolicyApply;
        }

        public void addRowAccessPolicy(RowAccessPolicyContext rowAccessPolicyContext) {
            rowAccessPolicyApply.add(rowAccessPolicyContext);
        }

        public List<RowAccessPolicyContext> getRowAccessPolicyApply() {
            return rowAccessPolicyApply;
        }

        public void dropMaskingPolicyContext(Long policyId) {
            contextLock.writeLock().lock();

            try {
                Iterator<Map.Entry<String, MaskingPolicyContext>> maskingPolicyContextIterator
                        = maskingPolicyApply.entrySet().iterator();
                while (maskingPolicyContextIterator.hasNext()) {
                    Map.Entry<String, MaskingPolicyContext> entry = maskingPolicyContextIterator.next();
                    MaskingPolicyContext maskingPolicyContext = entry.getValue();
                    if (maskingPolicyContext.getPolicyId().equals(policyId)) {
                        maskingPolicyContextIterator.remove();
                    }
                }
            } finally {
                contextLock.writeLock().unlock();
            }
        }

        public void dropRowAccessPolicyContext(Long policyId) {
            contextLock.writeLock().lock();

            try {
                Iterator<RowAccessPolicyContext> rowAccessPolicyContextIterator = rowAccessPolicyApply.iterator();
                while (rowAccessPolicyContextIterator.hasNext()) {
                    RowAccessPolicyContext rowAccessPolicyContext = rowAccessPolicyContextIterator.next();
                    if (rowAccessPolicyContext.getPolicyId().equals(policyId)) {
                        rowAccessPolicyContextIterator.remove();
                    }
                }
            } finally {
                contextLock.writeLock().unlock();
            }
        }

        public void clearRowAccessPolicy() {
            rowAccessPolicyApply.clear();
        }
    }

    public void applyMaskingPolicyContext(TableName tableName, String columnName, MaskingPolicyContext maskingPolicyContext)
            throws DdlException {

        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        if (policyContextMap.containsKey(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
            tableAppliedPolicyInfo.addMaskingPolicy(columnName, maskingPolicyContext);
        } else {
            PolicyContext tableAppliedPolicyInfo = new PolicyContext();
            tableAppliedPolicyInfo.addMaskingPolicy(columnName, maskingPolicyContext);
            policyContextMap.put(tablePEntryObject, tableAppliedPolicyInfo);
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
            tableAppliedPolicyInfo.dropMaskingPolicy(columnName);
        }
    }

    public void applyRowAccessPolicyContext(TableName tableName, RowAccessPolicyContext rowAccessPolicyContext)
            throws DdlException {
        TablePEntryObject tablePEntryObject;
        try {
            tablePEntryObject = TablePEntryObject.generate(GlobalStateMgr.getCurrentState(),
                    Lists.newArrayList(tableName.getCatalog(), tableName.getDb(), tableName.getTbl()));
        } catch (PrivilegeException e) {
            throw new DdlException(e.getMessage());
        }

        if (policyContextMap.containsKey(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
            tableAppliedPolicyInfo.addRowAccessPolicy(rowAccessPolicyContext);
        } else {
            PolicyContext tableAppliedPolicyInfo = new PolicyContext();
            tableAppliedPolicyInfo.addRowAccessPolicy(rowAccessPolicyContext);
            policyContextMap.put(tablePEntryObject, tableAppliedPolicyInfo);
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

        Policy policy = nameToPolicy.get(policyName);
        if (policyContextMap.containsKey(tablePEntryObject)) {
            PolicyContext tableAppliedPolicyInfo = policyContextMap.get(tablePEntryObject);
            tableAppliedPolicyInfo.dropRowAccessPolicyContext(policy.getPolicyId());
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
