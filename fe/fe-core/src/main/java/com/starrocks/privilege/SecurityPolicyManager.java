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

import com.starrocks.analysis.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.PolicyType;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.ast.WithRowAccessPolicy;
import ee.starrocks.persist.AlterPolicyInfo;
import ee.starrocks.persist.CreatePolicyInfo;
import ee.starrocks.persist.DropPolicyInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SecurityPolicyManager {

    public boolean hasTableAppliedPolicy(TablePEntryObject tablePEntryObject) {
        return false;
    }

    public PolicyContext getTableAppliedPolicyInfo(TablePEntryObject tableId) {
        return null;
    }

    public Policy getPolicyById(Long policyId) {
        return null;
    }

    public Policy getPolicyByName(PolicyType policyType, PolicyName policyName) {
        return null;
    }

    public void replayCreatePolicy(CreatePolicyInfo createPolicyInfo) {

    }

    public void replayDropPolicy(DropPolicyInfo dropPolicyInfo) throws DdlException {

    }

    public void replayAlterPolicy(AlterPolicyInfo alterPolicyInfo) {

    }

    public void applyMaskingPolicyContext(TableName tableName, String columnName, WithColumnMaskingPolicy withColumnMaskingPolicy)
            throws DdlException {
    }

    public void revokeMaskingPolicyContext(String catalog, String dbName, String tblName,
                                           String columnName) throws DdlException {
    }

    public void applyRowAccessPolicyContext(TableName tableName, WithRowAccessPolicy withRowAccessPolicy)
            throws DdlException {
    }

    public void revokeRowAccessPolicyContext(String catalog, String dbName, String tblName,
                                             PolicyName policyName) throws DdlException {
    }

    public void revokeALLRowAccessPolicyContext(String catalog, String dbName, String tblName) throws DdlException {

    }

    public void save(DataOutputStream dos) throws IOException {
    }

    public void load(DataInputStream dis) throws IOException, DdlException {
    }
}
