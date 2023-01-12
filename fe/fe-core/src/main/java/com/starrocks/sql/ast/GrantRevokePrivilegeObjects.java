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


package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.UserIdentity;

import java.util.List;

public class GrantRevokePrivilegeObjects implements ParseNode {
    private List<List<String>> privilegeObjectNameTokensList;
    private List<UserIdentity> userPrivilegeObjectList;

    private String privType;
    private boolean isAllDB;
    private String dbName;

    private FunctionArgsDef functionArgsDef;
    private String functionName;

    public List<List<String>> getPrivilegeObjectNameTokensList() {
        return privilegeObjectNameTokensList;
    }

    public void setPrivilegeObjectNameTokensList(List<List<String>> privilegeObjectNameTokensList) {
        this.privilegeObjectNameTokensList = privilegeObjectNameTokensList;
    }

    public List<UserIdentity> getUserPrivilegeObjectList() {
        return userPrivilegeObjectList;
    }

    public void setUserPrivilegeObjectList(List<UserIdentity> userPrivilegeObjectList) {
        this.userPrivilegeObjectList = userPrivilegeObjectList;
    }

    public String getPrivType() {
        return privType;
    }

    public boolean isAllDB() {
        return isAllDB;
    }

    public String getDbName() {
        return dbName;
    }

    public void setAllPrivilegeObject(String privilegeType, boolean isAllDB, String dbName) {
        this.privType = privilegeType;
        this.isAllDB = isAllDB;
        this.dbName = dbName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public FunctionArgsDef getFunctionArgsDef() {
        return functionArgsDef;
    }

    public void setFunctionArgsDef(FunctionArgsDef functionArgsDef) {
        this.functionArgsDef = functionArgsDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
