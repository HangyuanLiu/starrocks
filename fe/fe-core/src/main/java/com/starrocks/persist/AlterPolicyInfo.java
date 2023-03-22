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
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.PolicyName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterPolicyInfo implements Writable {
    @SerializedName(value = "policyName")
    private final PolicyName policyName;

    @SerializedName(value = "policyRenameObject")
    private PolicyRenameObject policyRenameObject;

    @SerializedName(value = "policySetBodyObject")
    private PolicySetBodyObject policySetBodyObject;

    public AlterPolicyInfo(PolicyName policyName, PolicyRenameObject policyRenameObject) {
        this.policyName = policyName;
        this.policyRenameObject = policyRenameObject;
    }

    public AlterPolicyInfo(PolicyName policyName, PolicySetBodyObject policySetBodyObject) {
        this.policyName = policyName;
        this.policySetBodyObject = policySetBodyObject;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public PolicyRenameObject getPolicyRenameObject() {
        return policyRenameObject;
    }

    public PolicySetBodyObject getPolicySetBodyObject() {
        return policySetBodyObject;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static RolePrivilegeCollectionInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RolePrivilegeCollectionInfo.class);
    }

    public static class PolicyRenameObject {
        @SerializedName(value = "newPolicyName")
        private final String newPolicyName;

        public PolicyRenameObject(String newPolicyName) {
            this.newPolicyName = newPolicyName;
        }

        public String getNewPolicyName() {
            return newPolicyName;
        }
    }

    public static class PolicySetBodyObject {
        @SerializedName(value = "policyBody")
        private final String policyBody;

        public PolicySetBodyObject(String policyBody) {
            this.policyBody = policyBody;
        }

        public String getPolicyBody() {
            return policyBody;
        }
    }
}
