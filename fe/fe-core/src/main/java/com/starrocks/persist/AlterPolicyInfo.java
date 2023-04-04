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
import com.starrocks.privilege.DbPEntryObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterPolicyInfo implements Writable {
    @SerializedName(value = "name")
    private String policyName;

    @SerializedName(value = "db")
    private DbPEntryObject dbPEntryObject;

    @SerializedName(value = "alterPolicyClauseInfo")
    private AlterPolicyClauseInfo alterPolicyClauseInfo;

    public AlterPolicyInfo(String policyName, DbPEntryObject dbPEntryObject, AlterPolicyClauseInfo alterPolicyClauseInfo) {
        this.policyName = policyName;
        this.dbPEntryObject = dbPEntryObject;
        this.alterPolicyClauseInfo = alterPolicyClauseInfo;
    }

    public String getPolicyName() {
        return policyName;
    }

    public DbPEntryObject getDbPEntryObject() {
        return dbPEntryObject;
    }

    public AlterPolicyClauseInfo getAlterPolicyClauseInfo() {
        return alterPolicyClauseInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterPolicyInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterPolicyInfo.class);
    }

    public abstract static class AlterPolicyClauseInfo {
    }

    public static class PolicyRenameInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "newPolicyName")
        private final String newPolicyName;

        public PolicyRenameInfo(String newPolicyName) {
            this.newPolicyName = newPolicyName;
        }

        public String getNewPolicyName() {
            return newPolicyName;
        }
    }

    public static class PolicySetBodyInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "policyBody")
        private final String policyBody;

        public PolicySetBodyInfo(String policyBody) {
            this.policyBody = policyBody;
        }

        public String getPolicyBody() {
            return policyBody;
        }
    }

    public static class PolicySetCommentInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "comment")
        private final String comment;

        public PolicySetCommentInfo(String comment) {
            this.comment = comment;
        }

        public String getComment() {
            return comment;
        }
    }
}
