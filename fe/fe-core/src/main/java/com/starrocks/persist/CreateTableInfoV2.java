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
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.WithColumnMaskingPolicy;
import com.starrocks.sql.ast.WithRowAccessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CreateTableInfoV2 implements Writable {
    public static final Logger LOG = LoggerFactory.getLogger(CreateTableInfo.class);

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "table")
    private Table table;

    @SerializedName(value = "maskingPolicyContextMap")
    private Map<String, WithColumnMaskingPolicy> maskingPolicyContextMap;

    @SerializedName(value = "rowAccessPolicyContextList")
    private List<WithRowAccessPolicy> withRowAccessPolicyList;

    public CreateTableInfoV2(String dbName, Table table, Map<String, WithColumnMaskingPolicy> maskingPolicyContextMap,
                           List<WithRowAccessPolicy> withRowAccessPolicyList) {
        this.dbName = dbName;
        this.table = table;
        this.maskingPolicyContextMap = maskingPolicyContextMap;
        this.withRowAccessPolicyList = withRowAccessPolicyList;
    }

    public String getDbName() {
        return dbName;
    }

    public Table getTable() {
        return table;
    }

    public Map<String, WithColumnMaskingPolicy> getMaskingPolicyContextMap() {
        return maskingPolicyContextMap;
    }

    public List<WithRowAccessPolicy> getRowAccessPolicyContextList() {
        return withRowAccessPolicyList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));

        // compatible with old version
        /*
        Text.writeString(out, ClusterNamespace.getFullName(dbName));
        table.write(out);

        Text.writeString(out, GsonUtils.GSON.toJson(maskingPolicyContextMap));
        Text.writeString(out, GsonUtils.GSON.toJson(rowAccessPolicyContextList));

         */
    }

    public static CreateTableInfoV2 read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreateTableInfoV2.class);
    }
}