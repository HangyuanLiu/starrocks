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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.connector.ConnectorTableId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata representation of an external StarRocks table accessed via the connector.
 * <p>
 * The table stores the latest {@code _query_plan} payload along with the pruned tablet routing
 * information returned by the remote FE. Those artifacts are later consumed by the planner/executor
 * when constructing scan ranges and RPC requests.
 */
public class StarRocksExternalTable extends Table {

    @SerializedName("cn")
    private String catalogName;

    @SerializedName("dn")
    private String remoteDbName;

    @SerializedName("tn")
    private String remoteTableName;

    @SerializedName("qp")
    private String opaquedQueryPlan;

    @SerializedName("tab")
    private Map<Long, Tablet> tablets = Collections.emptyMap();

    @SerializedName("rt")
    private long refreshedTimestamp;

    @SerializedName("props")
    private Map<String, String> executionProperties = Collections.emptyMap();

    public StarRocksExternalTable() {
        super(TableType.STARROCKS);
    }

    public StarRocksExternalTable(ConnectorTableId tableId,
                                  String catalogName,
                                  String remoteDbName,
                                  String remoteTableName,
                                  List<Column> schema,
                                  String opaquedQueryPlan,
                                  Map<Long, Tablet> tablets,
                                  long refreshedTimestamp,
                                  Map<String, String> executionProperties) {
        super(tableId.asInt(), remoteTableName, TableType.STARROCKS, cloneSchema(schema));
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName is null");
        this.remoteDbName = Objects.requireNonNull(remoteDbName, "remoteDbName is null");
        this.remoteTableName = Objects.requireNonNull(remoteTableName, "remoteTableName is null");
        this.opaquedQueryPlan = opaquedQueryPlan;
        this.tablets = ImmutableMap.copyOf(tablets);
        this.refreshedTimestamp = refreshedTimestamp;
        this.createTime = refreshedTimestamp;
        if (executionProperties != null && !executionProperties.isEmpty()) {
            this.executionProperties = ImmutableMap.copyOf(executionProperties);
        }
    }

    private static List<Column> cloneSchema(List<Column> schema) {
        if (schema == null || schema.isEmpty()) {
            return ImmutableList.of();
        }
        List<Column> copy = new ArrayList<>(schema.size());
        for (Column column : schema) {
            copy.add(new Column(column));
        }
        return ImmutableList.copyOf(copy);
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return remoteDbName;
    }

    @Override
    public String getCatalogTableName() {
        return remoteTableName;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, remoteDbName, remoteTableName);
    }

    public String getOpaquedQueryPlan() {
        return opaquedQueryPlan;
    }

    public void setOpaquedQueryPlan(String opaquedQueryPlan) {
        this.opaquedQueryPlan = opaquedQueryPlan;
    }

    public Map<Long, Tablet> getTablets() {
        return tablets;
    }

    public void setTablets(Map<Long, Tablet> tablets) {
        this.tablets = ImmutableMap.copyOf(tablets);
    }

    public long getRefreshedTimestamp() {
        return refreshedTimestamp;
    }

    public void setRefreshedTimestamp(long refreshedTimestamp) {
        this.refreshedTimestamp = refreshedTimestamp;
    }

    public Map<String, String> getExecutionProperties() {
        return executionProperties;
    }

    /**
     * Tablet metadata returned from the {@code _query_plan} REST API.
     */
    public static class Tablet {
        @SerializedName("id")
        private final long tabletId;
        @SerializedName("ver")
        private final long version;
        @SerializedName("schema_hash")
        private final int schemaHash;
        @SerializedName("ep")
        private final List<String> endpoints;

        public Tablet(long tabletId, long version, int schemaHash, List<String> endpoints) {
            this.tabletId = tabletId;
            this.version = version;
            this.schemaHash = schemaHash;
            this.endpoints = ImmutableList.copyOf(endpoints);
        }

        public long getTabletId() {
            return tabletId;
        }

        public long getVersion() {
            return version;
        }

        public int getSchemaHash() {
            return schemaHash;
        }

        public List<String> getEndpoints() {
            return endpoints;
        }
    }
}
