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

package com.starrocks.connector.starrocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StarRocksExternalTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TQueryPlanInfo;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TTabletVersionInfo;
import com.starrocks.thrift.TTupleDescriptor;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class StarRocksMetadataCacheTest {

    private static final Clock FIXED_CLOCK =
            Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneId.of("UTC"));

    @Test
    public void testListAndCacheMetadata() throws Exception {
        StarRocksConnectorConfig config = buildConfig();
        CountingJdbcClient jdbcClient = new CountingJdbcClient();
        StubRestClient restClient = new StubRestClient(buildPlanResponse());
        StarRocksMetadataCache cache = new StarRocksMetadataCache(
                buildContext(),
                config,
                jdbcClient,
                restClient,
                FIXED_CLOCK,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE);

        List<String> databases = cache.listDatabases();
        Assertions.assertEquals(ImmutableList.of("db1"), databases);
        // cache hit
        Assertions.assertEquals(ImmutableList.of("db1"), cache.listDatabases());
        Assertions.assertEquals(1, jdbcClient.listDbCalls);

        List<String> tables = cache.listTables("db1");
        Assertions.assertEquals(ImmutableList.of("tbl"), tables);
        // cache hit
        Assertions.assertEquals(ImmutableList.of("tbl"), cache.listTables("db1"));
        Assertions.assertEquals(1, jdbcClient.listTableCalls);

        StarRocksExternalTable table = cache.getTable("db1", "tbl");
        Assertions.assertEquals("sr_catalog", table.getCatalogName());
        Assertions.assertEquals("db1", table.getCatalogDBName());
        Assertions.assertEquals("tbl", table.getCatalogTableName());
        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertEquals("col_int", table.getBaseSchema().get(0).getName());
        Assertions.assertEquals(Type.INT, table.getBaseSchema().get(0).getType());
        Assertions.assertEquals("col_varchar", table.getBaseSchema().get(1).getName());
        Assertions.assertTrue(table.getOpaquedQueryPlan().length() > 0);
        Assertions.assertEquals(1, table.getTablets().size());
        StarRocksExternalTable.Tablet tablet = table.getTablets().values().iterator().next();
        Assertions.assertEquals(12345L, tablet.getTabletId());
        Assertions.assertEquals(77L, tablet.getVersion());
        Assertions.assertEquals(987, tablet.getSchemaHash());
        Assertions.assertEquals(ImmutableList.of("be1:9060", "be2:9060"), tablet.getEndpoints());
        Assertions.assertEquals("http://127.0.0.1:8030",
                table.getExecutionProperties().get("fe_http_urls"));
        Assertions.assertEquals("jdbc:mysql://127.0.0.1:9030",
                table.getExecutionProperties().get("fe_jdbc_url"));
        Assertions.assertEquals("test_user", table.getExecutionProperties().get("user"));
        Assertions.assertEquals("127.0.0.1:9060",
                table.getExecutionProperties().get("be_rpc_endpoints"));

        // cache hit for table metadata
        StarRocksExternalTable cachedAgain = cache.getTable("db1", "tbl");
        Assertions.assertSame(table, cachedAgain);
        Assertions.assertEquals(1, restClient.calls);
    }

    @Test
    public void testConnectorMetadataDelegatesToCache() {
        StarRocksConnectorConfig config = buildConfig();
        CountingJdbcClient jdbcClient = new CountingJdbcClient();
        StubRestClient restClient = new StubRestClient(buildPlanResponse());
        StarRocksMetadataCache cache = new StarRocksMetadataCache(
                buildContext(),
                config,
                jdbcClient,
                restClient,
                FIXED_CLOCK,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE);

        StarRocksConnectorMetadata metadata =
                new StarRocksConnectorMetadata(buildContext(), (ctx, cfg) -> cache);
        metadata.bindConfig(config);

        Assertions.assertEquals(ImmutableList.of("db1"), metadata.listDbNames(null));
        Assertions.assertEquals(ImmutableList.of("tbl"), metadata.listTableNames(null, "db1"));
        Assertions.assertNotNull(metadata.getTable(null, "db1", "tbl"));
        Assertions.assertNull(metadata.getTable(null, "db1", "missing"));

        metadata.close();
    }

    private static ConnectorContext buildContext() {
        return new ConnectorContext("sr_catalog", "starrocks", ImmutableMap.of());
    }

    private static StarRocksConnectorConfig buildConfig() {
        StarRocksConnectorConfig config = new StarRocksConnectorConfig();
        config.loadConfig(ImmutableMap.<String, String>builder()
                .put(StarRocksConnectorConfig.KEY_FE_HTTP_URL, "http://127.0.0.1:8030")
                .put(StarRocksConnectorConfig.KEY_FE_JDBC_URL, "jdbc:mysql://127.0.0.1:9030")
                .put(StarRocksConnectorConfig.KEY_USER, "test_user")
                .put(StarRocksConnectorConfig.KEY_PASSWORD, "")
                .put(StarRocksConnectorConfig.KEY_BE_RPC_ENDPOINTS, "127.0.0.1:9060")
                .build());
        return config;
    }

    private static StarRocksRestClient.QueryPlanResponse buildPlanResponse() {
        try {
            List<ColumnSpec> columns = ImmutableList.of(
                    new ColumnSpec("col_int", Type.INT),
                    new ColumnSpec("col_varchar", ScalarType.createVarcharType(20))
            );
            Map<Long, TTabletVersionInfo> tabletInfo = ImmutableMap.of(
                    12345L, new TTabletVersionInfo(12345L, 77L, 0L, 987)
            );
            String encodedPlan = encodePlan(columns, tabletInfo);
            Map<Long, StarRocksRestClient.QueryPlanResponse.TabletRouting> routing = ImmutableMap.of(
                    12345L, new StarRocksRestClient.QueryPlanResponse.TabletRouting(
                            ImmutableList.of("be1:9060", "be2:9060"), 77L, 987)
            );
            return new StarRocksRestClient.QueryPlanResponse(encodedPlan, routing);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    private static String encodePlan(List<ColumnSpec> columns,
                                     Map<Long, TTabletVersionInfo> tabletInfo) throws TException {
        List<TSlotDescriptor> slotDescriptors = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec column = columns.get(i);
            TSlotDescriptor slot = new TSlotDescriptor();
            slot.setId(i);
            slot.setParent(0);
            slot.setSlotIdx(i);
            slot.setColumnPos(i);
            slot.setSlotType(column.type.toThrift());
            slot.setColName(column.name);
            slot.setIsMaterialized(true);
            slot.setIsOutputColumn(true);
            slot.setIsNullable(true);
            slot.setCol_unique_id(i + 1);
            slotDescriptors.add(slot);
        }
        TTupleDescriptor tuple = new TTupleDescriptor();
        tuple.setId(0);
        tuple.setTableId(999);

        TDescriptorTable descriptorTable = new TDescriptorTable();
        descriptorTable.setSlotDescriptors(slotDescriptors);
        descriptorTable.setTupleDescriptors(ImmutableList.of(tuple));

        TQueryPlanInfo queryPlanInfo = new TQueryPlanInfo();
        queryPlanInfo.setDesc_tbl(descriptorTable);
        queryPlanInfo.setOutput_names(columns.stream().map(c -> c.name).collect(ImmutableList.toImmutableList()));
        queryPlanInfo.setTablet_info(tabletInfo);

        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer();
        byte[] serialized = serializer.serialize(queryPlanInfo);
        return Base64.getEncoder().encodeToString(serialized);
    }

    private static class ColumnSpec {
        private final String name;
        private final Type type;

        private ColumnSpec(String name, Type type) {
            this.name = name;
            this.type = type;
        }
    }

    private static class CountingJdbcClient implements StarRocksJdbcClient {
        int listDbCalls = 0;
        int listTableCalls = 0;

        @Override
        public List<String> listDatabases() {
            listDbCalls++;
            return ImmutableList.of("db1");
        }

        @Override
        public List<String> listTables(String dbName) {
            listTableCalls++;
            if (!"db1".equals(dbName)) {
                throw new StarRocksConnectorException(String.format(Locale.ROOT,
                        "Unknown database %s", dbName));
            }
            return ImmutableList.of("tbl");
        }

        @Override
        public void close() {
        }
    }

    private static class StubRestClient implements StarRocksRestClient {
        private final StarRocksRestClient.QueryPlanResponse response;
        int calls = 0;

        private StubRestClient(StarRocksRestClient.QueryPlanResponse response) {
            this.response = response;
        }

        @Override
        public StarRocksRestClient.QueryPlanResponse getQueryPlan(String dbName, String tableName, String sql) {
            calls++;
            if (!"db1".equals(dbName) || !"tbl".equals(tableName)) {
                throw new StarRocksConnectorException(
                        String.format(Locale.ROOT, "Unknown table %s.%s", dbName, tableName));
            }
            return response;
        }

        @Override
        public void close() {
        }
    }
}
