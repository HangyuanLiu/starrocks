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

package com.starrocks.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.MVPartitionCellBuilder;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.MethodName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@TestMethodOrder(MethodName.class)
public class PartitionBasedMvRefreshProcessorIcebergTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static void triggerRefreshMv(Database testDb, MaterializedView partitionedMaterializedView)
                throws Exception {
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
    }

    @Test
    public void testRefreshExternalTablePreciseFallsBackToWholeTable() throws Exception {
        String mvName = "iceberg_precise_mv";
        boolean originalConfig = Config.enable_materialized_view_external_table_precise_refresh;
        List<List<String>> calls = Lists.newArrayList();
        List<Boolean> onlyCachedPartitions = Lists.newArrayList();
        Config.enable_materialized_view_external_table_precise_refresh = true;
        try {
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`" + mvName + "`\n" +
                            "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"partition_refresh_number\" = \"1\"\n" +
                            ")\n" +
                            "AS SELECT id, data, date FROM `iceberg0`.`partitioned_db`.`t1` as a;");

            new MockUp<MetadataMgr>() {
                @Mock
                public void refreshTable(String catalogName, String srDbName, Table table,
                                         List<String> partitionNames, boolean onlyCached) {
                    if (table.isIcebergTable()) {
                        calls.add(Lists.newArrayList(partitionNames));
                        onlyCachedPartitions.add(onlyCached);
                    }
                }
            };

            starRocksAssert.refreshMvPartition("refresh materialized view " + mvName + " partition " +
                    "start('2020-01-01') end('2020-01-03')");
            Assertions.assertFalse(calls.isEmpty());
            Assertions.assertTrue(calls.stream().allMatch(List::isEmpty));
            Assertions.assertTrue(onlyCachedPartitions.stream().allMatch(value -> !value));
        } finally {
            Config.enable_materialized_view_external_table_precise_refresh = originalConfig;
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreateNonPartitionedMVForIceberg() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_mv1` " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0` as a;")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_mv2` " +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        // Partitioned base table
        {
            String mvName = "iceberg_mv2";
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
            refreshMVRange(mvName, true);
            List<String> partitionNames = mv.getPartitions().stream().map(Partition::getName)
                        .sorted().collect(Collectors.toList());
            Assertions.assertEquals(ImmutableList.of(mvName), partitionNames);
            String querySql = "SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1`";
            starRocksAssert.query(querySql).explainContains(mvName);
            starRocksAssert.dropMaterializedView(mvName);
        }

        // Non-Partitioned base table
        {
            String mvName = "iceberg_mv1";
            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
            refreshMVRange(mvName, true);
            List<String> partitionNames = mv.getPartitions().stream().map(Partition::getName)
                        .sorted().collect(Collectors.toList());
            Assertions.assertEquals(ImmutableList.of(mvName), partitionNames);

            // test rewrite
            String querySql = "SELECT id, data, date  FROM `iceberg0`.`unpartitioned_db`.`t0`";
            starRocksAssert.query(querySql).explainContains(mvName);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIceberg() throws Exception {
        String mvName = "iceberg_parttbl_mv1";
        starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_parttbl_mv1`\n" +
                                "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), "iceberg_parttbl_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(4, partitions.size());

        MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                                getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_db", "t1",
                    ImmutableList.of("date=2020-01-02"));
        // refresh only one partition
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "3: date >= '2020-01-02', 3: date < '2020-01-03'");

        Map<String, Long> partitionVersionMap = new HashMap<>();
        for (Partition p : partitionedMaterializedView.getPartitions()) {
            partitionVersionMap.put(p.getName(), p.getDefaultPhysicalPartition().getVisibleVersion());
        }

        Assertions.assertEquals(
                    ImmutableMap.of("p20200104_20200105", 2L,
                                "p20200101_20200102", 2L,
                                "p20200103_20200104", 2L,
                                "p20200102_20200103", 3L),
                    ImmutableMap.copyOf(partitionVersionMap));

        // add new row and refresh again
        mockIcebergMetadata.updatePartitions("partitioned_db", "t1",
                    ImmutableList.of("date=2020-01-01"));
        taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        processor = getPartitionBasedRefreshProcessor(taskRun);

        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "3: date >= '2020-01-01', 3: date < '2020-01-02'");

        // test rewrite
        starRocksAssert.query("SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1`")
                    .explainContains(mvName);
        starRocksAssert.query("SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` where date = '2020-01-01'")
                    .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform1() throws Exception {
        // test partition by year(ts)
        String mvName = "iceberg_year_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_year_mv1`\n" +
                        "PARTITION BY date_trunc('year', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_year_mv1"));
        Assertions.assertTrue(partitionedMaterializedView.getPartitionInfo().isRangePartition());
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(5, partitions.size());
        Set<String> expectedPartitionNames = ImmutableSet.of("p2019_2020", "p2021_2022", "p2022_2023",
                "p2020_2021", "p2023_2024");
        Assertions.assertEquals(expectedPartitionNames,
                partitions.stream().map(Partition::getName).collect(Collectors.toSet()));

        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_year",
                ImmutableList.of("ts_year=2020"));
        // refresh only one partition
        Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        assertPlanContains(execPlan, "3: ts >= '2020-01-01 00:00:00', 3: ts < '2021-01-01 00:00:00'");

        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform2() throws Exception {
        // test partition by month(ts)
        String mvName = "iceberg_month_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_month_mv1`\n" +
                        "PARTITION BY date_trunc('month', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_month_mv1"));
        Assertions.assertTrue(partitionedMaterializedView.getPartitionInfo().isRangePartition());
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(5, partitions.size());

        Set<String> expectedPartitionNames = ImmutableSet.of("p202203_202204", "p202201_202202", "p202204_202205",
                "p202202_202203", "p202205_202206");
        Assertions.assertEquals(expectedPartitionNames,
                partitions.stream().map(Partition::getName).collect(Collectors.toSet()));

        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform3() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_mv1`\n" +
                        "PARTITION BY date_trunc('day', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(5, partitions.size());
        Set<String> expectedPartitionNames = ImmutableSet.of("p20220102_20220103", "p20220103_20220104", "p20220105_20220106",
                "p20220101_20220102", "p20220104_20220105");
        Assertions.assertEquals(expectedPartitionNames,
                partitions.stream().map(Partition::getName).collect(Collectors.toSet()));
        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransform4() throws Exception {
        // test partition by hour(ts)
        String mvName = "iceberg_hour_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_hour_mv1`\n" +
                        "PARTITION BY date_trunc('hour', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"storage_medium\" = \"HDD\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_hour` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_hour_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(5, partitions.size());
        Set<String> expectedPartitionNames = ImmutableSet.of("p2022010102_2022010103",
                "p2022010104_2022010105", "p2022010103_2022010104",
                "p2022010101_2022010102", "p2022010100_2022010101");
        Assertions.assertEquals(expectedPartitionNames,
                partitions.stream().map(Partition::getName).collect(Collectors.toSet()));
        // test rewrite
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_hour`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testRefreshWithCachePartitionTraits() {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test_mv1`\n" +
                                "PARTITION BY str2date(`date`, '%Y-%m-%d')\n" +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                                "REFRESH DEFERRED MANUAL\n" +
                                "AS SELECT id, data, date  FROM `iceberg0`.`partitioned_db`.`t1` as a;",
                    () -> {
                        UtFrameUtils.mockEnableQueryContextCache();
                        MaterializedView mv = getMv("test", "test_mv1");
                        MVTaskRunProcessor mvTaskRunProcessor = getMVTaskRunProcessor("test", mv);
                        RuntimeProfile runtimeProfile = mvTaskRunProcessor.getRuntimeProfile();
                        QueryMaterializationContext.QueryCacheStats queryCacheStats = getQueryCacheStats(runtimeProfile);
                        Assertions.assertTrue(queryCacheStats != null);
                        queryCacheStats.getCounter().forEach((key, value) -> {
                            if (key.contains("cache_partitionNames_")) {
                                // After removing getPartitionKeyRange from CachedPartitionTraits,
                                // MVPartitionCellBuilder calls getPartitionNames directly each time,
                                // increasing cache hits while the actual remote call remains cached.
                                Assertions.assertTrue(value.longValue() >= 2L);
                            } else if (key.contains("cache_getPartitionNameWithPartitionInfo_")) {
                                Assertions.assertEquals(1L, value.longValue());
                            } else if (key.contains("cache_getUpdatedPartitionNames_")) {
                                Assertions.assertTrue(value.longValue() >= 1L);
                            }
                        });
                        Set<String> partitionsToRefresh1 = getPartitionNamesToRefreshForMv(mv);
                        Assertions.assertTrue(partitionsToRefresh1.isEmpty());
                    });
    }

    private void testCreateMVWithMultiPartitionColumns(String icebergTable,
                                                       String transform,
                                                       String updatePartitionName,
                                                       List<String> expectedPartitionNames,
                                                       String expectedExecPlan) throws Exception {
        String mvName = "test_mv1";
        try {
            String query = String.format("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.%s as a",
                    icebergTable);
            String ddl = String.format("CREATE MATERIALIZED VIEW `%s`\n" +
                    "PARTITION BY (id, data, date_trunc('%s', ts))\n" +
                    "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                    "REFRESH DEFERRED MANUAL\n" +
                    "AS %s;", mvName, transform, query);
            starRocksAssert.useDatabase("test").withMaterializedView(ddl);

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView partitionedMaterializedView =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), mvName));
            triggerRefreshMv(testDb, partitionedMaterializedView);

            Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
            Assertions.assertEquals(expectedPartitionNames.size(), partitions.size());
            List<String> partitionNames = partitions.stream().map(Partition::getName).collect(Collectors.toList());
            Assertions.assertTrue(partitionNames.stream().allMatch(expectedPartitionNames::contains));

            // update partition
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", icebergTable,
                    ImmutableList.of(updatePartitionName));

            // refresh only one partition
            Task task = TaskBuilder.buildMvTask(partitionedMaterializedView, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            assertPlanContains(execPlan, expectedExecPlan);

            // test rewrite
            QueryDebugOptions debugOptions = new QueryDebugOptions();
            debugOptions.setEnableQueryTraceLog(true);
            connectContext.getSessionVariable().setQueryDebugOptions(debugOptions.toString());
            String plan = UtFrameUtils.getFragmentPlan(connectContext, query);
            PlanTestBase.assertContains(plan, mvName);
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                starRocksAssert.dropMaterializedView(mvName);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsHour() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_hour", "hour",
                "id=1/data=a/ts_hour=2022-01-01-00",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220101010000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', 3: ts >= '2022-01-01 00:00:00', " +
                        "3: ts < '2022-01-01 01:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsDay() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_day", "day",
                "id=1/data=a/ts_day=2022-01-01",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220102000000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', 3: ts >= '2022-01-01 00:00:00', " +
                        "3: ts < '2022-01-02 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsMonth() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_month", "month",
                "id=1/data=a/ts_month=2022-01",
                ImmutableList.of("p1_a_20220101000000", "p2_a_20220201000000"),
                "PREDICATES: 1: id = 1, 2: data = 'a', 3: ts >= '2022-01-01 00:00:00', " +
                        "3: ts < '2022-02-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsYear() throws Exception {
        testCreateMVWithMultiPartitionColumns("t0_multi_year", "year",
                "id=2/data=a/ts_year=2024", ImmutableList.of("p1_a_20240101000000", "p2_a_20240101000000"),
                "PREDICATES: 1: id = 2, 2: data = 'a', 3: ts >= '2024-01-01 00:00:00', " +
                        "3: ts < '2025-01-01 00:00:00'");
    }

    @Test
    public void testCreatePartitionedMVWithMultiPartitionColumnsBucket() {
        try {
            testCreateMVWithMultiPartitionColumns("t0_multi_bucket", "bucket",
                    "id=1/data=a/ts_bucket=0",
                    ImmutableList.of("p1_a_20240101000000", "p2_a_20240101000000"),
                    "3: ts >= '2024-01-01 00:00:00', 3: ts < '2025-01-01 00:00:00'");
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Unsupported expr 'date_trunc('bucket', ts)' in PARTITION BY clause"));
        }
    }

    @Test
    public void testCreateMVForIcebergWithRetentionCondition1() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_mv1`\n" +
                "PARTITION BY date_trunc('day', ts)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_retention_condition\" = \"date_trunc('day', ts) >= current_date() - interval 1 year\"" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView = getMv(testDb.getFullName(), "iceberg_day_mv1");
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(0, partitions.size());
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateMVForIcebergWithRetentionCondition2() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_mv1`\n" +
                "PARTITION BY (id, data, date_trunc('day', ts))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_retention_condition\" = \"date_trunc('day', ts) >= current_date() - interval 1 year\"" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(0, partitions.size());
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day`")
                .explainWithout(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateMVForIcebergWithRetentionCondition3() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_tz_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_tz_mv1`\n" +
                "PARTITION BY (id, data, date_trunc('day', ts))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_retention_condition\" = \"date_trunc('day', ts) >= current_date() - interval 1 year\"" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day_tz` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_tz_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(0, partitions.size());
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day_tz`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateMVForIcebergWithRetentionCondition4() throws Exception {
        // test partition by day(ts)
        String mvName = "iceberg_day_tz_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_tz_mv1`\n" +
                "PARTITION BY (id, data, date_trunc('day', ts))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day_tz` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_tz_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);
        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(2, partitions.size());
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day_tz`")
                .explainContains(mvName);
        String alterTableSql = String.format("alter materialized view %s set (" +
                "\"partition_retention_condition\" = \"date_trunc('day', ts) >= current_date() - interval 1 year\")",
                mvName);
        starRocksAssert.alterMvProperties(alterTableSql);
        triggerRefreshMv(testDb, partitionedMaterializedView);

        // trigger ttl
        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        scheduler.runOnceForTest();

        partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(0, partitions.size());
        FeConstants.enablePruneEmptyOutputScan = true;
        starRocksAssert.query("SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_multi_day_tz`")
                .explainWithout(mvName);
        starRocksAssert.dropMaterializedView(mvName);
        FeConstants.enablePruneEmptyOutputScan = false;
    }

    @Test
    public void testCreateMVForIcebergWithRetentionCondition5() throws Exception {
        String mvName = "iceberg_day_mv1";
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_mv1`\n" +
                "PARTITION BY dt\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_retention_condition\" = \"dt >= current_date() - interval 10 year\"" +
                ")\n" +
                "AS SELECT count(1), date_trunc('day', ts) as dt " +
                "FROM `iceberg0`.`partitioned_transforms_db`.`t0_day_with_null_partition` as a " +
                "group by date_trunc('day', ts);");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView partitionedMaterializedView =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), "iceberg_day_mv1"));
        triggerRefreshMv(testDb, partitionedMaterializedView);

        Collection<Partition> partitions = partitionedMaterializedView.getPartitions();
        Assertions.assertEquals(5, partitions.size());
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransformMonthTZ() throws Exception {
        // test MONTH transform with TimestampType.withZone() — the original reason for LIST partition.
        // Verify RANGE partition handles timezone correctly: partition boundaries should be shifted
        // from UTC to session timezone.
        String mvName = "iceberg_month_tz_mv1";
        String prevTZ = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_month_tz_mv1`\n" +
                            "PARTITION BY date_trunc('month', ts)\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_month_tz` as a;");

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), "iceberg_month_tz_mv1"));
            // Must be RANGE, not LIST
            Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
            triggerRefreshMv(testDb, mv);

            Collection<Partition> partitions = mv.getPartitions();
            // Timezone offset may cause partition ranges to span an extra boundary,
            // resulting in 6 partitions from 5 Iceberg partitions. This is expected.
            Assertions.assertTrue(partitions.size() >= 5);

            // Verify incremental refresh predicates contain timezone-adjusted boundaries.
            // For Asia/Shanghai (UTC+8), Iceberg "ts_month=2022-01" (UTC January) should produce:
            //   lower bound = 2022-01-01 08:00:00 (CST), upper bound = 2022-02-01 08:00:00 (CST)
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_month_tz",
                    ImmutableList.of("ts_month=2022-01"));

            Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            // UTC+8: partition boundaries are shifted by +8 hours from UTC midnight.
            // The refresh may scan the full range since timezone-shifted ranges don't align
            // cleanly with MV partition boundaries. The key assertion is that the boundaries
            // contain the correct timezone offset (08:00:00 for Asia/Shanghai).
            assertPlanContains(execPlan, "3: ts >= '2022-01-01 08:00:00'");
            assertPlanContains(execPlan, "08:00:00'");

            starRocksAssert.query(
                            "SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`.`t0_month_tz`")
                    .explainContains(mvName);
        } finally {
            connectContext.getSessionVariable().setTimeZone(prevTZ);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransformDayTZ() throws Exception {
        // test DAY transform with TimestampType.withZone()
        String mvName = "iceberg_day_tz_mv1";
        String prevTZ = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_day_tz_mv1`\n" +
                            "PARTITION BY date_trunc('day', ts)\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_day_tz` as a;");

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), "iceberg_day_tz_mv1"));
            Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
            triggerRefreshMv(testDb, mv);

            Collection<Partition> partitions = mv.getPartitions();
            Assertions.assertTrue(partitions.size() >= 5);

            // Verify incremental refresh with timezone-shifted boundaries
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_day_tz",
                    ImmutableList.of("ts_day=2022-01-02"));

            Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            // UTC+8: partition boundaries shifted by +8h. Verify timezone offset is present.
            assertPlanContains(execPlan, "3: ts >= '2022-01-01 08:00:00'");
            assertPlanContains(execPlan, "08:00:00'");
        } finally {
            connectContext.getSessionVariable().setTimeZone(prevTZ);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransformHourTZ() throws Exception {
        // test HOUR transform with TimestampType.withZone()
        String mvName = "iceberg_hour_tz_mv1";
        String prevTZ = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_hour_tz_mv1`\n" +
                            "PARTITION BY date_trunc('hour', ts)\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_hour_tz` as a;");

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), "iceberg_hour_tz_mv1"));
            Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
            triggerRefreshMv(testDb, mv);

            Collection<Partition> partitions = mv.getPartitions();
            Assertions.assertTrue(partitions.size() >= 5);

            // Verify incremental refresh with timezone-shifted boundaries
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_hour_tz",
                    ImmutableList.of("ts_hour=2022-01-01-02"));

            Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            // UTC+8: 2022-01-01 02:00 UTC -> 2022-01-01 10:00:00 CST
            // HOUR granularity achieves precise single-partition refresh with correct timezone offset
            assertPlanContains(execPlan,
                    "3: ts >= '2022-01-01 10:00:00', 3: ts < '2022-01-01 11:00:00'");
        } finally {
            connectContext.getSessionVariable().setTimeZone(prevTZ);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIcebergWithPartitionTransformYearTZ() throws Exception {
        // test YEAR transform with TimestampType.withZone()
        String mvName = "iceberg_year_tz_mv1";
        String prevTZ = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("Asia/Shanghai");
            starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_year_tz_mv1`\n" +
                            "PARTITION BY date_trunc('year', ts)\n" +
                            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                            "REFRESH DEFERRED MANUAL\n" +
                            "PROPERTIES (\n" +
                            "\"replication_num\" = \"1\",\n" +
                            "\"storage_medium\" = \"HDD\"\n" +
                            ")\n" +
                            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year_tz` as a;");

            Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            MaterializedView mv =
                    ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                            .getTable(testDb.getFullName(), "iceberg_year_tz_mv1"));
            Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
            triggerRefreshMv(testDb, mv);

            Collection<Partition> partitions = mv.getPartitions();
            Assertions.assertTrue(partitions.size() >= 5);

            // Verify incremental refresh with timezone-shifted boundaries
            MockIcebergMetadata mockIcebergMetadata =
                    (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                            getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
            mockIcebergMetadata.updatePartitions("partitioned_transforms_db", "t0_year_tz",
                    ImmutableList.of("ts_year=2022"));

            Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
            TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
            initAndExecuteTaskRun(taskRun);
            MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

            MvTaskRunContext mvContext = processor.getMvContext();
            ExecPlan execPlan = mvContext.getExecPlan();
            // UTC+8: partition boundaries shifted by +8h. Verify timezone offset is present.
            assertPlanContains(execPlan, "3: ts >= '2019-01-01 08:00:00'");
            assertPlanContains(execPlan, "08:00:00'");
        } finally {
            connectContext.getSessionVariable().setTimeZone(prevTZ);
            starRocksAssert.dropMaterializedView(mvName);
        }
    }

    @Test
    public void testCreatePartitionedMVForIcebergMonthToDayEvolution() throws Exception {
        // T2-2: MONTH→DAY evolution with per-spec interval.
        // Spec 0 (MONTH): ts_month=2024-01, ts_month=2024-02
        // Spec 1 (DAY):   ts_day=2024-03-01, ts_day=2024-03-02, ts_day=2024-03-03
        // MV partitioned by date_trunc('day', ts) should create and refresh successfully.
        String mvName = "iceberg_month_to_day_mv1";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_month_to_day_mv1`\n" +
                        "PARTITION BY date_trunc('day', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`." +
                        "`t0_month_to_day_evolution` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv =
                ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(testDb.getFullName(), mvName));
        Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
        Set<String> expectedPartitionNames = ImmutableSet.of(
                "p20240101_20240201",
                "p20240201_20240301",
                "p20240301_20240302",
                "p20240302_20240303",
                "p20240303_20240304");

        Table baseTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext,
                MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME,
                "partitioned_transforms_db",
                "t0_month_to_day_evolution");
        PCellSortedSet basePartitionCells = MVPartitionCellBuilder.getPartitionKeyRange(
                baseTable, baseTable.getColumn("ts"), mv.getRangePartitionFirstExpr().get());
        Assertions.assertEquals(expectedPartitionNames, basePartitionCells.getPartitionNames());

        triggerRefreshMv(testDb, mv);

        Collection<Partition> partitions = mv.getPartitions();
        Assertions.assertEquals(5, partitions.size());
        Assertions.assertEquals(expectedPartitionNames,
                partitions.stream().map(Partition::getName).collect(Collectors.toSet()));

        // Verify incremental refresh: update a DAY partition
        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        mockIcebergMetadata.updatePartitions("partitioned_transforms_db",
                "t0_month_to_day_evolution",
                ImmutableList.of("ts_day=2024-03-01"));

        Task task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        MVPCTBasedRefreshProcessor processor = getPartitionBasedRefreshProcessor(taskRun);

        MvTaskRunContext mvContext = processor.getMvContext();
        ExecPlan execPlan = mvContext.getExecPlan();
        // The DAY partition should use DAY interval: [2024-03-01, 2024-03-02)
        assertPlanContains(execPlan,
                "3: ts >= '2024-03-01 00:00:00', 3: ts < '2024-03-02 00:00:00'");

        // Verify incremental refresh: update a MONTH partition
        mockIcebergMetadata.updatePartitions("partitioned_transforms_db",
                "t0_month_to_day_evolution",
                ImmutableList.of("ts_month=2024-01"));

        task = TaskBuilder.buildMvTask(mv, testDb.getFullName());
        taskRun = TaskRunBuilder.newBuilder(task).build();
        initAndExecuteTaskRun(taskRun);
        processor = getPartitionBasedRefreshProcessor(taskRun);

        mvContext = processor.getMvContext();
        execPlan = mvContext.getExecPlan();
        // The MONTH partition should use MONTH interval: [2024-01-01, 2024-02-01)
        assertPlanContains(execPlan,
                "3: ts >= '2024-01-01 00:00:00', 3: ts < '2024-02-01 00:00:00'");

        // test rewrite
        starRocksAssert.query("SELECT id, data, ts FROM " +
                        "`iceberg0`.`partitioned_transforms_db`.`t0_month_to_day_evolution`")
                .explainContains(mvName);
        starRocksAssert.dropMaterializedView(mvName);
    }

    @Test
    public void testCreateMvRejectsUnsafeIcebergMonthToTruncateEvolution() {
        Exception exception = Assertions.assertThrows(Exception.class, () -> starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`iceberg_unsafe_evolution_mv`\n" +
                        "PARTITION BY date_trunc('month', ts)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
                        "`t0_month_to_truncate_evolution` as a;"));
        Assertions.assertTrue(exception.getMessage().contains(
                "Do not support create materialized view when base iceberg table has partition evolution"));
    }

    @Test
    public void testRefreshMvWithIcebergBucketCurrentSpecAlignmentFallback() throws Exception {
        String mvName = "iceberg_bucket32_mv";
        starRocksAssert.useDatabase("test")
                .withMaterializedView("CREATE MATERIALIZED VIEW `test`.`" + mvName + "`\n" +
                        "PARTITION BY __iceberg_transform_bucket(id, 32)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "REFRESH DEFERRED MANUAL\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")\n" +
                        "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
                        "`t0_bucket16_to_bucket32_evolution` as a;");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = ((MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName));
        Assertions.assertTrue(mv.getPartitionInfo().isListPartition());

        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();
        IcebergTable baseTable = (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext,
                MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME,
                "partitioned_transforms_db",
                "t0_bucket16_to_bucket32_evolution");
        int historicalSpecId = baseTable.getNativeTable().specs().keySet().stream()
                .min(Integer::compareTo)
                .orElseThrow();
        mockIcebergMetadata.addRowsToPartitionWithSyntheticValues(
                "partitioned_transforms_db",
                "t0_bucket16_to_bucket32_evolution",
                1,
                "id_bucket=1",
                historicalSpecId,
                ImmutableList.of(1, 17));
        mockIcebergMetadata.addRowsToPartitionWithSyntheticValues(
                "partitioned_transforms_db",
                "t0_bucket16_to_bucket32_evolution",
                1,
                "id_bucket=2",
                historicalSpecId,
                ImmutableList.of(2, 18));
        mockIcebergMetadata.addRowsToPartitionWithSyntheticValues(
                "partitioned_transforms_db",
                "t0_bucket16_to_bucket32_evolution",
                1,
                "id_bucket=3",
                historicalSpecId,
                ImmutableList.of(3, 19));

        Set<String> expectedPartitionNames = ImmutableSet.of("p1", "p17", "p2", "p18", "p3", "p19");
        PCellSortedSet basePartitionCells =
                MVPartitionCellBuilder.getPartitionCells(baseTable, ImmutableList.of(baseTable.getColumn("id")));
        Assertions.assertEquals(expectedPartitionNames, basePartitionCells.getPartitionNames());

        triggerRefreshMv(testDb, mv);
        Assertions.assertEquals(expectedPartitionNames,
                mv.getPartitions().stream().map(Partition::getName).collect(Collectors.toSet()));

        starRocksAssert.dropMaterializedView(mvName);
    }
}
