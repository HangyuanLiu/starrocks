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

package com.starrocks.alter;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.scheduler.mv.ivm.MVIVMTestBase;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMVPartitionByClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.RemoveMVPartitionClause;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AlterMVPartitionByTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVIVMTestBase.beforeClass();
        starRocksAssert.useDatabase("test");
    }

    private static void alterMV(String sql, boolean expectedException) {
        try {
            AlterMaterializedViewStmt alterStmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            DDLStmtExecutor.execute(alterStmt, connectContext);
            if (expectedException) {
                Assertions.fail("Expected exception but none was thrown for: " + sql);
            }
        } catch (Exception e) {
            if (!expectedException) {
                Assertions.fail("Unexpected exception: " + e.getMessage() + " for: " + sql);
            }
        }
    }

    private MaterializedView getMvFromDb(String mvName) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        return (MaterializedView) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable(db.getFullName(), mvName);
    }

    @Test
    public void testRemovePartitioning() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_remove_part_tbl\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                " PARTITION p2 VALUES LESS THAN ('2020-02-01'))\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_remove_part\n" +
                "PARTITION BY date_trunc('month', k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT k1, sum(v1) as total from test_remove_part_tbl group by k1;");

        MaterializedView mv = getMvFromDb("mv_remove_part");
        Assertions.assertNotNull(mv);
        // Verify initially partitioned
        Assertions.assertFalse(mv.getPartitionInfo().isUnPartitioned());

        // Remove partitioning
        alterMV("alter materialized view mv_remove_part remove partitioning", false);

        // Verify now unpartitioned
        Assertions.assertTrue(mv.getPartitionInfo().isUnPartitioned());
        // Verify INACTIVE
        Assertions.assertFalse(mv.isActive());
        // Verify partition expression maps are cleared
        Assertions.assertTrue(mv.getPartitionExprMaps().isEmpty());
        Assertions.assertTrue(mv.getPartitionRefTableExprs().isEmpty());

        starRocksAssert.dropMaterializedView("mv_remove_part");
        starRocksAssert.dropTable("test_remove_part_tbl");
    }

    @Test
    public void testRemovePartitioningAlreadyUnpartitioned() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_unpart_tbl\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_already_unpart\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT k1, sum(v1) as total from test_unpart_tbl group by k1;");

        MaterializedView mv = getMvFromDb("mv_already_unpart");
        Assertions.assertNotNull(mv);
        Assertions.assertTrue(mv.getPartitionInfo().isUnPartitioned());

        // Should fail: already unpartitioned
        alterMV("alter materialized view mv_already_unpart remove partitioning", true);

        starRocksAssert.dropMaterializedView("mv_already_unpart");
        starRocksAssert.dropTable("test_unpart_tbl");
    }

    @Test
    public void testAlterPartitionByRangeToRange() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_alter_range_tbl\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES LESS THAN ('2020-01-01'),\n" +
                " PARTITION p2 VALUES LESS THAN ('2020-02-01'))\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_alter_range\n" +
                "PARTITION BY date_trunc('month', k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS SELECT k1, sum(v1) as total from test_alter_range_tbl group by k1;");

        MaterializedView mv = getMvFromDb("mv_alter_range");
        Assertions.assertNotNull(mv);
        PartitionInfo originalPartInfo = mv.getPartitionInfo();
        Assertions.assertTrue(originalPartInfo.isRangePartition());

        // ALTER PARTITION BY to a different range expression
        alterMV("alter materialized view mv_alter_range partition by date_trunc('day', k1)", false);

        // Verify partition info changed
        PartitionInfo newPartInfo = mv.getPartitionInfo();
        Assertions.assertTrue(newPartInfo.isRangePartition());
        // Verify INACTIVE
        Assertions.assertFalse(mv.isActive());
        // Verify all partitions dropped
        Assertions.assertTrue(mv.getPartitions().isEmpty());
        // Verify partition expr maps are set
        Assertions.assertFalse(mv.getPartitionExprMaps().isEmpty());

        starRocksAssert.dropMaterializedView("mv_alter_range");
        starRocksAssert.dropTable("test_alter_range_tbl");
    }

    @Test
    public void testAlterPartitionByEmptyExpression() throws Exception {
        // Parsing should fail for empty partition expression
        alterMV("alter materialized view mv_alter_range partition by", true);
    }

    @Test
    public void testParseAlterPartitionBy() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_parse_part_tbl\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES LESS THAN ('2020-01-01'))\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_parse_part\n" +
                "PARTITION BY date_trunc('month', k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT k1, sum(v1) as total from test_parse_part_tbl group by k1;");

        // Test that the syntax parses and analyzes correctly
        String sql = "ALTER MATERIALIZED VIEW mv_parse_part PARTITION BY date_trunc('day', k1)";
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertNotNull(stmt);
        Assertions.assertNotNull(stmt.getAlterTableClause());
        Assertions.assertInstanceOf(AlterMVPartitionByClause.class, stmt.getAlterTableClause());

        starRocksAssert.dropMaterializedView("mv_parse_part");
        starRocksAssert.dropTable("test_parse_part_tbl");
    }

    @Test
    public void testParseRemovePartitioning() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test_parse_remove_tbl\n" +
                "(\n" +
                "    k1 date,\n" +
                "    v1 int\n" +
                ")\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES LESS THAN ('2020-01-01'))\n" +
                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv_parse_remove\n" +
                "PARTITION BY date_trunc('month', k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES ('replication_num' = '1')\n" +
                "AS SELECT k1, sum(v1) as total from test_parse_remove_tbl group by k1;");

        // Test that the syntax parses and analyzes correctly
        String sql = "ALTER MATERIALIZED VIEW mv_parse_remove REMOVE PARTITIONING";
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        Assertions.assertNotNull(stmt);
        Assertions.assertNotNull(stmt.getAlterTableClause());
        Assertions.assertInstanceOf(RemoveMVPartitionClause.class, stmt.getAlterTableClause());

        starRocksAssert.dropMaterializedView("mv_parse_remove");
        starRocksAssert.dropTable("test_parse_remove_tbl");
    }
}
