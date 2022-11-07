// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class TableNameResolvedTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCTENamedQuery() {
        analyzeSuccess("select db1.t0.v1 from db1.t0 inner join db2.t0 on db1.t0.v1 = db2.t0.v1");

        analyzeSuccess("select db1.t0.v1 from db1.t0 inner join db2.t0 t on db1.t0.v1 = t.v1");
        analyzeSuccess("select db1.t0.v1 from db1.t0 inner join db2.t0 t on db1.t0.v1 = db2.t.v1");
        analyzeFail("select db1.t0.v1 from db1.t0 inner join db2.t0 t on db1.t0.v1 = db1.t.v1",
                "Column '`db2`.`t`.`v1`' cannot be resolved");
        analyzeFail("select db1.t0.v1 from db1.t0 inner join db2.t0 t on db1.t0.v1 = db2.t0.v1",
                "Column '`db2`.`t0`.`v1`' cannot be resolved");

        analyzeFail("select db1.t0.v1 from db1.t0 t inner join db2.t0 on db1.t0.v1 = db2.t0.v1",
                "Column '`db1`.`t0`.`v1`' cannot be resolved");
    }
}
