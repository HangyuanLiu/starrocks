// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Mocked;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OlapScanImplementationRuleTest {

    @Test
    public void transform(@Mocked OlapTable table) {
        Map<Long, List<Long>> selectedTablets = new HashMap<>();
        selectedTablets.put(1L, Lists.newArrayList(4L));
        selectedTablets.put(2L, new ArrayList<>());
        selectedTablets.put(3L, new ArrayList<>());

        LogicalOlapScanOperator logical = new LogicalOlapScanOperator(table, Maps.newHashMap(), Maps.newHashMap(),
                null, -1, ConstantOperator.createBoolean(true),
                1, null,
                selectedTablets, null);

        List<OptExpression> output =
                new OlapScanImplementationRule().transform(new OptExpression(logical), new OptimizerContext(
                        new Memo(), new ColumnRefFactory()));

        assertEquals(1, output.size());

        PhysicalOlapScanOperator physical = (PhysicalOlapScanOperator) output.get(0).getOp();
        assertEquals(1, physical.getSelectedIndexId());

        assertEquals(3, physical.getSelectedPartitionId().size());
        assertEquals(1, physical.getSelectedTabletId().size());
        assertEquals(ConstantOperator.createBoolean(true), physical.getPredicate());
    }

}