// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/mvrewrite/CountDistinctToBitmap.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.rewrite.mvrewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.rewrite.ExprRewriteRule;

import java.util.List;

/**
 * For duplicate table, the count distinct(k1) could be rewritten to bitmap_union_count(mv_to_bitmap_k1) when bitmap
 * mv exists.
 * For example:
 * Table: (k1 int, k2 int)
 * MV: (k1 int, mv_bitmap_union_k2 bitmap bitmap_union)
 * mv_to_bitmap_k2 = to_bitmap(k2)
 * Query: select k1, count(distinct k2) from table group by k1
 * Rewritten query: select k1, bitmap_union_count(mv_to_bitmap_k2) from table group by k1
 */
public class CountDistinctToBitmap implements ExprRewriteRule {

    public static final ExprRewriteRule INSTANCE = new CountDistinctToBitmap();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        // meet condition
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (!fnExpr.getParams().isDistinct()) {
            return expr;
        }
        if (!fnExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            return expr;
        }
        if (fnExpr.getChildren().size() != 1 || !(fnExpr.getChild(0) instanceof SlotRef)) {
            return expr;
        }
        SlotRef fnChild0 = (SlotRef) fnExpr.getChild(0);
        Column column = fnChild0.getColumn();
        Table table = fnChild0.getTable();
        if (column == null || table == null || !(table instanceof OlapTable)) {
            return expr;
        }
        OlapTable olapTable = (OlapTable) table;

        // check column
        String queryColumnName = column.getName();
        String mvColumnName = CreateMaterializedViewStmt
                .mvColumnBuilder(AggregateType.BITMAP_UNION.name().toLowerCase(), queryColumnName);
        Column mvColumn = olapTable.getVisibleColumn(mvColumnName);
        if (mvColumn == null) {
            return expr;
        }

        // rewrite expr
        return rewriteExpr(fnChild0, mvColumn, analyzer);
    }

    private Expr rewriteExpr(SlotRef queryColumnSlotRef, Column mvColumn, Analyzer analyzer) {
        Preconditions.checkNotNull(mvColumn);
        Preconditions.checkNotNull(queryColumnSlotRef);
        TableName tableName = queryColumnSlotRef.getTableName();
        Preconditions.checkNotNull(tableName);
        SlotRef mvSlotRef = new SlotRef(tableName, mvColumn.getName());
        List<Expr> newFnParams = Lists.newArrayList();
        newFnParams.add(mvSlotRef);
        FunctionCallExpr result = new FunctionCallExpr("bitmap_union_count", newFnParams);
        result.analyzeNoThrow(analyzer);
        return result;
    }
}
