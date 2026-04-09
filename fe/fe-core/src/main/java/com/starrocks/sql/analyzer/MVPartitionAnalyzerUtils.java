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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Shared utility methods for MV partition analysis, used by both CREATE MV and ALTER MV flows.
 */
public class MVPartitionAnalyzerUtils {

    /**
     * Determine MV partition type based on partition expressions and base table info.
     */
    public static PartitionType determineMVPartitionType(
            MaterializedView mv,
            List<Expr> mvPartitionByExprs,
            List<Expr> partitionRefTableExprs) {
        if (mvPartitionByExprs == null || mvPartitionByExprs.isEmpty()) {
            return PartitionType.UNPARTITIONED;
        }
        if (partitionRefTableExprs == null || partitionRefTableExprs.isEmpty()) {
            return PartitionType.UNPARTITIONED;
        }

        // Multiple partition expressions -> LIST
        if (partitionRefTableExprs.size() > 1) {
            return PartitionType.LIST;
        }

        Expr partitionRefTableExpr = partitionRefTableExprs.get(0);

        // Find the base table from the MV's base table infos
        Table refBaseTable = findRefBaseTable(mv, partitionRefTableExpr);

        if (refBaseTable != null && refBaseTable.isNativeTableOrMaterializedView()) {
            OlapTable refOlapTable = (OlapTable) refBaseTable;
            PartitionInfo refPartitionInfo = refOlapTable.getPartitionInfo();
            if (refPartitionInfo.isRangePartition()) {
                return PartitionType.RANGE;
            } else if (refPartitionInfo.isListPartition()) {
                return PartitionType.LIST;
            }
        } else {
            // External table logic
            if (Config.enable_mv_list_partition_for_external_table) {
                return PartitionType.LIST;
            }
            if (shouldMVPartitionByListType(mvPartitionByExprs, partitionRefTableExpr)) {
                return PartitionType.LIST;
            }
            return PartitionType.RANGE;
        }
        return PartitionType.RANGE;
    }

    /**
     * Check if MV should use LIST partition type.
     */
    public static boolean shouldMVPartitionByListType(
            List<Expr> mvPartitionByExprs,
            Expr partitionRefTableExpr) {
        // Iceberg BUCKET/TRUNCATE transforms must use LIST partition
        if (partitionRefTableExpr instanceof FunctionCallExpr) {
            String funcName = ((FunctionCallExpr) partitionRefTableExpr).getFunctionName();
            if (FunctionSet.ICEBERG_TRANSFORM_BUCKET.equalsIgnoreCase(funcName)
                    || FunctionSet.ICEBERG_TRANSFORM_TRUNCATE.equalsIgnoreCase(funcName)) {
                return true;
            }
        }
        // If all partition by exprs are simple SlotRefs (no functions) and the expression is not a function call,
        // check column type
        if (mvPartitionByExprs.stream().allMatch(t -> t instanceof SlotRef)
                && !(partitionRefTableExpr instanceof FunctionCallExpr)) {
            // For string type columns, use LIST
            for (Expr expr : mvPartitionByExprs) {
                if (expr instanceof SlotRef) {
                    Type type = expr.getType();
                    if (type != null && type.isStringType()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Build PartitionInfo from partition expressions, type, and columns.
     */
    public static PartitionInfo buildPartitionInfo(
            List<Expr> partitionByExprs,
            PartitionType partitionType,
            List<Column> mvPartitionColumns,
            Map<Integer, Column> generatedPartitionCols,
            List<Column> baseSchema) throws DdlException {
        if (partitionByExprs == null || partitionByExprs.isEmpty()) {
            return new SinglePartitionInfo();
        }

        if (partitionType == PartitionType.LIST) {
            List<Column> newPartitionColumns = new ArrayList<>();
            for (int i = 0; i < partitionByExprs.size(); i++) {
                if (generatedPartitionCols.containsKey(i)) {
                    Column generatedCol = generatedPartitionCols.get(i);
                    if (generatedCol == null) {
                        throw new DdlException("Partition expression for list must have a generated column");
                    }
                    baseSchema.add(generatedCol);
                    newPartitionColumns.add(generatedCol);
                } else {
                    newPartitionColumns.add(mvPartitionColumns.get(i));
                }
            }
            return new ListPartitionInfo(PartitionType.LIST, newPartitionColumns);
        } else {
            // RANGE partition
            if (partitionByExprs.size() > 1) {
                throw new DdlException("Only support one partition column for range partition");
            }
            Expr partitionByExpr = partitionByExprs.get(0);
            return new ExpressionRangePartitionInfo(
                    Collections.singletonList(ColumnIdExpr.create(baseSchema, partitionByExpr)),
                    mvPartitionColumns,
                    PartitionType.RANGE);
        }
    }

    /**
     * Create a generated partition column for function-based partition expressions.
     * Used for LIST partitions with BUCKET/TRUNCATE/date_trunc transforms.
     */
    public static Column createGeneratedPartitionColumn(
            Expr adjustedPartitionByExpr,
            int placeHolderSlotId,
            KeysType keysType) {
        Type type = adjustedPartitionByExpr.getType();
        if (type.isScalarType()) {
            ScalarType scalarType = (ScalarType) type;
            if (scalarType.isWildcardChar()) {
                type = TypeFactory.createCharType(TypeFactory.getOlapMaxVarcharLength());
            } else if (scalarType.isWildcardVarchar()) {
                type = TypeFactory.createVarcharType(TypeFactory.getOlapMaxVarcharLength());
            }
        }
        String columnName = FeConstants.GENERATED_PARTITION_COLUMN_PREFIX + placeHolderSlotId;
        TypeDef typeDef = new TypeDef(type);
        try {
            TypeDefAnalyzer.analyze(typeDef);
        } catch (Exception e) {
            throw new SemanticException("Generate partition column " + columnName
                    + " for multi expression partition error: " + e.getMessage());
        }
        AggregateType aggregateType = keysType == KeysType.DUP_KEYS ?
                AggregateType.NONE : AggregateType.REPLACE;
        ColumnDef generatedPartitionColumn = new ColumnDef(
                columnName, typeDef, null, false, aggregateType, null, true,
                ColumnDef.DefaultValueDef.NOT_SET, null, adjustedPartitionByExpr, "");
        return ColumnBuilder.buildGeneratedColumn(null, generatedPartitionColumn);
    }

    /**
     * Find the reference base table from the partition expression's slot ref.
     */
    private static Table findRefBaseTable(MaterializedView mv, Expr partitionRefTableExpr) {
        List<SlotRef> slotRefs = new ArrayList<>();
        partitionRefTableExpr.collect(SlotRef.class, slotRefs);
        if (slotRefs.isEmpty()) {
            return null;
        }
        SlotRef slotRef = slotRefs.get(0);
        String tableName = slotRef.getTblNameWithoutAnalyzed() != null ?
                slotRef.getTblNameWithoutAnalyzed().getTbl() : null;

        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (tableOpt.isPresent()) {
                Table table = tableOpt.get();
                if (tableName == null || table.getName().equalsIgnoreCase(tableName)) {
                    if (!table.isUnPartitioned()) {
                        return table;
                    }
                }
            }
        }
        // Fallback: return the first non-unpartitioned base table
        for (BaseTableInfo baseTableInfo : mv.getBaseTableInfos()) {
            Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (tableOpt.isPresent() && !tableOpt.get().isUnPartitioned()) {
                return tableOpt.get();
            }
        }
        return null;
    }
}
