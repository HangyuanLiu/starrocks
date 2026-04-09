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
package com.starrocks.connector.partitiontraits;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.MVPartitionCellBuilder;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class IcebergPartitionTraits extends DefaultTraits {
    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public String getTableName() {
        return table.getCatalogTableName();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new IcebergPartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(icebergTable.getCatalogName(), table, partitionNames);
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        IcebergTable icebergTable = (IcebergTable) table;
        return Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot()).map(Snapshot::timestampMillis);
    }

    @Override
    public List<String> getPartitionNames() {
        IcebergTable icebergTable = (IcebergTable) table;
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        if (table.isUnPartitioned() && nativeTable.specs().size() <= 1) {
            return Lists.newArrayList(table.getName());
        }

        Optional<Long> snapshotId = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot())
                .map(Snapshot::snapshotId);
        ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
        requestContext.setQueryMVRewrite(isQueryMVRewrite());
        requestContext.setTableVersionRange(TvrTableSnapshot.of(snapshotId));
        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getCatalogDBName(), getTableName(), requestContext);
    }

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> currentPartitionColumns = super.getPartitionColumns();
        if (!currentPartitionColumns.isEmpty()) {
            return currentPartitionColumns;
        }

        IcebergTable icebergTable = (IcebergTable) table;
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        if (!nativeTable.spec().isUnpartitioned() || nativeTable.specs().size() <= 1) {
            return currentPartitionColumns;
        }

        Set<String> historicalPartitionColumnNames = new LinkedHashSet<>();
        for (PartitionSpec spec : nativeTable.specs().values()) {
            for (PartitionField field : spec.fields()) {
                if (field.transform().isVoid()) {
                    continue;
                }
                String columnName = nativeTable.schema().findColumnName(field.sourceId());
                if (columnName != null) {
                    historicalPartitionColumnNames.add(columnName);
                }
            }
        }

        List<Column> historicalPartitionColumns = new ArrayList<>();
        for (String columnName : historicalPartitionColumnNames) {
            Column column = icebergTable.getColumn(columnName);
            if (column != null) {
                historicalPartitionColumns.add(column);
            }
        }
        return historicalPartitionColumns;
    }

    public PCellSortedSet getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException {
        return MVPartitionCellBuilder.getPartitionKeyRange(table, partitionColumn, partitionExpr);
    }

    @Override
    public PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException {
        Preconditions.checkState(partitionValues.size() == partitionColumns.size(),
                "columns size is %s, but values size is %s", partitionColumns.size(),
                partitionValues.size());

        IcebergTable icebergTable = (IcebergTable) table;
        List<PartitionField> partitionFields = Lists.newArrayList();
        for (Column column : partitionColumns) {
            PartitionField matched = findPartitionFieldForColumn(icebergTable, column.getName(),
                    partitionValues.size() > partitionFields.size()
                            ? partitionValues.get(partitionFields.size()) : null);
            if (matched != null) {
                partitionFields.add(matched);
            }
        }
        Preconditions.checkState(partitionFields.size() == partitionColumns.size(),
                "columns size is %s, but partitionFields size is %s", partitionColumns.size(),
                partitionFields.size());

        return IcebergPartitionUtils.createPartitionKey(icebergTable, partitionColumns, partitionValues, null);
    }

    /**
     * Find the matching PartitionField for a column across all specs.
     * For single-spec tables, uses the current spec. For evolution tables,
     * infers the correct spec by matching the partition value format to the transform.
     */
    private org.apache.iceberg.PartitionField findPartitionFieldForColumn(
            IcebergTable icebergTable, String columnName, String partitionValue) {
        return IcebergPartitionUtils.findPartitionFieldForColumn(icebergTable, columnName, partitionValue, null);
    }

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        IcebergTable icebergTable = (IcebergTable) table;
        Optional<Snapshot> snapshot = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot());
        return snapshot.map(value -> LocalDateTime.ofInstant(Instant.ofEpochMilli(value.timestampMillis()).
                plusSeconds(extraSeconds), Clock.systemDefaultZone().getZone())).orElse(null);
    }

    @Override
    public PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException {
        Preconditions.checkState(values.size() == types.size(),
                "columns size is %s, but values size is %s", types.size(), values.size());

        PartitionKey partitionKey = createEmptyKey();
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
            Type type = types.get(i);
            LiteralExpr exprValue;
            if (rawValue == null) {
                exprValue = NullLiteral.create(type);
            } else {
                exprValue = LiteralExprFactory.create(rawValue, type);
            }
            partitionKey.pushColumn(exprValue, type.getPrimitiveType());
        }

        for (int i = 0; i < types.size(); i++) {
            LiteralExpr exprValue = partitionKey.getKeys().get(i);
            if (exprValue.getType().isDecimalV3()) {
                exprValue.setType(types.get(i)); //keep the precision and scale.
            }
        }
        return partitionKey;
    }
}
