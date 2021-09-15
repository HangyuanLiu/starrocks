// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.Field;

import java.util.List;
import java.util.Map;

public class TableRelation extends Relation {
    private final TableName name;
    private Table table;
    private Map<Field, Column> columns;
    // Support temporary partition
    private PartitionNames partitionNames;
    private List<Long> tabletIds;
    private boolean isMetaQuery;

    public TableRelation(TableName name, Table table,
                         Map<Field, Column> columns,
                         PartitionNames partitionNames,
                         List<Long> tabletIds,
                         boolean isMetaQuery) {
        this.name = name;
        this.table = table;
        this.columns = columns;
        this.partitionNames = partitionNames;
        this.tabletIds = tabletIds;
        this.isMetaQuery = isMetaQuery;
    }

    public TableName getName() {
        return name;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public Column getColumn(Field field) {
        return columns.get(field);
    }

    public void setColumns(Map<Field, Column> columns) {
        this.columns = columns;
    }

    public Map<Field, Column> getColumns() {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTable(this, context);
    }

    public boolean isMetaQuery() {
        return isMetaQuery;
    }

    @Override
    public String toString() {
        return name.toString();
    }

    // ------------- New Analyzer --------------
    public TableRelation(TableName name) {
        this.name = name;
        partitionNames = null;
        tabletIds = Lists.newArrayList();
    }

    @Override
    public TableName getAlias() {
        if (alias != null) {
            return alias;
        } else {
            return name;
        }
    }

    public void setMetaQuery(boolean metaQuery) {
        isMetaQuery = metaQuery;
    }

    @Override
    public String toSql() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(name);

        if (alias != null) {
            sqlBuilder.append(" AS ").append(alias.getTbl());
        }
        return sqlBuilder.toString();
    }
}