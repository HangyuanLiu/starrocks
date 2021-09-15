// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

import java.util.List;

public class WithQuery implements ParseNode {
    private String name;
    private QueryStatement query;
    private List<String> columnNames;

    public WithQuery(String name, List<String> columnNames, QueryStatement query) {
        this.name = name;
        this.columnNames = columnNames;
        this.query = query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}