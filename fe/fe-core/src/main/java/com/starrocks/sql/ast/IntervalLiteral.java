// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.thrift.TExprNode;

public class IntervalLiteral extends LiteralExpr {
    private final Expr value;
    private final String timeUnitIdent;

    public IntervalLiteral(Expr value, String timeUnitIdent) {
        this.value = value;
        this.timeUnitIdent = timeUnitIdent;
    }

    public Expr getValue() {
        return value;
    }

    public String getTimeUnitIdent() {
        return timeUnitIdent;
    }

    @Override
    protected String toSqlImpl() {
        return null;
    }

    @Override
    protected void toThrift(TExprNode msg) {

    }

    @Override
    public Expr clone() {
        return null;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }
}
