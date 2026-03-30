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

package com.starrocks.planner.expression;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin wrapper that adapts an AST {@link Expr} into the {@link ExecExpr} hierarchy.
 * <p>
 * This is a transitional bridge for plan nodes (e.g., LoadScanNode, EsScanNode) that
 * still receive AST Expr objects but need to store them in ExecExpr-typed fields.
 * The wrapper delegates Thrift serialization and explain to the underlying AST Expr.
 * <p>
 * TODO: Remove once all plan nodes produce ExecExpr natively.
 */
public class ExecAstExprWrapper extends ExecExpr {
    private final Expr astExpr;

    public ExecAstExprWrapper(Expr astExpr) {
        super(astExpr.getType());
        this.astExpr = astExpr;
    }

    /**
     * Return the underlying AST Expr.
     */
    public Expr getAstExpr() {
        return astExpr;
    }

    /**
     * Wrap a single AST Expr into an ExecExpr wrapper.
     */
    public static ExecExpr wrap(Expr expr) {
        return new ExecAstExprWrapper(expr);
    }

    /**
     * Wrap a list of AST Expr into ExecExpr wrappers.
     */
    public static List<ExecExpr> wrapList(List<? extends Expr> exprs) {
        List<ExecExpr> result = new ArrayList<>(exprs.size());
        for (Expr expr : exprs) {
            result.add(new ExecAstExprWrapper(expr));
        }
        return result;
    }

    /**
     * Unwrap a list of ExecExpr back to AST Expr.
     * Throws IllegalStateException if any element is not an ExecAstExprWrapper.
     */
    public static List<Expr> unwrapList(List<? extends ExecExpr> exprs) {
        List<Expr> result = new ArrayList<>(exprs.size());
        for (ExecExpr expr : exprs) {
            if (expr instanceof ExecAstExprWrapper) {
                result.add(((ExecAstExprWrapper) expr).getAstExpr());
            } else {
                throw new IllegalStateException(
                        "Expected ExecAstExprWrapper but got " + expr.getClass().getSimpleName());
            }
        }
        return result;
    }

    @Override
    public boolean isNullable() {
        return astExpr.isNullable();
    }

    @Override
    public TExprNodeType getNodeType() {
        // ExecAstExprWrapper is serialized via ExecExprSerializer which delegates to
        // ExprToThrift for the full AST Expr tree. This method should not be called directly.
        throw new UnsupportedOperationException(
                "ExecAstExprWrapper.getNodeType() should not be called; "
                        + "serialization is handled by ExecExprSerializer");
    }

    @Override
    public void toThrift(TExprNode node) {
        // ExecAstExprWrapper is serialized via ExecExprSerializer which delegates to
        // ExprToThrift for the full AST Expr tree. This method should not be called directly.
        throw new UnsupportedOperationException(
                "ExecAstExprWrapper.toThrift() should not be called; "
                        + "serialization is handled by ExecExprSerializer");
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecAstExprWrapper(this, context);
    }

    @Override
    public ExecExpr clone() {
        return new ExecAstExprWrapper((Expr) astExpr.clone());
    }

    @Override
    public String toString() {
        return ExprToSql.toSql(astExpr);
    }
}
