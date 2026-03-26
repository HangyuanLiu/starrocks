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

import com.google.common.collect.Lists;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.expression.ExprToThrift;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.type.BooleanType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.util.List;

/**
 * Serializes an {@link ExecExpr} tree into a {@link TExpr} (Thrift representation).
 * This replaces {@link ExprToThrift#treeToThrift} for the ExecExpr hierarchy.
 */
public final class ExecExprSerializer {

    private ExecExprSerializer() {
    }

    public static TExpr serialize(ExecExpr expr) {
        TExpr result = new TExpr();
        serializeHelper(expr, result);
        return result;
    }

    public static List<TExpr> serializeList(List<? extends ExecExpr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (ExecExpr expr : exprs) {
            result.add(serialize(expr));
        }
        return result;
    }

    private static void serializeHelper(ExecExpr expr, TExpr container) {
        // For AST Expr wrappers, delegate to the original AST serialization path
        if (expr instanceof ExecAstExprWrapper) {
            TExpr astResult = ExprToThrift.treeToThrift(((ExecAstExprWrapper) expr).getAstExpr());
            if (astResult.getNodes() != null) {
                for (com.starrocks.thrift.TExprNode n : astResult.getNodes()) {
                    container.addToNodes(n);
                }
            }
            return;
        }

        Type exprType = expr.getType();

        // Replace NULL_TYPE with BOOLEAN, matching ExprToThrift behavior.
        // If the expression has null type, serialize a null literal with boolean type instead.
        if (exprType.isNull()) {
            serializeNullLiteral(container);
            return;
        }

        TExprNode node = new TExprNode();

        node.type = TypeSerializer.toThrift(exprType);
        node.num_children = expr.getNumChildren();
        node.setHas_nullable_child(expr.hasNullableChild());
        node.setIs_nullable(expr.isNullable());
        // BE no longer consumes output_scale; keep thrift field populated with a sentinel for wire compat.
        node.output_scale = -1;
        node.setIs_monotonic(expr.isMonotonic());
        node.setIs_index_only_filter(expr.isIndexOnlyFilter());
        node.node_type = expr.getNodeType();

        // Let the concrete ExecExpr populate type-specific fields
        expr.toThrift(node);

        container.addToNodes(node);

        // Recursively serialize children (depth-first pre-order)
        for (ExecExpr child : expr.getChildren()) {
            serializeHelper(child, container);
        }
    }

    /**
     * Serialize a NULL literal with BOOLEAN type, matching the behavior of
     * {@link ExprToThrift#treeToThriftHelper} when it encounters a null-typed expression.
     */
    private static void serializeNullLiteral(TExpr container) {
        Type boolType = BooleanType.BOOLEAN;
        // Replace NULL_TYPE with BOOLEAN to ensure correctness on the backend
        boolType = AnalyzerUtils.replaceNullType2Boolean(boolType);

        TExprNode node = new TExprNode();
        node.type = TypeSerializer.toThrift(boolType);
        node.num_children = 0;
        node.setHas_nullable_child(false);
        node.setIs_nullable(true);
        node.output_scale = -1;
        node.setIs_monotonic(false);
        node.setIs_index_only_filter(false);
        node.node_type = com.starrocks.thrift.TExprNodeType.NULL_LITERAL;

        container.addToNodes(node);
    }
}
