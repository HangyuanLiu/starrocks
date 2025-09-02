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

package com.starrocks.sql.formatter;

import com.google.common.base.Joiner;
import com.starrocks.planner.expr.ArithmeticExpr;
import com.starrocks.planner.expr.ArrayExpr;
import com.starrocks.planner.expr.ArraySliceExpr;
import com.starrocks.planner.expr.ArrowExpr;
import com.starrocks.planner.expr.BetweenPredicate;
import com.starrocks.planner.expr.BinaryPredicate;
import com.starrocks.planner.expr.BoolLiteral;
import com.starrocks.planner.expr.CastExpr;
import com.starrocks.planner.expr.CloneExpr;
import com.starrocks.planner.expr.CollectionElementExpr;
import com.starrocks.planner.expr.CompoundPredicate;
import com.starrocks.planner.expr.DateLiteral;
import com.starrocks.planner.expr.DictMappingExpr;
import com.starrocks.planner.expr.DictQueryExpr;
import com.starrocks.planner.expr.DictionaryGetExpr;
import com.starrocks.planner.expr.ExistsPredicate;
import com.starrocks.planner.expr.Expr;
import com.starrocks.planner.expr.ExprVisitorBase;
import com.starrocks.planner.expr.FunctionCallExpr;
import com.starrocks.planner.expr.InPredicate;
import com.starrocks.planner.expr.InformationFunction;
import com.starrocks.planner.expr.IsNullPredicate;
import com.starrocks.planner.expr.LambdaFunctionExpr;
import com.starrocks.planner.expr.LargeStringLiteral;
import com.starrocks.planner.expr.LikePredicate;
import com.starrocks.planner.expr.LiteralExpr;
import com.starrocks.planner.expr.MapExpr;
import com.starrocks.planner.expr.MatchExpr;
import com.starrocks.planner.expr.MaxLiteral;
import com.starrocks.planner.expr.MultiInPredicate;
import com.starrocks.planner.expr.NullLiteral;
import com.starrocks.planner.expr.PlaceHolderExpr;
import com.starrocks.planner.expr.StringLiteral;
import com.starrocks.planner.expr.SubfieldExpr;
import com.starrocks.planner.expr.VarBinaryLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
 * @Todo: merge with AST2StringVisitor
 */
public class ExprExplainVisitor extends ExprVisitorBase<String, Void> {
    private FormatOptions options = FormatOptions.allEnable();

    public ExprExplainVisitor() {
        options.setEnableDigest(false);
    }

    public ExprExplainVisitor(FormatOptions options) {
        this.options = options;
    }

    // ========================================= Helper Methods =========================================
    private List<String> visitChildren(List<Expr> children) {
        if (children == null || children.isEmpty()) {
            return List.of();
        }
        return children.stream()
                .map(child -> child.accept(this, null))
                .collect(Collectors.toList());
    }

    // ========================================= Literal Expressions =========================================
    public String visitLiteral(LiteralExpr node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }
        return node.getStringValue();
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        String sql = node.getStringValue();
        if (sql != null) {
            if (sql.contains("\\")) {
                sql = sql.replace("\\", "\\\\");
            }
            sql = sql.replace("'", "\\'");
        }
        return "'" + sql + "'";
    }

    @Override
    public String visitBoolLiteral(BoolLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }
        return node.getValue() ? "TRUE" : "FALSE";
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Void context) {
        return "NULL";
    }

    @Override
    public String visitDateLiteral(DateLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        return "'" + node.getStringValue() + "'";
    }

    @Override
    public String visitVarBinaryLiteral(VarBinaryLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        return "'" + node.getStringValue() + "'";
    }

    @Override
    public String visitLargeStringLiteral(LargeStringLiteral node, Void context) {
        if (options.isEnableDigest()) {
            return "?";
        }

        String fullSql = visitStringLiteral(node, context);
        fullSql = fullSql.substring(0, LargeStringLiteral.LEN_LIMIT);
        return fullSql + "...'";
    }

    @Override
    public String visitMaxLiteral(MaxLiteral node, Void context) {
        return "MAXVALUE";
    }

    // ========================================= Arithmetic and Predicates =========================================

    @Override
    public String visitArithmeticExpr(ArithmeticExpr node, Void context) {
        if (node.getChildren().size() == 1) {
            return node.getOp().toString() + " " + node.getChild(0).accept(this, context);
        } else {
            String left = node.getChild(0).accept(this, context);
            String right = node.getChild(1).accept(this, context);
            return left + " " + node.getOp().toString() + " " + right;
        }
    }

    @Override
    public String visitBinaryPredicate(BinaryPredicate node, Void context) {
        String left = node.getChild(0).accept(this, context);
        String right = node.getChild(1).accept(this, context);
        return left + " " + node.getOp().toString() + " " + right;
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicate node, Void context) {
        String operator = node.getOp().toString();

        if (node.getOp() == CompoundPredicate.Operator.NOT) {
            return "NOT (" + node.getChild(0).accept(this, context) + ")";
        } else {
            String left = node.getChild(0).accept(this, context);
            String right = node.getChild(1).accept(this, context);
            return "(" + left + ") " + operator + " (" + right + ")";
        }
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        String lower = node.getChild(1).accept(this, context);
        String upper = node.getChild(2).accept(this, context);
        String notStr = node.isNotBetween() ? " NOT" : "";
        return expr + notStr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitInPredicate(InPredicate node, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getChild(0).accept(this, context));

        if (node.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(node.getChildren().stream()
                .skip(1).map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        return expr + (node.isNotNull() ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String visitLikePredicate(LikePredicate node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        String pattern = node.getChild(1).accept(this, context);
        return expr + " " + node.getOp().toString() + " " + pattern;
    }

    @Override
    public String visitExistsPredicate(ExistsPredicate node, Void context) {
        String notStr = node.isNotExists() ? "NOT " : "";
        return notStr + "EXISTS " + node.getChild(0).accept(this, context);
    }

    // ========================================= Function Calls =========================================

    @Override
    public String visitFunctionCall(FunctionCallExpr node, Void context) {
        StringBuilder sb = new StringBuilder();

        sb.append(node.getFnName());
        sb.append("(");

        if (node.getFnParams().isStar()) {
            sb.append("*");
        }
        if (node.isDistinct()) {
            sb.append("DISTINCT ");
        }

        String childrenSql = node.getChildren().stream()
                .limit(node.getChildren().size() - node.getFnParams().getOrderByElemNum())
                .map(c -> visit(c, context))
                .collect(Collectors.joining(", "));
        sb.append(childrenSql);

        if (node.getFnParams().getOrderByElements() != null) {
            sb.append(node.getFnParams().getOrderByStringToSql());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitInformationFunction(InformationFunction node, Void context) {
        String funcName = node.getFuncType();
        return funcName + "()";
    }

    // ========================================= Cast and Case Expressions =========================================
    @Override
    public String visitCastExpr(CastExpr node, Void context) {
        String expr = node.getChild(0).accept(this, context);
        if (node.getTargetTypeDef() == null) {
            return "CAST(" + expr + " AS " + node.getType() + ")";
        } else {
            return "CAST(" + expr + " AS " + node.getTargetTypeDef() + ")";
        }
    }

    // ========================================= Collection Expressions =========================================

    @Override
    public String visitArrayExpr(ArrayExpr node, Void context) {
        return "[" + Joiner.on(",").join(visitChildren(node.getChildren())) + "]";
    }

    @Override
    public String visitMapExpr(MapExpr node, Void context) {
        List<String> pairs = new ArrayList<>();
        List<Expr> children = node.getChildren();

        for (int i = 0; i < children.size(); i += 2) {
            String key = children.get(i).accept(this, context);
            String value = children.get(i + 1).accept(this, context);
            pairs.add(key + ":" + value);
        }
        return "map{" + Joiner.on(",").join(pairs) + "}";
    }

    @Override
    public String visitCollectionElementExpr(CollectionElementExpr node, Void context) {
        String collection = node.getChild(0).accept(this, context);
        String index = node.getChild(1).accept(this, context);
        return collection + "[" + index + "]";
    }

    // ========================================= Multi-Value Predicates =========================================
    @Override
    public String visitMultiInPredicate(MultiInPredicate node, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(node.getChildren().stream()
                .limit(node.getNumberOfColumns())
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        if (node.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(node.getChildren().stream()
                .skip(node.getNumberOfColumns())
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitCloneExpr(CloneExpr node, Void context) {
        return "clone(" + node.getChild(0).accept(this, context) + ")";
    }

    // ========================================= Lambda Expressions =========================================
    @Override
    public String visitLambdaFunctionExpr(LambdaFunctionExpr node, Void context) {
        StringBuilder names = new StringBuilder(visit(node.getChild(1)));
        int realChildrenNum = node.getChildren().size() - 2 * node.getCommonSubOperatorNum();
        if (realChildrenNum > 2) {
            for (int i = 2; i < realChildrenNum; ++i) {
                names.append(", ").append(visit(node.getChild(i)));
            }
            names = new StringBuilder("(" + names + ")");
        }

        StringBuilder commonSubOp = new StringBuilder();
        if (node.getCommonSubOperatorNum() > 0) {
            commonSubOp.append("\n        lambda common expressions:");
        }

        for (int i = realChildrenNum; i < realChildrenNum + node.getCommonSubOperatorNum(); ++i) {
            commonSubOp.append("{")
                    .append(visit(node.getChild(i)))
                    .append(" <-> ")
                    .append(visit(node.getChild(i + node.getCommonSubOperatorNum())))
                    .append("}");
        }
        if (node.getCommonSubOperatorNum() > 0) {
            commonSubOp.append("\n        ");
        }
        return String.format("%s -> %s%s", names, visit(node.getChild(0)), commonSubOp);
    }

    // ========================================= Dictionary Expressions =========================================
    @Override
    public String visitDictionaryGetExpr(DictionaryGetExpr node, Void context) {
        String message = "DICTIONARY_GET(";
        int size = (node.getChildren().size() == 3) ? node.getChildren().size() - 1 : node.getChildren().size();
        message += node.getChildren().stream().limit(size)
                .map(this::visit)
                .collect(Collectors.joining(", "));
        message += ", " + (node.getNullIfNotExist() ? "true" : "false");
        message += ")";
        return message;
    }

    @Override
    public String visitDictMappingExpr(DictMappingExpr node, Void context) {
        String fnName = node.getType().matchesType(node.getChild(1).getType()) ? "DictDecode" : "DictDefine";

        if (node.getChildren().size() == 2) {
            return fnName + "(" + visit(node.getChild(0)) + ", [" + visit(node.getChild(1)) + "])";
        }
        return fnName + "(" + visit(node.getChild(0)) + ", ["
                + visit(node.getChild(1)) + "], "
                + visit(node.getChild(2)) + ")";
    }

    @Override
    public String visitDictQueryExpr(DictQueryExpr node, Void context) {
        return visitFunctionCall(node, context);
    }

    // ========================================= Arrow and Subfield Expressions =========================================

    @Override
    public String visitArraySliceExpr(ArraySliceExpr node, Void context) {
        return visit(node.getChild(0))
                + "[" + visit(node.getChild(1))
                + ":" + visit(node.getChild(2)) + "]";
    }

    @Override
    public String visitArrowExpr(ArrowExpr node, Void context) {
        return node.getItem().accept(this, context) + "->" +
                node.getKey().accept(this, context);
    }

    @Override
    public String visitSubfieldExpr(SubfieldExpr node, Void context) {
        return node.getChild(0).accept(this, context)
                + "." + String.join(".", node.getFieldNames())
                + "[" + node.isCopyFlag() + "]";
    }

    // ========================================= Match Expression =========================================

    @Override
    public String visitMatchExpr(MatchExpr node, Void context) {
        return visit(node.getChild(0)) + " " + node.getMatchOperator().getName() + " " + visit(node.getChild(1));
    }

    @Override
    public String visitPlaceHolderExpr(PlaceHolderExpr node, Void context) {
        return "<place-holder>";
    }
}
