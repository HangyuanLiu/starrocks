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

import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.util.stream.Collectors;

/**
 * Generates human-readable strings from {@link ExecExpr} trees for EXPLAIN output.
 * Follows the same formatting conventions as {@code ExprExplainVisitor} to ensure
 * test compatibility.
 */
public class ExecExprExplain implements ExecExprVisitor<String, Void> {

    private static final ExecExprExplain INSTANCE = new ExecExprExplain();

    public static String explain(ExecExpr expr) {
        return expr.accept(INSTANCE, null);
    }

    public static String explainList(java.util.List<? extends ExecExpr> exprs) {
        return exprs.stream()
                .map(ExecExprExplain::explain)
                .collect(Collectors.joining(", "));
    }

    /**
     * Verbose explain format: [slotId: label, TYPE, nullable]
     * Used for VERBOSE and COSTS EXPLAIN levels.
     */
    public static String verboseExplain(ExecExpr expr) {
        if (expr instanceof ExecSlotRef) {
            ExecSlotRef slot = (ExecSlotRef) expr;
            // Match ExprVerboseVisitor.visitSlot format: [label, type, nullable]
            // Use the ExecSlotRef's type (which may have been set from ColumnRefOperator)
            // rather than the descriptor type (which is the physical column type)
            if (slot.getLabel() != null) {
                return "[" + slot.getLabel() + ", " + slot.getType() + ", " + slot.isNullable() + "]";
            } else {
                return "[" + slot.getSlotId().asInt() + ", " + slot.getType() + ", " + slot.isNullable() + "]";
            }
        }
        if (expr instanceof ExecAstExprWrapper) {
            return com.starrocks.sql.ast.expression.ExprToSql.explain(
                    ((ExecAstExprWrapper) expr).getAstExpr());
        }
        return explain(expr);
    }

    public static String verboseExplainList(java.util.List<? extends ExecExpr> exprs) {
        return exprs.stream()
                .map(ExecExprExplain::verboseExplain)
                .collect(Collectors.joining(", "));
    }

    @Override
    public String visitExecExpr(ExecExpr expr, Void context) {
        return "<unknown-exec-expr>";
    }

    @Override
    public String visitExecAstExprWrapper(ExecAstExprWrapper expr, Void context) {
        return com.starrocks.sql.ast.expression.ExprToSql.explain(expr.getAstExpr());
    }

    @Override
    public String visitExecSlotRef(ExecSlotRef expr, Void context) {
        if (expr.getLabel() != null) {
            return expr.getLabel();
        }
        return "<slot " + expr.getSlotId().asInt() + ">";
    }

    @Override
    public String visitExecLiteral(ExecLiteral expr, Void context) {
        ConstantOperator value = expr.getValue();
        if (value.isNull()) {
            return "NULL";
        }
        Type t = expr.getType();
        if (t.isBoolean()) {
            return value.getBoolean() ? "TRUE" : "FALSE";
        } else if (t.isTinyint()) {
            return String.valueOf(value.getTinyInt());
        } else if (t.isSmallint()) {
            return String.valueOf(value.getSmallint());
        } else if (t.isInt()) {
            return String.valueOf(value.getInt());
        } else if (t.isBigint()) {
            return String.valueOf(value.getBigint());
        } else if (t.isLargeint()) {
            return value.getLargeInt().toString();
        } else if (t.isFloatingPointType()) {
            return String.valueOf(value.getDouble());
        } else if (t.isTime()) {
            return String.valueOf(value.getDouble());
        } else if (t.isStringType() || t.isChar() || t.isVarchar()) {
            String s = value.getVarchar();
            s = s.replace("\\", "\\\\");
            s = s.replace("'", "\\'");
            return "'" + s + "'";
        } else if (t.isDate()) {
            LocalDateTime dt = value.getDatetime();
            return String.format("'%04d-%02d-%02d'", dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth());
        } else if (t.isDatetime()) {
            LocalDateTime dt = value.getDatetime();
            return String.format("'%04d-%02d-%02d %02d:%02d:%02d'",
                    dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                    dt.getHour(), dt.getMinute(), dt.getSecond());
        } else if (t.isDecimalOfAnyVersion()) {
            return value.getDecimal().toPlainString();
        } else if (t.isBinaryType()) {
            return "X'" + bytesToHex(value.getBinary()) + "'";
        }
        return String.valueOf(value.getValue());
    }

    @Override
    public String visitExecFunctionCall(ExecFunctionCall expr, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(expr.getFnName()).append("(");
        if (expr.isDistinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitExecCast(ExecCast expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return "CAST(" + child + " AS " + expr.getType() + ")";
    }

    @Override
    public String visitExecBinaryPredicate(ExecBinaryPredicate expr, Void context) {
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + binaryTypeToString(expr.getOp()) + " " + right;
    }

    @Override
    public String visitExecCompoundPredicate(ExecCompoundPredicate expr, Void context) {
        CompoundPredicate.Operator op = expr.getCompoundType();
        if (op == CompoundPredicate.Operator.NOT) {
            return "NOT (" + expr.getChild(0).accept(this, context) + ")";
        }
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return "(" + left + ") " + op.toString() + " (" + right + ")";
    }

    @Override
    public String visitExecInPredicate(ExecInPredicate expr, Void context) {
        StringBuilder sb = new StringBuilder();
        sb.append(expr.getChild(0).accept(this, context));
        if (expr.isNotIn()) {
            sb.append(" NOT");
        }
        sb.append(" IN (");
        sb.append(expr.getChildren().stream()
                .skip(1)
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitExecIsNullPredicate(ExecIsNullPredicate expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return child + (expr.isNotNull() ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String visitExecLikePredicate(ExecLikePredicate expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        String pattern = expr.getChild(1).accept(this, context);
        String opName = expr.isRegexp() ? "REGEXP" : "LIKE";
        return child + " " + opName + " " + pattern;
    }

    @Override
    public String visitExecBetweenPredicate(ExecBetweenPredicate expr, Void context) {
        String e = expr.getChild(0).accept(this, context);
        String lower = expr.getChild(1).accept(this, context);
        String upper = expr.getChild(2).accept(this, context);
        String notStr = expr.isNotBetween() ? " NOT" : "";
        return e + notStr + " BETWEEN " + lower + " AND " + upper;
    }

    @Override
    public String visitExecCaseWhen(ExecCaseWhen expr, Void context) {
        StringBuilder sb = new StringBuilder("CASE");
        int childIdx = 0;
        if (expr.hasCase()) {
            sb.append(" ").append(expr.getChild(childIdx++).accept(this, context));
        }
        while (childIdx + 2 <= expr.getNumChildren()) {
            sb.append(" WHEN ").append(expr.getChild(childIdx++).accept(this, context));
            sb.append(" THEN ").append(expr.getChild(childIdx++).accept(this, context));
        }
        if (expr.hasElse()) {
            sb.append(" ELSE ").append(expr.getChild(expr.getNumChildren() - 1).accept(this, context));
        }
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public String visitExecMatchExpr(ExecMatchExpr expr, Void context) {
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + matchOpToString(expr.getMatchOp()) + " " + right;
    }

    @Override
    public String visitExecArrayExpr(ExecArrayExpr expr, Void context) {
        return "[" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(",")) + "]";
    }

    @Override
    public String visitExecMapExpr(ExecMapExpr expr, Void context) {
        StringBuilder sb = new StringBuilder("map{");
        java.util.List<ExecExpr> children = expr.getChildren();
        for (int i = 0; i < children.size(); i += 2) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(children.get(i).accept(this, context));
            sb.append(":");
            sb.append(children.get(i + 1).accept(this, context));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String visitExecCollectionElement(ExecCollectionElement expr, Void context) {
        String collection = expr.getChild(0).accept(this, context);
        String index = expr.getChild(1).accept(this, context);
        return collection + "[" + index + "]";
    }

    @Override
    public String visitExecArraySlice(ExecArraySlice expr, Void context) {
        String array = expr.getChild(0).accept(this, context);
        String lower = expr.getChild(1).accept(this, context);
        String upper = expr.getChild(2).accept(this, context);
        return array + "[" + lower + ":" + upper + "]";
    }

    @Override
    public String visitExecSubfield(ExecSubfield expr, Void context) {
        String child = expr.getChild(0).accept(this, context);
        return child + "." + String.join(".", expr.getFieldNames());
    }

    @Override
    public String visitExecLambdaFunction(ExecLambdaFunction expr, Void context) {
        // First child is the body, remaining children are arguments
        String body = expr.getChild(0).accept(this, context);
        if (expr.getNumChildren() == 2) {
            String arg = expr.getChild(1).accept(this, context);
            return arg + " -> " + body;
        }
        StringBuilder args = new StringBuilder("(");
        for (int i = 1; i < expr.getNumChildren(); i++) {
            if (i > 1) {
                args.append(", ");
            }
            args.append(expr.getChild(i).accept(this, context));
        }
        args.append(")");
        return args + " -> " + body;
    }

    @Override
    public String visitExecDictMapping(ExecDictMapping expr, Void context) {
        return "dict_mapping(" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitExecClone(ExecClone expr, Void context) {
        return "clone(" + expr.getChild(0).accept(this, context) + ")";
    }

    @Override
    public String visitExecDictQuery(ExecDictQuery expr, Void context) {
        return "dict_query(" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitExecDictionaryGet(ExecDictionaryGet expr, Void context) {
        return "dictionary_get(" + expr.getChildren().stream()
                .map(c -> c.accept(this, context))
                .collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String visitExecPlaceHolder(ExecPlaceHolder expr, Void context) {
        return "<placeholder slot_id=" + expr.getSlotId() + ">";
    }

    @Override
    public String visitExecArithmetic(ExecArithmetic expr, Void context) {
        ArithmeticExpr.Operator op = expr.getOp();
        if (expr.getNumChildren() == 1) {
            return op.toString() + " " + expr.getChild(0).accept(this, context);
        }
        String left = expr.getChild(0).accept(this, context);
        String right = expr.getChild(1).accept(this, context);
        return left + " " + op.toString() + " " + right;
    }

    @Override
    public String visitExecInformationFunction(ExecInformationFunction expr, Void context) {
        return expr.getFuncName() + "()";
    }

    // ---- Helpers ----

    private static String binaryTypeToString(BinaryType op) {
        switch (op) {
            case EQ:
                return "=";
            case NE:
                return "!=";
            case LT:
                return "<";
            case LE:
                return "<=";
            case GT:
                return ">";
            case GE:
                return ">=";
            case EQ_FOR_NULL:
                return "<=>";
            default:
                return op.toString();
        }
    }

    private static String matchOpToString(MatchExpr.MatchOperator op) {
        switch (op) {
            case MATCH:
                return "MATCH";
            case MATCH_ANY:
                return "MATCH_ANY";
            case MATCH_ALL:
                return "MATCH_ALL";
            default:
                return op.toString();
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
