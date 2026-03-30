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

package com.starrocks.sql.expression;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.planner.expression.ThriftEnumConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AnalysisContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.AssertNumRowsElement;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArraySliceExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CloneExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.DefaultValueExpr;
import com.starrocks.sql.ast.expression.DictMappingExpr;
import com.starrocks.sql.ast.expression.DictQueryExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FieldReference;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LambdaArgument;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LargeInPredicate;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.PlaceHolderExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TAggregateExpr;
import com.starrocks.thrift.TAnalyticWindow;
import com.starrocks.thrift.TAssertion;
import com.starrocks.thrift.TBinaryLiteral;
import com.starrocks.thrift.TBoolLiteral;
import com.starrocks.thrift.TCaseExpr;
import com.starrocks.thrift.TDateLiteral;
import com.starrocks.thrift.TDecimalLiteral;
import com.starrocks.thrift.TDictionaryGetExpr;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TFloatLiteral;
import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TInPredicate;
import com.starrocks.thrift.TInfoFunc;
import com.starrocks.thrift.TIntLiteral;
import com.starrocks.thrift.TJoinOp;
import com.starrocks.thrift.TKeysType;
import com.starrocks.thrift.TLargeIntLiteral;
import com.starrocks.thrift.TPlaceHolder;
import com.starrocks.thrift.TSlotRef;
import com.starrocks.thrift.TStringLiteral;
import com.starrocks.thrift.TVarType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.InvalidType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Convert {@link Expr} nodes into their Thrift representation via {@link AstVisitorExtendInterface}.
 *
 * @deprecated Use {@link com.starrocks.planner.expression.ExecExprSerializer} instead.
 *     For AST Expr serialization, use {@code ExecExprSerializer.serializeAstExpr(Expr)}.
 *     For literal-to-node extraction, use {@code ExecExprSerializer.serializeLiteralToNode(LiteralExpr)}.
 *     For enum conversions, use {@link com.starrocks.planner.expression.ThriftEnumConverter}.
 */
@Deprecated
public final class ExprToThrift {

    private static final Visitor VISITOR = new Visitor();

    // AnalysisContext for the current treeToThrift call, used by Visitor.visitFunctionCall.
    // This is scoped to the call (set/cleared around treeToThrift), not a global state.
    private static AnalysisContext currentAnalysisContext;

    private ExprToThrift() {
    }

    public static AstVisitorExtendInterface<Void, TExprNode> getVisitor() {
        return VISITOR;
    }

    // Convert this expr, including all children, to its Thrift representation.
    public static TExpr treeToThrift(Expr expr) {
        return treeToThrift(expr, null);
    }

    public static TExpr treeToThrift(Expr expr, AnalysisContext analysisContext) {
        AnalysisContext prev = currentAnalysisContext;
        currentAnalysisContext = analysisContext;
        try {
            TExpr result = new TExpr();
            treeToThriftHelper(expr, result, VISITOR::visit);
            return result;
        } finally {
            currentAnalysisContext = prev;
        }
    }

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(treeToThrift(expr));
        }
        return result;
    }

    /** @deprecated Use {@link ThriftEnumConverter#joinOperatorToThrift} instead. */
    @Deprecated
    public static TJoinOp joinOperatorToThrift(JoinOperator joinOperator) {
        return ThriftEnumConverter.joinOperatorToThrift(joinOperator);
    }

    /** @deprecated Use {@link ThriftEnumConverter#setTypeToThrift} instead. */
    @Deprecated
    public static TVarType setTypeToThrift(SetType setType) {
        return ThriftEnumConverter.setTypeToThrift(setType);
    }

    /** @deprecated Use {@link ThriftEnumConverter#assertionToThrift} instead. */
    @Deprecated
    public static TAssertion assertionToThrift(AssertNumRowsElement.Assertion assertion) {
        return ThriftEnumConverter.assertionToThrift(assertion);
    }

    /** @deprecated Use {@link ThriftEnumConverter#setTypeFromThrift} instead. */
    @Deprecated
    public static SetType setTypeFromThrift(TVarType thriftType) {
        return ThriftEnumConverter.setTypeFromThrift(thriftType);
    }

    /** @deprecated Use {@link ThriftEnumConverter#keysTypeToThrift} instead. */
    @Deprecated
    public static TKeysType keysTypeToThrift(KeysType keysType) {
        return ThriftEnumConverter.keysTypeToThrift(keysType);
    }

    /** @deprecated Use {@link ThriftEnumConverter#compoundPredicateOperatorToThrift} instead. */
    @Deprecated
    public static TExprOpcode compoundPredicateOperatorToThrift(CompoundPredicate.Operator operator) {
        return ThriftEnumConverter.compoundPredicateOperatorToThrift(operator);
    }

    /** @deprecated Use {@link ThriftEnumConverter#analyticWindowToThrift} instead. */
    @Deprecated
    public static TAnalyticWindow analyticWindowToThrift(AnalyticWindow window) {
        return ThriftEnumConverter.analyticWindowToThrift(window);
    }

    public static void treeToThriftHelper(Expr expr, TExpr container, BiConsumer<Expr, TExprNode> consumer) {
        if (expr.getType().isNull()) {
            Preconditions.checkState(expr instanceof NullLiteral || expr instanceof SlotRef);
            treeToThriftHelper(NullLiteral.create(BooleanType.BOOLEAN), container, consumer);
            return;
        }

        TExprNode msg = new TExprNode();

        Preconditions.checkState(java.util.Objects.equals(expr.getType(),
                AnalyzerUtils.replaceNullType2Boolean(expr.getType())),
                "NULL_TYPE is illegal in thrift stage");

        msg.type = TypeSerializer.toThrift(expr.getType());
        List<Expr> children = expr.getChildren();
        msg.num_children = children.size();
        msg.setHas_nullable_child(expr.hasNullableChild());
        msg.setIs_nullable(expr.isNullable());
        // BE no longer consumes output_scale; keep thrift field populated with a sentinel to preserve wire compatibility.
        msg.output_scale = -1;
        msg.setIs_monotonic(expr.isMonotonic());
        msg.setIs_index_only_filter(expr.isIndexOnlyFilter());
        consumer.accept(expr, msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            treeToThriftHelper(child, container, consumer);
        }
    }

    private static final class Visitor implements AstVisitorExtendInterface<Void, TExprNode> {

        private static final Function IS_NULL_FN = buildNullPredicateFn("is_null_pred");
        private static final Function IS_NOT_NULL_FN = buildNullPredicateFn("is_not_null_pred");

        private Visitor() {
        }

        private static Function buildNullPredicateFn(String name) {
            Function fn = new Function(new FunctionName(name),
                    new Type[] {InvalidType.INVALID}, BooleanType.BOOLEAN, false);
            fn.setBinaryType(TFunctionBinaryType.BUILTIN);
            return fn;
        }

        @Override
        public Void visitExpression(Expr node, TExprNode msg) {
            throw new StarRocksPlannerException("Not implement toThrift function", ErrorType.INTERNAL_ERROR);
        }

        @Override
        public Void visitBetweenPredicate(BetweenPredicate node, TExprNode msg) {
            throw new IllegalStateException(
            "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
        }

        @Override
        public Void visitArrayExpr(ArrayExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.ARRAY_EXPR);
            return null;
        }

        @Override
        public Void visitCollectionElementExpr(CollectionElementExpr node, TExprNode msg) {
            if (node.getChild(0).getType().isArrayType()) {
                msg.setNode_type(TExprNodeType.ARRAY_ELEMENT_EXPR);
            } else {
                msg.setNode_type(TExprNodeType.MAP_ELEMENT_EXPR);
            }
            msg.setCheck_is_out_of_bounds(node.isCheckIsOutOfBounds());
            return null;
        }

        @Override
        public Void visitArrowExpr(ArrowExpr node, TExprNode msg) {
            throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
        }

        @Override
        public Void visitStringLiteral(StringLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.STRING_LITERAL;
            msg.string_literal = new TStringLiteral(node.getUnescapedValue());
            return null;
        }

        @Override
        public Void visitArithmeticExpr(ArithmeticExpr node, TExprNode msg) {
            msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
            msg.setOpcode(ExprOpcodeRegistry.getArithmeticOpcode(node.getOp()));
            return null;
        }

        @Override
        public Void visitDictQueryExpr(DictQueryExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.DICT_QUERY_EXPR);
            msg.setDict_query_expr(node.getDictQueryExpr());
            return null;
        }

        @Override
        public Void visitLikePredicate(LikePredicate node, TExprNode msg) {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
            Function fn = ExprUtils.getBuiltinFunction(node.getOp().name(),
                    new Type[] {node.getChild(0).getType(), node.getChild(1).getType()},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn != null) {
                TFunction tfn = fn.toThrift();
                tfn.setIgnore_nulls(node.getIgnoreNulls());
                msg.setFn(tfn);
                if (fn.hasVarArgs()) {
                    msg.setVararg_start_idx(fn.getNumArgs() - 1);
                }
            }
            return null;
        }

        @Override
        public Void visitLambdaArguments(LambdaArgument node, TExprNode msg) {
            throw new StarRocksPlannerException("not support", ErrorType.INTERNAL_ERROR);
        }

        @Override
        public Void visitInformationFunction(InformationFunction node, TExprNode msg) {
            msg.node_type = TExprNodeType.INFO_FUNC;
            msg.info_func = new TInfoFunc(node.getIntValue(), node.getStrValue());
            if (FunctionSet.CURRENT_WAREHOUSE.equalsIgnoreCase(node.getFuncType())
                    && node.getStrValue() != null) {
                msg.info_func.setStr_value(node.getStrValue());
            }
            return null;
        }

        @Override
        public Void visitSubqueryExpr(Subquery node, TExprNode msg) {
            return null;
        }

        @Override
        public Void visitArraySliceExpr(ArraySliceExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.ARRAY_SLICE_EXPR);
            return null;
        }

        @Override
        public Void visitSlot(SlotRef node, TExprNode msg) {
            msg.node_type = TExprNodeType.SLOT_REF;
            if (node.hasSlotId()) {
                int tupleId = node.hasTupleId() ? node.getTupleId() : 0;
                msg.slot_ref = new TSlotRef(node.getSlotId(), tupleId);
            } else {
                msg.slot_ref = new TSlotRef(0, 0);
            }
            return null;
        }

        @Override
        public Void visitDecimalLiteral(DecimalLiteral node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.DECIMAL_LITERAL);
            TDecimalLiteral decimalLiteral = new TDecimalLiteral();
            decimalLiteral.setValue(node.getValue().toPlainString());
            decimalLiteral.setInteger_value(node.packDecimal());
            msg.setDecimal_literal(decimalLiteral);
            return null;
        }

        @Override
        public Void visitCaseWhenExpr(CaseExpr node, TExprNode msg) {
            msg.node_type = TExprNodeType.CASE_EXPR;
            msg.case_expr = new TCaseExpr(node.hasCaseExpr(), node.hasElseExpr());
            msg.setChild_type(TypeSerializer.toThrift(node.getChild(0).getType().getPrimitiveType()));
            return null;
        }

        @Override
        public Void visitDictionaryGetExpr(DictionaryGetExpr node, TExprNode msg) {
            TDictionaryGetExpr dictionaryGetExpr = new TDictionaryGetExpr();
            dictionaryGetExpr.setDict_id(node.getDictionaryId());
            dictionaryGetExpr.setTxn_id(node.getDictionaryTxnId());
            dictionaryGetExpr.setKey_size(node.getKeySize());
            dictionaryGetExpr.setNull_if_not_exist(node.getNullIfNotExist());

            msg.setNode_type(TExprNodeType.DICTIONARY_GET_EXPR);
            msg.setDictionary_get_expr(dictionaryGetExpr);
            return null;
        }

        @Override
        public Void visitVarBinaryLiteral(VarBinaryLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.BINARY_LITERAL;
            msg.binary_literal = new TBinaryLiteral(ByteBuffer.wrap(node.getValue()));
            return null;
        }

        @Override
        public Void visitFunctionCall(FunctionCallExpr node, TExprNode msg) {
            if (ExprUtils.isAggregate(node) || node.isAnalyticFnCall()) {
                msg.node_type = TExprNodeType.AGG_EXPR;
                msg.setAgg_expr(new TAggregateExpr(node.isMergeAggFn()));
            } else {
                msg.node_type = TExprNodeType.FUNCTION_CALL;
            }
            // Function is looked up from the AnalysisContext stored in the enclosing
            // treeToThrift call. For paths without AnalysisContext, fn will be null
            // and TFunction won't be set (the ExecExpr path handles this separately).
            Function fn = currentAnalysisContext != null ? currentAnalysisContext.getFunction(node) : null;
            if (fn != null) {
                TFunction tfn = fn.toThrift();
                tfn.setIgnore_nulls(node.getIgnoreNulls());
                msg.setFn(tfn);
                if (fn.hasVarArgs()) {
                    msg.setVararg_start_idx(fn.getNumArgs() - 1);
                }
            } else if (node.hasFnId() && node.isFnHasVarArgs()) {
                msg.setVararg_start_idx(node.getFnNumArgs() - 1);
            }
            return null;
        }

        @Override
        public Void visitIsNullPredicate(IsNullPredicate node, TExprNode msg) {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
            Function fn = node.isNotNull() ? IS_NOT_NULL_FN : IS_NULL_FN;
            TFunction tfn = fn.toThrift();
            tfn.setIgnore_nulls(node.getIgnoreNulls());
            msg.setFn(tfn);
            if (fn.hasVarArgs()) {
                msg.setVararg_start_idx(fn.getNumArgs() - 1);
            }
            return null;
        }

        @Override
        public Void visitMatchExpr(MatchExpr node, TExprNode msg) {
            msg.node_type = TExprNodeType.MATCH_EXPR;
            msg.setOpcode(ExprOpcodeRegistry.getMatchOpcode(node.getMatchOperator()));
            return null;
        }

        @Override
        public Void visitLambdaFunctionExpr(LambdaFunctionExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.LAMBDA_FUNCTION_EXPR);
            msg.setOutput_column(node.getCommonSubOperatorNum());
            boolean exists = AnalyzerUtils.containsNonDeterministicFunction(node.getChild(0)).first;
            msg.setIs_nondeterministic(exists);
            return null;
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicate node, TExprNode msg) {
            msg.node_type = TExprNodeType.COMPOUND_PRED;
            msg.setOpcode(compoundPredicateOperatorToThrift(node.getOp()));
            return null;
        }

        @Override
        public Void visitLargeIntLiteral(LargeIntLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.LARGE_INT_LITERAL;
            msg.large_int_literal = new TLargeIntLiteral(node.getValue().toString());
            return null;
        }

        @Override
        public Void visitCastExpr(CastExpr node, TExprNode msg) {
            msg.node_type = TExprNodeType.CAST_EXPR;
            msg.setOpcode(ExprOpcodeRegistry.getCastOpcode());
            Type childType = node.getChild(0).getType();
            if (childType.isComplexType() || childType.isDecimalOfAnyVersion() || childType.isChar()
                    || childType.isVarchar()) {
                msg.setChild_type_desc(TypeSerializer.toThrift(childType));
            }
            if (!childType.isComplexType()) {
                msg.setChild_type(TypeSerializer.toThrift(childType.getPrimitiveType()));
            }
            if (childType.isStructType() && node.getType().isStructType()) {
                ConnectContext ctx = ConnectContext.get();
                if (ctx != null && SqlModeHelper.check(ctx.getSessionVariable().getSqlMode(),
                        SqlModeHelper.MODE_STRUCT_CAST_BY_NAME)) {
                    msg.setCast_struct_by_name(true);
                }
            }
            return null;
        }

        @Override
        public Void visitSubfieldExpr(SubfieldExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.SUBFIELD_EXPR);
            msg.setUsed_subfield_names(node.getFieldNames());
            msg.setCopy_flag(node.isCopyFlag());
            return null;
        }

        @Override
        public Void visitMaxLiteral(MaxLiteral node, TExprNode msg) {
            return null;
        }

        @Override
        public Void visitCloneExpr(CloneExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.CLONE_EXPR);
            return null;
        }

        @Override
        public Void visitLargeInPredicate(LargeInPredicate node, TExprNode msg) {
            throw new UnsupportedOperationException(
            "LargeInPredicate cannot be serialized to Thrift. " +
            "It should be transformed to Left semi/anti join via LargeInPredicateToJoinRule.");
        }

        @Override
        public Void visitBinaryPredicate(BinaryPredicate node, TExprNode msg) {
            msg.node_type = TExprNodeType.BINARY_PRED;
            msg.setOpcode(ExprOpcodeRegistry.getBinaryOpcode(node.getOp()));
            msg.setVector_opcode(TExprOpcode.INVALID_OPCODE);
            if (node.getChild(0).getType().isComplexType()) {
                msg.setChild_type_desc(TypeSerializer.toThrift(node.getChild(0).getType()));
            } else {
                msg.setChild_type(TypeSerializer.toThrift(node.getChild(0).getType().getPrimitiveType()));
            }
            return null;
        }

        @Override
        public Void visitIntLiteral(IntLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.INT_LITERAL;
            msg.int_literal = new TIntLiteral(node.getValue());
            return null;
        }

        @Override
        public Void visitDateLiteral(DateLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.DATE_LITERAL;
            msg.date_literal = new TDateLiteral(node.getStringValue());
            return null;
        }

        @Override
        public Void visitMapExpr(MapExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.MAP_EXPR);
            return null;
        }

        @Override
        public Void visitInPredicate(InPredicate node, TExprNode msg) {
            Preconditions.checkState(!node.contains(Subquery.class));
            msg.in_predicate = new TInPredicate(node.isNotIn());
            msg.node_type = TExprNodeType.IN_PRED;
            msg.setOpcode(ExprOpcodeRegistry.getInPredicateOpcode(node.isNotIn()));
            msg.setVector_opcode(TExprOpcode.INVALID_OPCODE);
            if (node.getChild(0).getType().isComplexType()) {
                msg.setChild_type_desc(TypeSerializer.toThrift(node.getChild(0).getType()));
            } else {
                msg.setChild_type(TypeSerializer.toThrift(node.getChild(0).getType().getPrimitiveType()));
            }
            return null;
        }

        @Override
        public Void visitFieldReference(FieldReference node, TExprNode msg) {
            throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
        }

        @Override
        public Void visitAnalyticExpr(AnalyticExpr node, TExprNode msg) {
            return null;
        }

        @Override
        public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr node, TExprNode msg) {
            msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
            msg.setOpcode(ExprOpcodeRegistry.getExprOpcode(node));
            Function fn = ExprUtils.getTimestampArithmeticFunction(node);
            Preconditions.checkNotNull(fn, "TimestampArithmeticExpr must resolve to a builtin function");
            TFunction tfn = fn.toThrift();
            tfn.setIgnore_nulls(node.getIgnoreNulls());
            msg.setFn(tfn);
            if (fn.hasVarArgs()) {
                msg.setVararg_start_idx(fn.getNumArgs() - 1);
            }
            return null;
        }

        @Override
        public Void visitBoolLiteral(BoolLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.BOOL_LITERAL;
            msg.bool_literal = new TBoolLiteral(node.getValue());
            return null;
        }

        @Override
        public Void visitFloatLiteral(FloatLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.FLOAT_LITERAL;
            msg.float_literal = new TFloatLiteral(node.getValue());
            return null;
        }

        @Override
        public Void visitExistsPredicate(ExistsPredicate node, TExprNode msg) {
            Preconditions.checkState(false);
            return null;
        }

        @Override
        public Void visitIntervalLiteral(IntervalLiteral node, TExprNode msg) {
            throw new StarRocksPlannerException("IntervalLiteral not implement toThrift", ErrorType.INTERNAL_ERROR);
        }

        @Override
        public Void visitDictMappingExpr(DictMappingExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.DICT_EXPR);
            return null;
        }

        @Override
        public Void visitNullLiteral(NullLiteral node, TExprNode msg) {
            msg.node_type = TExprNodeType.NULL_LITERAL;
            return null;
        }

        @Override
        public Void visitPlaceHolderExpr(PlaceHolderExpr node, TExprNode msg) {
            msg.setNode_type(TExprNodeType.PLACEHOLDER_EXPR);
            msg.setVslot_ref(new TPlaceHolder());
            msg.vslot_ref.setNullable(node.isNullable());
            msg.vslot_ref.setSlot_id(node.getSlotId());
            return null;
        }

        @Override
        public Void visitDefaultValueExpr(DefaultValueExpr node, TExprNode msg) {
            return null;
        }
    }
}
