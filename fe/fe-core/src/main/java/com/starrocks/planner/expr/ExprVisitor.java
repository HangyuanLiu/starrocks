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

package com.starrocks.planner.expr;

/**
 * Base interface for visiting Expr nodes in the planner expression tree.
 * This follows the Visitor pattern to allow operations on expression trees
 * without modifying the Expr classes themselves.
 * 
 * @param <R> The return type of the visit methods
 * @param <C> The context type passed to visit methods
 */
public interface ExprVisitor<R, C> {
    
    /**
     * Visit an Expr node with context.
     * 
     * @param expr the expression to visit
     * @param context the context for the visit
     * @return the result of visiting the expression
     */
    default R visit(Expr expr, C context) {
        return expr.accept(this, context);
    }
    
    /**
     * Visit an Expr node without context.
     * 
     * @param expr the expression to visit
     * @return the result of visiting the expression
     */
    default R visit(Expr expr) {
        return visit(expr, null);
    }
    
    /**
     * Default visit method for any Expr node.
     * Subclasses can override this to provide default behavior.
     * 
     * @param expr the expression to visit
     * @param context the context for the visit
     * @return the result of visiting the expression
     */
    default R visitExpr(Expr expr, C context) {
        return null;
    }
    
    // ------------------------------------------- Literal Expressions ------------------------------------------
    
    default R visitLiteral(LiteralExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitBoolLiteral(BoolLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitIntLiteral(IntLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitLargeIntLiteral(LargeIntLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitFloatLiteral(FloatLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitDecimalLiteral(DecimalLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitStringLiteral(StringLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitLargeStringLiteral(LargeStringLiteral expr, C context) {
        return visitStringLiteral(expr, context);
    }
    
    default R visitDateLiteral(DateLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitNullLiteral(NullLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitVarBinaryLiteral(VarBinaryLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    default R visitMaxLiteral(MaxLiteral expr, C context) {
        return visitLiteral(expr, context);
    }
    
    // ------------------------------------------- Reference Expressions ----------------------------------------
    
    default R visitSlot(SlotRef expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Function Expressions -----------------------------------------
    
    default R visitFunctionCall(FunctionCallExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitGroupingFunctionCall(GroupingFunctionCallExpr expr, C context) {
        return visitFunctionCall(expr, context);
    }
    
    default R visitInformationFunction(InformationFunction expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Arithmetic Expressions ---------------------------------------
    
    default R visitArithmeticExpr(ArithmeticExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Predicate Expressions ----------------------------------------
    
    default R visitPredicate(Expr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitBinaryPredicate(BinaryPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitCompoundPredicate(CompoundPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitBetweenPredicate(BetweenPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitInPredicate(InPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitMultiInPredicate(MultiInPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitIsNullPredicate(IsNullPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitLikePredicate(LikePredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitExistsPredicate(ExistsPredicate expr, C context) {
        return visitPredicate(expr, context);
    }
    
    default R visitMatchExpr(MatchExpr expr, C context) {
        return visitPredicate(expr, context);
    }
    
    // ------------------------------------------- Collection Expressions --------------------------------------
    
    default R visitArrayExpr(ArrayExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitMapExpr(MapExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitCollectionElementExpr(CollectionElementExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitArraySliceExpr(ArraySliceExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitArrowExpr(ArrowExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitSubfieldExpr(SubfieldExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Other Expressions -------------------------------------------
    
    default R visitCaseExpr(CaseExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitCastExpr(CastExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitCloneExpr(CloneExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitSubquery(Subquery expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitLambdaFunctionExpr(LambdaFunctionExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Dictionary Expressions ---------------------------------------
    
    default R visitDictionaryGetExpr(DictionaryGetExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitDictQueryExpr(DictQueryExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitDictMappingExpr(DictMappingExpr expr, C context) {
        return visitExpr(expr, context);
    }
    
    // ------------------------------------------- Parameter Expressions ----------------------------------------
    
    default R visitParameter(Parameter expr, C context) {
        return visitExpr(expr, context);
    }
    
    default R visitPlaceHolderExpr(PlaceHolderExpr expr, C context) {
        return visitExpr(expr, context);
    }
}