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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Base class for ExprVisitor implementations that provides default behavior
 * for visiting expression trees. This class implements the visitor pattern
 * with default traversal behavior that visits all child expressions.
 * 
 * @param <R> The return type of the visit methods
 * @param <C> The context type passed to visit methods
 */
public abstract class ExprVisitorBase<R, C> implements ExprVisitor<R, C> {
    
    /**
     * Default implementation that visits all children of the expression.
     * Subclasses can override this to provide custom traversal behavior.
     * 
     * @param expr the expression to visit
     * @param context the context for the visit
     * @return the result of visiting the expression and its children
     */
    @Override
    public R visitExpr(Expr expr, C context) {
        R result = null;
        
        // Visit all children
        for (Expr child : expr.getChildren()) {
            R childResult = visit(child, context);
            if (childResult != null) {
                result = childResult;
            }
        }
        
        return result;
    }
    
    /**
     * Visit a list of expressions and return the results.
     * 
     * @param exprs the list of expressions to visit
     * @param context the context for the visit
     * @return the list of results from visiting each expression
     */
    public List<R> visitList(List<? extends Expr> exprs, C context) {
        List<R> results = Lists.newArrayList();
        for (Expr expr : exprs) {
            R result = visit(expr, context);
            results.add(result);
        }
        return results;
    }
    
    /**
     * Visit a list of expressions and collect non-null results.
     * 
     * @param exprs the list of expressions to visit
     * @param context the context for the visit
     * @return the list of non-null results from visiting each expression
     */
    public List<R> visitListNonNull(List<? extends Expr> exprs, C context) {
        List<R> results = Lists.newArrayList();
        for (Expr expr : exprs) {
            R result = visit(expr, context);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }
}