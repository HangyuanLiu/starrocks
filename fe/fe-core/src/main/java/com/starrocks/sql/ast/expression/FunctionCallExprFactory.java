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

package com.starrocks.sql.ast.expression;

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.sql.analyzer.AnalysisContext;

/**
 * Populates cached typed fields on FunctionCallExpr and registers the resolved Function
 * in the given AnalysisContext.
 * <p>
 * The Function object is NOT stored on FunctionCallExpr.
 * Use AnalysisContext to retrieve the Function by fnId.
 */
public class FunctionCallExprFactory {

    /**
     * Populate all cached typed fields and register in AnalysisContext.
     * Use this when AnalysisContext is available (analysis and transformation paths).
     */
    public static void setFn(FunctionCallExpr expr, Function fn, AnalysisContext ctx) {
        if (ctx != null) {
            ctx.registerFunction(expr, fn);
        }
        setFnFields(expr, fn);
    }

    /**
     * Populate cached typed fields only, without registering in AnalysisContext.
     * Use this for paths that don't need Function lookup later (planner, statistics).
     */
    public static void setFn(FunctionCallExpr expr, Function fn) {
        setFn(expr, fn, null);
    }

    /**
     * Get the resolved Function from AnalysisContext by fnId.
     */
    public static Function getFn(FunctionCallExpr expr, AnalysisContext ctx) {
        if (ctx != null) {
            return ctx.getFunction(expr);
        }
        return null;
    }

    private static void setFnFields(FunctionCallExpr expr, Function fn) {
        expr.setAggregateFn(fn instanceof AggregateFunction);
        expr.setFnNullable(fn.isNullable());
        expr.setFnArgTypes(fn.getArgs());
        expr.setFnHasVarArgs(fn.hasVarArgs());
        expr.setFnNumArgs(fn.getNumArgs());
        expr.setWindowFunction(fn instanceof AggregateFunction && ((AggregateFunction) fn).isAnalyticFn());
    }
}
