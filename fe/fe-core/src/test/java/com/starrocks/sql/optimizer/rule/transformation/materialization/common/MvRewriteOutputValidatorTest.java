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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MvRewriteOutputValidatorTest {

    @Test
    void validCallPasses() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.BIGINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assertions.assertTrue(MvRewriteOutputValidator.isCoherent(sum));
    }

    @Test
    void callWithMismatchedArgTypeFails() {
        // CallOperator claims sum(SMALLINT) but its only child is BIGINT.
        // This is the exact bug the validator must catch.
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator badSum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assertions.assertFalse(MvRewriteOutputValidator.isCoherent(badSum));
    }
}
