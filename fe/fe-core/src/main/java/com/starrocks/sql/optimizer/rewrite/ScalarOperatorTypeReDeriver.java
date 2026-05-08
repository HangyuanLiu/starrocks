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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

/**
 * Bottom-up immutable shuttle that re-derives ScalarOperator types and
 * rebinds CallOperator Function references after a leaf type change.
 *
 * <p>Designed to run over the output of {@code ReplaceColumnRefRewriter}
 * when a leaf substitution crosses type boundaries (e.g. MV rewrite
 * substituting a SMALLINT column with a BIGINT MV column). On any
 * unrecoverable type mismatch, throws {@link TypeReDeriveException}.
 *
 * <p>This visitor has NO MV-specific knowledge. It only knows ScalarOperator
 * type rules. MV-specific concerns (rollup-fn family mapping, candidate
 * rejection plumbing) live in MvColumnRefSubstitutor (added in a later phase).
 *
 * <p>This is the leaves-only skeleton. CallOperator, IF/CaseWhen, Cast, and
 * predicate support are added in subsequent phases.
 */
public final class ScalarOperatorTypeReDeriver
        extends ScalarOperatorVisitor<ScalarOperator, Void> {

    public static ScalarOperator reDerive(ScalarOperator input) {
        return input.accept(new ScalarOperatorTypeReDeriver(), null);
    }

    private ScalarOperatorTypeReDeriver() {}

    @Override
    public ScalarOperator visit(ScalarOperator op, Void ctx) {
        // Default fallback for non-leaf, non-overridden shapes.
        // Filled in by Phase 1.8. For now, pass through unchanged so leaves and
        // simple wrapped shapes can already round-trip.
        return op;
    }

    @Override
    public ScalarOperator visitVariableReference(ColumnRefOperator op, Void ctx) {
        return op;
    }

    @Override
    public ScalarOperator visitConstant(ConstantOperator op, Void ctx) {
        return op;
    }
}
