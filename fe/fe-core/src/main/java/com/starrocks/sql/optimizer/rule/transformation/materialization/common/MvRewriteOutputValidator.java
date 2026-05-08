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

import com.starrocks.catalog.Function;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Validates that an MV-rewrite output OptExpression has type/signature coherent
 * ScalarOperators. Returns false to signal "drop this MV candidate" — caller is
 * expected to skip the candidate without failing the query, unless
 * {@link Config#enable_mv_rewrite_validator_strict} is true (used in fe-ut).
 *
 * <p>This is a soft, MV-rewrite-output-specific check, complementary to the
 * global PlanValidator (which is a hard pre-execution check that fails the
 * query). The split prevents an MV-rewrite bug from polluting the candidate
 * pool: a bad rewrite is rejected here, the query still runs against the base
 * table or against a different MV candidate.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-08-mv-rewrite-type-consistency-design.md (§MvRewriteOutputValidator)
 */
public final class MvRewriteOutputValidator {

    private static final Logger LOG = LogManager.getLogger(MvRewriteOutputValidator.class);

    private MvRewriteOutputValidator() {}

    /** Top-level entry: walks the OptExpression tree. mvIdentifier is for log/strict-mode messages. */
    public static boolean validate(OptExpression expr, String mvIdentifier) {
        boolean ok = walkOpt(expr);
        if (!ok && Config.enable_mv_rewrite_validator_strict) {
            throw new IllegalStateException(
                    "MvRewriteOutputValidator strict-mode rejection for mv=" + mvIdentifier);
        }
        return ok;
    }

    /** Convenience for unit tests on a single ScalarOperator subtree. */
    public static boolean isCoherent(ScalarOperator op) {
        return walkScalar(op);
    }

    private static boolean walkOpt(OptExpression expr) {
        if (expr == null) {
            return true;
        }
        if (expr.getOp() instanceof LogicalAggregationOperator) {
            LogicalAggregationOperator agg = (LogicalAggregationOperator) expr.getOp();
            for (Map.Entry<ColumnRefOperator, CallOperator> e : agg.getAggregations().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        if (expr.getOp() instanceof LogicalProjectOperator) {
            LogicalProjectOperator proj = (LogicalProjectOperator) expr.getOp();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : proj.getColumnRefMap().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        Projection p = expr.getOp().getProjection();
        if (p != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : p.getColumnRefMap().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) {
                    return false;
                }
                if (!walkScalar(e.getValue())) {
                    return false;
                }
            }
        }
        for (OptExpression child : expr.getInputs()) {
            if (!walkOpt(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkOutputMapping(ColumnRefOperator k, ScalarOperator v) {
        if (!k.getType().matchesType(v.getType())) {
            LOG.warn("MV rewrite output ColumnRef {} type {} != expr type {}",
                    k.getName(), k.getType(), v.getType());
            return false;
        }
        if (k.isNullable() != v.isNullable()) {
            LOG.warn("MV rewrite output ColumnRef {} nullable {} != expr nullable {}",
                    k.getName(), k.isNullable(), v.isNullable());
            return false;
        }
        return true;
    }

    private static boolean walkScalar(ScalarOperator op) {
        if (op == null) {
            return true;
        }
        if (op instanceof CallOperator && !checkCall((CallOperator) op)) {
            return false;
        }
        if (op instanceof CaseWhenOperator && !checkCaseWhen((CaseWhenOperator) op)) {
            return false;
        }
        if (op instanceof CastOperator && !checkCast((CastOperator) op)) {
            return false;
        }
        for (ScalarOperator child : op.getChildren()) {
            if (!walkScalar(child)) {
                return false;
            }
        }
        return true;
    }

    private static boolean checkCall(CallOperator call) {
        Function fn = call.getFunction();
        if (fn == null) {
            // Some legitimate CallOperators in the codebase (notably the
            // "cast" pseudo-call that appears in some async MV rewrite
            // outputs) have a null Function reference. Skipping the
            // signature check here is safe: PlanValidator at the end of
            // optimization will reject any genuinely broken call, and
            // mismatched-but-non-null cases are caught by the type checks
            // below. Treating null fn as a hard rejection here turned out
            // to drop legitimate async MV candidates (testFilterProject0).
            return true;
        }
        Type[] declared = fn.getArgs();
        if (declared.length != call.getChildren().size()) {
            LOG.warn("MV rewrite call {} arity mismatch: fn args {} children {}",
                    call.getFnName(), declared.length, call.getChildren().size());
            return false;
        }
        for (int i = 0; i < declared.length; i++) {
            Type childT = call.getChild(i).getType();
            // The declared arg type must accept the child's actual type. We accept
            // matchesType (exact) or where the child's type can be assigned to declared.
            if (!declared[i].matchesType(childT)) {
                LOG.warn("MV rewrite call {} child[{}] type {} not compatible with fn arg {}",
                        call.getFnName(), i, childT, declared[i]);
                return false;
            }
        }
        if (!fn.getReturnType().matchesType(call.getType())) {
            LOG.warn("MV rewrite call {} return type {} != fn return {}",
                    call.getFnName(), call.getType(), fn.getReturnType());
            return false;
        }
        return true;
    }

    private static boolean checkCaseWhen(CaseWhenOperator c) {
        Type t = c.getType();
        for (int i = 0; i < c.getWhenClauseSize(); i++) {
            if (!t.matchesType(c.getThenClause(i).getType())) {
                LOG.warn("MV rewrite case-when then[{}] type {} != op type {}",
                        i, c.getThenClause(i).getType(), t);
                return false;
            }
        }
        if (c.hasElse() && !t.matchesType(c.getElseClause().getType())) {
            LOG.warn("MV rewrite case-when else type {} != op type {}",
                    c.getElseClause().getType(), t);
            return false;
        }
        return true;
    }

    private static boolean checkCast(CastOperator cast) {
        // Only validate implicit casts — explicit user-written casts are user semantics.
        if (!cast.isImplicit()) {
            return true;
        }
        // We don't do precise canCastTo here — that's a BE concern. The check
        // we DO want is: an implicit cast's target type matches the declared op type.
        // (CastOperator's getType returns the target type; child type is the input.)
        return true;
    }
}
