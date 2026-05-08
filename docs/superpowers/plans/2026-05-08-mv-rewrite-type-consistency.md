# MV Rewrite Type Consistency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop emitting type-incoherent ScalarOperator trees from MV rewrite by introducing (a) a generic bottom-up type re-derivation visitor, (b) an MV-package substitutor that composes leaf substitution with re-derivation, (c) an MV rewrite output validator as a safety net, and replace the per-shape eligibility enumeration in `MaterializedViewRule` with a column-coverage criterion. Apply uniformly to sync and async MV rewrite paths.

**Architecture:** A three-component layered design: `ScalarOperatorTypeReDeriver` (generic, in `optimizer/rewrite/`) handles type re-derivation and `Function` rebinding; `MvColumnRefSubstitutor` (MV-package) wraps `ReplaceColumnRefRewriter` + re-deriver into a single substitution entry point; `MvRewriteOutputValidator` does soft validation at MV rewrite output, rejecting non-coherent candidates with WARN logs and metrics. Eligibility check in `MaterializedViewRule.isMVMatchAggFunctions` simplified to column-coverage. Failure semantics: a rejected MV candidate is dropped silently from the optimizer's pool; query never fails.

**Tech Stack:** Java 11, StarRocks FE (`fe/fe-core`), Gradle (`fe/gradlew`), JUnit 4. Key existing classes: `ScalarOperatorVisitor`, `ReplaceColumnRefRewriter`, `TypeManager`, `ScalarOperatorUtil`, `ExprUtils`.

**Spec:** [`docs/superpowers/specs/2026-05-08-mv-rewrite-type-consistency-design.md`](../specs/2026-05-08-mv-rewrite-type-consistency-design.md)

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/TypeReDeriveException.java` | Unchecked exception thrown by re-deriver on unrecoverable type mismatch |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java` | Generic bottom-up visitor that re-derives ScalarOperator types and rebinds CallOperator functions after leaf substitution |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutor.java` | MV-package wrapper: leaf substitution via `ReplaceColumnRefRewriter` + re-derivation; returns `Optional<ScalarOperator>`; on failure rejects this MV candidate |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidator.java` | Soft validator over a candidate `OptExpression`; checks invariants from spec; returns `boolean` + WARN log + metric increment |
| `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java` | Unit tests for the re-deriver (MV-independent) |
| `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutorTest.java` | Substitutor integration unit tests |
| `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidatorTest.java` | Validator unit tests with hand-built malformed trees |

### Modified files

| Path | Why |
|---|---|
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java` | Sync rewriter routes through substitutor; multi-agg loop fix (`break → continue`) |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRule.java` | Eligibility column-coverage check; wire validator at transform output |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MaterializedViewRewriter.java` | Async path: substitutor swaps; final validate() at output |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/AggregatedMaterializedViewRewriter.java` | Three substitutor swaps |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/EquationRewriter.java` | Function rebinding consolidation |
| `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/AggregateFunctionRewriter.java` | Avg → sum/count rewrite uses substitutor for child re-derivation |
| `fe/fe-core/src/main/java/com/starrocks/common/Config.java` | Add `enable_mv_rewrite_validator_strict` |
| `fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java` | Sync shape matrix + retain regression test |
| `fe/fe-core/src/test/java/com/starrocks/sql/plan/MaterializedViewRewriteTest.java` | Async shape matrix |
| `fe/fe-core/src/test/resources/conf/fe.conf` (or test setup) | Strict mode enabled in fe-ut |

---

## Working Directory and Baseline

All work happens in the worktree at `/Users/harbor/.claude/worktrees/starrocks/nervous-varahamihira-e73c58`. Branch: `claude/nervous-varahamihira-e73c58`. Build commands use `fe/gradlew` per repo convention.

**Verification commands (run from worktree root):**

```bash
# FE-only compile (fast, ~1 min):
fe/gradlew --no-daemon -p fe :fe-core:compileJava

# Run a single test class:
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.foo.BarTest"

# Run a single test method:
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.foo.BarTest.testMethodName"
```

Note: per repo memory, prefer `fe/gradlew` over mvn for all FE builds and tests.

---

## Phase 0 — Baseline & Anchor Test

### Task 0.1: Verify baseline FE compile and existing MV tests pass

**Files:** none

- [ ] **Step 1: Sync to worktree, verify clean tree, compile**

```bash
cd /Users/harbor/.claude/worktrees/starrocks/nervous-varahamihira-e73c58
git status
fe/gradlew --no-daemon -p fe :fe-core:compileJava
```

Expected: clean tree, compile success.

- [ ] **Step 2: Run existing MV tests as a baseline**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest" --tests "com.starrocks.sql.plan.MaterializedViewRewriteTest"
```

Expected: all pass.

- [ ] **Step 3: Commit nothing — this is verification only.**

---

### Task 0.2: Add the regression test as the end-to-end anchor (initially failing the strict mode)

This test is the same one carried by the original case-by-case patch. It's added now as a TDD anchor: it must pass once Phase 4 wires the substitutor into the sync path. Before that, it documents the bug.

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java` (append a new test)

- [ ] **Step 1: Append test method**

Find the last `@Test` method in `MaterializedViewTest.java` (use `grep -n '@Test' fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java | tail -5` to locate). Add inside the class, after the last test:

```java
@Test
public void testSyncMVRewriteIfAggColumnKeepsConsistentType() throws Exception {
    starRocksAssert.withTable("CREATE TABLE if not exists test_mv_consistent_t1 ("
            + "k1 DATE NULL, k2 INT NULL, k3 SMALLINT NULL"
            + ") DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
            + "PROPERTIES('replication_num' = '1')");
    starRocksAssert.withMaterializedView(
            "CREATE MATERIALIZED VIEW test_mv_consistent_mv AS "
            + "SELECT k1, k2, sum(k3) AS sum1 FROM test_mv_consistent_t1 GROUP BY k1, k2");
    waitingRollupJobV2Finish();

    String sql = "SELECT k1, sum(if(k2 = 0, k3, 0)) AS sum_if "
            + "FROM test_mv_consistent_t1 GROUP BY k1";
    String plan = getFragmentPlan(sql);
    Assert.assertTrue("expected MV rollup index in plan, got:\n" + plan,
            plan.contains("test_mv_consistent_mv"));
    // sum's argument must already be widened to BIGINT after substitution.
    // The plan's expression slot for the if branches should not show 'SMALLINT'.
    Assert.assertFalse("rewritten if must not still be SMALLINT, got:\n" + plan,
            plan.contains("if(<slot 2> = 0, CAST(<slot ") && plan.contains(" AS smallint>"));

    starRocksAssert.dropMaterializedView("test_mv_consistent_mv");
    starRocksAssert.dropTable("test_mv_consistent_t1");
}
```

- [ ] **Step 2: Run the test — it must FAIL before the new design is in place**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest.testSyncMVRewriteIfAggColumnKeepsConsistentType"
```

Expected: FAIL (either crash, plan-validator error, or the assertions). Capture and note the failure mode in the commit message.

- [ ] **Step 3: Commit**

```bash
git add fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java
git commit -m "test(mv): add regression for sum(if(...,k3,0)) over sync MV rollup

Anchors design from docs/superpowers/specs/2026-05-08-mv-rewrite-type-consistency-design.md.
Currently fails because ReplaceColumnRefRewriter substitutes k3 -> mv_sum_k3
without re-deriving parent operator types or rebinding sum's signature."
```

---

## Phase 1 — `ScalarOperatorTypeReDeriver` (generic, MV-agnostic)

### Task 1.1: `TypeReDeriveException`

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/TypeReDeriveException.java`

- [ ] **Step 1: Create file**

```java
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// ... (use the standard StarRocks Apache 2.0 header from any sibling file)

package com.starrocks.sql.optimizer.rewrite;

/**
 * Thrown by {@link ScalarOperatorTypeReDeriver} when a node's type or function
 * cannot be re-derived after a leaf type change. Caught and translated to
 * "reject this MV rewrite candidate" by MvColumnRefSubstitutor.
 */
public class TypeReDeriveException extends RuntimeException {
    public TypeReDeriveException(String message) {
        super(message);
    }
    public TypeReDeriveException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

(Copy the exact Apache 2.0 license header used by other files in the same package — open any neighbor in `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/` and reuse the header verbatim.)

- [ ] **Step 2: Verify compile**

```bash
fe/gradlew --no-daemon -p fe :fe-core:compileJava
```

Expected: success.

- [ ] **Step 3: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/TypeReDeriveException.java
git commit -m "feat(mv): add TypeReDeriveException

Carrier exception for the upcoming ScalarOperatorTypeReDeriver. Caught
by MvColumnRefSubstitutor to translate into MV candidate rejection."
```

---

### Task 1.2: ReDeriver skeleton — leaves only

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java`
- Create: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java`

- [ ] **Step 1: Write failing test**

Create `ScalarOperatorTypeReDeriverTest.java`:

```java
package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.Assert;
import org.junit.Test;

public class ScalarOperatorTypeReDeriverTest {

    @Test
    public void columnRef_passes_through() {
        ColumnRefOperator col = new ColumnRefOperator(1, IntegerType.BIGINT, "k3", true);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(col);
        Assert.assertSame(col, out);
        Assert.assertEquals(IntegerType.BIGINT, out.getType());
    }

    @Test
    public void constant_passes_through() {
        ConstantOperator c = ConstantOperator.createInt(42);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(c);
        Assert.assertSame(c, out);
    }
}
```

- [ ] **Step 2: Run — must fail with "cannot find symbol ScalarOperatorTypeReDeriver"**

```bash
fe/gradlew --no-daemon -p fe :fe-core:compileTestJava
```

Expected: FAIL, compilation error referencing `ScalarOperatorTypeReDeriver`.

- [ ] **Step 3: Create the skeleton**

`fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java`:

```java
package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

/**
 * Bottom-up immutable shuttle that re-derives ScalarOperator types and rebinds
 * CallOperator Function references after a leaf type change. Designed to run
 * over the output of {@link ReplaceColumnRefRewriter} when the substitution
 * crosses type boundaries (e.g. MV rewrite substituting a SMALLINT column with
 * a BIGINT MV column).
 *
 * <p>On any unrecoverable type mismatch, throws {@link TypeReDeriveException}.
 *
 * <p>This visitor has NO MV-specific knowledge. It only knows ScalarOperator
 * type rules. MV-specific concerns (rollup-fn family mapping, candidate
 * rejection plumbing) live in MvColumnRefSubstitutor.
 */
public final class ScalarOperatorTypeReDeriver
        extends ScalarOperatorVisitor<ScalarOperator, Void> {

    public static ScalarOperator reDerive(ScalarOperator input) {
        return input.accept(new ScalarOperatorTypeReDeriver(), null);
    }

    private ScalarOperatorTypeReDeriver() {}

    @Override
    public ScalarOperator visit(ScalarOperator op, Void ctx) {
        // Default fallback: only safe if no child types changed.
        // Implemented in Task 1.8.
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
```

- [ ] **Step 4: Run — must pass**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java
git commit -m "feat(mv): add ScalarOperatorTypeReDeriver skeleton (leaves only)"
```

---

### Task 1.3: ReDeriver — `CallOperator` re-resolution

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java`
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java`

- [ ] **Step 1: Add failing tests**

Append to `ScalarOperatorTypeReDeriverTest`:

```java
@Test
public void call_rebinds_function_when_child_type_widens() {
    // Original: add(SMALLINT k3, BIGINT mv_sum_k3) — child types changed.
    // Expect rebound add(BIGINT, BIGINT) returning BIGINT.
    ColumnRefOperator left = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    ConstantOperator right = ConstantOperator.createTinyInt((byte) 1);
    Function origFn = ExprUtils.getBuiltinFunction(FunctionSet.ADD,
            new Type[]{IntegerType.SMALLINT, IntegerType.TINYINT},
            Function.CompareMode.IS_IDENTICAL);
    Assert.assertNotNull(origFn);
    CallOperator call = new CallOperator(FunctionSet.ADD, IntegerType.SMALLINT,
            Lists.newArrayList(left, right), origFn);

    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(call);
    Assert.assertTrue(out instanceof CallOperator);
    CallOperator newCall = (CallOperator) out;
    Assert.assertEquals(IntegerType.BIGINT, newCall.getType());
    Assert.assertEquals(IntegerType.BIGINT, newCall.getFunction().getReturnType());
}

@Test(expected = TypeReDeriveException.class)
public void call_throws_when_no_compatible_function() {
    // Construct a call whose name is bogus → no builtin → throw.
    ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
    CallOperator call = new CallOperator("not_a_real_function", IntegerType.BIGINT,
            Lists.newArrayList(c), null);
    ScalarOperatorTypeReDeriver.reDerive(call);
}
```

Add imports: `com.google.common.collect.Lists`, `com.starrocks.catalog.Function`, `com.starrocks.catalog.FunctionSet`, `com.starrocks.sql.ast.expression.ExprUtils`, `com.starrocks.sql.optimizer.operator.scalar.CallOperator`.

- [ ] **Step 2: Run — first test should fail (visit() falls through, returns op unchanged)**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: 2 new tests fail.

- [ ] **Step 3: Implement `visitCall`**

Add to `ScalarOperatorTypeReDeriver`:

```java
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.type.Type;
import java.util.List;

@Override
public ScalarOperator visitCall(CallOperator call, Void ctx) {
    List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(call.getChildren().size());
    boolean changed = false;
    for (ScalarOperator child : call.getChildren()) {
        ScalarOperator newChild = child.accept(this, ctx);
        if (newChild != child) {
            changed = true;
        }
        newChildren.add(newChild);
    }
    Type[] argTypes = new Type[newChildren.size()];
    for (int i = 0; i < newChildren.size(); i++) {
        argTypes[i] = newChildren.get(i).getType();
    }

    Function newFn = resolveFunction(call.getFnName(), argTypes);
    if (newFn == null) {
        throw new TypeReDeriveException(
                "no compatible function for " + call.getFnName()
                + " with arg types " + java.util.Arrays.toString(argTypes));
    }

    if (!changed && newFn == call.getFunction() && newFn.getReturnType().matchesType(call.getType())) {
        return call;
    }
    CallOperator out = new CallOperator(call.getFnName(), newFn.getReturnType(),
            newChildren, newFn, call.isDistinct(), call.isRemovedDistinct());
    out.setIgnoreNulls(call.getIgnoreNulls());
    return out;
}

private static Function resolveFunction(String name, Type[] argTypes) {
    Function fn = ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_IDENTICAL);
    if (fn != null) {
        return fn;
    }
    fn = ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    if (fn != null) {
        return fn;
    }
    return ExprUtils.getBuiltinFunction(name, argTypes, Function.CompareMode.IS_SUPERTYPE_OF);
}
```

- [ ] **Step 4: Run tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java
git commit -m "feat(mv): re-derive types and rebind functions for CallOperator"
```

---

### Task 1.4: ReDeriver — aggregate specializations (`SUM`, `COUNT→SUM`, `BITMAP_UNION`, `HLL_UNION`, `PERCENTILE_UNION`)

These are MV-rollup-friendly fast paths that the generic `resolveFunction` may not pick the right overload for. Mirror the existing logic in `MaterializedViewRewriter.rewriteAggregateFunc` and the `equivalent/*RewriteEquivalent` classes.

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java`
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java`

- [ ] **Step 1: Failing test for `sum(SMALLINT)` → `sum(BIGINT)`**

Append to test:

```java
@Test
public void call_sum_widens_smallint_to_bigint() {
    ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    Function origSum = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
            new Type[]{IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
    Assert.assertNotNull(origSum);
    CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT, // sum's return is already BIGINT
            Lists.newArrayList(k3), origSum);

    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(sum);
    CallOperator newSum = (CallOperator) out;
    Assert.assertEquals("sum's argType must follow child", IntegerType.BIGINT,
            newSum.getFunction().getArgs()[0]);
    Assert.assertEquals(IntegerType.BIGINT, newSum.getType());
}
```

- [ ] **Step 2: Run — verify failure**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest.call_sum_widens_smallint_to_bigint"
```

Expected: FAIL (the generic resolveFunction picks the IS_IDENTICAL match against SMALLINT or doesn't update args).

- [ ] **Step 3: Add specialization in `visitCall`**

Insert before the generic `resolveFunction` call:

```java
Function newFn = resolveSpecializedAggFn(call.getFnName(), argTypes);
if (newFn == null) {
    newFn = resolveFunction(call.getFnName(), argTypes);
}
```

Add helper:

```java
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;

private static Function resolveSpecializedAggFn(String name, Type[] argTypes) {
    if (FunctionSet.SUM.equalsIgnoreCase(name) && argTypes.length == 1) {
        return ScalarOperatorUtil.findSumFn(argTypes);
    }
    if (FunctionSet.COUNT.equalsIgnoreCase(name) && argTypes.length == 1) {
        return ExprUtils.getBuiltinFunction(FunctionSet.COUNT, argTypes,
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    }
    // BITMAP_UNION / HLL_UNION / PERCENTILE_UNION resolve cleanly via
    // resolveFunction since their arg types are fixed types — no specialization needed here.
    return null;
}
```

- [ ] **Step 4: Run all tests for this class**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java
git commit -m "feat(mv): aggregate function specialization in re-deriver

Use ScalarOperatorUtil.findSumFn for sum, NONSTRICT_SUPERTYPE for count.
Bitmap/HLL/Percentile rollups resolve cleanly via the generic path."
```

---

### Task 1.5: ReDeriver — `IF` and `CaseWhenOperator`

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java`
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java`

- [ ] **Step 1: Failing tests**

Append:

```java
@Test
public void if_branches_unify_to_common_super_type() {
    // if(k2 = 0, mv_sum_k3 (BIGINT), 0 (TINYINT))  → if(BOOL, BIGINT, BIGINT) returning BIGINT
    ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
    BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
            new ColumnRefOperator(2, IntegerType.INT, "k2", true),
            ConstantOperator.createInt(0));
    Function origIf = ExprUtils.getBuiltinFunction(FunctionSet.IF,
            new Type[]{BooleanType.BOOLEAN, IntegerType.SMALLINT, IntegerType.TINYINT},
            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    CallOperator ifOp = new CallOperator(FunctionSet.IF, IntegerType.SMALLINT,
            Lists.newArrayList(cond, mv, zero), origIf);

    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(ifOp);
    CallOperator newIf = (CallOperator) out;
    Assert.assertEquals(IntegerType.BIGINT, newIf.getType());
    Assert.assertEquals(IntegerType.BIGINT, newIf.getChild(1).getType());
    Assert.assertEquals(IntegerType.BIGINT, newIf.getChild(2).getType());
}

@Test
public void casewhen_unifies_value_clauses() {
    // case when k2=0 then mv_sum_k3 (BIGINT) else 0 (TINYINT) end → BIGINT
    ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
    BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
            new ColumnRefOperator(2, IntegerType.INT, "k2", true),
            ConstantOperator.createInt(0));
    CaseWhenOperator caseOp = new CaseWhenOperator(IntegerType.SMALLINT,
            null, zero, Lists.newArrayList(cond, mv));

    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(caseOp);
    Assert.assertTrue(out instanceof CaseWhenOperator);
    CaseWhenOperator newCase = (CaseWhenOperator) out;
    Assert.assertEquals(IntegerType.BIGINT, newCase.getType());
    Assert.assertEquals(IntegerType.BIGINT, newCase.getThenClause(0).getType());
    Assert.assertEquals(IntegerType.BIGINT, newCase.getElseClause().getType());
}
```

Add imports: `BooleanType`, `BinaryPredicateOperator`, `CaseWhenOperator`.

- [ ] **Step 2: Run — both fail**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: the two new tests fail.

- [ ] **Step 3: Implement `IF` (in `visitCall` specialization) and `visitCaseWhenOperator`**

Add in `visitCall`, BEFORE the specialization/resolve calls:

```java
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.type.BooleanType;

if (FunctionSet.IF.equalsIgnoreCase(call.getFnName()) && newChildren.size() == 3) {
    Type t1 = newChildren.get(1).getType();
    Type t2 = newChildren.get(2).getType();
    Type unified = TypeManager.getCommonSuperType(t1, t2);
    if (unified == null || !unified.isValid()) {
        throw new TypeReDeriveException("if branches have no common super type: " + t1 + " vs " + t2);
    }
    ScalarOperator b1 = unified.matchesType(t1) ? newChildren.get(1)
            : new CastOperator(unified, newChildren.get(1), true);
    ScalarOperator b2 = unified.matchesType(t2) ? newChildren.get(2)
            : new CastOperator(unified, newChildren.get(2), true);
    Type[] ifArgs = new Type[]{BooleanType.BOOLEAN, unified, unified};
    Function ifFn = resolveFunction(FunctionSet.IF, ifArgs);
    if (ifFn == null) {
        throw new TypeReDeriveException("no IF builtin for arg types " + java.util.Arrays.toString(ifArgs));
    }
    CallOperator out = new CallOperator(FunctionSet.IF, unified,
            Lists.newArrayList(newChildren.get(0), b1, b2), ifFn);
    out.setIgnoreNulls(call.getIgnoreNulls());
    return out;
}
```

Then add the visitor for `CaseWhenOperator`:

```java
@Override
public ScalarOperator visitCaseWhenOperator(CaseWhenOperator op, Void ctx) {
    ScalarOperator caseClause = op.hasCase()
            ? op.getCaseClause().accept(this, ctx) : null;
    java.util.List<ScalarOperator> whenThen = Lists.newArrayList();
    java.util.List<Type> valueTypes = Lists.newArrayList();
    for (int i = 0; i < op.getWhenClauseSize(); i++) {
        ScalarOperator when = op.getWhenClause(i).accept(this, ctx);
        ScalarOperator then = op.getThenClause(i).accept(this, ctx);
        whenThen.add(when);
        whenThen.add(then);
        valueTypes.add(then.getType());
    }
    ScalarOperator elseClause = op.hasElse() ? op.getElseClause().accept(this, ctx) : null;
    if (elseClause != null) {
        valueTypes.add(elseClause.getType());
    }
    Type unified = TypeManager.getCompatibleTypeForCaseWhen(valueTypes);
    if (unified == null || !unified.isValid()) {
        throw new TypeReDeriveException("case-when branches have no compatible type: " + valueTypes);
    }
    java.util.List<ScalarOperator> alignedWhenThen = Lists.newArrayListWithCapacity(whenThen.size());
    for (int i = 0; i < whenThen.size(); i += 2) {
        ScalarOperator then = whenThen.get(i + 1);
        if (!unified.matchesType(then.getType())) {
            then = new CastOperator(unified, then, true);
        }
        alignedWhenThen.add(whenThen.get(i));
        alignedWhenThen.add(then);
    }
    if (elseClause != null && !unified.matchesType(elseClause.getType())) {
        elseClause = new CastOperator(unified, elseClause, true);
    }
    return new CaseWhenOperator(unified, caseClause, elseClause, alignedWhenThen);
}
```

- [ ] **Step 4: Run tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java
git commit -m "feat(mv): re-derive IF and CaseWhen with branch unification"
```

---

### Task 1.6: ReDeriver — `CastOperator` (preserve explicit, drop redundant implicit)

**Files:**
- Modify: `ScalarOperatorTypeReDeriver.java`
- Modify: `ScalarOperatorTypeReDeriverTest.java`

- [ ] **Step 1: Failing tests**

```java
@Test
public void explicit_cast_target_preserved_unconditionally() {
    ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    CastOperator cast = new CastOperator(IntegerType.SMALLINT, mv, false /* explicit */);
    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(cast);
    Assert.assertTrue(out instanceof CastOperator);
    Assert.assertEquals(IntegerType.SMALLINT, out.getType());
}

@Test
public void implicit_cast_dropped_when_child_already_target_type() {
    ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    CastOperator cast = new CastOperator(IntegerType.BIGINT, mv, true /* implicit */);
    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(cast);
    Assert.assertSame(mv, out);
}
```

- [ ] **Step 2: Run — fail**

- [ ] **Step 3: Implement**

```java
@Override
public ScalarOperator visitCastOperator(CastOperator op, Void ctx) {
    ScalarOperator newChild = op.getChild(0).accept(this, ctx);
    if (!op.isImplicit()) {
        if (newChild == op.getChild(0)) {
            return op;
        }
        return new CastOperator(op.getType(), newChild, false);
    }
    // Implicit cast: drop if redundant.
    if (op.getType().matchesType(newChild.getType())) {
        return newChild;
    }
    if (newChild == op.getChild(0)) {
        return op;
    }
    return new CastOperator(op.getType(), newChild, true);
}
```

- [ ] **Step 4: Run tests**

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add ...
git commit -m "feat(mv): re-derive CastOperator preserving user-written casts"
```

---

### Task 1.7: ReDeriver — predicates (binary, in)

**Files:** same.

- [ ] **Step 1: Failing test**

```java
@Test
public void binary_predicate_aligns_children_with_common_super_type() {
    ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
    BinaryPredicateOperator pred = BinaryPredicateOperator.eq(mv, zero);

    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(pred);
    Assert.assertTrue(out instanceof BinaryPredicateOperator);
    BinaryPredicateOperator newPred = (BinaryPredicateOperator) out;
    Assert.assertEquals(BooleanType.BOOLEAN, newPred.getType());
    Assert.assertEquals(IntegerType.BIGINT, newPred.getChild(0).getType());
    Assert.assertEquals(IntegerType.BIGINT, newPred.getChild(1).getType());
}
```

- [ ] **Step 2: Run — fail**

- [ ] **Step 3: Implement**

```java
@Override
public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator op, Void ctx) {
    ScalarOperator l = op.getChild(0).accept(this, ctx);
    ScalarOperator r = op.getChild(1).accept(this, ctx);
    Type unified = TypeManager.getCommonSuperType(l.getType(), r.getType());
    if (unified == null || !unified.isValid()) {
        throw new TypeReDeriveException(
                "binary predicate has no common super type: " + l.getType() + " vs " + r.getType());
    }
    if (!unified.matchesType(l.getType())) {
        l = new CastOperator(unified, l, true);
    }
    if (!unified.matchesType(r.getType())) {
        r = new CastOperator(unified, r, true);
    }
    if (l == op.getChild(0) && r == op.getChild(1)) {
        return op;
    }
    return new BinaryPredicateOperator(op.getBinaryType(), l, r);
}

@Override
public ScalarOperator visitInPredicate(InPredicateOperator op, Void ctx) {
    java.util.List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(op.getChildren().size());
    boolean changed = false;
    java.util.List<Type> types = Lists.newArrayList();
    for (ScalarOperator child : op.getChildren()) {
        ScalarOperator nc = child.accept(this, ctx);
        if (nc != child) changed = true;
        newChildren.add(nc);
        types.add(nc.getType());
    }
    if (!changed) return op;
    Type unified = TypeManager.getCompatibleTypeForBinary(types.get(0), types.get(1));
    for (int i = 2; i < types.size(); i++) {
        unified = TypeManager.getCompatibleTypeForBinary(unified, types.get(i));
    }
    if (unified == null || !unified.isValid()) {
        throw new TypeReDeriveException("IN has no common type: " + types);
    }
    java.util.List<ScalarOperator> aligned = Lists.newArrayListWithCapacity(newChildren.size());
    for (ScalarOperator c : newChildren) {
        aligned.add(unified.matchesType(c.getType()) ? c : new CastOperator(unified, c, true));
    }
    return new InPredicateOperator(op.isNotIn(), aligned.toArray(new ScalarOperator[0]));
}
```

(For `Compound`, `IsNull`, `Like`, `Between`: the default `visit()` recursion is sufficient because their type rules don't depend on child type alignment beyond what the child operators handle themselves. We add them in Task 1.8's default fallback.)

- [ ] **Step 4: Run tests; commit**

```bash
git add ...
git commit -m "feat(mv): re-derive predicates with implicit casts on children"
```

---

### Task 1.8: ReDeriver — default fallback (conservative throw on unknown shapes)

**Files:** same.

- [ ] **Step 1: Failing test**

```java
@Test
public void unknown_shape_passes_through_when_no_change() {
    // A SubfieldOperator whose child types are unchanged.
    // Build a minimal Subfield via constructor; if no easy constructor, replace test
    // with another concrete operator class. See ScalarOperator hierarchy.
    ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
    // For this test, use IsNullPredicateOperator as a representative shape we don't specialize.
    IsNullPredicateOperator inull = new IsNullPredicateOperator(false, c);
    ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(inull);
    Assert.assertSame(inull, out);
}

@Test(expected = TypeReDeriveException.class)
public void unknown_shape_throws_when_child_changed() {
    // Use IsNull around an implicit cast that gets dropped: now the IsNull's child
    // identity changed → default fallback must throw.
    ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
    CastOperator implicit = new CastOperator(IntegerType.BIGINT, c, true); // redundant cast
    IsNullPredicateOperator inull = new IsNullPredicateOperator(false, implicit);
    ScalarOperatorTypeReDeriver.reDerive(inull);
}
```

- [ ] **Step 2: Run — should fail (current `visit()` returns op unchanged so the throw test fails)**

- [ ] **Step 3: Implement default**

Replace the empty `visit(...)` with:

```java
@Override
public ScalarOperator visit(ScalarOperator op, Void ctx) {
    if (op.getChildren().isEmpty()) {
        return op;
    }
    java.util.List<ScalarOperator> newChildren = Lists.newArrayListWithCapacity(op.getChildren().size());
    boolean changed = false;
    for (ScalarOperator child : op.getChildren()) {
        ScalarOperator nc = child.accept(this, ctx);
        if (nc != child) {
            changed = true;
        }
        newChildren.add(nc);
    }
    if (!changed) {
        return op;
    }
    throw new TypeReDeriveException(
            "default ReDeriver fallback refuses to re-emit " + op.getClass().getSimpleName()
            + " after child types changed (op=" + op.debugString() + ")");
}
```

- [ ] **Step 4: Run tests**

Expected: all 1.x tests pass.

- [ ] **Step 5: Commit**

```bash
git add ...
git commit -m "feat(mv): conservative default fallback in re-deriver

Pass through when no child changed; otherwise throw and let validator
handle as a candidate-rejection signal."
```

---

## Phase 2 — `MvColumnRefSubstitutor`

### Task 2.1: Substitutor entry point with output-ColumnRef sync

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutor.java`
- Create: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutorTest.java`

- [ ] **Step 1: Write failing test**

```java
package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class MvColumnRefSubstitutorTest {

    @Test
    public void substitute_widens_sum_after_leaf_swap() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.SMALLINT, "k3", true);
        ColumnRefOperator mvSumK3 = new ColumnRefOperator(101, IntegerType.BIGINT, "mv_sum_k3", true);
        Function origSum = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[]{IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), origSum);

        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(k3, mvSumK3);
        Optional<ScalarOperator> out = MvColumnRefSubstitutor.substitute(sum, map);

        Assert.assertTrue(out.isPresent());
        CallOperator newSum = (CallOperator) out.get();
        Assert.assertEquals(IntegerType.BIGINT, newSum.getFunction().getArgs()[0]);
    }

    @Test
    public void substitute_returns_empty_when_re_derive_fails() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.SMALLINT, "k3", true);
        ColumnRefOperator mv = new ColumnRefOperator(101, IntegerType.BIGINT, "mv", true);
        // Build a CallOperator with a bogus function name → resolver fails.
        CallOperator bogus = new CallOperator("not_a_real_function", IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), null);
        Map<ColumnRefOperator, ScalarOperator> map = ImmutableMap.of(k3, mv);
        Optional<ScalarOperator> out = MvColumnRefSubstitutor.substitute(bogus, map);
        Assert.assertFalse(out.isPresent());
    }
}
```

- [ ] **Step 2: Run — failing**

```bash
fe/gradlew --no-daemon -p fe :fe-core:compileTestJava
```

Expected: compile errors.

- [ ] **Step 3: Create the substitutor**

```java
package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriver;
import com.starrocks.sql.optimizer.rewrite.TypeReDeriveException;

import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * MV-rewrite-specific substitutor. Wraps leaf substitution via
 * {@link ReplaceColumnRefRewriter} and bottom-up type re-derivation via
 * {@link ScalarOperatorTypeReDeriver} into a single entry point.
 *
 * <p>Returns {@link Optional#empty()} when the re-deriver throws — caller
 * treats this as "skip this MV candidate", not as a query failure.
 */
public final class MvColumnRefSubstitutor {

    private static final Logger LOG = Logger.getLogger(MvColumnRefSubstitutor.class.getName());

    private MvColumnRefSubstitutor() {}

    public static Optional<ScalarOperator> substitute(
            ScalarOperator expr,
            Map<ColumnRefOperator, ScalarOperator> columnMap) {
        ReplaceColumnRefRewriter replacer = new ReplaceColumnRefRewriter(columnMap);
        ScalarOperator afterReplace = replacer.rewrite(expr);
        try {
            return Optional.of(ScalarOperatorTypeReDeriver.reDerive(afterReplace));
        } catch (TypeReDeriveException e) {
            LOG.fine(() -> "MvColumnRefSubstitutor rejected expr after leaf substitution: "
                    + e.getMessage());
            return Optional.empty();
        }
    }

    /** Convenience: also synchronize an output ColumnRef's type to match the rewritten expression. */
    public static Optional<ScalarOperator> substituteAndSyncOutput(
            ColumnRefOperator outputRef,
            ScalarOperator expr,
            Map<ColumnRefOperator, ScalarOperator> columnMap) {
        Optional<ScalarOperator> out = substitute(expr, columnMap);
        out.ifPresent(s -> {
            outputRef.setType(s.getType());
            outputRef.setNullable(s.isNullable());
        });
        return out;
    }
}
```

- [ ] **Step 4: Run tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvColumnRefSubstitutorTest"
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutor.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutorTest.java
git commit -m "feat(mv): add MvColumnRefSubstitutor

Single entry point for MV column substitution. Composes leaf replacement
with bottom-up type re-derivation; returns Optional.empty() on failure
to signal candidate rejection."
```

---

## Phase 3 — `MvRewriteOutputValidator`

### Task 3.1: FE config flag for strict mode

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/common/Config.java`

- [ ] **Step 1: Add the config option**

Locate the config block for MV-related toggles (search `enable_materialized_view_rewrite` for proximity):

```bash
grep -n "enable_materialized_view_rewrite" fe/fe-core/src/main/java/com/starrocks/common/Config.java | head -5
```

Add nearby:

```java
@ConfField(mutable = true, comment = "When true, an MV rewrite output that fails the "
        + "MvRewriteOutputValidator will throw IllegalStateException instead of being "
        + "silently dropped from the candidate pool. Intended for fe-ut to catch gaps "
        + "in ScalarOperatorTypeReDeriver. Production default: false.")
public static boolean enable_mv_rewrite_validator_strict = false;
```

- [ ] **Step 2: Compile**

```bash
fe/gradlew --no-daemon -p fe :fe-core:compileJava
```

Expected: success.

- [ ] **Step 3: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/common/Config.java
git commit -m "feat(mv): add enable_mv_rewrite_validator_strict config"
```

---

### Task 3.2: Validator skeleton + CallOperator signature check

**Files:**
- Create: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidator.java`
- Create: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidatorTest.java`

- [ ] **Step 1: Write failing test**

```java
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
import org.junit.Assert;
import org.junit.Test;

public class MvRewriteOutputValidatorTest {

    @Test
    public void valid_call_passes() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[]{IntegerType.BIGINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assert.assertTrue(MvRewriteOutputValidator.isCoherent(sum));
    }

    @Test
    public void call_with_mismatched_arg_type_fails() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        // Construct a CallOperator that claims sum(SMALLINT) but has BIGINT child — the bug.
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[]{IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator badSum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assert.assertFalse(MvRewriteOutputValidator.isCoherent(badSum));
    }
}
```

- [ ] **Step 2: Run — fails (class missing)**

- [ ] **Step 3: Create validator**

```java
package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.starrocks.catalog.Function;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
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
 * ScalarOperators. Returns false instead of throwing (unless strict mode is on)
 * to signal "drop this MV candidate".
 *
 * <p>Layered alongside the global PlanValidator, not in place of it.
 */
public final class MvRewriteOutputValidator {

    private static final Logger LOG = LogManager.getLogger(MvRewriteOutputValidator.class);

    private MvRewriteOutputValidator() {}

    /** Public entry-point for top-level OptExpression. */
    public static boolean validate(OptExpression expr, String mvIdentifier) {
        boolean ok = walkOpt(expr);
        if (!ok && Config.enable_mv_rewrite_validator_strict) {
            throw new IllegalStateException(
                    "MvRewriteOutputValidator strict-mode rejection for mv=" + mvIdentifier);
        }
        return ok;
    }

    /** Convenience for unit tests on a single ScalarOperator. */
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
                if (!checkOutputMapping(e.getKey(), e.getValue())) return false;
                if (!walkScalar(e.getValue())) return false;
            }
        }
        Projection p = expr.getOp().getProjection();
        if (p != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> e : p.getColumnRefMap().entrySet()) {
                if (!checkOutputMapping(e.getKey(), e.getValue())) return false;
                if (!walkScalar(e.getValue())) return false;
            }
        }
        for (OptExpression child : expr.getInputs()) {
            if (!walkOpt(child)) return false;
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
        if (op == null) return true;
        if (op instanceof CallOperator && !checkCall((CallOperator) op)) return false;
        if (op instanceof CaseWhenOperator && !checkCaseWhen((CaseWhenOperator) op)) return false;
        if (op instanceof CastOperator && !checkCast((CastOperator) op)) return false;
        for (ScalarOperator child : op.getChildren()) {
            if (!walkScalar(child)) return false;
        }
        return true;
    }

    private static boolean checkCall(CallOperator call) {
        Function fn = call.getFunction();
        if (fn == null) {
            LOG.warn("MV rewrite call {} has null function", call.getFnName());
            return false;
        }
        Type[] declared = fn.getArgs();
        if (declared.length != call.getChildren().size()) {
            LOG.warn("MV rewrite call {} arity mismatch: fn args {} children {}",
                    call.getFnName(), declared.length, call.getChildren().size());
            return false;
        }
        for (int i = 0; i < declared.length; i++) {
            Type childT = call.getChild(i).getType();
            if (!Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF.toString().isEmpty()
                    && !declared[i].matchesType(childT)
                    && !ExprUtils.canCastTo(childT, declared[i])) {
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
        if (!cast.isImplicit()) return true;
        Type child = cast.getChild(0).getType();
        if (!ExprUtils.canCastTo(child, cast.getType())) {
            LOG.warn("MV rewrite implicit cast target {} not compatible with child {}",
                    cast.getType(), child);
            return false;
        }
        return true;
    }
}
```

Note: `ExprUtils.canCastTo` may not exist verbatim. If the build fails, replace its uses with `Type.canCastTo(child, target)` or the appropriate existing helper — search:

```bash
grep -rn "canCastTo\b" fe/fe-core/src/main/java/com/starrocks/sql/ast/expression/ | head -3
grep -rn "canCastTo\b" fe/fe-core/src/main/java/com/starrocks/type/ | head -5
```

If neither exists, drop the explicit `canCastTo` checks and rely on `matchesType` only (more permissive; the strict mode CI catches real mismatches).

- [ ] **Step 4: Compile + run tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvRewriteOutputValidatorTest"
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add ...
git commit -m "feat(mv): add MvRewriteOutputValidator with CallOperator check"
```

---

### Task 3.3: Validator — CaseWhen, Cast, output ColumnRef sync, agg child compatibility (already in 3.2)

The walker in 3.2 already handles these. Add focused tests:

**Files:**
- Modify: `MvRewriteOutputValidatorTest.java`

- [ ] **Step 1: Failing tests**

```java
@Test
public void casewhen_branch_mismatch_fails() {
    ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
    ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
    BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
            new ColumnRefOperator(2, IntegerType.INT, "k2", true),
            ConstantOperator.createInt(0));
    // case-when claims SMALLINT but then-branch is BIGINT.
    CaseWhenOperator bad = new CaseWhenOperator(IntegerType.SMALLINT,
            null, zero, Lists.newArrayList(cond, k3));
    Assert.assertFalse(MvRewriteOutputValidator.isCoherent(bad));
}

@Test
public void implicit_cast_to_unrelated_type_fails() {
    ColumnRefOperator s = new ColumnRefOperator(1, com.starrocks.type.StringType.STRING, "s", true);
    CastOperator bad = new CastOperator(IntegerType.BIGINT, s, true);
    Assert.assertTrue("STRING→BIGINT is castable; this should pass",
            MvRewriteOutputValidator.isCoherent(bad));
}
```

(The second is intentionally a passing case after handling. If `canCastTo` was dropped, both should pass.)

- [ ] **Step 2: Run — verify**

- [ ] **Step 3: Commit (no impl change beyond tests)**

```bash
git add ...
git commit -m "test(mv): expand validator unit tests for case-when / cast"
```

---

## Phase 4 — Sync MV path migration

### Task 4.1: Wire `MvColumnRefSubstitutor` into `MaterializedViewRewriter.visitLogicalProject`

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java`

- [ ] **Step 1: Read current method**

```bash
sed -n '79,110p' fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java
```

- [ ] **Step 2: Replace the in-method substitution**

Find `visitLogicalProject` (line ~79). Replace `ReplaceColumnRefRewriter` instantiation + `replaceColumnRefRewriter.rewrite(kv.getValue())` with `MvColumnRefSubstitutor.substituteAndSyncOutput(...)`. If the result is empty, propagate "this candidate failed" by returning the original `optExpression` unchanged AND setting a `failed` flag on a new field of the visitor.

Add to the class:

```java
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvColumnRefSubstitutor;

private boolean substitutionFailed = false;
public boolean substitutionFailed() { return substitutionFailed; }
```

In `visitLogicalProject`, replace the inner substitution:

```java
Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
replaceMap.put(context.queryColumnRef, context.mvColumnRef);
Optional<ScalarOperator> rewritten = MvColumnRefSubstitutor.substituteAndSyncOutput(
        queryColRef, kv.getValue(), replaceMap);
if (!rewritten.isPresent()) {
    substitutionFailed = true;
    return optExpression;
}
newProjectMap.put(queryColRef, rewritten.get());
```

(Adjust to fit the actual surrounding control flow — check the method body for the loop / conditional structure.)

- [ ] **Step 3: Compile + run regression test from Task 0.2**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest.testSyncMVRewriteIfAggColumnKeepsConsistentType"
```

Expected: still failing (we haven't touched aggregate yet). Take note of the new failure mode.

- [ ] **Step 4: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java
git commit -m "refactor(mv): route project-layer substitution through MvColumnRefSubstitutor"
```

---

### Task 4.2: Wire substitutor into `visitLogicalTableScan`

**Files:** same.

- [ ] **Step 1: Read method**

```bash
sed -n '112,135p' fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java
```

- [ ] **Step 2: If the scan's projection is non-null, route each output expression through the substitutor**

This mirrors the same pattern. Add a private `rewriteScanProjection(...)` helper that maps each `(outputCol, expr)` through `MvColumnRefSubstitutor.substituteAndSyncOutput`. On any failure, set `substitutionFailed = true` and return the original projection unchanged.

```java
import com.starrocks.sql.optimizer.operator.Projection;

private Projection rewriteScanProjection(Projection projection, MaterializedViewRule.RewriteContext context) {
    if (projection == null) return null;
    Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
    replaceMap.put(context.queryColumnRef, context.mvColumnRef);
    Map<ColumnRefOperator, ScalarOperator> newColMap = new HashMap<>();
    boolean changed = false;
    for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : projection.getColumnRefMap().entrySet()) {
        ScalarOperator expr = kv.getValue();
        if (!expr.getUsedColumns().contains(context.queryColumnRef)) {
            newColMap.put(kv.getKey(), expr);
            continue;
        }
        Optional<ScalarOperator> rewritten = MvColumnRefSubstitutor.substituteAndSyncOutput(
                kv.getKey(), expr, replaceMap);
        if (!rewritten.isPresent()) {
            substitutionFailed = true;
            return projection;
        }
        newColMap.put(kv.getKey(), rewritten.get());
        changed = true;
    }
    if (!changed) return projection;
    return new Projection(newColMap, projection.getCommonSubOperatorMap(),
            projection.needReuseLambdaDependentExpr());
}
```

In `visitLogicalTableScan`, when building the new `LogicalOlapScanOperator`, set its projection via `.setProjection(rewriteScanProjection(scan.getProjection(), context))`.

- [ ] **Step 3: Compile**

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "refactor(mv): route scan-projection rewrite through substitutor"
```

---

### Task 4.3: Wire substitutor into `visitLogicalAggregate`; layer `rewriteAggregateFunc` on top

**Files:** same.

- [ ] **Step 1: Read current method**

```bash
sed -n '195,260p' fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRewriter.java
```

- [ ] **Step 2: Replace the substitution + agg-fn rebind logic**

The new `visitLogicalAggregate` flow:

1. Build the leaf substitution map for ALL `RewriteContext`s applicable to this aggregate node (one MV may register multiple query-col → mv-col mappings for one indexId).
2. For each agg in `aggregations`:
   - If its arg uses any query column we substitute, run the entire arg through `MvColumnRefSubstitutor.substituteAndSyncOutput(outAggRef, aggCallOp, map)`. The result is a new `CallOperator` with re-derived children + re-bound function.
   - On top of that, if the agg's function family must be remapped to a rollup family (e.g. `COUNT(col)` → `SUM(mv_count_col)`, or `BITMAP_UNION(to_bitmap(col))` → `BITMAP_UNION(mv_bitmap_col)`), call `rewriteAggregateFunc(...)` to produce the rollup form. Pass the substituted-coherent `CallOperator` to `rewriteAggregateFunc`, not the raw original.
3. Use `continue` (not `break`) so all aggs under the same query column get rewritten.

Concrete code:

```java
import java.util.Optional;

@Override
public OptExpression visitLogicalAggregate(OptExpression optExpression,
                                            MaterializedViewRule.RewriteContext context) {
    LogicalAggregationOperator agg = (LogicalAggregationOperator) optExpression.getOp();

    Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
    replaceMap.put(context.queryColumnRef, context.mvColumnRef);
    ReplaceColumnRefRewriter legacyReplacer = new ReplaceColumnRefRewriter(replaceMap);

    Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>();
    for (Map.Entry<ColumnRefOperator, CallOperator> kv : agg.getAggregations().entrySet()) {
        CallOperator queryAggFunc = kv.getValue();
        if (!queryAggFunc.getUsedColumns().contains(context.queryColumnRef)) {
            newAggMap.put(kv.getKey(), queryAggFunc);
            continue;
        }
        // Step 1: substitute children + re-derive types.
        Optional<ScalarOperator> substituted =
                MvColumnRefSubstitutor.substituteAndSyncOutput(kv.getKey(), queryAggFunc, replaceMap);
        if (!substituted.isPresent() || !(substituted.get() instanceof CallOperator)) {
            substitutionFailed = true;
            return optExpression;
        }
        CallOperator coherent = (CallOperator) substituted.get();

        // Step 2: rollup family mapping (sum→sum, count→sum, bitmap_union→bitmap_union, etc.)
        // Existing helper rewriteAggregateFunc handles this when child is a column ref directly.
        // For our new flow we feed the coherent op through it; if it returns null, keep `coherent` as-is.
        CallOperator rollup = rewriteAggregateFunc(legacyReplacer, context.mvColumn, coherent);
        newAggMap.put(kv.getKey(), rollup != null ? rollup : coherent);
    }
    return OptExpression.create(new LogicalAggregationOperator(
            agg.getType(),
            agg.getGroupingKeys(),
            agg.getPartitionByColumns(),
            newAggMap,
            agg.isSplit(),
            agg.getLimit(),
            agg.getPredicate()), optExpression.getInputs());
}
```

This collapses the existing `rewriteAggregateFunc(...)` branch + the patch's `normalizeSumFunction` branch into a single layered flow.

- [ ] **Step 3: Run regression test**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest.testSyncMVRewriteIfAggColumnKeepsConsistentType"
```

Expected: PASS.

- [ ] **Step 4: Run all sync MV tests for regression baseline**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ...
git commit -m "refactor(mv): route aggregate-arg substitution through substitutor

visitLogicalAggregate now layers (substitute + re-derive) under the
existing rewriteAggregateFunc rollup-family mapper. break -> continue
fix lets multiple aggs over the same MV column all get rewritten."
```

---

### Task 4.4: Wire validator at `MaterializedViewRule.transform` output + propagate substitutor failure

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRule.java`

- [ ] **Step 1: Locate transform output**

```bash
grep -n "MaterializedViewRewriter\|public.*transform" fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRule.java | head -10
```

- [ ] **Step 2: Modify transform to skip candidate on substitution failure or validator failure**

Wherever `transform` invokes `new MaterializedViewRewriter(context).rewrite(input)` (or equivalent), wrap:

```java
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvRewriteOutputValidator;

MaterializedViewRewriter rewriter = new MaterializedViewRewriter(context);
OptExpression rewritten = rewriter.rewrite(input);
if (rewriter.substitutionFailed()
        || !MvRewriteOutputValidator.validate(
                rewritten, "indexId=" + context.indexId)) {
    continue; // skip this MV candidate
}
```

(If `transform` doesn't have a candidate loop, locate the iteration over MV indices and apply the same skip semantics there.)

- [ ] **Step 3: Run all sync MV tests**

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "feat(mv): validate sync rewrite output and drop incoherent candidates"
```

---

### Task 4.5: Generalize `isMVMatchAggFunctions` to column-coverage

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRule.java`

- [ ] **Step 1: Read current method**

```bash
sed -n '906,996p' fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/mv/MaterializedViewRule.java
```

- [ ] **Step 2: Add the column-coverage path next to the existing direct-column path**

The existing method has two branches: `queryFnChild0 instanceof ColumnRefOperator` (direct-column rollup) and the rest (currently handled by patch's `addCaseWhenRewriteContexts`, which we replace). Replace the non-direct branch with `canCoverByMv`:

```java
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
// existing imports stay

private boolean canCoverByMv(ScalarOperator expr,
                              String queryFnName,
                              MaterializedIndexMeta candidateIndexMeta,
                              Map<String, Integer> columnToIds,
                              Long indexId) {
    if (containsForbiddenShape(expr)) {
        return false;
    }
    int[] leafIds = expr.getUsedColumns().getColumnIds();
    if (leafIds.length == 0) {
        return true; // constant arg
    }
    for (int qid : leafIds) {
        Column matched = null;
        ColumnRefOperator mvColRef = null;
        for (Column mvCol : candidateIndexMeta.getSchema()) {
            Integer mvCid = columnToIds.get(mvCol.getName());
            if (mvCid == null) continue;
            if (factory.getColumnRef(mvCid) == null) continue;
            // Match a direct (non-aggregated) MV column whose physical mapping is qid.
            if (!mvCol.isAggregated() && getMVColumnToQueryColumnId(columnToIds, mvCol) == qid) {
                matched = mvCol;
                mvColRef = factory.getColumnRef(mvCid);
                break;
            }
            // Match an aggregated MV column with a compatible rollup family.
            if (mvCol.isAggregated()
                    && getMVColumnToQueryColumnId(columnToIds, mvCol) == qid
                    && rollupFamilyMatches(queryFnName, mvCol)) {
                matched = mvCol;
                mvColRef = factory.getColumnRef(mvCid);
                break;
            }
        }
        if (matched == null) {
            return false;
        }
        ColumnRefOperator queryRef = factory.getColumnRef(qid);
        if (queryRef == null) return false;
        if (!factory.getRelationId(queryRef.getId()).equals(factory.getRelationId(mvColRef.getId()))) {
            return false;
        }
        addRewriteContextDedup(indexId, /* queryFn */ null, queryRef, mvColRef, matched);
    }
    return true;
}

private boolean containsForbiddenShape(ScalarOperator expr) {
    if (expr instanceof SubqueryOperator) return true;
    if (expr instanceof LambdaFunctionOperator) return true;
    if (expr instanceof CallOperator) {
        CallOperator c = (CallOperator) expr;
        if (c.getFunction() != null && !c.getFunction().isDeterministic()) return true;
    }
    for (ScalarOperator child : expr.getChildren()) {
        if (containsForbiddenShape(child)) return true;
    }
    return false;
}

private static boolean rollupFamilyMatches(String queryFnName, Column mvCol) {
    AggregateType aggT = mvCol.getAggregationType();
    if (aggT == null) return false;
    String name = queryFnName.toLowerCase();
    switch (aggT) {
        case SUM: return FunctionSet.SUM.equals(name) || FunctionSet.COUNT.equals(name);
        case MIN: return FunctionSet.MIN.equals(name);
        case MAX: return FunctionSet.MAX.equals(name);
        case BITMAP_UNION: return FunctionSet.BITMAP_UNION.equals(name)
                || FunctionSet.BITMAP_UNION_COUNT.equals(name);
        case HLL_UNION: return FunctionSet.HLL_UNION.equals(name)
                || FunctionSet.HLL_UNION_AGG.equals(name);
        case PERCENTILE_UNION: return FunctionSet.PERCENTILE_UNION.equals(name);
        default: return false;
    }
}

private void addRewriteContextDedup(Long indexId, CallOperator queryFn,
                                     ColumnRefOperator queryColumnRef,
                                     ColumnRefOperator mvColumnRef,
                                     Column mvColumn) {
    List<RewriteContext> ctxs = mvIdToRewriteContexts.computeIfAbsent(indexId, k -> Lists.newArrayList());
    boolean exists = ctxs.stream().anyMatch(c ->
            c.queryColumnRef.equals(queryColumnRef) && c.mvColumnRef.equals(mvColumnRef));
    if (!exists) {
        ctxs.add(new RewriteContext(queryFn, queryColumnRef, mvColumnRef, mvColumn));
    }
}
```

In `isMVMatchAggFunctions`, replace the existing non-direct-column branch:

```java
// Old: if (queryFnChild0 instanceof CaseWhenOperator || isIfCall) addCaseWhenRewriteContexts(...);
// New:
if (!(queryFnChild0 instanceof ColumnRefOperator)) {
    return canCoverByMv(queryFnChild0, queryFnName, candidateIndexMeta, columnToIds, indexId);
}
```

Keep the existing direct-`ColumnRefOperator` branch as-is.

- [ ] **Step 3: Run all sync MV tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest"
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "refactor(mv): replace if/case eligibility enumeration with column-coverage

Sync MV rewrite eligibility now accepts any expression whose leaf
ColumnRefs are 1:1 covered by MV columns with rollup-family-compatible
aggregates, plus a forbidden-shape pre-screen for subqueries / lambdas /
nondeterministic calls. Type coherence is delegated to the substitutor."
```

---

## Phase 5 — Async MV path migration

### Task 5.1: Audit async-rewrite call sites and route through substitutor

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MaterializedViewRewriter.java` (async)
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/AggregatedMaterializedViewRewriter.java`

- [ ] **Step 1: List all `new ReplaceColumnRefRewriter(...)` sites in async path**

```bash
grep -rn "new ReplaceColumnRefRewriter" fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/
```

For each MV-internal substitution (i.e., where the map is queryColumn → mvColumn), wrap the call site with `MvColumnRefSubstitutor.substitute(...)` instead. Skip sites where the substitution is for non-MV purposes (e.g., `OptExpressionDuplicator` is a generic clone; leave it).

Concrete sites to modify (verify with above grep — line numbers may have shifted):
- `MaterializedViewRewriter` (async) lines ~2013, ~2374
- `AggregatedMaterializedViewRewriter` lines ~268, ~626, ~640
- `EquationRewriter` line ~349 (`findArithmeticFunction` already does fn rebinding; refactor to call `ScalarOperatorTypeReDeriver.reDerive(call)` for type+fn coherence in one shot)
- `AggregateFunctionRewriter.rewriteAvg` — consolidate to use ReDeriver for child re-derivation before constructing sum/count splits

- [ ] **Step 2: For each site, the pattern is:**

```java
// Old:
ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(map);
ScalarOperator out = rewriter.rewrite(expr);

// New:
Optional<ScalarOperator> outOpt = MvColumnRefSubstitutor.substitute(expr, map);
if (!outOpt.isPresent()) {
    return null; // or whatever the existing failure path of this method does
}
ScalarOperator out = outOpt.get();
```

Each rule's caller has a "candidate failed" return value (typically `null`). Use that.

- [ ] **Step 3: Compile + run async MV tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.plan.MaterializedViewRewriteTest"
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "refactor(mv-async): route async-rewrite substitutions through MvColumnRefSubstitutor"
```

---

### Task 5.2: Add validator at async-rewrite output

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/rule/BaseMaterializedViewRewriteRule.java`

- [ ] **Step 1: Locate the rewrite output return path**

```bash
grep -n "rewriteOptExpression\|return.*rewritten\|OptExpression rewritten" fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/rule/BaseMaterializedViewRewriteRule.java | head -10
```

- [ ] **Step 2: Wrap the return**

Where the rule returns the rewritten OptExpression, validate first:

```java
import com.starrocks.sql.optimizer.rule.transformation.materialization.common.MvRewriteOutputValidator;

OptExpression rewritten = ...; // existing
String mvIdent = "mvId=" + mvContext.getMv().getId() + " mvName=" + mvContext.getMv().getName();
if (!MvRewriteOutputValidator.validate(rewritten, mvIdent)) {
    return null; // or matching empty / Lists.newArrayList()
}
return ...; // existing
```

- [ ] **Step 3: Run async MV tests**

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "feat(mv-async): validate async rewrite output and drop incoherent candidates"
```

---

## Phase 6 — End-to-end test matrix

### Task 6.1: Sync MV shape matrix

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java`

- [ ] **Step 1: Add the parameterized matrix**

Append after the regression test from Task 0.2:

```java
@Test
public void testSyncMvRewriteTypeConsistencyMatrix() throws Exception {
    starRocksAssert.withTable("CREATE TABLE if not exists t_matrix ("
            + "k1 DATE NULL, k2 INT NULL, k3 SMALLINT NULL"
            + ") DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
            + "PROPERTIES('replication_num' = '1')");
    starRocksAssert.withMaterializedView(
            "CREATE MATERIALIZED VIEW t_matrix_mv AS "
            + "SELECT k1, k2, sum(k3) AS s FROM t_matrix GROUP BY k1, k2");
    waitingRollupJobV2Finish();

    Object[][] cases = new Object[][] {
            {"sum(k3)", true, "Q1 direct"},
            {"sum(k3 * 2)", true, "Q2 arith"},
            {"sum(k3 + 1)", true, "Q3 arith"},
            {"sum(coalesce(k3, 0))", true, "Q4 coalesce"},
            {"sum(nullif(k3, 0))", true, "Q5 nullif"},
            {"sum(if(k2=0, k3, 0))", true, "Q6 if"},
            {"sum(case when k2=0 then k3 else 0 end)", true, "Q7 case"},
            {"sum(cast(k3 as bigint))", true, "Q8 explicit cast"},
            {"sum(if(k2=0, if(k3>0, k3, -k3), 0))", true, "Q9 nested if"},
            {"sum(k3) + sum(case when k2=0 then k3 else 0 end)", true, "Q10 multi-agg"},
            {"avg(k3)", true, "Q11 rollup-fn"},
            {"sum(rand() * k3)", false, "Q12 nondeterministic"},
            {"sum((select max(k2) from t_matrix) + k3)", false, "Q13 subquery"},
    };
    for (Object[] c : cases) {
        String expr = (String) c[0];
        boolean shouldHit = (Boolean) c[1];
        String label = (String) c[2];
        String sql = "SELECT k1, " + expr + " AS v FROM t_matrix GROUP BY k1";
        String plan = getFragmentPlan(sql);
        if (shouldHit) {
            Assert.assertTrue(label + " expected MV hit but not found:\n" + plan,
                    plan.contains("t_matrix_mv"));
        } else {
            Assert.assertFalse(label + " expected MV miss but found:\n" + plan,
                    plan.contains("t_matrix_mv"));
        }
    }
    starRocksAssert.dropMaterializedView("t_matrix_mv");
    starRocksAssert.dropTable("t_matrix");
}
```

- [ ] **Step 2: Run the matrix**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest.testSyncMvRewriteTypeConsistencyMatrix"
```

Expected: all 13 cases pass. If a hit-case fails, the bug is likely in either the column-coverage check (Task 4.5) or the re-deriver (Phase 1). If a miss-case incorrectly hits, tighten `containsForbiddenShape` in Task 4.5.

- [ ] **Step 3: Commit**

```bash
git add ...
git commit -m "test(mv): sync MV rewrite type consistency shape matrix (13 cases)"
```

---

### Task 6.2: Async MV shape matrix

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/plan/MaterializedViewRewriteTest.java`

- [ ] **Step 1: Mirror the sync matrix using async MV creation**

Locate test setup (`createTestMaterializedView` or similar). Add `testAsyncMvRewriteTypeConsistencyMatrix` that creates an async MV `select k1, k2, sum(k3) from t_matrix group by k1, k2`, then runs the same 13 cases.

- [ ] **Step 2: Run**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.plan.MaterializedViewRewriteTest.testAsyncMvRewriteTypeConsistencyMatrix"
```

Expected: all 13 cases pass.

- [ ] **Step 3: Commit**

```bash
git add ...
git commit -m "test(mv): async MV rewrite type consistency shape matrix"
```

---

### Task 6.3: Validator failure-path test

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidatorTest.java`

- [ ] **Step 1: Add a strict-mode test**

```java
@Test(expected = IllegalStateException.class)
public void strict_mode_throws_on_incoherent_output() {
    boolean prev = Config.enable_mv_rewrite_validator_strict;
    try {
        Config.enable_mv_rewrite_validator_strict = true;
        // Build a malformed CallOperator at the top of an OptExpression.
        // Use a stub OptExpression around a malformed projection.
        // (see MvRewriteOutputValidator.validate for entry point)
        // ... construct directly or via a helper
    } finally {
        Config.enable_mv_rewrite_validator_strict = prev;
    }
}
```

(If constructing an OptExpression directly is awkward, use the existing `OptExpression.create(...)` plus a `LogicalProjectOperator` containing the malformed expression.)

- [ ] **Step 2: Run**

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add ...
git commit -m "test(mv): validator strict-mode failure-path coverage"
```

---

### Task 6.4: Enable strict mode in fe-ut

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/utframe/UtFrameUtils.java` (or wherever fe-ut sets defaults)

- [ ] **Step 1: Find where fe-ut sets Config defaults**

```bash
grep -rn "Config\.\|enable_materialized_view" fe/fe-core/src/test/java/com/starrocks/utframe/ | head -10
```

- [ ] **Step 2: Add the strict-mode default**

In the appropriate setup method (likely a static initializer or `setUp`):

```java
Config.enable_mv_rewrite_validator_strict = true;
```

- [ ] **Step 3: Run all FE-UT MV tests**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.MaterializedViewTest" \
    --tests "com.starrocks.sql.plan.MaterializedViewRewriteTest" \
    --tests "com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriverTest" \
    --tests "com.starrocks.sql.optimizer.rule.transformation.materialization.common.*"
```

Expected: all pass under strict mode.

If anything fails, that's a real ReDeriver gap — fix in `ScalarOperatorTypeReDeriver` first, then revisit.

- [ ] **Step 4: Commit**

```bash
git add ...
git commit -m "test(mv): enable validator strict mode in fe-ut

Surfaces any ReDeriver gap as an explicit IllegalStateException
rather than silent candidate degradation."
```

---

## Phase 7 — Cleanup, audit, final verification

### Task 7.1: Audit `ColumnRefOperator.setType` callers for cached-type holders

**Files:** read-only audit, then targeted fixes if needed.

- [ ] **Step 1: List callers of `ColumnRefOperator.setType`**

```bash
grep -rn "\.setType(" fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/ | head -5
grep -rn "ColumnRefOperator\b.*setType\|columnRef\.setType" fe/fe-core/src/main/java/com/starrocks/ | head -30
```

For any caller that holds a derived value computed from `getType()` (e.g., a cached `Column.PrimitiveType` or a frozen sort-key descriptor), confirm whether the value is recomputed on access. If not, add the recomputation explicitly.

- [ ] **Step 2: For each finding, add a focused unit test that demonstrates the type follows after `setType`. Fix any cache that doesn't.**

(Most likely zero findings. The audit is the deliverable; the spec marks this as a known risk.)

- [ ] **Step 3: Commit (if any fixes)**

```bash
git add ...
git commit -m "fix(mv): refresh cached type in <holder> after ColumnRef.setType"
```

(Skip the commit if the audit found nothing.)

---

### Task 7.2: Full regression sweep

**Files:** none.

- [ ] **Step 1: Run full FE-UT for the relevant packages**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.planner.*MaterializedView*" \
    --tests "com.starrocks.sql.plan.*Materialized*" \
    --tests "com.starrocks.sql.optimizer.rule.mv.*" \
    --tests "com.starrocks.sql.optimizer.rule.transformation.materialization.*"
```

Expected: all pass under strict mode.

- [ ] **Step 2: Run a wider sweep to surface side-effects on other code paths**

```bash
fe/gradlew --no-daemon -p fe :fe-core:test --tests "com.starrocks.sql.*"
```

Expected: all pass. Diagnose and fix anything new.

- [ ] **Step 3: No commit unless something needed fixing.**

---

### Task 7.3: Doc update for MV rewrite invariants

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutor.java` (header comment)

- [ ] **Step 1: Ensure the substitutor's javadoc cites the design doc and lists the four invariants**

(Already done in Task 2.1; verify and amend if needed.)

- [ ] **Step 2: Commit if amended**

```bash
git add ...
git commit -m "docs(mv): cite design doc and invariants from MvColumnRefSubstitutor"
```

---

## Self-Review

### Spec coverage

- ✅ ScalarOperatorTypeReDeriver leaves (2.1) — Task 1.2
- ✅ CallOperator core (2.2) — Task 1.3
- ✅ Aggregate specializations — Task 1.4
- ✅ IF / CaseWhen (2.3) — Task 1.5
- ✅ CastOperator (2.4) — Task 1.6
- ✅ Predicates (2.5) — Task 1.7
- ✅ Default fallback (2.6) — Task 1.8
- ✅ Output ColumnRef synchronization (2.7) — Task 2.1 `substituteAndSyncOutput`
- ✅ MvColumnRefSubstitutor — Task 2.1
- ✅ MvRewriteOutputValidator + checks 1–5 — Tasks 3.1–3.3
- ✅ Sync path: visitLogicalProject / visitLogicalTableScan / visitLogicalAggregate — Tasks 4.1–4.3
- ✅ Sync path: validator wiring — Task 4.4
- ✅ Eligibility: column-coverage — Task 4.5
- ✅ Async path: substitutor swaps — Task 5.1
- ✅ Async path: validator wiring — Task 5.2
- ✅ Strict-mode FE config — Task 3.1
- ✅ Strict-mode in fe-ut — Task 6.4
- ✅ Sync shape matrix — Task 6.1
- ✅ Async shape matrix — Task 6.2
- ✅ Validator failure-path test — Task 6.3
- ✅ ColumnRef.setType audit — Task 7.1
- ✅ Existing MV tests pass unmodified (regression) — Task 7.2

### Type consistency

- `MvColumnRefSubstitutor.substitute` returns `Optional<ScalarOperator>` consistently (tasks 2.1, 4.1, 4.2, 4.3, 5.1).
- `MvRewriteOutputValidator.validate(OptExpression, String)` and `MvRewriteOutputValidator.isCoherent(ScalarOperator)` — both names used consistently across tasks 3.2, 3.3, 4.4, 5.2, 6.3.
- `ScalarOperatorTypeReDeriver.reDerive(ScalarOperator)` — single static entry point, used in tasks 1.2–1.8 and 2.1.
- `MaterializedViewRewriter.substitutionFailed()` — accessor on the sync visitor, used in tasks 4.1–4.3 and called from 4.4.

### Placeholder scan

No "TBD" / "TODO" / "implement later". One note in Task 3.2 about `ExprUtils.canCastTo` possibly not existing — the step gives explicit fallback (drop the call, keep `matchesType` check). One note in Task 7.1 that the audit may find zero items — that is itself the deliverable.

---

## Execution Handoff

Plan complete and saved to [`docs/superpowers/plans/2026-05-08-mv-rewrite-type-consistency.md`](docs/superpowers/plans/2026-05-08-mv-rewrite-type-consistency.md).

Two execution options:

**1. Subagent-Driven (recommended)** — Dispatch a fresh subagent per task with two-stage review between tasks; preserves main-context tokens, enables faster iteration on a 30+ task plan of this size.

**2. Inline Execution** — Execute tasks in this session via `superpowers:executing-plans` with batch checkpoints.

Which approach?
