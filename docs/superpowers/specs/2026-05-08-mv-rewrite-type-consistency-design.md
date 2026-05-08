# MV Rewrite Type Consistency — General Fix Design

**Date**: 2026-05-08
**Owner**: harbor.liu@celerdata.com
**Related issue**: https://github.com/StarRocks/starrocks/issues/72799
**Status**: design — awaiting user review before plan generation

## Background

Single-table sync materialized views in StarRocks store rollup-aggregate columns (e.g. `sum(k3)` where `k3 SMALLINT`) using the aggregate's intermediate physical type (`BIGINT`). When the optimizer rewrites a query against an MV, it substitutes the original column reference (`k3`) deep inside an expression with the MV column reference (`mv_sum_k3`). The substitution itself is performed by `ReplaceColumnRefRewriter`, which is a structural leaf-only shuttle: it replaces matching `ColumnRefOperator` nodes but never re-derives parent operator types nor re-resolves `Function` signatures.

This produces type-incoherent ScalarOperator trees whenever the substituted column sits inside any expression that carries type semantics. Two known reproducers:

```sql
-- Reproducer 1: BE crash via VectorizedIfExpr down_cast assert
SELECT k1, sum(if(k2 = 0, k3, 0)) AS sum_if FROM t1 GROUP BY k1;

-- Reproducer 2: PlanValidator "Invalid plan" — sum's signature still claims SMALLINT
SELECT k1,
       sum(k3) AS sum1,
       sum(case when k2 = 0 then k3 else 0 end) AS sum2
FROM t1 GROUP BY k1;
```

A working narrow fix already exists locally that adds case-by-case `normalize*` helpers for `IF` / `CaseWhen` / `sum` plus a `addCaseWhenRewriteContexts` eligibility extension. The narrow fix is correct for the two reproducers but is structurally fragile: any new wrapping shape (`coalesce`, `nullif`, arithmetic, nested `cast`, `least` / `greatest`, etc.) reintroduces the same defect class. The async MV rewrite path under `rule/transformation/materialization/` shares the same root-cause mechanism — it also performs blind column substitution via `ReplaceColumnRefRewriter` and bolts on per-function fixups (`AggregateFunctionRewriter`, `EquationRewriter`).

## Goal

A single, expression-shape-agnostic mechanism that:

1. Guarantees that any ScalarOperator subtree produced by an MV column substitution is type- and signature-coherent.
2. Stops enumerating "supported" wrapping shapes (`if`, `case`) in eligibility checks; instead relies on a column-coverage criterion plus the type mechanism above.
3. Provides a self-check safety net that rejects MV rewrite candidates which violate the invariants, even if a future ScalarOperator subclass is missed by the re-derivation visitor.
4. Is applied uniformly to **both** sync and async MV rewrite paths.

Non-goals:
- Improving rewrite *recall* on shapes the existing pipeline already handles correctly. Recall stays the same or improves; quality stays the same or improves.
- Refactoring `ReplaceColumnRefRewriter` itself. Its ~50 non-MV callers must remain unaffected.
- Backport. This change is main-only by design.

## Architecture

```
[query expr containing k3]
       │
       │  (1) MvColumnRefSubstitutor                           [NEW; MV-package]
       │      ├─ ReplaceColumnRefRewriter        (existing)    leaf substitution k3 → mv_sum_k3
       │      └─ ScalarOperatorTypeReDeriver     [NEW; rewrite-package]
       │           bottom-up type re-derivation + Function rebinding
       ▼
[rewritten subtree, types coherent]
       │
       │  (2) MvRewriteOutputValidator           [NEW; MV-package]
       │      ├─ pass → emit candidate
       │      └─ fail → reject candidate, log WARN, optimizer continues
       ▼
[validated OptExpression]

(3) MaterializedViewRule.isMVMatchAggFunctions   [CHANGED]
    enumeration of if/case shapes  →  column-coverage criterion
```

### Package layout

| Component | Path |
|---|---|
| `ScalarOperatorTypeReDeriver` | `com.starrocks.sql.optimizer.rewrite` |
| `TypeReDeriveException` | `com.starrocks.sql.optimizer.rewrite` |
| `MvColumnRefSubstitutor` | `com.starrocks.sql.optimizer.rule.transformation.materialization.common` |
| `MvRewriteOutputValidator` | `com.starrocks.sql.optimizer.rule.transformation.materialization.common` |

The re-deriver lives in the generic `rewrite/` package because it has no MV semantics: any future caller that performs column substitution can compose it. The substitutor and validator live under the MV `common/` package because they encode MV-rewrite-specific failure semantics ("reject this candidate, keep evaluating others").

### Design contract

For any ScalarOperator subtree `s` returned by `MvColumnRefSubstitutor.substitute(...)`:

- ∀ `CallOperator c ∈ s`: `c.fn.argTypes` is `IS_NONSTRICT_SUPERTYPE_OF` `c.children.types`, and `c.type == c.fn.returnType`
- ∀ `CaseWhenOperator / IfOperator c ∈ s`: all then/else/value branches have a single unified type `T`, with implicit `CastOperator` inserted where needed; `c.type == T`
- ∀ `CastOperator(target, child) ∈ s`: if implicit, target is a supertype of `child.type`; if explicit, target is preserved as user-written
- ∀ output `ColumnRefOperator k → ScalarOperator v` mapping in `Projection.columnRefMap` / `LogicalAggregationOperator.aggregations`: `k.type == v.type` and `k.nullable == v.nullable`

`MvRewriteOutputValidator` checks these invariants on every rewrite output as a safety net.

## ScalarOperatorTypeReDeriver

An immutable bottom-up shuttle. Each `visit*` returns a semantically equivalent, type-coherent `ScalarOperator` (or the same instance when nothing changed). On any unrecoverable mismatch, throws `TypeReDeriveException`, caught by the substitutor and translated into "reject this MV candidate".

### 2.1 Leaves
- `ColumnRefOperator` — type was set during leaf substitution; pass through.
- `ConstantOperator` — type is intrinsic; pass through.

### 2.2 `CallOperator` (core)
1. Recurse into all children.
2. Collect actual child types as `argTypes`.
3. Try `ExprUtils.getBuiltinFunction(fnName, argTypes, IS_NONSTRICT_SUPERTYPE_OF)`:
   - Hit → rebuild `CallOperator` with the new `Function`, `type = fn.getReturnType()`, preserving `isDistinct` / `ignoreNulls` / etc.
   - Miss → fall back to `IS_SUPERTYPE_OF`, then `IS_INDISTINGUISHABLE`. Final miss → throw.
4. Specialized aggregate paths consolidated here (replacing scattered helpers in current code):
   - `SUM`, `COUNT(*) → SUM` rollup: try `ScalarOperatorUtil.findSumFn(argTypes)` first.
   - `BITMAP_UNION` / `HLL_UNION` / `PERCENTILE_UNION` rollups: keep their existing dedicated lookup helpers (currently in `MaterializedViewRewriter.rewriteAggregateFunc` + `equivalent/*RewriteEquivalent`).

### 2.3 `CaseWhenOperator` and `IF`
- Recurse into children.
- Compute unified branch type `T` via `TypeManager.getCompatibleTypeForCaseWhen(types)` (CASE) or `TypeManager.getCommonSuperType(thenT, elseT)` (IF).
- Wrap any branch whose type ≠ `T` with implicit `CastOperator`.
- Rebuild operator with `type = T`. `IF` reaches step 2.2 to re-resolve its `Function`.
- `T == Type.INVALID` → throw.

### 2.4 `CastOperator`
- Recurse into child.
- If `isImplicit() == false` (user-written `CAST(x AS BIGINT)`): preserve target unconditionally; correctness of cast is a BE concern.
- If `isImplicit() == true` and `child.type == target`: drop the cast (return child).
- If `isImplicit() == true` and child is no longer a subtype of target: throw.

### 2.5 Predicates (`BinaryPredicateOperator`, `Compound`, `IsNull`, `In`, `Like`, `Between`)
Operator's `type` stays `BOOLEAN`. Children are aligned:
- Binary comparison: compute `getCommonSuperType(left, right)`; cast non-matching side.
- `IN`: align probe against value list common super type.
- `Compound` / `IsNull` / `Like` / `Between`: follow the existing analyzer rules in `Expr.analyzeImpl`. Do not reinvent.

### 2.6 Default fallback
For ScalarOperator subclasses not enumerated above (`Subfield`, `ArrayElement`, `Map`, `Lambda`, `Subquery`, `DictMapping`, ...):
- Recurse children.
- If all child types are unchanged → return original.
- Otherwise → throw `TypeReDeriveException`. Conservative by design; the validator will translate this into a clean candidate-rejection rather than emitting a possibly-wrong tree.

### 2.7 Output ColumnRef synchronization
After visiting an expression, `MvColumnRefSubstitutor` updates the output column ref:
- For each `ColumnRefOperator k` whose mapped expression `v` had its type re-derived, call `k.setType(v.getType())` and `k.setNullable(v.isNullable())`.
- `ColumnRefOperator` is identified globally by id; `ColumnRefFactory.getColumnRef(id)` returns the same instance project-wide. Mutating type is the correct semantic — downstream consumers genuinely should observe the new physical type. Upstream `CallOperator`s that consume this column must be re-derived in turn (see "Sync path integration" below).

## Eligibility — column-coverage criterion in `MaterializedViewRule`

Replace the current `addCaseWhenRewriteContexts` enumeration with a shape-agnostic criterion.

```
canSubstituteAggArg(queryAggFunc, candidateIndexMeta):
    queryArg     = queryAggFunc.getChild(0)
    queryFnName  = queryAggFunc.getFnName()
    leafColumns  = queryArg.getUsedColumns()

    if leafColumns is empty:
        return true            # constant arg — trivial rewrite

    if queryArg instanceof ColumnRefOperator:
        return existing isMVMatchAggFunctions(...) path  # legacy direct-column case

    return canCoverByMv(queryArg, queryFnName, candidateIndexMeta)


canCoverByMv(expr, queryFnName, candidateIndexMeta):
    for each leaf col_q in expr.usedColumns:
        find a non-aggregated MV column col_mv in candidateIndexMeta whose
        defineExpr is a column ref to col_q
        OR: find a rollup MV column whose rollup function is compatible with queryFnName
            using the existing rollup-fn matrix (sum/count/bitmap_union/hll_union/percentile_union)

        if no such col_mv:
            return false

        register RewriteContext(queryAggFunc, col_q, col_mv, mvColumnMeta)  # dedup

    return true
```

Pre-screen with `IsDeterministicNoSubqueryValidator` (existing semantic guard, kept) — reject any expression containing subqueries, lambdas, non-deterministic functions (`rand`, `now`, ...), or subfield access. These are MV equivalence concerns, not type concerns, and remain ineligible.

The `RewriteContext(queryFn, queryColumnRef, mvColumnRef, mvColumn)` model is unchanged. One query agg may produce multiple `RewriteContext`s (one per leaf column).

## MvRewriteOutputValidator

A single top-down walk of a candidate `OptExpression`, returning `boolean`. Checks:

1. **CallOperator signature**: `fn != null` ∧ `fn.argTypes.length == children.size()` ∧ each `children[i].type` is `IS_NONSTRICT_SUPERTYPE_OF` of `fn.argTypes[i]` ∧ `callOp.type == fn.returnType`.
2. **Branch unification**: `IfOperator` / `CaseWhenOperator` branches share a single type, equal to operator type.
3. **Implicit cast legality**: implicit `CastOperator.target` is a supertype of `child.type`. Explicit casts not validated.
4. **Output column-ref synchronization**: each `Projection.columnRefMap` and `LogicalAggregationOperator.aggregations` entry `k → v` satisfies `k.type == v.type` and `k.nullable == v.nullable`.
5. **Aggregate function child compatibility**: each agg `CallOperator`'s child types are compatible with its declared signature. This duplicates a portion of FE-final `PlanValidator` but applies it at MV rewrite output to avoid polluting the candidate pool.

### Failure semantics

- Triggers: (a) `MvColumnRefSubstitutor.substitute` catches a `TypeReDeriveException`, or (b) the validator returns false on the assembled rewrite output.
- Default behavior: `MvColumnRefSubstitutor.substitute(...)` returns `Optional.empty()`; the calling rule treats it as "skip this MV candidate" (no exception propagated to query execution). Optimizer continues to evaluate other MV candidates / falls back to base table.
- Logging: one `WARN` per rejected candidate — MV identifier (sync path: `dbId.tableId.indexId`; async path: `mvId` + `mvName`), failed check category, offending operator's debug string truncated to 200 chars. Full subtree dump under `LOG.isDebugEnabled()`.
- Metric: counter `mv_rewrite_validator_reject_total` keyed by `(path, mvIdentifier)` where `path ∈ {sync, async}`.
- Strict mode: FE config `enable_mv_rewrite_validator_strict` (default `false` in production, `true` in fe-ut). When true, rejection becomes `IllegalStateException` — surfaces missed cases in CI rather than silently degrading recall.

`MvRewriteOutputValidator` does **not** replace `PlanValidator`. The two are complementary: `PlanValidator` is the last-line global check (failure → query failure). The MV validator is a soft filter at MV rewrite output (failure → skip candidate).

## Sync path integration

```
MaterializedViewRule.transform()
  for each MV candidate:
    try:
      newExpr = MaterializedViewRewriter(context).rewrite(input)
      if !MvRewriteOutputValidator.validate(newExpr):
          continue   # skip this MV
      emit(newExpr)
    catch TypeReDeriveException:
      continue       # skip this MV
```

Specific changes in `rule/mv/MaterializedViewRewriter.java`:
- `visitLogicalProject`: route through `MvColumnRefSubstitutor.substitute`. Delete `normalizeIfOperator`, `normalizeCaseWhenOperator`, `normalizeConditionalOperator`, `castIfNeeded`.
- `visitLogicalTableScan`: scan-projection rewrite goes through substitutor.
- `visitLogicalAggregate`: layered as
  1. for each agg `CallOperator`, route its children through `MvColumnRefSubstitutor` first (substitution + type coherence);
  2. then call `rewriteAggregateFunc(...)` on the agg with already-coherent children to perform MV-specific rollup-function family mapping (e.g. `count(col) → sum(mv_count_col)`, `sum(col) → sum(mv_sum_col)`, `bitmap_union(to_bitmap(col)) → bitmap_union(mv_bitmap_col)`).

  This keeps a clean split: the re-deriver knows generic ScalarOperator typing rules; `rewriteAggregateFunc` retains the MV-specific knowledge of which query agg families map to which MV agg families. Delete `normalizeSumFunction`. **Keep** the patch's `break → continue` change in the agg loop — it was a real bug fix unrelated to type consistency (multiple aggs over the same MV column must all be rewritten in one pass).

Top-level rewrite entry catches `TypeReDeriveException` and runs `MvRewriteOutputValidator.validate`.

## Async path integration

The principle from Section 1 (single MV-internal column-substitution entry point) means every async rewrite site that constructs a `ReplaceColumnRefRewriter` for the purpose of MV column rewriting moves to `MvColumnRefSubstitutor`. Audit list:

- `BaseMaterializedViewRewriteRule.rewriteOptExpression(...)` — wrap return path with `validate(...)`.
- `AggregatedMaterializedViewRewriter` lines 268, 626, 640 (`new ReplaceColumnRefRewriter(...)`) — replace with `MvColumnRefSubstitutor`.
- `MaterializedViewRewriter` (async) lines 2013, 2374 — same.
- `EquationRewriter.findArithmeticFunction` (line 349) — already does `IS_IDENTICAL` re-bind. Refactor to call into the re-deriver's shared `reResolveCall(...)` helper to dedupe.
- `AggregateFunctionRewriter.rewriteAvg` — uses `findSumFn` directly. Consolidate so that the avg → sum/count rewrite runs the children through the re-deriver before constructing the new sum/count ops.

Same failure semantics: rule-level catch returns `null`, candidate is dropped from the optimizer's memo.

## Test strategy

### Layer 1 — `ScalarOperatorTypeReDeriver` unit tests
`fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java`. MV-independent ScalarOperator → ScalarOperator fixtures:
- CallOperator child type changes trigger fn rebinding (sum SMALLINT→BIGINT, add(SMALLINT, BIGINT)→add(BIGINT, BIGINT))
- IF / CaseWhen branch mismatch produces implicit cast + common super type
- Explicit `CastOperator` preserved; implicit cast removed when child already equals target
- Predicate child cast (e.g. `mv_sum_k3 = 0` casts literal to BIGINT)
- No matching fn → `TypeReDeriveException` thrown

### Layer 2 — `MvColumnRefSubstitutor` integration tests
`materialization/common/MvColumnRefSubstitutorTest.java`. Provide `Map<ColumnRef, ColumnRef>` + input expression → assert output expression types are coherent and output ColumnRef type/nullable are synchronized.

### Layer 3 — `MvRewriteOutputValidator` unit tests
`materialization/common/MvRewriteOutputValidatorTest.java`. Hand-construct deliberately malformed subtrees (fn vs child mismatch, output ColumnRef vs expression mismatch). Assert validator returns `false` and log/metric carry the right tag.

### Layer 4 — Sync MV end-to-end shape matrix
Extend `fe/fe-core/src/test/java/com/starrocks/planner/MaterializedViewTest.java` with `testSyncMvRewriteTypeConsistencyMatrix` (parameterized).

Schema: `t1(k1 DATE, k2 INT, k3 SMALLINT)`. Sync MV: `select k1, k2, sum(k3) from t1 group by k1, k2`.

| # | query expression | expected |
|---|---|---|
| 1 | `sum(k3)` | hit |
| 2 | `sum(k3 * 2)` | hit (CallOperator rebind) |
| 3 | `sum(k3 + 1)` | hit |
| 4 | `sum(coalesce(k3, 0))` | hit |
| 5 | `sum(nullif(k3, 0))` | hit |
| 6 | `sum(if(k2=0, k3, 0))` | hit (covers current bug) |
| 7 | `sum(case when k2=0 then k3 else 0 end)` | hit (covers current bug) |
| 8 | `sum(cast(k3 as bigint))` | hit (explicit cast preserved) |
| 9 | `sum(if(k2=0, if(k3>0, k3, -k3), 0))` | hit (nested) |
| 10 | `sum(k3) + sum(case when k2=0 then k3 else 0 end)` | both aggs hit |
| 11 | `avg(k3)` | hit (rollup fn mapping) |
| 12 | `sum(rand() * k3)` | reject (nondeterministic) |
| 13 | `sum((select max(k2) from t1) + k3)` | reject (subquery) |

Each row asserts: explain plan contains/excludes MV name, plan validates under `enable_mv_rewrite_validator_strict=true`, BE smoke run succeeds for a representative subset.

### Layer 5 — Async MV shape matrix
Extend `MaterializedViewRewriteTest.java` with `testAsyncMvRewriteTypeConsistencyMatrix`. Same shape matrix, async MV.

### Layer 6 — Regression baseline
All existing `MaterializedViewTest` / `MaterializedViewRewriteTest` cases must pass unmodified. Hard gate: confirms the re-deriver does not change the recall surface.

Retain the patch's `testSyncMVRewriteIfAggColumnKeepsConsistentType` as a pin.

### Layer 7 — Validator failure path
Inject a stubbed re-deriver that skips one rule. Assert validator catches the resulting bad subtree, candidate is rejected, `mv_rewrite_validator_reject_total` increments, query falls back to base table successfully.

### Strict-mode CI gate
Default `enable_mv_rewrite_validator_strict=true` in `system_under_test` config for fe-ut. Production default stays `false`.

## Migration of existing case-by-case patch

`MaterializedViewRewriter.java`:

| current patch element | disposition |
|---|---|
| `rewriteProjectExpression` | keep shape; body becomes `mvColumnRefSubstitutor.substitute(...)` one-liner |
| `normalizeConditionalOperator` | delete (covered by re-deriver) |
| `normalizeIfOperator` | delete |
| `normalizeCaseWhenOperator` | delete |
| `castIfNeeded` | move into `ScalarOperatorTypeReDeriver` as private helper |
| `rewriteProjection` (scan) | keep; body uses substitutor |
| `rewriteProjectionMap` | keep |
| `normalizeSumFunction` | delete (covered by re-deriver CallOperator rebinding) |
| `visitLogicalAggregate` `break → continue` | keep (real bug fix, multi-agg semantics) |
| imports for `BooleanType` / `TypeManager` / `Function` introduced solely for normalize | delete |

`MaterializedViewRule.java`:

| current patch element | disposition |
|---|---|
| `addCaseWhenRewriteContexts` | delete |
| `collectCaseWhenReturnColumnIds` | delete |
| `addColumnIds` | delete |
| `addRewriteContext` | keep (rename `addRewriteContextDedup`) |
| `canRewriteQueryAggFunc` extra params (`columnToIds`, `candidateIndexMeta`) | delete (inlined into `canCoverByMv`) |
| `isMVMatchAggFunctions` if/case branch | delete; replace with column-coverage |
| `CaseWhenOperator` import | delete |

`MaterializedViewTest.java`:

| current patch element | disposition |
|---|---|
| `testSyncMVRewriteIfAggColumnKeepsConsistentType` | keep as compatibility pin |

## New files

```
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriver.java
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/TypeReDeriveException.java
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutor.java
fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidator.java

fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rewrite/ScalarOperatorTypeReDeriverTest.java
fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvColumnRefSubstitutorTest.java
fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/common/MvRewriteOutputValidatorTest.java
```

## Risks and open points

1. **`ColumnRefOperator.setType` mutation propagation.** Design assumes that `ColumnRef` instances are id-keyed and mutating `type` is the correct semantic for downstream consumers. Implementation must audit every ColumnRef holder to confirm none cache type as a derived field. `ColumnRefSet` is id-only (safe). `ColumnRefFactory`'s `colId → Column` reverse map does not store type (safe). Any cache discovered during implementation gets explicit handling in the plan.

2. **`enable_mv_rewrite_validator_strict` default.** Recommended: production `false`, fe-ut `true`. Strict mode in CI surfaces gaps; soft mode in production preserves availability under unknown bug shapes.

3. **`AggregateFunctionRewriter.rewriteAvg` consolidation hazard.** It has subtle splitting logic for `avg → sum/count`. When folding it into the re-deriver's pipeline, the implementation step must verify the splitting still produces the same `RewriteContext` registrations and rollup-fn assignments. Add a dedicated unit test for `avg(SMALLINT)` rollup as part of Layer 5.

4. **Backport (not in current scope).** If a need arises later to backport a partial fix to a stable branch, the recommended subset is "tighten eligibility (Section: Eligibility) + add validator (Section: MvRewriteOutputValidator)" without the re-deriver. Worst case: lose recall on some shapes; never produce a bad plan.

## Out of scope

- Refactoring the synchronous and asynchronous MV rewriters into a single pipeline.
- Adding new rollup function families (e.g. APPROX_TOP_K rollups).
- Extending sync MV support to multi-table joins.
- Performance benchmarking. The added bottom-up walks are bounded by rewrite output size, which is a small constant fraction of plan size; impact is expected to be in the noise. A `bench` step in the implementation plan can confirm this empirically before merge.
