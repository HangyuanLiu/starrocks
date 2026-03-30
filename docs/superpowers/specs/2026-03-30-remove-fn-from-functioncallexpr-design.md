# Design: Remove Object fn from FunctionCallExpr

**Date**: 2026-03-30
**Goal**: Make FunctionCallExpr a pure AST class. It should not appear in execution plans and should not be used to pass Function objects between phases.

## Background

Currently `FunctionCallExpr` carries an `Object fn` field that holds a resolved `Function` reference. This creates a dependency chain: AST node → catalog Function → Thrift serialization. Multiple paths reconstruct `FunctionCallExpr` in the planner phase solely to transport the Function object to Thrift serialization.

An `AnalysisContext` (keyed by `fnId`) and typed cached fields (`fnArgTypes`, `isAggregateFn`, etc.) have already been introduced. The main query path already uses `ScalarOperator → ExecExpr → ExecExprSerializer` and does not depend on `FunctionCallExpr.fn`. The remaining dependencies are in special paths (Load, Sink, partition pruning, constant folding).

## Architecture: Current vs Target

```
CURRENT:
  Analysis: FunctionCallExpr.fn = Function  (sets Object fn)
  Optimizer: SqlToScalarOperatorTranslator reads fn → CallOperator(fn)
  Planner:  ScalarOperatorToExpr reconstructs FunctionCallExpr with fn
            ExprToThrift reads fn → TFunction
            Load/Sink nodes use FunctionCallExpr with fn

TARGET:
  Analysis: AnalysisContext.registerFunction(expr, fn)  (fnId → Function)
  Optimizer: SqlToScalarOperatorTranslator reads AnalysisContext → CallOperator(fn)
  Planner:  ScalarOperatorToExecExpr → ExecFunctionCall(fn) → ExecExprSerializer
            Load/Sink nodes use ExecExpr directly
            FunctionCallExpr never appears in planner phase
```

## Approach: Phase C — Layered Elimination

Remove consumers of `Object fn` first (phases 1-2), then delete the field (phase 3). Each phase is independently verifiable.

---

## Phase 1: Eliminate ScalarOperatorToExpr

### Problem

`ScalarOperatorToExpr` converts optimizer ScalarOperator back to AST Expr. This is the main channel through which `Object fn` leaks into execution paths. It creates new `FunctionCallExpr` instances with `fn` set via `FunctionCallExprFactory.setFn()`.

### Callers (5 total)

**Type 1: ListPartitionPruner (3 sites)**
- `evaluateConstant()` — converts ConstantOperator to LiteralExpr for type casting
- `evalBinaryPredicate()` — converts ConstantOperator to LiteralExpr for partition map lookup
- `evalInPredicate()` — same as above for IN clauses

These only need `ConstantOperator → LiteralExpr` conversion, not the full ScalarOperatorToExpr.

**Type 2: ExprUtils (2 methods, ~10 downstream callers)**
- `analyzeAndCastFold()` — Expr → ScalarOperator → rewrite → Expr roundtrip for optimization
- `analyzeLoadExpr()` — same pattern for load expressions

Downstream callers (LoadScanNode, OlapTableSink, SchemaChange, etc.) eventually serialize the result to Thrift via `ExprToThrift.treeToThrift()`.

### Changes

1. **ListPartitionPruner**: Replace `ScalarOperatorToExpr.buildExecExpression(constantOp)` with a direct `ConstantOperator → LiteralExpr` utility method. Only ConstantOperator needs conversion; no FunctionCallExpr involved.

2. **ExprUtils.analyzeAndCastFold / analyzeLoadExpr**: Change return type from `Expr` to `ExecExpr`. The roundtrip becomes:
   ```
   Expr → ScalarOperator → rewrite → ExecExpr (via ScalarOperatorToExecExpr)
   ```
   Callers adapt to receive ExecExpr instead of Expr.

3. **Delete** `ScalarOperatorToExpr.java`.

### Verification

- Compile clean
- All existing tests pass (557+)

---

## Phase 2: Migrate Special Paths to ExecExpr, Deprecate ExprToThrift

### Problem

After phase 1, `ExprToThrift` still has direct callers that serialize AST Expr to Thrift.

### Remaining ExprToThrift Callers

| Caller | Input Expr Type | Migration |
|--------|----------------|-----------|
| StreamLoadScanNode | Analyzed Expr from ExprUtils | Phase 1 already returns ExecExpr → use ExecExprSerializer |
| FileScanNode | Same | Same |
| OlapTableSink | partition/where from ExprUtils | Same |
| SchemaChangeJobV2 / LakeTableSchemaChangeJob | Generated column expr from ExprUtils | Same |
| HiveTable | LiteralExpr (partition keys) | Use ExecLiteral or simple TExpr builder |
| ColumnAccessPath | StringLiteral | Same as above |
| ExecExprSerializer (ExecAstExprWrapper fallback) | Any AST Expr | All sources eliminated → change fallback to throw |

### Changes

1. **Load/Sink nodes**: These receive ExecExpr from phase 1's modified ExprUtils. Replace `ExprToThrift.treeToThrift(expr)` with `ExecExprSerializer.serialize(execExpr)`.

2. **Simple literal paths** (HiveTable, ColumnAccessPath): Add a lightweight `LiteralExpr → TExpr` utility or create ExecLiteral nodes.

3. **ExecAstExprWrapper fallback** in ExecExprSerializer: Change from delegating to ExprToThrift to throwing `UnsupportedOperationException` (defensive — should never be reached after migration).

4. **Mark ExprToThrift as `@Deprecated`** and remove all callers. Delete if no external consumers.

### Verification

- No code references `ExprToThrift.treeToThrift` (except Deprecated class itself)
- All existing tests pass

---

## Phase 3: Delete Object fn from FunctionCallExpr

### Precondition

Phases 1+2 guarantee that FunctionCallExpr never appears in execution plan paths. The only remaining `Object fn` usages are in analysis and transformation phases.

### Changes

**FunctionCallExpr (fe-parser)**:
- Delete field: `protected Object fn`
- Delete methods: `getFn()`, `setFn()`
- `isAggregateFunction()` guard: `fn != null` → `hasFnId()`
- `isNullable()` guard: `fn != null` → `hasFnId()`
- `hashCode()`: replace `fn` with `Arrays.hashCode(fnArgTypes)`
- `equalsWithoutChild()`: replace `Objects.equals(fn, o.fn)` with `Arrays.equals(fnArgTypes, o.fnArgTypes)`
- Copy constructors: remove `fn = other.fn` (fnId already copied)
- `resetAnalysisState()`: remove `fn = null` (fnId preserved for re-analysis)
- `copyFnFieldsFrom()`: remove `this.fn = other.fn`

**FunctionCallExprFactory (fe-core)**:
- `setFn(expr, fn, ctx)`: remove `expr.setFn(fn)` call
- `setFn(expr, fn)` (2-arg): only sets typed fields, no fn storage
- `getFn(expr, ctx)`: only reads from AnalysisContext via fnId, no fallback to `expr.getFn()`

**Callers using getFn() for null-check**:
- `SelectAnalyzer`: `funcCall.getFn() != null` → `funcCall.hasFnId()`
- `ExprCastFunction`: `expr.getFn() == null` → `!expr.hasFnId()`
- `SPMPlanner`: `node.getFn()` → `node.hasFnId()`

**2-arg setFn callers** (PartitionExprAnalyzer, StatisticsCollectJob, HyperQueryJob, DefaultExpr, IcebergTable, DictQueryExpr, SPMFunctions, CreateSyncMVStmtAnalyzer):
- These only need typed fields (isAggregateFn, fnArgTypes, etc.)
- Their FunctionCallExprs never reach SqlToScalarOperatorTranslator (guaranteed by phases 1+2)
- No change needed — 2-arg setFn already sets typed fields without fn

**WindowTransformer / RelationTransformer**:
- Change from 2-arg to 3-arg setFn (pass AnalysisContext) since their exprs DO reach SqlToScalarOperatorTranslator

### Verification

- `Object fn` completely removed from FunctionCallExpr
- No `getFn()`/`setFn()` references remain
- All 557+ tests pass

---

## Key Design Decisions

1. **Phase ordering**: Consumers first, field last. Avoids the "28 failures" problem we hit when deleting fn before removing consumers.

2. **fnId survives clone**: `long fnId` is copied in all constructors. AnalysisContext uses `HashMap<Long, Function>` keyed by fnId. Cloned FunctionCallExprs find the same Function via the same fnId.

3. **fnId reuse on re-analysis**: `resetAnalysisState()` does NOT reset fnId. `AnalysisContext.registerFunction()` reuses existing fnId if present, overwriting the Function entry. This ensures clones made before re-analysis still find a valid Function.

4. **2-arg setFn paths are safe**: After phases 1+2, FunctionCallExprs from utility/planner paths never reach SqlToScalarOperatorTranslator. They only need typed fields, not Function lookup.

5. **AnalysisContext is per-statement**: Created in `Analyzer.analyze()`, attached to `StatementBase`, passed explicitly through `TransformerContext`. No ThreadLocal, no ConnectContext.

## Files Affected (Estimated)

| Phase | Files | Scope |
|-------|-------|-------|
| Phase 1 | ~8 | ScalarOperatorToExpr deletion, ListPartitionPruner, ExprUtils, Load/SchemaChange adapters |
| Phase 2 | ~10 | StreamLoadScanNode, FileScanNode, OlapTableSink, SchemaChange, HiveTable, ExecExprSerializer |
| Phase 3 | ~15 | FunctionCallExpr, FunctionCallExprFactory, SelectAnalyzer, ExprCastFunction, SPMPlanner, WindowTransformer, RelationTransformer, tests |

## Risk Mitigation

- Each phase has clear verification: compile clean + 557+ tests pass
- Phase 1 is the highest risk (changing partition pruning and load expression paths). Run extended test suites (PartitionPruneTest, LoadTest) in addition to plan tests.
- Phase 3 is the lowest risk — by then, all consumers are eliminated.
