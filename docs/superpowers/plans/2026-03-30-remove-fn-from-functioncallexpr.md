# Remove Object fn from FunctionCallExpr — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make FunctionCallExpr a pure AST class by eliminating ScalarOperatorToExpr, deprecating ExprToThrift, and removing the `Object fn` field.

**Architecture:** Three-phase layered elimination. Phase 1 removes ScalarOperatorToExpr (the main fn leak channel). Phase 2 migrates all ExprToThrift callers to ExecExprSerializer. Phase 3 deletes `Object fn` from FunctionCallExpr. Each phase is independently verifiable.

**Tech Stack:** Java 17, StarRocks FE (fe-parser, fe-core), Thrift RPC, Gradle build

**Design Spec:** `docs/superpowers/specs/2026-03-30-remove-fn-from-functioncallexpr-design.md`

---

## Phase 1: Eliminate ScalarOperatorToExpr

### Task 1: Add ConstantOperator.toLiteralExpr() utility

ListPartitionPruner needs `ConstantOperator → LiteralExpr` conversion. Currently uses the full ScalarOperatorToExpr for this. Extract a focused utility.

**Files:**
- Create: `fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertor.java`
- Test: `fe-core/src/test/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertorTest.java`

- [ ] **Step 1: Create ConstantOperatorConvertor with toLiteralExpr()**

The conversion logic lives in `ScalarOperatorToExpr.Formatter.visitConstant()` (lines 239-290). Extract it as a standalone utility:

```java
// fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertor.java
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.type.Type;

import java.math.BigInteger;
import java.time.LocalDateTime;

/**
 * Converts ConstantOperator to LiteralExpr for cases that need AST literal
 * representation (e.g., partition value map lookups).
 */
public class ConstantOperatorConvertor {

    public static LiteralExpr toLiteralExpr(ConstantOperator constant) {
        if (constant.isNull()) {
            return new NullLiteral();
        }
        Type type = constant.getType();
        PrimitiveType primitiveType = type.getPrimitiveType();
        switch (primitiveType) {
            case BOOLEAN:
                return new BoolLiteral(constant.getBoolean());
            case TINYINT:
                return new IntLiteral(constant.getTinyInt(), type);
            case SMALLINT:
                return new IntLiteral(constant.getSmallint(), type);
            case INT:
                return new IntLiteral(constant.getInt(), type);
            case BIGINT:
                return new IntLiteral(constant.getBigint(), type);
            case LARGEINT:
                return new LargeIntLiteral(constant.getLargeInt().toString());
            case FLOAT:
            case DOUBLE:
                return new FloatLiteral(constant.getDouble(), type);
            case DATE:
            case DATETIME: {
                LocalDateTime dt = constant.getDatetime();
                return new DateLiteral(dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
                        dt.getHour(), dt.getMinute(), dt.getSecond(), dt.getNano() / 1000, type);
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new DecimalLiteral(constant.getDecimal());
            case CHAR:
            case VARCHAR:
                return StringLiteral.create(constant.getVarchar());
            case VARBINARY:
                return new VarBinaryLiteral(constant.getVarbinary());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported constant type for LiteralExpr conversion: " + primitiveType);
        }
    }
}
```

- [ ] **Step 2: Write unit test**

```java
// fe-core/src/test/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertorTest.java
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.IntType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class ConstantOperatorConvertorTest {

    @Test
    void testNullConversion() {
        ConstantOperator op = ConstantOperator.createNull(IntType.INT);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(NullLiteral.class, result);
    }

    @Test
    void testIntConversion() {
        ConstantOperator op = ConstantOperator.createInt(42);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(IntLiteral.class, result);
        assertEquals(42, ((IntLiteral) result).getValue());
    }

    @Test
    void testBoolConversion() {
        ConstantOperator op = ConstantOperator.TRUE;
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(BoolLiteral.class, result);
        assertTrue(((BoolLiteral) result).getValue());
    }

    @Test
    void testVarcharConversion() {
        ConstantOperator op = ConstantOperator.createVarchar("hello");
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(StringLiteral.class, result);
        assertEquals("hello", ((StringLiteral) result).getStringValue());
    }

    @Test
    void testDoubleConversion() {
        ConstantOperator op = ConstantOperator.createDouble(3.14);
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(FloatLiteral.class, result);
    }

    @Test
    void testDateConversion() {
        ConstantOperator op = ConstantOperator.createDatetime(
                LocalDateTime.of(2026, 3, 30, 12, 0, 0));
        LiteralExpr result = ConstantOperatorConvertor.toLiteralExpr(op);
        assertInstanceOf(DateLiteral.class, result);
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.optimizer.operator.scalar.ConstantOperatorConvertorTest" --rerun`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add fe-core/src/main/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertor.java \
        fe-core/src/test/java/com/starrocks/sql/optimizer/operator/scalar/ConstantOperatorConvertorTest.java
git commit -m "Add ConstantOperatorConvertor utility for ConstantOperator to LiteralExpr conversion"
```

---

### Task 2: Migrate ListPartitionPruner away from ScalarOperatorToExpr

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/ListPartitionPruner.java`

- [ ] **Step 1: Replace ScalarOperatorToExpr usage in evaluateConstant() (line ~569)**

Replace:
```java
ScalarOperatorToExpr.FormatterContext formatterContext =
        new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
LiteralExpr literal = (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(child, formatterContext);
```
With:
```java
LiteralExpr literal = ConstantOperatorConvertor.toLiteralExpr(child);
```

- [ ] **Step 2: Replace in evalBinaryPredicate() (line ~615)**

Replace:
```java
ScalarOperatorToExpr.FormatterContext formatterContext =
        new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
LiteralExpr literal = (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(rightChild, formatterContext);
```
With:
```java
LiteralExpr literal = ConstantOperatorConvertor.toLiteralExpr((ConstantOperator) rightChild);
```

- [ ] **Step 3: Replace in evalInPredicate() (line ~767)**

Replace:
```java
ScalarOperatorToExpr.FormatterContext formatterContext =
        new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
LiteralExpr literal =
        (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(inPredicate.getChild(i), formatterContext);
```
With:
```java
LiteralExpr literal = ConstantOperatorConvertor.toLiteralExpr((ConstantOperator) inPredicate.getChild(i));
```

- [ ] **Step 4: Remove ScalarOperatorToExpr import, add ConstantOperatorConvertor import**

- [ ] **Step 5: Run partition pruning tests**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.optimizer.rule.transformation.ListPartitionPrunerTest" --tests "com.starrocks.sql.plan.PartitionPruneTest" --rerun`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/ListPartitionPruner.java
git commit -m "Migrate ListPartitionPruner from ScalarOperatorToExpr to ConstantOperatorConvertor"
```

---

### Task 3: Change ExprUtils to return ExecExpr instead of Expr

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/sql/ast/expression/ExprUtils.java`

- [ ] **Step 1: Change analyzeAndCastFold() to return ExecExpr**

Replace current implementation (lines 371-384):
```java
public static ExecExpr analyzeAndCastFold(Expr expr) {
    ExpressionAnalyzer.analyzeExpressionIgnoreSlot(expr, ConnectContext.get());
    try {
        ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translate(expr);
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        return ScalarOperatorToExecExpr.buildIgnoreSlot(scalarOperator,
                new ScalarOperatorToExecExpr.FormatterContext(Maps.newHashMap()));
    } catch (UnsupportedException e) {
        return com.starrocks.planner.expression.ExecAstExprWrapper.wrap(expr);
    }
}
```

Note: The fallback wraps in `ExecAstExprWrapper` (temporary — will be cleaned up in Phase 2).

- [ ] **Step 2: Change analyzeLoadExpr() to return ExecExpr**

Replace current implementation (lines 386-399):
```java
public static ExecExpr analyzeLoadExpr(Expr expr,
        java.util.function.Function<SlotRef, ColumnRefOperator> slotResolver) {
    ExpressionAnalyzer.analyzeExpressionIgnoreSlot(expr, ConnectContext.get());
    try {
        ScalarOperator scalarOperator = SqlToScalarOperatorTranslator.translateLoadExpr(expr, slotResolver);
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        scalarOperator = scalarRewriter.rewrite(scalarOperator, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        return ScalarOperatorToExecExpr.buildIgnoreSlot(scalarOperator,
                new ScalarOperatorToExecExpr.FormatterContext(Maps.newHashMap()));
    } catch (UnsupportedException e) {
        return com.starrocks.planner.expression.ExecAstExprWrapper.wrap(expr);
    }
}
```

- [ ] **Step 3: Add necessary imports**

Add imports for `ExecExpr`, `ScalarOperatorToExecExpr`, `ExecAstExprWrapper`.
Remove `ScalarOperatorToExpr` import.

- [ ] **Step 4: Compile check**

Run: `cd fe && ./gradlew :fe-core:compileJava`
Expected: Compilation errors in callers of analyzeAndCastFold/analyzeLoadExpr (return type changed from Expr to ExecExpr). These are fixed in Task 4.

- [ ] **Step 5: Commit** (compile may fail — OK, callers updated in Task 4)

```bash
git add fe-core/src/main/java/com/starrocks/sql/ast/expression/ExprUtils.java
git commit -m "Change ExprUtils.analyzeAndCastFold to return ExecExpr instead of Expr"
```

---

### Task 4: Update all callers of ExprUtils.analyzeAndCastFold/analyzeLoadExpr

Callers now receive `ExecExpr` instead of `Expr`. Each caller that passes the result to `ExprToThrift.treeToThrift(expr)` must switch to `ExecExprSerializer.serialize(execExpr)`.

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/planner/StreamLoadScanNode.java`
- Modify: `fe-core/src/main/java/com/starrocks/planner/FileScanNode.java`
- Modify: `fe-core/src/main/java/com/starrocks/planner/OlapTableSink.java`
- Modify: `fe-core/src/main/java/com/starrocks/planner/LoadScanNode.java`
- Modify: `fe-core/src/main/java/com/starrocks/alter/SchemaChangeJobV2.java`
- Modify: `fe-core/src/main/java/com/starrocks/alter/LakeTableSchemaChangeJob.java`
- Modify: `fe-core/src/main/java/com/starrocks/alter/RollupJobV2.java`
- Modify: `fe-core/src/main/java/com/starrocks/sql/analyzer/SetStmtAnalyzer.java`
- Modify: `fe-core/src/main/java/com/starrocks/load/Load.java`

- [ ] **Step 1: StreamLoadScanNode (line ~369)**

Replace:
```java
paramCreateContext.params.putToExpr_of_dest_slot(dstSlotDesc.getId().asInt(), ExprToThrift.treeToThrift(expr));
```
With:
```java
paramCreateContext.params.putToExpr_of_dest_slot(dstSlotDesc.getId().asInt(), ExecExprSerializer.serialize(expr));
```

The `expr` variable type changes from `Expr` to `ExecExpr`. Also update the `castToSlot()` call — it currently takes `Expr` and returns `Expr`. This needs adapting: either wrap the `castToSlot` result in `ExecAstExprWrapper`, or do the cast at the ScalarOperator level before converting to ExecExpr.

The simplest approach: keep `castToSlot` operating on `Expr`, then wrap its result:
```java
Expr rawExpr = ...;  // original Expr before analyzeAndCastFold
ExprUtils.analyzeAndCastFold(rawExpr);  // returns ExecExpr but we need Expr for castToSlot
```

Actually, since `castToSlot` adds a CastExpr wrapper, the cleanest approach is:
1. Do `analyzeAndCastFold` first → ExecExpr
2. The cast should be applied at ScalarOperator level inside `analyzeAndCastFold`
3. Or accept the ExecExpr and do the cast wrapping on ExecExpr (add ExecCastExpr wrapper)

For this task, use the pragmatic approach: **wrap the entire flow**. Where a caller needs both `analyzeAndCastFold` AND additional Expr manipulation (like `castToSlot`), do the Expr manipulation first, then call `analyzeAndCastFold` as the final step.

Read each call site carefully and adapt the ordering.

- [ ] **Step 2: FileScanNode (line ~456)** — same pattern as StreamLoadScanNode

- [ ] **Step 3: OlapTableSink (line ~544)**

Replace:
```java
indexSchema.setWhere_clause(ExprToThrift.treeToThrift(whereClause));
```
With:
```java
ExecExpr execWhereClause = ExprUtils.analyzeAndCastFold(whereClause);
indexSchema.setWhere_clause(ExecExprSerializer.serialize(execWhereClause));
```

- [ ] **Step 4: LoadScanNode (line ~119)** — already wraps in ExecAstExprWrapper, adapt to use ExecExpr directly

- [ ] **Step 5: SchemaChangeJobV2 (line ~746)**

Replace:
```java
mcExprs.put(columnIndex, ExprToThrift.treeToThrift(generatedColumnExpr));
```
With:
```java
ExecExpr execGenColExpr = ExprUtils.analyzeAndCastFold(generatedColumnExpr);
mcExprs.put(columnIndex, ExecExprSerializer.serialize(execGenColExpr));
```

- [ ] **Step 6: LakeTableSchemaChangeJob (line ~677)** — same pattern as SchemaChangeJobV2

- [ ] **Step 7: RollupJobV2 (line ~498)** — uses the result for type checking, not Thrift. Adapt to extract type from ExecExpr.

- [ ] **Step 8: SetStmtAnalyzer (lines ~119, ~595)** — uses result to extract LiteralExpr. Change to extract constant value from ExecLiteral.

- [ ] **Step 9: Load.java (line ~801)** — uses analyzeLoadExpr. Adapt to ExecExpr.

- [ ] **Step 10: Compile check**

Run: `cd fe && ./gradlew :fe-core:compileJava`
Expected: Clean compilation

- [ ] **Step 11: Run comprehensive tests**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.plan.TPCHPlanTest" --tests "com.starrocks.sql.plan.AggregateTest" --tests "com.starrocks.sql.plan.SubqueryTest" --tests "com.starrocks.sql.plan.ExpressionTest" --rerun --no-build-cache`
Expected: All PASS (557+)

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "Migrate ExprUtils callers from ExprToThrift to ExecExprSerializer"
```

---

### Task 5: Delete ScalarOperatorToExpr

**Files:**
- Delete: `fe-core/src/main/java/com/starrocks/sql/plan/ScalarOperatorToExpr.java`

- [ ] **Step 1: Delete the file**

- [ ] **Step 2: Remove any remaining imports of ScalarOperatorToExpr across the codebase**

Run: `grep -rn "ScalarOperatorToExpr" --include="*.java" fe-core/src/main/java/`
Fix any remaining references.

- [ ] **Step 3: Compile check**

Run: `cd fe && ./gradlew :fe-core:compileJava`
Expected: Clean compilation. If test files reference ScalarOperatorToExpr, fix those too.

- [ ] **Step 4: Run full test suite**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.plan.*" --rerun --no-build-cache`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Delete ScalarOperatorToExpr — no longer needed"
```

---

## Phase 2: Migrate Remaining ExprToThrift Callers, Deprecate ExprToThrift

### Task 6: Migrate OlapTableSink partition literal serialization

OlapTableSink has 3 call sites (lines ~777, ~818, ~825) that serialize `LiteralExpr` to `TExprNode` for partition values.

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/planner/OlapTableSink.java`

- [ ] **Step 1: Create a lightweight literalToTExprNode helper**

Add to `ExecExprSerializer` or a new utility:
```java
public static TExprNode literalToTExprNode(LiteralExpr literal) {
    TExpr texpr = ExprToThrift.treeToThrift(literal);  // temporary — will be replaced
    return texpr.getNodes().get(0);
}
```

Actually, since we're eliminating ExprToThrift, create this in a new utility using ExecLiteral:
```java
public static TExprNode literalToTExprNode(LiteralExpr literal) {
    ConstantOperator constant = ConstantOperator.createObject(
            literal.getRealObjectValue(), literal.getType());
    ExecLiteral execLiteral = new ExecLiteral(constant, literal.getType());
    TExpr texpr = ExecExprSerializer.serialize(execLiteral);
    return texpr.getNodes().get(0);
}
```

- [ ] **Step 2: Replace 3 ExprToThrift calls in OlapTableSink with the new helper**

- [ ] **Step 3: Compile and test**

Run: `cd fe && ./gradlew :fe-core:compileJava && ./gradlew :fe-core:test --tests "com.starrocks.planner.OlapTableSinkTest" --rerun`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Migrate OlapTableSink partition serialization from ExprToThrift to ExecExprSerializer"
```

---

### Task 7: Migrate metadata serialization paths

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/catalog/Column.java` (default expr)
- Modify: `fe-core/src/main/java/com/starrocks/catalog/IcebergTable.java` (partition expr)
- Modify: `fe-core/src/main/java/com/starrocks/catalog/HiveTable.java` (partition keys)
- Modify: `fe-core/src/main/java/com/starrocks/catalog/ColumnAccessPath.java` (path string)
- Modify: `fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergConnectorScanRangeSource.java` (extended columns)
- Modify: `fe-core/src/main/java/com/starrocks/alter/AlterReplicaTask.java` (MV/schema change exprs)
- Modify: `fe-core/src/main/java/com/starrocks/load/SparkLoadJob.java` (spark load)

These all serialize simple LiteralExpr or analyzed Expr to TExpr. Pattern: replace `ExprToThrift.treeToThrift(expr)` with the Task 6 helper for literals, or `ExecExprSerializer.serialize(ExecAstExprWrapper.wrap(expr))` for complex expressions.

- [ ] **Step 1: Migrate each file** — replace every `ExprToThrift.treeToThrift(...)` call

For `LiteralExpr` inputs (HiveTable, ColumnAccessPath, OlapTableSink partition values):
```java
ExecExprSerializer.serialize(new ExecLiteral(
    ConstantOperator.createObject(literal.getRealObjectValue(), literal.getType()),
    literal.getType()))
```

For complex Expr inputs (Column default expr, IcebergTable partition, AlterReplicaTask):
```java
ExecExprSerializer.serialize(ExecAstExprWrapper.wrap(expr))
```

- [ ] **Step 2: Compile and test**

Run: `cd fe && ./gradlew :fe-core:compileJava`

- [ ] **Step 3: Run tests**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.plan.*" --tests "com.starrocks.catalog.*" --rerun --no-build-cache`

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "Migrate metadata serialization from ExprToThrift to ExecExprSerializer"
```

---

### Task 8: Remove ExecAstExprWrapper fallback and deprecate ExprToThrift

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/planner/expression/ExecExprSerializer.java`
- Modify: `fe-core/src/main/java/com/starrocks/sql/expression/ExprToThrift.java`

- [ ] **Step 1: Change ExecAstExprWrapper fallback in ExecExprSerializer to throw**

Replace (lines 53-60):
```java
if (expr instanceof ExecAstExprWrapper) {
    TExpr astResult = ExprToThrift.treeToThrift(((ExecAstExprWrapper) expr).getAstExpr());
    ...
}
```
With:
```java
if (expr instanceof ExecAstExprWrapper) {
    throw new UnsupportedOperationException(
            "ExecAstExprWrapper should not reach serialization. " +
            "Migrate the caller to produce ExecExpr directly.");
}
```

- [ ] **Step 2: Annotate ExprToThrift with @Deprecated**

```java
@Deprecated(since = "4.x", forRemoval = true)
public final class ExprToThrift {
```

- [ ] **Step 3: Verify no remaining callers**

Run: `grep -rn "ExprToThrift\." --include="*.java" fe-core/src/main/java/ | grep -v "ExprToThrift.java" | grep -v "@Deprecated" | grep -v "import"`
Expected: No results (or only the ExecExprSerializer fallback which now throws)

- [ ] **Step 4: Run full test suite**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.plan.*" --tests "com.starrocks.catalog.*" --rerun --no-build-cache`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "Deprecate ExprToThrift, remove ExecAstExprWrapper fallback"
```

---

## Phase 3: Delete Object fn from FunctionCallExpr

### Task 9: Remove Object fn field and getFn/setFn methods

**Files:**
- Modify: `fe-parser/src/main/java/com/starrocks/sql/ast/expression/FunctionCallExpr.java`
- Modify: `fe-core/src/main/java/com/starrocks/sql/ast/expression/FunctionCallExprFactory.java`

- [ ] **Step 1: Remove from FunctionCallExpr**

In `fe-parser/src/main/java/com/starrocks/sql/ast/expression/FunctionCallExpr.java`:

1. Delete field: `protected Object fn;`
2. Delete methods: `getFn()`, `setFn(Object fn)`
3. Replace `isAggregateFunction()` guard: `Preconditions.checkState(fn != null)` → `Preconditions.checkState(hasFnId())`
4. Replace `isNullable()` guard: `if (fn != null && !fnNullable)` → `if (hasFnId() && !fnNullable)`
5. Replace `hashCode()`: `Objects.hash(..., fn)` → `Objects.hash(..., java.util.Arrays.hashCode(fnArgTypes))`
6. Replace `equalsWithoutChild()`: `Objects.equals(fn, o.fn)` → `java.util.Arrays.equals(fnArgTypes, o.fnArgTypes)`
7. Remove from copy constructors: all `fn = other.fn` / `fn = e.fn` lines
8. Remove from `resetAnalysisState()`: `fn = null` line
9. Remove from `copyFnFieldsFrom()`: `this.fn = other.fn` line

- [ ] **Step 2: Update FunctionCallExprFactory**

In `fe-core/src/main/java/com/starrocks/sql/ast/expression/FunctionCallExprFactory.java`:

1. In `setFn(expr, fn, ctx)`: remove `expr.setFn(fn)` call
2. In `getFn(expr, ctx)`: remove fallback `return (Function) expr.getFn()`, only return from AnalysisContext or null

- [ ] **Step 3: Compile check**

Run: `cd fe && ./gradlew :fe-core:compileJava`
Expected: Errors in callers of `getFn()`/`setFn()` — fixed in Task 10

---

### Task 10: Update all remaining getFn/setFn callers

**Files:**
- Modify: `fe-core/src/main/java/com/starrocks/sql/analyzer/SelectAnalyzer.java`
- Modify: `fe-core/src/main/java/com/starrocks/sql/ast/expression/ExprCastFunction.java`
- Modify: `fe-core/src/main/java/com/starrocks/sql/spm/SPMPlanner.java`
- Modify: test files that reference `getFn()`/`setFn()`

- [ ] **Step 1: SelectAnalyzer** — `funcCall.getFn() != null` → `funcCall.hasFnId()`

- [ ] **Step 2: ExprCastFunction** — `expr.getFn() == null` → `!expr.hasFnId()`

- [ ] **Step 3: SPMPlanner** — `Preconditions.checkNotNull(node.getFn())` → `Preconditions.checkState(node.hasFnId())`

- [ ] **Step 4: Fix test files**

Search: `grep -rn "\.getFn()\|\.setFn(" --include="*.java" fe-core/src/test/`
Replace each:
- `expr.setFn(fn)` → `FunctionCallExprFactory.setFn(expr, fn)`
- `expr.getFn()` → `FunctionCallExprFactory.getFn(expr, ctx)` or use `hasFnId()` for null checks

- [ ] **Step 5: Compile clean**

Run: `cd fe && ./gradlew :fe-core:compileTestJava`
Expected: Clean

- [ ] **Step 6: Checkstyle**

Run: `cd fe && ./gradlew :fe-parser:checkstyleMain :fe-core:checkstyleMain`
Expected: No errors

- [ ] **Step 7: Run full test suite**

Run: `cd fe && ./gradlew :fe-core:test --tests "com.starrocks.sql.plan.TPCHPlanTest" --tests "com.starrocks.sql.plan.AggregateTest" --tests "com.starrocks.sql.plan.WindowTest" --tests "com.starrocks.sql.plan.SubqueryTest" --tests "com.starrocks.sql.plan.ExpressionTest" --tests "com.starrocks.sql.plan.JoinTest" --tests "com.starrocks.sql.plan.LowCardinalityTest" --tests "com.starrocks.catalog.ExpressionRangePartitionInfoTest" --rerun --no-build-cache`
Expected: All PASS (557+)

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "Remove Object fn from FunctionCallExpr — pure AST class achieved"
```

---

## Verification Checklist

After all tasks complete:

- [ ] `grep -rn "Object fn" fe-parser/src/main/java/com/starrocks/sql/ast/expression/FunctionCallExpr.java` → no results
- [ ] `grep -rn "\.getFn()\|\.setFn(" --include="*.java" fe-parser/src/main/java/` → no results (except typed field getters like getFnArgTypes)
- [ ] `grep -rn "ScalarOperatorToExpr" --include="*.java" fe-core/src/main/java/` → no results (file deleted)
- [ ] `grep -rn "ExprToThrift\.treeToThrift" --include="*.java" fe-core/src/main/java/ | grep -v ExprToThrift.java` → no results
- [ ] Full test suite passes
