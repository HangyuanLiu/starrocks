# Iceberg Time-Family Partition Evolution (Minimal) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Support MV creation and incremental refresh on Iceberg tables with time-family partition evolution (e.g., day(ts) -> month(ts)), single column only, with `date_trunc` MV partition expression.

**Architecture:** New branch from `367cd3e050` (parent of current feature commit). Extract minimal changes from `00383eff92` scoped to time-transform evolution only. Add rewrite isolation (return null from computePartitionDiff when isQueryRewrite + evolution detected). No BUCKET/TRUNCATE support, no multi-column evolution.

**Tech Stack:** Java (StarRocks FE), Iceberg API, JUnit 5

**Base branch point:** `367cd3e050` (includes refactoring from `3ce3ac374f`)

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `connector/iceberg/IcebergPartitionUtils.java` | Modify | Add `isSafePartitionEvolution`, `isAllTimeTransforms`, `getDateTimeIntervalFromPartition`, `getDateTimeIntervalFromPartitionExpr` |
| `connector/iceberg/IcebergPartitionKeyResolver.java` | Modify | Add Phase 2: `resolveByHistoricalSpec` for time-evolved partitions |
| `connector/MVPartitionCellBuilder.java` | Modify | Per-partition interval resolution via `resolveInterval`, `ResolvedPartitionKey`, range-based partition naming |
| `connector/ExternalPartitionMappingContext.java` | Modify | Add `partitionInfosByName` field for spec-aware resolution |
| `sql/analyzer/mv/IcebergTablePartitionHandler.java` | Modify | Relax evolution check for safe time evolution, require `date_trunc` |
| `sql/analyzer/MaterializedViewAnalyzer.java` | Modify | Partition type RANGE for time transforms on evolution tables |
| `scheduler/mv/BaseMVRefreshProcessor.java` | Modify | Allow refresh for safe time evolution; null-safe metadata refresh |
| `sql/common/RangePartitionDiffer.java` | Modify | Add `shouldUseAlreadyMappedRangeDiff`; rewrite isolation (return null) |
| `sql/common/PartitionDiffer.java` | Modify | Pass `mvPartitionExpr` through `collectExternalPartitionNameMapping` |
| `sql/common/ListPartitionDiffer.java` | Modify | Pass null for mvPartitionExpr; improve error logging |
| `scheduler/mv/pct/PCTPredicateBuilder.java` | Modify | Add `narrowWithMVPartitionRanges`, `clearSlotRefTableNames` |
| `sql/optimizer/.../MvUtils.java` | Modify | Tolerate duplicate ranges in `mergeRanges` |
| `connector/partitiontraits/IcebergPartitionTraits.java` | Modify | Enhanced traits for evolved partition specs |
| `connector/PartitionUtil.java` | Modify | Null-safe handling for external partition mappings |
| `connector/iceberg/MockIcebergMetadata.java` (test) | Modify | Add `t0_month_to_day_evolution` mock table |
| `scheduler/PartitionBasedMvRefreshProcessorIcebergTest.java` (test) | Modify | Add evolution refresh tests |
| `sql/optimizer/.../MvRefreshAndRewriteIcebergTest.java` (test) | Modify | Add creation + rewrite isolation tests |

All paths are relative to `fe/fe-core/src/main/java/com/starrocks/` (or `src/test/java/` for tests).

---

### Task 1: Create Branch and Setup

**Files:** None (git operations only)

- [ ] **Step 1: Create new branch from parent of feature commit**

```bash
git checkout 367cd3e050
git checkout -b iceberg-time-evolution-minimal
```

- [ ] **Step 2: Verify baseline compiles**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 3: Verify baseline tests pass for Iceberg MV**

```bash
cd fe && mvn test -pl fe-core -Dtest="PartitionBasedMvRefreshProcessorIcebergTest" -DfailIfNoTests=false -q 2>&1 | tail -5
```

Expected: Tests pass (or are skipped gracefully)

---

### Task 2: Add Evolution Detection Utilities to IcebergPartitionUtils

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergPartitionUtils.java`

- [ ] **Step 1: Add `isAllTimeTransforms` method**

Add after existing methods (around line 442):

```java
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import java.util.Map;

/**
 * Check if all partition transforms for a given source field across ALL specs are time-family
 * (YEAR, MONTH, DAY, HOUR). This is the core safety check for partition evolution support.
 */
public static boolean isAllTimeTransforms(org.apache.iceberg.Table icebergTable, int sourceFieldId) {
    for (Map.Entry<Integer, PartitionSpec> entry : icebergTable.specs().entrySet()) {
        PartitionSpec spec = entry.getValue();
        for (PartitionField field : spec.fields()) {
            if (field.sourceId() == sourceFieldId) {
                String transformName = field.transform().toString().toLowerCase();
                if (!isTimeTransform(transformName)) {
                    return false;
                }
            }
        }
    }
    return true;
}

private static boolean isTimeTransform(String transformName) {
    return "year".equals(transformName)
            || "month".equals(transformName)
            || "day".equals(transformName)
            || "hour".equals(transformName);
}
```

- [ ] **Step 2: Add `isSafePartitionEvolution` method**

```java
/**
 * Check if an Iceberg table's partition evolution is safe for MV partition mapping.
 * Safe means: the given column only uses time-family transforms across all specs.
 */
public static boolean isSafePartitionEvolution(IcebergTable table, Column partitionColumn) {
    org.apache.iceberg.Table icebergTable = table.getNativeTable();
    if (icebergTable.specs().size() <= 1) {
        return false; // No evolution happened
    }
    org.apache.iceberg.types.Types.NestedField field =
            icebergTable.schema().findField(partitionColumn.getName());
    if (field == null) {
        return false;
    }
    return isAllTimeTransforms(icebergTable, field.fieldId());
}
```

- [ ] **Step 3: Add `getDateTimeIntervalFromPartition` method**

```java
/**
 * Get the DateTimeInterval for a specific partition based on its spec's transform.
 * For evolved tables, different partitions may have different intervals
 * (e.g., old monthly partitions vs new daily partitions).
 */
public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartition(
        IcebergTable table, Column partitionColumn, PartitionInfo partitionInfo) {
    if (!(partitionInfo instanceof com.starrocks.connector.iceberg.Partition)) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    com.starrocks.connector.iceberg.Partition icebergPartition =
            (com.starrocks.connector.iceberg.Partition) partitionInfo;
    int specId = icebergPartition.getSpecId();
    if (specId < 0) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    org.apache.iceberg.Table nativeTable = table.getNativeTable();
    PartitionSpec spec = nativeTable.specs().get(specId);
    if (spec == null) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    org.apache.iceberg.types.Types.NestedField schemaField =
            nativeTable.schema().findField(partitionColumn.getName());
    if (schemaField == null) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    for (PartitionField field : spec.fields()) {
        if (field.sourceId() == schemaField.fieldId()) {
            return transformToInterval(field.transform().toString().toLowerCase());
        }
    }
    return PartitionUtil.DateTimeInterval.NONE;
}

private static PartitionUtil.DateTimeInterval transformToInterval(String transformName) {
    switch (transformName) {
        case "year": return PartitionUtil.DateTimeInterval.YEAR;
        case "month": return PartitionUtil.DateTimeInterval.MONTH;
        case "day": return PartitionUtil.DateTimeInterval.DAY;
        case "hour": return PartitionUtil.DateTimeInterval.HOUR;
        default: return PartitionUtil.DateTimeInterval.NONE;
    }
}
```

- [ ] **Step 4: Add `getDateTimeIntervalFromPartitionExpr` method**

```java
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.Type;

/**
 * Extract the DateTimeInterval from a partition expression like date_trunc('month', col).
 */
public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromPartitionExpr(
        Expr mvPartitionExpr, Type columnType) {
    if (!(mvPartitionExpr instanceof FunctionCallExpr)) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    FunctionCallExpr funcExpr = (FunctionCallExpr) mvPartitionExpr;
    String funcName = funcExpr.getFunctionName();
    if (!FunctionSet.DATE_TRUNC.equalsIgnoreCase(funcName)) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    if (funcExpr.getChildren().isEmpty() || !(funcExpr.getChild(0) instanceof StringLiteral)) {
        return PartitionUtil.DateTimeInterval.NONE;
    }
    String granularity = ((StringLiteral) funcExpr.getChild(0)).getStringValue().toLowerCase();
    switch (granularity) {
        case "year": return PartitionUtil.DateTimeInterval.YEAR;
        case "month": return PartitionUtil.DateTimeInterval.MONTH;
        case "day": return PartitionUtil.DateTimeInterval.DAY;
        case "hour": return PartitionUtil.DateTimeInterval.HOUR;
        default: return PartitionUtil.DateTimeInterval.NONE;
    }
}
```

- [ ] **Step 5: Compile and verify**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergPartitionUtils.java
git commit -m "feat(iceberg): add time-family partition evolution detection utilities"
```

---

### Task 3: Add Mock Iceberg Table with Time-Family Evolution

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/connector/iceberg/MockIcebergMetadata.java`

- [ ] **Step 1: Add table constant and partition names**

Add constant near existing table names (around line 103):

```java
public static final String MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME = "t0_month_to_day_evolution";
```

Add partition names in `getTransformTablePartitionNames` method. Find the switch/if block that returns partition names for each table. Add a new case:

```java
case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME:
    return Lists.newArrayList(
            "ts_month=2024-01", "ts_month=2024-02",     // old monthly partitions
            "ts_day=2024-03-01", "ts_day=2024-03-02",    // new daily partitions
            "ts_day=2024-03-03"
    );
```

- [ ] **Step 2: Create the mock table with month-to-day evolution**

In the `mockPartitionTransforms()` method or `getPartitionTransformTable`, add creation logic for the evolution table. Follow the existing `t0_date_month_identity_evolution` pattern but use month → day:

```java
case MOCKED_PARTITIONED_EVOLUTION_MONTH_TO_DAY_TABLE_NAME: {
    // Initial spec: month("ts")
    PartitionSpec monthSpec = PartitionSpec.builderFor(schema).month("ts").build();
    TestTables.TestTable table = TestTables.create(
            new java.io.File(baseDir, tblName), tblName, schema, monthSpec, 1);
    // Evolve to day("ts")
    TableMetadata evolutionMetaData = TableMetadata.buildFrom(table.ops().current())
            .addPartitionSpec(
                    PartitionSpec.builderFor(table.ops().current().schema())
                            .day("ts").build())
            .build();
    table.ops().commit(table.ops().current(), evolutionMetaData);
    return table;
}
```

- [ ] **Step 3: Wire up the IcebergTable wrapper**

In `getPartitionTransformIcebergTable`, add the case for the new table name so it returns a properly wrapped `MockIcebergTable` with the correct column schema (id INT, data STRING, ts DATETIME).

- [ ] **Step 4: Compile test code**

```bash
cd fe && mvn test-compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/test/java/com/starrocks/connector/iceberg/MockIcebergMetadata.java
git commit -m "test(iceberg): add mock table with month-to-day partition evolution"
```

---

### Task 4: Write Failing Tests for MV Creation on Evolved Table

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRefreshAndRewriteIcebergTest.java`

- [ ] **Step 1: Add test for MV creation with date_trunc on evolved table**

```java
@Test
public void testCreateMvWithIcebergMonthToDayEvolutionUsesRangePartition() throws Exception {
    String mvName = "test_mv1";
    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
            "PARTITION BY date_trunc('day', ts)\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\"\n" +
            ")\n" +
            "AS SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month_to_day_evolution` as a;");
    final MaterializedView mv = getMv(mvName);
    Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
}
```

- [ ] **Step 2: Run test — verify it fails**

```bash
cd fe && mvn test -pl fe-core -Dtest="MvRefreshAndRewriteIcebergTest#testCreateMvWithIcebergMonthToDayEvolutionUsesRangePartition" -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: FAIL — current code throws "Do not support create materialized view when base iceberg table has partition evolution"

- [ ] **Step 3: Commit failing test**

```bash
git add fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRefreshAndRewriteIcebergTest.java
git commit -m "test(iceberg): add failing test for MV creation on evolved table"
```

---

### Task 5: Allow MV Creation for Safe Time Evolution

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IcebergTablePartitionHandler.java`

- [ ] **Step 1: Relax evolution check in `checkPartitionColumn`**

Replace the block at lines 47-49 (the `specs().size() > 1` rejection) with:

```java
import com.starrocks.catalog.Column;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;

// In checkPartitionColumn method, replace:
//   if (icebergTable.specs().size() > 1) {
//       throw new SemanticException("Do not support create materialized view when " +
//               "base iceberg table has partition evolution");
//   }
// With:
Column partitionColumn = table.getColumn(slotRef.getColumnName());
if (icebergTable.specs().size() > 1) {
    if (partitionColumn == null || !IcebergPartitionUtils.isSafePartitionEvolution(table, partitionColumn)) {
        throw new SemanticException("Do not support create materialized view when " +
                "base iceberg table has partition evolution");
    }
}

boolean hasTimeEvolution = partitionColumn != null
        && icebergTable.specs().size() > 1
        && icebergTable.schema().findField(slotRef.getColumnName()) != null
        && IcebergPartitionUtils.isAllTimeTransforms(
        icebergTable, icebergTable.schema().findField(slotRef.getColumnName()).fieldId());
```

- [ ] **Step 2: Update time-transform validation to allow different granularities for evolution**

In the switch cases for YEAR/MONTH/DAY/HOUR, change:

```java
case YEAR:
case MONTH:
case DAY:
case HOUR:
    if (hasTimeEvolution) {
        // For evolution, MV must use date_trunc but granularity can differ from current spec
        if (!MvUtils.isFuncCallExpr(partitionByExpr, FunctionSet.DATE_TRUNC)) {
            throw new SemanticException("Materialized view partition expr %s " +
                            "must use date_trunc for time-family partition evolution.",
                    ExprToSql.toSql(partitionByExpr));
        }
    } else if (!isDateTruncWithUnit(partitionByExpr, transform.name())) {
        throw new SemanticException("Materialized view partition expr %s " +
                "must be the same with base table partition transform %s, please use date_trunc" +
                "(<transform>, <partition_colum_name>) instead.",
                ExprToSql.toSql(partitionByExpr), transform.name());
    }
    context.getStatement().setRefBaseTablePartitionWithTransform(true);
    break;
```

- [ ] **Step 3: Add fallback for when column not found in current spec but evolution is safe**

After the for loop over partitionSpec.fields(), add:

```java
boolean found = false;
for (PartitionField partitionField : partitionSpec.fields()) {
    String partitionColumnName = icebergTable.schema().findColumnName(partitionField.sourceId());
    if (partitionColumnName.equalsIgnoreCase(slotRef.getColumnName())) {
        found = true;
        // ... existing switch logic ...
        break;
    }
}

// Column not in current spec but safe evolution detected — allow with date_trunc
if (!found && hasTimeEvolution && MvUtils.isFuncCallExpr(partitionByExpr, FunctionSet.DATE_TRUNC)) {
    context.getStatement().setRefBaseTablePartitionWithTransform(true);
    return;
}
if (!found) {
    throw new SemanticException("Materialized view partition column in partition exp " +
            "must be base table partition column");
}
```

- [ ] **Step 4: Run the previously failing test**

```bash
cd fe && mvn test -pl fe-core -Dtest="MvRefreshAndRewriteIcebergTest#testCreateMvWithIcebergMonthToDayEvolutionUsesRangePartition" -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS

- [ ] **Step 5: Run existing Iceberg MV tests to check for regressions**

```bash
cd fe && mvn test -pl fe-core -Dtest="MvRefreshAndRewriteIcebergTest" -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: All existing tests pass

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IcebergTablePartitionHandler.java
git commit -m "feat(iceberg): allow MV creation for safe time-family partition evolution"
```

---

### Task 6: Add Per-Partition Interval Resolution to MVPartitionCellBuilder

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/connector/MVPartitionCellBuilder.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/connector/ExternalPartitionMappingContext.java`

- [ ] **Step 1: Add `partitionInfosByName` to ExternalPartitionMappingContext**

Add field and accessor:

```java
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

// Add field:
private final Map<String, PartitionInfo> partitionInfosByName;

// Add to constructor parameter list and body:
// private ExternalPartitionMappingContext(..., Map<String, PartitionInfo> partitionInfosByName) {
//     ...
//     this.partitionInfosByName = partitionInfosByName;
// }

// Add accessor:
public Optional<PartitionInfo> getPartitionInfo(String partitionName) {
    return Optional.ofNullable(partitionInfosByName.get(partitionName));
}
```

Update all existing `create()` factory methods to pass `Collections.emptyMap()` for the new field. Add new factory methods that accept `Collection<String> basePartitionNames`:

```java
public static ExternalPartitionMappingContext create(Table baseTable,
                                                     List<Column> mvRefBasePartitionColumns,
                                                     Collection<String> basePartitionNames)
        throws AnalysisException {
    return create(baseTable, mvRefBasePartitionColumns, null, basePartitionNames);
}

public static ExternalPartitionMappingContext create(Table baseTable,
                                                     Column mvRefBasePartitionColumn,
                                                     Expr mvPartitionExpr,
                                                     Collection<String> basePartitionNames)
        throws AnalysisException {
    return create(baseTable, Collections.singletonList(mvRefBasePartitionColumn), mvPartitionExpr, basePartitionNames);
}

public static ExternalPartitionMappingContext create(Table baseTable,
                                                     List<Column> mvRefBasePartitionColumns,
                                                     Expr mvPartitionExpr,
                                                     Collection<String> basePartitionNames)
        throws AnalysisException {
    // ... existing column resolution logic ...
    // Replace: return new ExternalPartitionMappingContext(..., mvPartitionExpr);
    // With:    return new ExternalPartitionMappingContext(..., mvPartitionExpr, buildPartitionInfoMap(baseTable, basePartitionNames));
}

private static Map<String, PartitionInfo> buildPartitionInfoMap(Table baseTable,
                                                                Collection<String> basePartitionNames) {
    if (!baseTable.isIcebergTable() || basePartitionNames == null || basePartitionNames.isEmpty()) {
        return Collections.emptyMap();
    }
    return PartitionUtil.getPartitionNameWithPartitionInfo(baseTable, Lists.newArrayList(basePartitionNames));
}
```

- [ ] **Step 2: Add `ResolvedPartitionKey` inner class and `resolveInterval` to MVPartitionCellBuilder**

```java
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;

private static final class ResolvedPartitionKey {
    private final String basePartitionName;
    private final String mvPartitionName;
    private final PartitionKey partitionKey;
    private final PartitionUtil.DateTimeInterval interval;

    private ResolvedPartitionKey(String basePartitionName, String mvPartitionName,
                                 PartitionKey partitionKey, PartitionUtil.DateTimeInterval interval) {
        this.basePartitionName = basePartitionName;
        this.mvPartitionName = mvPartitionName;
        this.partitionKey = partitionKey;
        this.interval = interval;
    }
}

private static PartitionUtil.DateTimeInterval resolveInterval(
        ExternalPartitionMappingContext mappingContext,
        String basePartitionName,
        Column baseTablePartitionColumn,
        PartitionUtil.DateTimeInterval defaultInterval) {
    if (!mappingContext.getBaseTable().isIcebergTable()) {
        return defaultInterval;
    }
    PartitionUtil.DateTimeInterval resolvedInterval = mappingContext.getPartitionInfo(basePartitionName)
            .map(partitionInfo -> IcebergPartitionUtils.getDateTimeIntervalFromPartition(
                    (IcebergTable) mappingContext.getBaseTable(), baseTablePartitionColumn, partitionInfo))
            .orElse(defaultInterval);
    if (mappingContext.getMvPartitionExpr() == null) {
        return resolvedInterval;
    }
    PartitionUtil.DateTimeInterval exprInterval = IcebergPartitionUtils.getDateTimeIntervalFromPartitionExpr(
            mappingContext.getMvPartitionExpr(), baseTablePartitionColumn.getType());
    if (exprInterval == PartitionUtil.DateTimeInterval.NONE) {
        return resolvedInterval;
    }
    // For historical specs, keep the spec's own interval (coarser granularity)
    int currentSpecId = ((IcebergTable) mappingContext.getBaseTable()).getNativeTable().spec().specId();
    if (mappingContext.getPartitionInfo(basePartitionName)
            .filter(com.starrocks.connector.iceberg.Partition.class::isInstance)
            .map(com.starrocks.connector.iceberg.Partition.class::cast)
            .filter(partition -> partition.getSpecId() >= 0
                    && partition.getSpecId() != currentSpecId
                    && resolvedInterval != PartitionUtil.DateTimeInterval.NONE)
            .isPresent()) {
        return resolvedInterval;
    }
    if (exprInterval != resolvedInterval) {
        return exprInterval;
    }
    return resolvedInterval;
}
```

- [ ] **Step 3: Refactor `resolveAndSort` to return `List<ResolvedPartitionKey>`**

Replace the existing `resolveAndSort` method:

```java
private static List<ResolvedPartitionKey> resolveAndSort(
        ExternalPartitionMappingContext mappingContext,
        ExternalPartitionKeyResolver partitionKeyResolver,
        Collection<String> basePartitionNames,
        Column baseTablePartitionColumn,
        PartitionUtil.DateTimeInterval defaultInterval)
        throws AnalysisException {
    List<ResolvedPartitionKey> resolvedPartitionKeys = Lists.newArrayList();
    for (String basePartitionName : basePartitionNames) {
        PartitionKeyResolutionResult resolutionResult =
                partitionKeyResolver.resolve(mappingContext, basePartitionName);
        PartitionUtil.DateTimeInterval interval =
                resolveInterval(mappingContext, basePartitionName, baseTablePartitionColumn, defaultInterval);
        for (PartitionKey mvPartitionKey : resolutionResult.getKeys()) {
            resolvedPartitionKeys.add(new ResolvedPartitionKey(
                    basePartitionName, generateMVPartitionName(mvPartitionKey), mvPartitionKey, interval));
        }
    }
    resolvedPartitionKeys.sort((left, right) -> left.partitionKey.compareTo(right.partitionKey));
    return resolvedPartitionKeys;
}
```

- [ ] **Step 4: Update `buildRangeCells` and `buildClosedOpenRangeCells` to use `ResolvedPartitionKey`**

Update `buildRangeCells` to pass partition column and interval to `resolveAndSort`:

```java
public static PCellSortedSet buildRangeCells(Table baseTable, Column baseTablePartitionColumn,
                                             Collection<String> basePartitionNames,
                                             Expr mvPartitionExpr,
                                             PartitionUtil.DateTimeInterval basePartitionInterval)
        throws AnalysisException {
    ExternalPartitionMappingContext mappingContext =
            ExternalPartitionMappingContext.create(
                    baseTable, baseTablePartitionColumn, mvPartitionExpr, basePartitionNames);
    ExternalPartitionKeyResolver partitionKeyResolver = getResolver(baseTable);
    if (baseTable.isJDBCTable()) {
        return buildOpenClosedRangeCells(
                resolveAndSort(mappingContext, partitionKeyResolver, basePartitionNames,
                        baseTablePartitionColumn, basePartitionInterval),
                baseTablePartitionColumn, mvPartitionExpr);
    }
    return buildClosedOpenRangeCells(
            resolveAndSort(mappingContext, partitionKeyResolver, basePartitionNames,
                    baseTablePartitionColumn, basePartitionInterval),
            baseTablePartitionColumn, mvPartitionExpr);
}
```

Update `buildClosedOpenRangeCells` signature and body to use `List<ResolvedPartitionKey>` and per-partition interval. Replace `basePartitionInterval` parameter with per-entry `resolvedPartitionKey.interval`. Generate partition names from ranges via `generateRangePartitionName`.

- [ ] **Step 5: Add range-based partition name generation**

```java
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.TimeUnitUtils;
import java.time.LocalDateTime;

private static String generateRangePartitionName(Range<PartitionKey> range,
                                                 Expr mvPartitionExpr,
                                                 String defaultName) {
    if (mvPartitionExpr == null) {
        return defaultName;
    }
    String granularity = extractTimeGranularity(mvPartitionExpr);
    if (granularity != null) {
        return generateTimeRangePartitionName(range, granularity);
    }
    return generateGenericRangePartitionName(range);
}

private static String extractTimeGranularity(Expr mvPartitionExpr) {
    if (!(mvPartitionExpr instanceof FunctionCallExpr)) {
        return null;
    }
    FunctionCallExpr functionCallExpr = (FunctionCallExpr) mvPartitionExpr;
    String functionName = functionCallExpr.getFunctionName();
    if (FunctionSet.STR2DATE.equalsIgnoreCase(functionName)) {
        return TimeUnitUtils.DAY;
    }
    if (!FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
        return null;
    }
    if (functionCallExpr.getChildren().isEmpty()
            || !(functionCallExpr.getChild(0) instanceof StringLiteral)) {
        return null;
    }
    return ((StringLiteral) functionCallExpr.getChild(0)).getStringValue().toLowerCase();
}

private static String generateTimeRangePartitionName(Range<PartitionKey> range, String granularity) {
    LocalDateTime lower = toRangeDateTime(range.lowerEndpoint(), false);
    LocalDateTime upper = toRangeDateTime(range.upperEndpoint(), true);
    return SyncPartitionUtils.getMVPartitionName(lower, upper, granularity);
}

private static LocalDateTime toRangeDateTime(PartitionKey partitionKey, boolean isUpperBound) {
    LiteralExpr literalExpr = partitionKey.getKeys().get(0);
    if (literalExpr instanceof com.starrocks.sql.ast.expression.DateLiteral) {
        return ((com.starrocks.sql.ast.expression.DateLiteral) literalExpr).toLocalDateTime();
    }
    if (literalExpr == MaxLiteral.MAX_VALUE) {
        PrimitiveType primitiveType = partitionKey.getTypes().get(0);
        com.starrocks.sql.ast.expression.DateLiteral maxDate =
                com.starrocks.sql.ast.expression.DateLiteral.createMaxValue(
                        primitiveType == PrimitiveType.DATE ? com.starrocks.type.DateType.DATE
                                : com.starrocks.type.DateType.DATETIME);
        return maxDate.toLocalDateTime();
    }
    if (literalExpr.isMinValue()) {
        PrimitiveType primitiveType = partitionKey.getTypes().get(0);
        com.starrocks.sql.ast.expression.DateLiteral minDate =
                com.starrocks.sql.ast.expression.DateLiteral.createMinValue(
                        primitiveType == PrimitiveType.DATE ? com.starrocks.type.DateType.DATE
                                : com.starrocks.type.DateType.DATETIME);
        return minDate.toLocalDateTime();
    }
    throw new IllegalArgumentException(String.format(
            "Unsupported %s partition literal for range naming: %s",
            isUpperBound ? "upper" : "lower", literalExpr));
}

private static String generateGenericRangePartitionName(Range<PartitionKey> range) {
    return "p"
            + generatePartitionNameFragments(range.lowerEndpoint())
            + "_"
            + generatePartitionNameFragments(range.upperEndpoint());
}

private static String generatePartitionNameFragments(PartitionKey partitionKey) {
    return Joiner.on("_").join(partitionKey.getKeys().stream()
            .map(MVPartitionCellBuilder::generateLiteralPartitionName)
            .collect(Collectors.toList()));
}
```

- [ ] **Step 6: Add `buildRangePartitionNameMap` for range-based partition name mapping**

```java
public static PartitionNameSetMap buildMVPartitionNameMap(Table baseTable,
                                                          List<Column> mvRefBasePartitionColumns,
                                                          List<String> basePartitionNames,
                                                          Expr mvPartitionExpr)
        throws AnalysisException {
    if (mvPartitionExpr != null && mvRefBasePartitionColumns.size() == 1 && !baseTable.isJDBCTable()) {
        return buildRangePartitionNameMap(baseTable, mvRefBasePartitionColumns.get(0),
                basePartitionNames, mvPartitionExpr);
    }
    // Fall through to existing list-based logic (original buildMVPartitionNameMap body)
    ExternalPartitionMappingContext mappingContext =
            ExternalPartitionMappingContext.create(baseTable, mvRefBasePartitionColumns, basePartitionNames);
    ExternalPartitionKeyResolver partitionKeyResolver = getResolver(baseTable);
    PartitionNameSetMap mvPartitionKeySetMap = PartitionNameSetMap.of();
    for (String basePartitionName : basePartitionNames) {
        PartitionKeyResolutionResult resolutionResult =
                partitionKeyResolver.resolve(mappingContext, basePartitionName);
        PartitionKey mvPartitionKey = resolutionResult.getSingleKey();
        String mvPartitionName = generateMVPartitionName(mvPartitionKey);
        mvPartitionKeySetMap.put(mvPartitionName, basePartitionName);
    }
    return mvPartitionKeySetMap;
}

private static PartitionNameSetMap buildRangePartitionNameMap(Table baseTable,
                                                              Column baseTablePartitionColumn,
                                                              List<String> basePartitionNames,
                                                              Expr mvPartitionExpr)
        throws AnalysisException {
    ExternalPartitionMappingContext mappingContext =
            ExternalPartitionMappingContext.create(
                    baseTable, baseTablePartitionColumn, mvPartitionExpr, basePartitionNames);
    ExternalPartitionKeyResolver partitionKeyResolver = getResolver(baseTable);
    PartitionNameSetMap mvPartitionKeySetMap = PartitionNameSetMap.of();
    PartitionUtil.DateTimeInterval defaultInterval =
            PartitionUtil.getDateTimeInterval(baseTable, baseTablePartitionColumn);
    boolean isConvertToDate = PartitionUtil.isConvertToDate(mvPartitionExpr, baseTablePartitionColumn);
    PrimitiveType basePartitionColumnPrimitiveType =
            isConvertToDate ? PrimitiveType.DATE : baseTablePartitionColumn.getPrimitiveType();
    for (String basePartitionName : basePartitionNames) {
        PartitionKeyResolutionResult resolutionResult =
                partitionKeyResolver.resolve(mappingContext, basePartitionName);
        PartitionUtil.DateTimeInterval interval =
                resolveInterval(mappingContext, basePartitionName, baseTablePartitionColumn, defaultInterval);
        for (PartitionKey mvPartitionKey : resolutionResult.getKeys()) {
            PartitionKey basePartitionLowerBound =
                    isConvertToDate ? PartitionUtil.convertToDate(mvPartitionKey) : mvPartitionKey;
            if (basePartitionLowerBound.getKeys().get(0).isNullable()) {
                basePartitionLowerBound = PartitionKey.createInfinityPartitionKeyWithType(
                        ImmutableList.of(basePartitionColumnPrimitiveType), false);
            }
            PartitionKey basePartitionUpperBound = nextPartitionKey(
                    basePartitionLowerBound, interval, basePartitionColumnPrimitiveType);
            String mvPartitionName = generateRangePartitionName(
                    Range.closedOpen(basePartitionLowerBound, basePartitionUpperBound),
                    mvPartitionExpr,
                    generateMVPartitionName(mvPartitionKey));
            mvPartitionKeySetMap.put(mvPartitionName, basePartitionName);
        }
    }
    return mvPartitionKeySetMap;
}
```

- [ ] **Step 7: Update `buildListCells` to pass basePartitionNames**

```java
public static PCellSortedSet buildListCells(Table baseTable, List<Column> mvRefBasePartitionColumns,
                                            Collection<String> basePartitionNames) throws AnalysisException {
    ExternalPartitionMappingContext mappingContext =
            ExternalPartitionMappingContext.create(baseTable, mvRefBasePartitionColumns, basePartitionNames);
    // ... rest unchanged ...
}
```

- [ ] **Step 8: Compile**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/connector/MVPartitionCellBuilder.java \
        fe/fe-core/src/main/java/com/starrocks/connector/ExternalPartitionMappingContext.java
git commit -m "feat(iceberg): per-partition interval resolution for time evolution"
```

---

### Task 7: Change Partition Type from LIST to RANGE for Time Transforms

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java`

This is critical: without this change, Iceberg time-transform MVs get LIST partitions and the range-based diff/refresh logic won't work.

- [ ] **Step 1: Find `shouldUseListPartitioning` method**

Search for the method in MaterializedViewAnalyzer (inner class `MaterializedViewAnalyzerVisitor`). It currently returns `true` when `statement.isRefBaseTablePartitionWithTransform()` is true, which includes ALL Iceberg transforms (time + bucket + truncate).

- [ ] **Step 2: Update to exclude time transforms from LIST partitioning**

Replace the check so that only BUCKET/TRUNCATE use LIST. Time transforms (YEAR/MONTH/DAY/HOUR) should use RANGE:

```java
// Old:
if (statement.isRefBaseTablePartitionWithTransform()) {
    return true;
}

// New:
if (partitionRefTableExpr instanceof FunctionCallExpr) {
    String functionName = ((FunctionCallExpr) partitionRefTableExpr).getFunctionName();
    if (FunctionSet.ICEBERG_TRANSFORM_BUCKET.equalsIgnoreCase(functionName)
            || FunctionSet.ICEBERG_TRANSFORM_TRUNCATE.equalsIgnoreCase(functionName)) {
        return true;
    }
}
// Iceberg time transforms (YEAR/MONTH/DAY/HOUR) now use RANGE partition.
```

- [ ] **Step 3: Also remove the early evolution rejection in `analyzeQueryAndTables`**

Find the block around line 215 that checks `icebergTable.getNativeTable().specs().size() > 1` and throws. Replace with a comment that defers the check to the partition column validation:

```java
// Partition evolution check for Iceberg tables is deferred to the partition column
// validation where we know whether the MV is partitioned.
// Non-partitioned MVs are immune to partition evolution.
```

- [ ] **Step 4: Compile and run creation test**

```bash
cd fe && mvn test -pl fe-core -Dtest="MvRefreshAndRewriteIcebergTest#testCreateMvWithIcebergMonthToDayEvolutionUsesRangePartition" -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: PASS (MV created with RANGE partition)

- [ ] **Step 5: Run existing tests for regression**

```bash
cd fe && mvn test -pl fe-core -Dtest="MvRefreshAndRewriteIcebergTest" -DfailIfNoTests=false 2>&1 | tail -20
```

Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/analyzer/MaterializedViewAnalyzer.java
git commit -m "feat(iceberg): use RANGE partition for time-family transforms, defer evolution check"
```

---

### Task 8: Implement Historical Spec Resolution in IcebergPartitionKeyResolver

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergPartitionKeyResolver.java`

- [ ] **Step 1: Add Phase 2 `resolveByHistoricalSpec` method**

```java
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;

import java.util.Map;

// Update resolve() to try historical spec when current spec fails:
@Override
public PartitionKeyResolutionResult resolve(ExternalPartitionMappingContext mappingContext,
                                            String basePartitionName)
        throws AnalysisException {
    try {
        return resolveByCurrentSpec(mappingContext, basePartitionName);
    } catch (Exception e) {
        // Phase 2: try historical spec resolution for evolved tables
        if (mappingContext.getBaseTable().isIcebergTable()) {
            PartitionKeyResolutionResult historicalResult =
                    resolveByHistoricalSpec(mappingContext, basePartitionName);
            if (historicalResult != null) {
                return historicalResult;
            }
        }
        throw e;
    }
}

/**
 * Phase 2: Resolve partition key using historical partition specs.
 * When a table evolves from month(ts) to day(ts), old partitions like "ts_month=2024-01"
 * cannot be resolved by the current spec. We search historical specs to find the matching
 * transform and create the partition key accordingly.
 */
private PartitionKeyResolutionResult resolveByHistoricalSpec(
        ExternalPartitionMappingContext mappingContext,
        String basePartitionName) throws AnalysisException {
    IcebergTable table = (IcebergTable) mappingContext.getBaseTable();
    org.apache.iceberg.Table nativeTable = table.getNativeTable();
    int currentSpecId = nativeTable.spec().specId();

    // Try each historical spec
    for (Map.Entry<Integer, PartitionSpec> entry : nativeTable.specs().entrySet()) {
        if (entry.getKey() == currentSpecId) {
            continue; // Skip current spec (already tried in Phase 1)
        }
        PartitionSpec historicalSpec = entry.getValue();
        PartitionKeyResolutionResult result =
                tryResolveWithSpec(mappingContext, basePartitionName, historicalSpec);
        if (result != null) {
            return result;
        }
    }
    return null;
}

/**
 * Try to resolve partition name using a specific partition spec.
 * Returns null if the partition name doesn't match this spec's format.
 */
private PartitionKeyResolutionResult tryResolveWithSpec(
        ExternalPartitionMappingContext mappingContext,
        String basePartitionName,
        PartitionSpec spec) throws AnalysisException {
    List<String> partitionValues = PartitionUtil.toPartitionValues(basePartitionName);
    List<Integer> mvRefBasePartitionColumnIndexes = mappingContext.getMvRefBasePartitionColumnIndexes();

    // For time-family evolution, the partition value format differs between specs.
    // E.g., month spec produces "ts_month=2024-01", day spec produces "ts_day=2024-03-01".
    // We need to find which spec's field matches the partition name prefix.
    for (PartitionField field : spec.fields()) {
        String columnName = mappingContext.getBaseTable() instanceof IcebergTable
                ? ((IcebergTable) mappingContext.getBaseTable()).getNativeTable()
                .schema().findColumnName(field.sourceId())
                : null;
        if (columnName == null) {
            continue;
        }
        // Check if this field's column matches any MV ref partition column
        for (int i = 0; i < mappingContext.getMvRefBasePartitionColumns().size(); i++) {
            if (columnName.equalsIgnoreCase(mappingContext.getMvRefBasePartitionColumns().get(i).getName())) {
                try {
                    // Try to create partition key from the value using this spec's transform context
                    List<String> values = mvRefBasePartitionColumnIndexes.stream()
                            .map(partitionValues::get)
                            .collect(Collectors.toList());
                    List<com.starrocks.catalog.Column> columns = mvRefBasePartitionColumnIndexes.stream()
                            .map(mappingContext.getBaseTablePartitionColumns()::get)
                            .collect(Collectors.toList());
                    PartitionKey mvPartitionKey = PartitionUtil.createPartitionKey(
                            values, columns, mappingContext.getBaseTable());
                    return PartitionKeyResolutionResult.of(
                            mvPartitionKey, PartitionKeyResolutionPath.ICEBERG_HISTORICAL_SPEC);
                } catch (Exception e) {
                    // This spec doesn't match — continue to next
                    break;
                }
            }
        }
    }
    return null;
}
```

- [ ] **Step 2: Compile**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/connector/iceberg/IcebergPartitionKeyResolver.java
git commit -m "feat(iceberg): add historical spec resolution for partition key resolver"
```

---

### Task 9: Allow MV Refresh for Evolved Tables

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/BaseMVRefreshProcessor.java`

- [ ] **Step 1: Replace blanket evolution rejection with safe-check**

Find the evolution check block (around line 738-745). Replace:

```java
// Old:
if (table instanceof IcebergTable) {
    IcebergTable icebergTable = (IcebergTable) table;
    if (icebergTable.getNativeTable().specs().size() > 1) {
        throw new DmlException("...");
    }
}

// New:
import com.starrocks.connector.iceberg.IcebergPartitionUtils;

if (table instanceof IcebergTable && !mv.getPartitionInfo().isUnPartitioned()) {
    IcebergTable icebergTable = (IcebergTable) table;
    if (icebergTable.getNativeTable().specs().size() > 1) {
        Map<Table, List<Column>> refPartitionColumns = mv.getRefBaseTablePartitionColumns();
        List<Column> partitionColumns = refPartitionColumns.get(icebergTable);
        boolean safe = false;
        if (partitionColumns != null) {
            for (Column col : partitionColumns) {
                if (IcebergPartitionUtils.isSafePartitionEvolution(icebergTable, col)) {
                    safe = true;
                    break;
                }
            }
        }
        if (!safe) {
            throw new DmlException("Do not support refresh materialized view when base iceberg table " +
                    table.getName() + " has done partition evolution. Please realign the MV " +
                    "partition scheme with ALTER MATERIALIZED VIEW ... PARTITION BY ... first.");
        }
    }
}
```

- [ ] **Step 2: Add null-safe metadata refresh for evolved partition names**

Find the metadata refresh block (around line 628-635) where `realPartitionNames` is built. Replace:

```java
// Old:
final List<String> realPartitionNames = basePartitions.stream()
        .flatMap(pCell -> mvContext.getExternalTableRealPartitionName(table, pCell.name()).stream())
        .collect(Collectors.toList());
connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
        baseTableInfo.getDbName(), table, realPartitionNames, false);

// New:
boolean missingRealPartitionNames = false;
final List<String> realPartitionNames = new ArrayList<>();
for (PCellWithName pCell : basePartitions.getPartitions()) {
    Set<String> mappedPartitionNames =
            mvContext.getExternalTableRealPartitionName(table, pCell.name());
    if (mappedPartitionNames == null || mappedPartitionNames.isEmpty()) {
        logger.info("Cannot resolve real partition names for table {} logical partition {}, " +
                        "fallback to full metadata refresh",
                table.getName(), pCell.name());
        missingRealPartitionNames = true;
        break;
    }
    realPartitionNames.addAll(mappedPartitionNames);
}
connectContext.getGlobalStateMgr().getMetadataMgr().refreshTable(baseTableInfo.getCatalogName(),
        baseTableInfo.getDbName(), table,
        missingRealPartitionNames ? Lists.newArrayList() : realPartitionNames,
        missingRealPartitionNames);
```

Add import: `import com.starrocks.sql.common.PCellWithName;`

- [ ] **Step 3: Compile**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/scheduler/mv/BaseMVRefreshProcessor.java
git commit -m "feat(iceberg): allow refresh for safe time-family partition evolution"
```

---

### Task 10: Add Partition Diff Support and Rewrite Isolation

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/common/RangePartitionDiffer.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/common/PartitionDiffer.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/common/ListPartitionDiffer.java`
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvUtils.java`

- [ ] **Step 1: Add `shouldUseAlreadyMappedRangeDiff` to RangePartitionDiffer**

```java
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.sql.ast.expression.SlotRef;

private boolean shouldUseAlreadyMappedRangeDiff(Map<Table, List<Column>> refBaseTablePartitionColumns,
                                                Expr mvPartitionExpr) {
    if (mvPartitionExpr == null || mvPartitionExpr instanceof SlotRef) {
        return false;
    }
    return refBaseTablePartitionColumns.entrySet().stream().allMatch(entry ->
            entry.getKey().isIcebergTable()
                    && entry.getValue().size() == 1
                    && IcebergPartitionUtils.isSafePartitionEvolution(
                    (IcebergTable) entry.getKey(), entry.getValue().get(0)));
}
```

- [ ] **Step 2: Add rewrite isolation in `computePartitionDiff`**

In the `computePartitionDiff` method, after computing `useAlreadyMappedRangeDiff`, add the rewrite isolation:

```java
boolean useAlreadyMappedRangeDiff =
        shouldUseAlreadyMappedRangeDiff(refBaseTablePartitionColumns, mvPartitionExpr);

// Rewrite isolation: evolved tables are not eligible for query rewrite
if (useAlreadyMappedRangeDiff && queryRewriteParams.isQueryRewrite()) {
    return null;
}
```

- [ ] **Step 3: Use `shouldUseAlreadyMappedRangeDiff` for refresh diff routing**

Update the diff computation (both the unit test block and the main block) to use `useAlreadyMappedRangeDiff`:

```java
// In unit test block:
PartitionDiff diff = useAlreadyMappedRangeDiff
        ? SyncPartitionUtils.getRangePartitionDiffOfSlotRef(basePartitionMap, mvPartitionCells, differ)
        : PartitionUtil.getPartitionDiff(mvPartitionExpr, basePartitionMap, mvPartitionCells, differ);

// In main block:
PartitionDiff rangePartitionDiff = useAlreadyMappedRangeDiff
        ? SyncPartitionUtils.getRangePartitionDiffOfSlotRef(mergedRBTPartitionKeyMap, mvPartitionCells, differ)
        : PartitionUtil.getPartitionDiff(mvPartitionExpr, mergedRBTPartitionKeyMap, mvPartitionCells, differ);
```

- [ ] **Step 4: Update `PartitionDiffer.collectExternalPartitionNameMapping` to pass mvPartitionExpr**

```java
// Change signature:
public static void collectExternalPartitionNameMapping(Map<Table, List<Column>> partitionTableAndColumns,
                                                       Expr mvPartitionExpr,
                                                       Map<Table, PartitionNameSetMap> result) throws AnalysisException {
    for (Map.Entry<Table, List<Column>> e : partitionTableAndColumns.entrySet()) {
        Table refBaseTable = e.getKey();
        List<Column> refPartitionColumns = e.getValue();
        collectExternalBaseTablePartitionMapping(refBaseTable, refPartitionColumns, mvPartitionExpr, result);
    }
}

// Update private method:
private static void collectExternalBaseTablePartitionMapping(
        Table refBaseTable,
        List<Column> refTablePartitionColumns,
        Expr mvPartitionExpr,
        Map<Table, PartitionNameSetMap> result) throws AnalysisException {
    if (refBaseTable.isNativeTableOrMaterializedView()) {
        return;
    }
    PartitionNameSetMap mvPartitionNameMap = MVPartitionCellBuilder.buildMVPartitionNameMap(refBaseTable,
            refTablePartitionColumns, PartitionUtil.getPartitionNames(refBaseTable), mvPartitionExpr);
    result.put(refBaseTable, mvPartitionNameMap);
}
```

Add `import com.starrocks.sql.ast.expression.Expr;` to PartitionDiffer.

- [ ] **Step 5: Update callers of `collectExternalPartitionNameMapping`**

In `RangePartitionDiffer.computePartitionDiff`:
```java
collectExternalPartitionNameMapping(refBaseTablePartitionColumns, mvPartitionExpr, extRBTMVPartitionNameMap);
```

In `ListPartitionDiffer`:
```java
collectExternalPartitionNameMapping(mv.getRefBaseTablePartitionColumns(), null, externalPartitionMaps);
```

- [ ] **Step 6: Tolerate duplicate ranges in MvUtils.mergeRanges**

First overload — replace the overlap throw with:
```java
Range<PartitionKey> intersection = currentRange.intersection(mergedRange);
if (!intersection.isEmpty()) {
    if (currentRange.equals(mergedRange)) {
        merged = true;
        break;
    }
    throw new IllegalStateException("Partition ranges overlap: " +
            currentRange + " and " + mergedRange);
}
```

Second overload — replace Preconditions.checkState with:
```java
Range<PartitionKey> intersection = currentRange.intersection(mergedRange);
if (!intersection.isEmpty()) {
    Preconditions.checkState(currentRange.equals(mergedRange));
    queryMergeRangesToPartitionIds.get(Box.of(mergedRange)).add(partitionId);
    merged = true;
    break;
}
```

- [ ] **Step 7: Compile**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/common/RangePartitionDiffer.java \
        fe/fe-core/src/main/java/com/starrocks/sql/common/PartitionDiffer.java \
        fe/fe-core/src/main/java/com/starrocks/sql/common/ListPartitionDiffer.java \
        fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvUtils.java
git commit -m "feat(iceberg): partition diff routing and rewrite isolation for evolution"
```

---

### Task 11: Add Refresh Predicate Narrowing (PCTPredicateBuilder)

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/pct/PCTPredicateBuilder.java`

- [ ] **Step 1: Add `narrowWithMVPartitionRanges` method**

```java
/**
 * Narrow base table partition ranges by intersecting with MV partition ranges.
 * When MV is finer-grained than base table (e.g., MV daily vs Iceberg monthly),
 * this narrows [2024-01-01, 2024-02-01) to [2024-01-15, 2024-01-16) for the specific
 * MV partitions being refreshed.
 */
private static List<Range<PartitionKey>> narrowWithMVPartitionRanges(
        PCTPartitionTopology partitionTopology,
        Table table,
        List<Range<PartitionKey>> baseTableRanges) {
    if (baseTableRanges.isEmpty() || partitionTopology == null) {
        return baseTableRanges;
    }
    Map<String, Map<Table, PCellSortedSet>> mvToBaseNameRefs =
            partitionTopology.getMvRefBaseTableIntersectedPartitions();
    if (mvToBaseNameRefs == null || mvToBaseNameRefs.isEmpty()) {
        return baseTableRanges;
    }
    PCellSortedSet mvToCellMap = partitionTopology.getMvToCellMap();
    if (mvToCellMap == null || mvToCellMap.isEmpty()) {
        return baseTableRanges;
    }

    List<Range<PartitionKey>> mvRanges = Lists.newArrayList();
    for (Map.Entry<String, Map<Table, PCellSortedSet>> entry : mvToBaseNameRefs.entrySet()) {
        String mvPartitionName = entry.getKey();
        Map<Table, PCellSortedSet> tablePartitions = entry.getValue();
        if (tablePartitions.containsKey(table) && mvToCellMap.containsName(mvPartitionName)) {
            PRangeCell mvRangeCell = (PRangeCell) mvToCellMap.getPCell(mvPartitionName);
            mvRanges.add(mvRangeCell.getRange());
        }
    }
    if (mvRanges.isEmpty()) {
        return baseTableRanges;
    }
    mvRanges = MvUtils.mergeRanges(mvRanges);

    List<Range<PartitionKey>> narrowed = Lists.newArrayList();
    for (Range<PartitionKey> baseRange : baseTableRanges) {
        for (Range<PartitionKey> mvRange : mvRanges) {
            if (baseRange.isConnected(mvRange)) {
                Range<PartitionKey> intersection = baseRange.intersection(mvRange);
                if (!intersection.isEmpty()) {
                    narrowed.add(intersection);
                }
            }
        }
    }
    if (narrowed.isEmpty()) {
        return baseTableRanges;
    }
    return MvUtils.mergeRanges(narrowed);
}
```

- [ ] **Step 2: Call narrowing in `buildRangePartitionPredicate`**

In `buildRangePartitionPredicate`, after building `sourceTablePartitionRange` from the loop and before the `MvUtils.mergeRanges` call, add:

```java
sourceTablePartitionRange = narrowWithMVPartitionRanges(partitionTopology, table, sourceTablePartitionRange);
```

- [ ] **Step 3: Add `clearSlotRefTableNames` helper and call it**

```java
private static void clearSlotRefTableNames(Expr expr) {
    if (expr instanceof SlotRef slotRef) {
        slotRef.setTblName(null);
    }
    for (Expr child : expr.getChildren()) {
        clearSlotRefTableNames(child);
    }
}
```

Call this in the list partition predicate building method where `mvPartitionExpr` is used for IN predicate generation (find where `MvUtils.convertToInPredicate` is called):

```java
Expr mvPartitionExpr = mvPartitionExprs.get(0);
clearSlotRefTableNames(mvPartitionExpr);
```

- [ ] **Step 4: Compile**

```bash
cd fe && mvn compile -pl fe-core -am -DskipTests -q
```

Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
git add fe/fe-core/src/main/java/com/starrocks/scheduler/mv/pct/PCTPredicateBuilder.java
git commit -m "feat(iceberg): add range narrowing for evolution refresh predicates"
```

---

### Task 12: Write and Run Comprehensive Tests

**Files:**
- Modify: `fe/fe-core/src/test/java/com/starrocks/scheduler/PartitionBasedMvRefreshProcessorIcebergTest.java`
- Modify: `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRefreshAndRewriteIcebergTest.java`

- [ ] **Step 1: Add refresh test for month-to-day evolution**

In `PartitionBasedMvRefreshProcessorIcebergTest.java`:

```java
@Test
public void testRefreshMvWithIcebergMonthToDayEvolution() throws Exception {
    String mvName = "test_mv_evolution";
    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" +
            "PARTITION BY date_trunc('day', ts)\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES (\"replication_num\" = \"1\")\n" +
            "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month_to_day_evolution` as a;");
    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    MaterializedView mv = getMv(mvName);

    // Trigger refresh
    triggerRefreshMv(testDb, mv);

    // Verify MV has partitions covering both monthly and daily ranges
    Assertions.assertTrue(mv.getPartitionInfo().isRangePartition());
    Assertions.assertFalse(mv.getPartitions().isEmpty(),
            "MV should have partitions after refresh");
}
```

- [ ] **Step 2: Add test verifying non-evolution Iceberg still works**

```java
@Test
public void testRefreshMvWithNonEvolvedIcebergTableUnchanged() throws Exception {
    // This tests that non-evolution tables work exactly as before
    String mvName = "test_mv_no_evolution";
    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName + "\n" +
            "PARTITION BY date_trunc('month', ts)\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES (\"replication_num\" = \"1\")\n" +
            "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month` as a;");
    Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    MaterializedView mv = getMv(mvName);

    triggerRefreshMv(testDb, mv);
    Assertions.assertFalse(mv.getPartitions().isEmpty());
}
```

- [ ] **Step 3: Add test for rewrite isolation**

In `MvRefreshAndRewriteIcebergTest.java`:

```java
@Test
public void testQueryRewriteBlockedForEvolvedIcebergTable() throws Exception {
    String mvName = "test_mv1";
    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
            "PARTITION BY date_trunc('day', ts)\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES (\"replication_num\" = \"1\")\n" +
            "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month_to_day_evolution` as a;");

    // Query should NOT be rewritten using the MV (evolution = no rewrite)
    String query = "SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month_to_day_evolution` WHERE ts >= '2024-01-01' AND ts < '2024-02-01'";
    String plan = getFragmentPlan(query);
    // Should NOT contain the MV name in the plan (no rewrite)
    Assertions.assertFalse(plan.contains("test_mv1"),
            "Query should not be rewritten using MV on evolved table");
}
```

- [ ] **Step 4: Add test for non-evolution rewrite still works**

```java
@Test
public void testQueryRewriteStillWorksForNonEvolvedIcebergTable() throws Exception {
    // Verify existing rewrite functionality is not broken
    String mvName = "test_mv1";
    starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test_mv1\n" +
            "PARTITION BY date_trunc('month', ts)\n" +
            "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
            "REFRESH DEFERRED MANUAL\n" +
            "PROPERTIES (\"replication_num\" = \"1\")\n" +
            "AS SELECT id, data, ts FROM `iceberg0`.`partitioned_transforms_db`." +
            "`t0_month` as a;");
    // Non-evolved table should still be eligible for rewrite after refresh
    // (Exact assertion depends on whether refresh is run in test setup)
}
```

- [ ] **Step 5: Run all tests**

```bash
cd fe && mvn test -pl fe-core -Dtest="PartitionBasedMvRefreshProcessorIcebergTest,MvRefreshAndRewriteIcebergTest" -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: All tests pass (both new and existing)

- [ ] **Step 6: Commit**

```bash
git add fe/fe-core/src/test/java/com/starrocks/scheduler/PartitionBasedMvRefreshProcessorIcebergTest.java \
        fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/materialization/MvRefreshAndRewriteIcebergTest.java
git commit -m "test(iceberg): add tests for time-family evolution MV creation, refresh, and rewrite isolation"
```

---

### Task 13: Run Full Regression Suite and Fix Issues

**Files:** Any files from previous tasks that need fixes

- [ ] **Step 1: Run full Iceberg-related test suite**

```bash
cd fe && mvn test -pl fe-core -Dtest="*Iceberg*" -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: All tests pass

- [ ] **Step 2: Run MV-related test suite for regression check**

```bash
cd fe && mvn test -pl fe-core -Dtest="*MvRefresh*,*MvRewrite*,*MVTest*" -DfailIfNoTests=false 2>&1 | tail -30
```

Expected: All tests pass — no regressions

- [ ] **Step 3: Run checkstyle**

```bash
cd fe && mvn checkstyle:check -pl fe-core -q
```

Expected: No violations

- [ ] **Step 4: Fix any issues found and commit**

If any tests fail, diagnose the root cause, fix, and commit with descriptive message.

- [ ] **Step 5: Final commit if needed**

```bash
git log --oneline HEAD~10..HEAD
```

Review the commit history to ensure it's clean and logical.
