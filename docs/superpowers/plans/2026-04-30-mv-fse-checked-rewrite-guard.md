# MV FSE CHECKED Rewrite Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `query_rewrite_consistency = CHECKED` 的 MV fast schema evolution ADD COLUMN 场景增加
query-column-aware、partition-aware 的 rewrite guard，并用 SQL case 固定用户可感知行为。

**Architecture:** 在 `MaterializedView.AsyncRefreshContext` 持久化按 FSE column 维护的 invalid base partitions。
ALTER MV ADD COLUMN 初始化 metadata；MV refresh 成功后从 invalid set 中移除已刷新 partitions；MV rewrite prepare 阶段
只在查询实际依赖 FSE 新增列时把 invalid partitions 注入 `MvUpdateInfo`，交给现有 transparent compensation 处理。

**Tech Stack:** StarRocks FE Java 17、Gson `@SerializedName` edit-log 持久化、MV timeliness/compensation framework、
`test/` SQL-tester。

---

## 文件结构

- 修改 `fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedView.java`
  - 在 `AsyncRefreshContext` 下新增 FSE column freshness metadata。
  - 提供新增、查询、refresh pruning、partition cleanup 相关方法。
- 修改 `fe/fe-core/src/main/java/com/starrocks/alter/AlterMVJobExecutor.java`
  - 在 `ALTER MATERIALIZED VIEW ... ADD COLUMN ...` 成功后初始化 FSE metadata。
  - 对本期支持范围内的 CHECKED strict 场景放宽原先的 affected-partition 拒绝。
  - simple base column reference 才标记为 null-safe；表达式和 aggregate measure refresh 前保守 invalid。
- 修改 `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVVersionManager.java`
  - MV refresh 更新 base version map 时同步清理 FSE invalid partitions。
- 修改 `fe/fe-core/src/main/java/com/starrocks/catalog/MvUpdateInfo.java`
  - 增加把 `NO_REFRESH` copy 成 `PARTIAL` 的 helper，保证 FSE 注入 partitions 后 compensation 能生效。
- 修改 `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/MvRewritePreprocessor.java`
  - 在普通 CHECKED timeliness 后、`createScanMvOperator` 前应用 FSE guard。
  - 收集查询实际使用的 base columns；只有命中 FSE column 才注入 invalid partitions。
- 新增 `test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql`
  - SQL-first 覆盖非聚合、聚合 GROUP BY 维度、aggregate measure 保守行为、refresh 后转 safe。
- 新增 `test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result`
  - 记录 SQL-tester 期望输出。

## Task 1: 写 SQL-first 失败用例

**Files:**
- Create: `test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql`
- Create: `test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result`

- [ ] **Step 1: 创建 T 文件，固定 CHECKED strict 非聚合行为**

写入 `test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql` 的第一段：

```sql
-- name: test_mv_fse_checked_rewrite_guard @sequential
-- Test Point:
--   1. CHECKED strict MV FSE ADD COLUMN can succeed with tracked invalid partitions.
--   2. Query not referencing the FSE column still rewrites normally.
--   3. Query referencing the FSE column reads safe partitions from MV and invalid partitions from base compensation.
-- Method: Use p1/p2/p3 partition timeline, assert query results and EXPLAIN plan fragments.
-- Scope: MV Fast Schema Evolution, CHECKED rewrite, transparent compensation.

admin set frontend config('alter_scheduler_interval_millisecond' = '100');
admin set frontend config('mv_fast_schema_change_mode' = 'strict');

create database db_${uuid0};
use db_${uuid0};

set enable_materialized_view_rewrite = true;
set enable_materialized_view_union_rewrite = true;
set cbo_materialized_view_rewrite_candidate_limit = 8;

drop materialized view if exists mv_fse_detail;
drop table if exists fse_detail;

create table fse_detail (
  dt date not null,
  k int not null,
  v int null
) engine=olap
duplicate key(dt, k)
partition by range(dt) (
  partition p1 values [('2026-01-01'), ('2026-01-02')),
  partition p2 values [('2026-01-02'), ('2026-01-03')),
  partition p3 values [('2026-01-03'), ('2026-01-04'))
)
distributed by hash(k) buckets 1
properties ("replication_num" = "1");

insert into fse_detail values
('2026-01-01', 1, 10),
('2026-01-02', 2, 20);

create materialized view mv_fse_detail
partition by dt
distributed by hash(k) buckets 1
properties (
  "replication_num" = "1",
  "query_rewrite_consistency" = "checked"
)
as select dt, k, v from fse_detail;

[UC]refresh materialized view mv_fse_detail with sync mode;

alter table fse_detail add column c3 int null;
function: wait_alter_table_finish()

insert into fse_detail values ('2026-01-02', 22, 220, 200);

alter materialized view mv_fse_detail add column c3 as c3;
function: wait_alter_table_finish()

insert into fse_detail values ('2026-01-03', 3, 30, 300);
[UC]refresh materialized view mv_fse_detail partition start ('2026-01-03') end ('2026-01-04') with sync mode;

function: print_hit_materialized_view("select dt, k, v from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04'", "mv_fse_detail")

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", "TABLE: fse_detail")

[ORDER]select dt, k, c3 from fse_detail
where dt >= '2026-01-01' and dt < '2026-01-04'
order by dt, k;
-- result:
2026-01-01	1	NULL
2026-01-02	2	NULL
2026-01-02	22	200
2026-01-03	3	300
-- !result

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt = '2026-01-02' order by dt, k", "TABLE: fse_detail")
```

- [ ] **Step 2: 追加聚合 GROUP BY 维度和 aggregate measure 行为**

继续写入同一个 T 文件：

```sql
drop materialized view if exists mv_fse_agg;
drop table if exists fse_agg;

create table fse_agg (
  dt date not null,
  k int not null,
  v int null
) engine=olap
duplicate key(dt, k)
partition by range(dt) (
  partition p1 values [('2026-02-01'), ('2026-02-02')),
  partition p2 values [('2026-02-02'), ('2026-02-03')),
  partition p3 values [('2026-02-03'), ('2026-02-04'))
)
distributed by hash(k) buckets 1
properties ("replication_num" = "1");

insert into fse_agg values
('2026-02-01', 1, 10),
('2026-02-02', 2, 20);

create materialized view mv_fse_agg
partition by dt
distributed by hash(k) buckets 1
properties (
  "replication_num" = "1",
  "query_rewrite_consistency" = "checked"
)
as select dt, k, sum(v) as sum_v from fse_agg group by dt, k;

[UC]refresh materialized view mv_fse_agg with sync mode;

alter table fse_agg add column c3 int null;
function: wait_alter_table_finish()

insert into fse_agg values ('2026-02-02', 22, 220, 200);

alter materialized view mv_fse_agg add column c3 as c3;
function: wait_alter_table_finish()

insert into fse_agg values ('2026-02-03', 3, 30, 300);
[UC]refresh materialized view mv_fse_agg partition start ('2026-02-03') end ('2026-02-04') with sync mode;

function: print_hit_materialized_view("select dt, k, sum(v) as sum_v from fse_agg group by dt, k", "mv_fse_agg")

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", "TABLE: fse_agg")

[ORDER]select dt, k, c3, sum(v) from fse_agg
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-01	1	NULL	10
2026-02-02	2	NULL	20
2026-02-02	22	200	220
2026-02-03	3	300	30
-- !result

alter table fse_agg add column c4 int null;
function: wait_alter_table_finish()

insert into fse_agg values ('2026-02-02', 23, 230, 201, 400);

alter materialized view mv_fse_agg add column sum_c4 as sum(c4);
function: wait_alter_table_finish()

function: assert_query_contains("explain select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg")

[UC]refresh materialized view mv_fse_agg with sync mode;

function: print_hit_materialized_view("select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3", "mv_fse_agg")

drop materialized view mv_fse_agg;
drop table fse_agg;
drop materialized view mv_fse_detail;
drop table fse_detail;
drop database db_${uuid0} force;
```

- [ ] **Step 3: 创建 R 文件**

创建 `test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result`，内容先与 T 文件一致。实现完成后用 record 模式更新
helper 函数输出，保留手写的 `-- result:` 数据断言。

```bash
cp test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql \
  test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result
```

- [ ] **Step 4: 跑当前失败，确认测试能卡住旧行为**

运行：

```bash
cd test
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
.venv/bin/python run.py \
  -d sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result \
  --skip_reruns -v -c 1 -t 300
```

Expected: FAIL。当前旧行为会在 strict 模式下拒绝 `alter materialized view mv_fse_detail add column c3 as c3`，
错误中包含 `partitions in the base table` 或 `unsupported materialized view fast schema evolution`。

- [ ] **Step 5: 提交 SQL 失败用例**

```bash
git add test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql \
        test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result
git commit -m "test: add MV FSE checked rewrite guard SQL cases"
```

## Task 2: 增加 FSE column freshness metadata

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedView.java`

- [ ] **Step 1: 在 `AsyncRefreshContext` 前新增 metadata class**

在 `MaterializedView.BasePartitionInfo` 后、`AsyncRefreshContext` 前加入：

```java
    public static class FSEColumnFreshnessInfo {
        @SerializedName(value = "mvColumnName")
        private String mvColumnName;
        @SerializedName(value = "mvColumnUniqueId")
        private int mvColumnUniqueId;
        @SerializedName(value = "baseTableId")
        private long baseTableId;
        @SerializedName(value = "baseColumnName")
        private String baseColumnName;
        @SerializedName(value = "baseColumnUniqueId")
        private int baseColumnUniqueId;
        @SerializedName(value = "baseColumnCreatedTime")
        private long baseColumnCreatedTime;
        @SerializedName(value = "mvColumnAddedTime")
        private long mvColumnAddedTime;
        @SerializedName(value = "simpleBaseColumnRef")
        private boolean simpleBaseColumnRef;
        @SerializedName(value = "invalidBasePartitionNames")
        private Set<String> invalidBasePartitionNames = Sets.newConcurrentHashSet();

        public FSEColumnFreshnessInfo() {
        }

        public FSEColumnFreshnessInfo(String mvColumnName, int mvColumnUniqueId, long baseTableId,
                                      String baseColumnName, int baseColumnUniqueId, long baseColumnCreatedTime,
                                      long mvColumnAddedTime, boolean simpleBaseColumnRef,
                                      Set<String> invalidBasePartitionNames) {
            this.mvColumnName = mvColumnName;
            this.mvColumnUniqueId = mvColumnUniqueId;
            this.baseTableId = baseTableId;
            this.baseColumnName = baseColumnName;
            this.baseColumnUniqueId = baseColumnUniqueId;
            this.baseColumnCreatedTime = baseColumnCreatedTime;
            this.mvColumnAddedTime = mvColumnAddedTime;
            this.simpleBaseColumnRef = simpleBaseColumnRef;
            this.invalidBasePartitionNames = Sets.newConcurrentHashSet();
            if (invalidBasePartitionNames != null) {
                this.invalidBasePartitionNames.addAll(invalidBasePartitionNames);
            }
        }

        public String getMvColumnName() {
            return mvColumnName;
        }

        public int getMvColumnUniqueId() {
            return mvColumnUniqueId;
        }

        public long getBaseTableId() {
            return baseTableId;
        }

        public String getBaseColumnName() {
            return baseColumnName;
        }

        public int getBaseColumnUniqueId() {
            return baseColumnUniqueId;
        }

        public long getBaseColumnCreatedTime() {
            return baseColumnCreatedTime;
        }

        public long getMvColumnAddedTime() {
            return mvColumnAddedTime;
        }

        public boolean isSimpleBaseColumnRef() {
            return simpleBaseColumnRef;
        }

        public Set<String> getInvalidBasePartitionNames() {
            if (invalidBasePartitionNames == null) {
                invalidBasePartitionNames = Sets.newConcurrentHashSet();
            }
            return invalidBasePartitionNames;
        }

        public void removeInvalidBasePartitions(Set<String> partitionNames) {
            if (CollectionUtils.isEmpty(partitionNames)) {
                return;
            }
            getInvalidBasePartitionNames().removeAll(partitionNames);
        }

        public void retainInvalidBasePartitions(Set<String> visiblePartitionNames) {
            if (CollectionUtils.isEmpty(visiblePartitionNames)) {
                getInvalidBasePartitionNames().clear();
                return;
            }
            getInvalidBasePartitionNames().removeIf(partitionName -> !visiblePartitionNames.contains(partitionName));
        }
    }
```

- [ ] **Step 2: 在 `AsyncRefreshContext` 增加 map 字段和初始化**

加入字段：

```java
        @SerializedName("fseColumnFreshnessInfoMap")
        private final Map<String, FSEColumnFreshnessInfo> fseColumnFreshnessInfoMap;
```

在 `AsyncRefreshContext()` 构造函数中初始化：

```java
            this.fseColumnFreshnessInfoMap = Maps.newConcurrentMap();
```

- [ ] **Step 3: 增加 metadata 访问和更新方法**

在 `AsyncRefreshContext` 的 getter 区域加入：

```java
        public Map<String, FSEColumnFreshnessInfo> getFSEColumnFreshnessInfoMap() {
            return fseColumnFreshnessInfoMap;
        }

        public void putFSEColumnFreshnessInfo(FSEColumnFreshnessInfo info) {
            if (info == null) {
                return;
            }
            fseColumnFreshnessInfoMap.put(info.getMvColumnName(), info);
        }

        public void removeInvalidBasePartitionsForFSEColumns(long baseTableId, Set<String> partitionNames) {
            if (CollectionUtils.isEmpty(partitionNames)) {
                return;
            }
            for (FSEColumnFreshnessInfo info : fseColumnFreshnessInfoMap.values()) {
                if (info.getBaseTableId() == baseTableId) {
                    info.removeInvalidBasePartitions(partitionNames);
                }
            }
        }

        public void retainInvalidBasePartitionsForFSEColumns(long baseTableId, Set<String> visiblePartitionNames) {
            for (FSEColumnFreshnessInfo info : fseColumnFreshnessInfoMap.values()) {
                if (info.getBaseTableId() == baseTableId) {
                    info.retainInvalidBasePartitions(visiblePartitionNames);
                }
            }
        }
```

- [ ] **Step 4: 清理 visible version map 时同步清理 FSE invalid metadata**

在 `clearVisibleVersionMap()` 中追加：

```java
            this.fseColumnFreshnessInfoMap.clear();
```

在 `clearVisibleVersionMapByMVPartitions(Set<String> mvPartitionNames)` 收集 `associatedBasePartitionNames` 后追加：

```java
            if (CollectionUtils.isNotEmpty(associatedBasePartitionNames)) {
                for (FSEColumnFreshnessInfo info : fseColumnFreshnessInfoMap.values()) {
                    info.removeInvalidBasePartitions(associatedBasePartitionNames);
                }
            }
```

- [ ] **Step 5: 编译检查 metadata class**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。如果失败，优先修复 import 或 `CollectionUtils` 包冲突，保持使用当前文件已有
`org.apache.commons.collections.CollectionUtils`。

- [ ] **Step 6: 提交 metadata 变更**

```bash
git add fe/fe-core/src/main/java/com/starrocks/catalog/MaterializedView.java
git commit -m "feat: add MV FSE column freshness metadata"
```

## Task 3: ALTER MV ADD COLUMN 初始化 metadata 并放宽 CHECKED strict

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/alter/AlterMVJobExecutor.java`

- [ ] **Step 1: 增加 CHECKED 判断 helper**

在 `isMvFastSchemaChangeForceMode()` 前加入：

```java
    private static boolean isCheckedRewriteMode(MaterializedView mv) {
        return mv.getTableProperty() != null
                && mv.getTableProperty().getQueryRewriteConsistencyMode()
                == TableProperty.QueryRewriteConsistencyMode.CHECKED;
    }

    private static boolean isSimpleBaseColumnReference(Expr addColumnExpr) {
        return addColumnExpr instanceof SlotRef;
    }
```

- [ ] **Step 2: 调整 affected partition 检查的拒绝条件**

把 `checkMVVisibleVersionAffectedBySchemaChange(...)` 的 strict 拒绝逻辑改成：

```java
            if (!toRefreshPartitionNames.isEmpty()
                    && !isCheckedRewriteMode(mv)
                    && !mv.isSupportFastSchemaEvolutionInDanger()
                    && !isMvFastSchemaChangeForceMode()) {
                LOG.warn("After adding column to materialized view {}, to remove partition infos {} " +
                                "to trigger full refresh, base column created time: {}, " +
                                "partition visible version time: {}, to-refresh partitions: {}",
                        mv.getName(), toRefreshPartitionNames, baseColumnCreatedTime,
                        olapTableVisiblePartitionMap.get(baseTable.getId()), toRefreshPartitionNames);
                String reason = String.format("Cannot add column " +
                                "'%s' to materialized view '%s' because partitions in the base table '%s' are affected " +
                                "by this schema change and need to be refreshed: %s.",
                        columnName, mv.getName(), baseTable.getName(), toRefreshPartitionNames);
                throw new SemanticException(MaterializedViewExceptions.unSupportedReasonForMVFSE(reason));
            }
```

在 base column `createdTime == -1` 的分支保持保守：CHECKED 不能证明分区安全时继续拒绝，除非 force/danger 路径允许。

- [ ] **Step 3: 初始化 pending FSE metadata 信息**

在 `boolean isAggregateFunction = false;` 之后保留现有 aggregate 判断；在 slot/base column 确认后增加：

```java
        boolean isSimpleBaseColumnRef = isSimpleBaseColumnReference(addColumnExpr);
```

在 `toRefreshPartitionNames` 计算完成后、`schemaChangeHandler.process(...)` 前，保留 `toRefreshPartitionNames`
作为 invalid set 的来源。

- [ ] **Step 4: schema change 成功后写入 FSE metadata**

在 `mv.initUniqueId();` 后加入：

```java
            Column mvColumn = mv.getColumn(columnName);
            if (mvColumn != null) {
                MaterializedView.FSEColumnFreshnessInfo freshnessInfo =
                        new MaterializedView.FSEColumnFreshnessInfo(
                                columnName,
                                mvColumn.getUniqueId(),
                                baseTable.getId(),
                                baseColumnName,
                                baseColumn.getUniqueId(),
                                baseColumnCreatedTime,
                                System.currentTimeMillis(),
                                isSimpleBaseColumnRef,
                                toRefreshPartitionNames);
                mvAsyncRefreshContext.putFSEColumnFreshnessInfo(freshnessInfo);
            }
```

保留已有 edit log：`AlterMaterializedViewBaseTableInfosLog(... AlterType.ADD_COLUMN)` 会持久化当前 MV 对象。

- [ ] **Step 5: 确认 force clear 不再是 CHECKED 正确性依赖**

保留这段已有逻辑不动：

```java
            if (isMvFastSchemaChangeClearPartition() && !toRefreshPartitionNames.isEmpty()) {
                ...
            }
```

本期 CHECKED strict 通过 metadata guard 保证正确性，不依赖 force clear。

- [ ] **Step 6: 编译**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。

- [ ] **Step 7: 提交 ALTER 初始化变更**

```bash
git add fe/fe-core/src/main/java/com/starrocks/alter/AlterMVJobExecutor.java
git commit -m "feat: track MV FSE column invalid partitions"
```

## Task 4: Refresh 成功后将 invalid partition 转 safe

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVVersionManager.java`

- [ ] **Step 1: 在 OLAP refresh metadata 更新后 pruning FSE invalid set**

在 `updateMetaForOlapTable(...)` 中 `currentTablePartitionInfo.putAll(partitionInfoMap);` 后加入：

```java
            refreshContext.removeInvalidBasePartitionsForFSEColumns(tableId, partitionInfoMap.keySet());
```

- [ ] **Step 2: base partition drop 后清理陈旧 invalid entry**

在 `visiblePartitionNames` 计算后、iterator 清理后追加：

```java
                refreshContext.retainInvalidBasePartitionsForFSEColumns(tableId, visiblePartitionNames);
```

- [ ] **Step 3: 编译**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。

- [ ] **Step 4: 提交 refresh pruning**

```bash
git add fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVVersionManager.java
git commit -m "feat: mark refreshed MV FSE partitions safe"
```

## Task 5: 让 `MvUpdateInfo` 支持 FSE guard 注入 partitions

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/catalog/MvUpdateInfo.java`

- [ ] **Step 1: 增加 copy helper，避免 `NO_REFRESH` 阻断 compensation**

在 `partialRefresh(...)` 后加入：

```java
    public static MvUpdateInfo partialRefreshFrom(MvUpdateInfo other) {
        MvUpdateInfo copied = new MvUpdateInfo(other.mv, MvToRefreshType.PARTIAL,
                other.queryRewriteConsistencyMode);
        copied.mvToRefreshPCells.addAll(other.mvToRefreshPCells);
        copied.baseTableUpdateInfos.putAll(other.baseTableUpdateInfos);
        copied.basePartNameToMVPCells.putAll(other.basePartNameToMVPCells);
        copied.mvPartNameToBasePCells.putAll(other.mvPartNameToBasePCells);
        copied.refBaseNestedMVPCells.addAll(other.refBaseNestedMVPCells);
        return copied;
    }
```

- [ ] **Step 2: 增加显式注入 helper**

在 `getMVPartNameToBasePCells()` 后加入：

```java
    public void addMVToRefreshBaseTablePCells(PCellWithName mvPCell, Table baseTable, PCellWithName basePCell) {
        addMVToRefreshPartitionNames(mvPCell);
        mvPartNameToBasePCells.computeIfAbsent(mvPCell.name(), k -> Maps.newHashMap())
                .computeIfAbsent(baseTable, k -> PCellSortedSet.of())
                .add(basePCell);
    }
```

- [ ] **Step 3: 编译**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。

- [ ] **Step 4: 提交 `MvUpdateInfo` helper**

```bash
git add fe/fe-core/src/main/java/com/starrocks/catalog/MvUpdateInfo.java
git commit -m "feat: allow MV rewrite guard to inject refresh partitions"
```

## Task 6: 在 MV rewrite prepare 阶段应用 CHECKED FSE guard

**Files:**
- Modify: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/MvRewritePreprocessor.java`

- [ ] **Step 1: 增加 import**

补充：

```java
import com.starrocks.sql.common.PCellSetMapping;
import com.starrocks.sql.common.PCellWithName;
```

- [ ] **Step 2: 在普通 timeliness 后应用 guard**

在 `prepareRelatedMVs(...)` 中，替换：

```java
                mvInfos.add(Pair.create(wrapper, mvUpdateInfo));
```

为：

```java
                MvUpdateInfo guardedMvUpdateInfo = applyFSEColumnGuard(queryTables, queryOptExpression, mv, mvUpdateInfo);
                if (guardedMvUpdateInfo == null || !guardedMvUpdateInfo.isValidRewrite()) {
                    OptimizerTraceUtil.logMVRewriteFailReason(mv.getName(), "FSE guarded stale partitions {}",
                            guardedMvUpdateInfo);
                    continue;
                }
                mvInfos.add(Pair.create(wrapper, guardedMvUpdateInfo));
```

同时把 `prepareRelatedMVs` 签名改为：

```java
    public void prepareRelatedMVs(Set<Table> queryTables,
                                  List<MaterializedViewWrapper> mvWithPlanContexts,
                                  OptExpression queryOptExpression) {
```

并把调用点改成：

```java
                    prepareRelatedMVs(queryTables, mvWithPlanContexts, queryOptExpression);
```

- [ ] **Step 3: 增加 CHECKED guard 主方法**

在 `prepareMV(...)` 前加入：

```java
    private static MvUpdateInfo applyFSEColumnGuard(Set<Table> queryTables,
                                                    OptExpression queryOptExpression,
                                                    MaterializedView mv,
                                                    MvUpdateInfo mvUpdateInfo) {
        if (mv.getTableProperty() == null ||
                mv.getTableProperty().getQueryRewriteConsistencyMode() !=
                        com.starrocks.catalog.TableProperty.QueryRewriteConsistencyMode.CHECKED) {
            return mvUpdateInfo;
        }
        Map<String, MaterializedView.FSEColumnFreshnessInfo> freshnessInfoMap = mv.getRefreshScheme()
                .getAsyncRefreshContext()
                .getFSEColumnFreshnessInfoMap();
        if (freshnessInfoMap.isEmpty()) {
            return mvUpdateInfo;
        }
        Map<Long, Set<String>> usedBaseColumns = collectUsedBaseColumnNames(queryOptExpression);
        MvUpdateInfo guardedInfo = mvUpdateInfo;
        for (MaterializedView.FSEColumnFreshnessInfo freshnessInfo : freshnessInfoMap.values()) {
            Set<String> usedColumns = usedBaseColumns.get(freshnessInfo.getBaseTableId());
            if (usedColumns == null || !usedColumns.contains(freshnessInfo.getBaseColumnName())) {
                continue;
            }
            if (freshnessInfo.getInvalidBasePartitionNames().isEmpty()) {
                continue;
            }
            Table baseTable = queryTables.stream()
                    .filter(table -> table.getId() == freshnessInfo.getBaseTableId())
                    .findFirst()
                    .orElse(null);
            if (baseTable == null) {
                logMVPrepare("Skip MV {} for FSE column {} because base table {} is not in query",
                        mv.getName(), freshnessInfo.getMvColumnName(), freshnessInfo.getBaseTableId());
                return null;
            }
            if (guardedInfo.getMVToRefreshType() == MvUpdateInfo.MvToRefreshType.NO_REFRESH) {
                guardedInfo = MvUpdateInfo.partialRefreshFrom(guardedInfo);
            }
            if (!injectFSEInvalidPartitions(mv, guardedInfo, baseTable, freshnessInfo)) {
                return null;
            }
        }
        return guardedInfo;
    }
```

- [ ] **Step 4: 增加查询依赖列收集方法**

在 guard 主方法后加入：

```java
    private static Map<Long, Set<String>> collectUsedBaseColumnNames(OptExpression queryOptExpression) {
        ColumnRefSet usedColumnRefs = collectUsedColumnRefs(queryOptExpression);
        Map<Long, Set<String>> result = Maps.newHashMap();
        for (com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator scanOperator :
                com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils.getScanOperator(queryOptExpression)) {
            Table table = scanOperator.getTable();
            if (table == null) {
                continue;
            }
            for (Map.Entry<Column, ColumnRefOperator> entry : scanOperator.getColumnMetaToColRefMap().entrySet()) {
                if (usedColumnRefs.contains(entry.getValue())) {
                    result.computeIfAbsent(table.getId(), ignored -> Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER))
                            .add(entry.getKey().getName());
                }
            }
        }
        return result;
    }

    private static ColumnRefSet collectUsedColumnRefs(OptExpression expression) {
        ColumnRefSet usedColumnRefs = new ColumnRefSet();
        if (expression.getRowOutputInfo() != null) {
            usedColumnRefs.union(expression.getRowOutputInfo().getUsedColumnRefSet());
            usedColumnRefs.union(expression.getRowOutputInfo().getOutputColumnRefSet());
        }
        if (expression.getOp().getPredicate() != null) {
            usedColumnRefs.union(expression.getOp().getPredicate().getUsedColumns());
        }
        if (expression.getOp().getProjection() != null) {
            usedColumnRefs.union(expression.getOp().getProjection().getUsedColumns());
        }
        for (OptExpression input : expression.getInputs()) {
            usedColumnRefs.union(collectUsedColumnRefs(input));
        }
        return usedColumnRefs;
    }
```

- [ ] **Step 5: 增加 invalid partitions 注入方法**

在列收集方法后加入：

```java
    private static boolean injectFSEInvalidPartitions(MaterializedView mv,
                                                      MvUpdateInfo mvUpdateInfo,
                                                      Table baseTable,
                                                      MaterializedView.FSEColumnFreshnessInfo freshnessInfo) {
        MvBaseTableUpdateInfo baseTableUpdateInfo = mvUpdateInfo.getBaseTableUpdateInfos().get(baseTable);
        if (baseTableUpdateInfo == null) {
            logMVPrepare("Skip MV {} for FSE column {} because base table partition cells are unavailable",
                    mv.getName(), freshnessInfo.getMvColumnName());
            return false;
        }
        PCellSetMapping baseToMvMapping = mvUpdateInfo.getBasePartNameToMVPCells().get(baseTable);
        if (baseToMvMapping == null || baseToMvMapping.isEmpty()) {
            logMVPrepare("Skip MV {} for FSE column {} because base-to-mv partition mapping is unavailable",
                    mv.getName(), freshnessInfo.getMvColumnName());
            return false;
        }
        PCellSortedSet basePartitionCells = baseTableUpdateInfo.getRefBaseTablePCells();
        for (String basePartitionName : freshnessInfo.getInvalidBasePartitionNames()) {
            com.starrocks.sql.common.PCell basePCell = basePartitionCells.getPCell(basePartitionName);
            PCellSortedSet mvPCells = baseToMvMapping.get(basePartitionName);
            if (basePCell == null || mvPCells == null || mvPCells.isEmpty()) {
                logMVPrepare("Skip MV {} for FSE column {} because partition mapping is missing for base partition {}",
                        mv.getName(), freshnessInfo.getMvColumnName(), basePartitionName);
                return false;
            }
            PCellWithName basePCellWithName = PCellWithName.of(basePartitionName, basePCell);
            for (PCellWithName mvPCell : mvPCells.getPartitions()) {
                mvUpdateInfo.addMVToRefreshBaseTablePCells(mvPCell, baseTable, basePCellWithName);
            }
        }
        return true;
    }
```

补充 import：

```java
import com.starrocks.catalog.MvBaseTableUpdateInfo;
```

- [ ] **Step 6: 编译**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。

- [ ] **Step 7: 提交 rewrite guard**

```bash
git add fe/fe-core/src/main/java/com/starrocks/sql/optimizer/MvRewritePreprocessor.java
git commit -m "feat: guard checked MV rewrite for FSE columns"
```

## Task 7: 运行并录制 SQL case

**Files:**
- Modify: `test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result`
- Modify if record output exposes unstable text: `test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql`

- [ ] **Step 1: record 单 case**

运行：

```bash
cd test
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
.venv/bin/python run.py \
  -d sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result \
  --skip_reruns -r -c 1 -t 300
```

Expected: PASS，R 文件被更新为真实 helper 输出。

- [ ] **Step 2: validate 单 case**

运行：

```bash
cd test
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
.venv/bin/python run.py \
  -d sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result \
  --skip_reruns -v -c 1 -t 300
```

Expected: PASS。

- [ ] **Step 3: 如 helper 输出不稳定，收窄断言**

如果 record 后 R 文件包含不稳定 task id、耗时、随机 uuid，修改 T 文件只保留稳定断言：

```sql
function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", "TABLE: fse_detail")
```

不要断言整段 `EXPLAIN` 文本。

- [ ] **Step 4: 提交 SQL 结果更新**

```bash
git add test/sql/test_mv/T/test_mv_fse_checked_rewrite_guard.sql \
        test/sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result
git commit -m "test: verify MV FSE checked rewrite guard"
```

## Task 8: 最终验证

**Files:**
- No source edits expected.

- [ ] **Step 1: FE compile**

运行：

```bash
cd fe
./gradlew :fe-core:compileJava
```

Expected: PASS。

- [ ] **Step 2: FE checkstyle main**

运行：

```bash
cd fe
./gradlew :fe-core:checkstyleMain
```

Expected: PASS。

- [ ] **Step 3: SQL case validate**

运行：

```bash
cd test
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
.venv/bin/python run.py \
  -d sql/test_mv/R/test_mv_fse_checked_rewrite_guard.result \
  --skip_reruns -v -c 1 -t 300
```

Expected: PASS。

- [ ] **Step 4: 检查最终 diff**

运行：

```bash
git status --short
git log --oneline --max-count=8
```

Expected: 只有计划内文件变更，且最近提交按 SQL test、metadata、ALTER、refresh、rewrite guard、SQL result 的顺序排列。

## 自检结果

- Spec 覆盖：计划覆盖 CHECKED-only、strict 放宽、simple column null-safe、表达式/aggregate measure 保守、refresh 转 safe、
  query 不引用 FSE column 不受影响、SQL-first 验收。
- 分区混合改写：Task 6 注入 `MvUpdateInfo`，复用 `createScanMvOperator` 排除 MV unsafe partitions，复用现有
  `MVCompensationBuilder` / `OlapTableCompensation` 构造 base compensation。
- 非 CHECKED：Task 6 直接跳过 guard，不改变 `NOCHECK` / `LOOSE` / `FORCE_MV` 语义。
- 无占位符：本计划中的待确定点都已落成具体代码入口、文件路径和验证命令。
