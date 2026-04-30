# MV Fast Schema Evolution 的 CHECKED 改写保护

## 目标

为通过 fast schema evolution 变更的物化视图增加查询改写正确性保护，也就是
`ALTER MATERIALIZED VIEW ... ADD COLUMN ...` 之后的 MV rewrite guard。本期只保证
`query_rewrite_consistency = CHECKED` 的查询改写正确性。

对于 CHECKED 改写：

- 如果查询不依赖通过 MV fast schema evolution 新增的列，新 guard 不能降低原有改写机会。
- 如果查询依赖 FSE 新增的 MV 列，则从 MV 读取的每个分区都必须能证明对该列是安全的。
- 对不安全分区，应尽量复用现有 transparent compensation 路径，从 base table 读取。
- 如果无法构造正确的 compensation plan，则放弃该 MV 候选改写。

非 CHECKED 模式，也就是 `NOCHECK`、`LOOSE`、`FORCE_MV`，不属于本期正确性合同。它们本身在设计上就允许
返回过期数据，因此不作为本 guard 的验收标准。

## 范围

本期支持范围：

- Native OLAP base table。
- 单 base table 的 MV。
- 分区 MV，并且现有 MV partition 到 base partition 的映射可用。
- CHECKED 查询改写。
- 以 SQL case 覆盖用户可感知的结果正确性，以及关键的 rewrite / no-rewrite 行为。

本期不支持范围：

- 外部 catalog 表。
- 多 base table MV。
- 非分区 MV。
- `NOCHECK`、`LOOSE`、`FORCE_MV` 的正确性保证。
- 任意表达式列或 aggregate measure 的 null-safety 证明。

## 当前问题

`AlterMVJobExecutor` 现在已经会比较 base column 的 `createdTime` 和每个 base partition 的
`visibleVersionTime`。如果 `visibleVersionTime > createdTime`，说明该分区可能已经包含新增列的真实值。
在 force 模式下，受影响分区可以从 MV 的 base version map 中删除。

这不足以保证 CHECKED 改写正确性：

- strict 模式当前会直接拒绝，而不是保留足够元数据来支持安全改写和 compensation。
- force 相关模式是粗粒度的，不是 column-aware。
- 普通 freshness metadata 不知道查询是否实际引用了 FSE 新增列。
- FSE 之后，一个分区对旧列可能是安全的，但只对新增列不安全。

## 元数据模型

在物化视图上持久化按 FSE column 维护的 metadata，优先放在 async refresh context 下，使其跟随现有 refresh
metadata 生命周期。

必需字段：

- MV column name 和 unique id。
- Base table id。
- Base column name 和 unique id。
- Base column created time，也就是 `Tb`。
- MV column added time，也就是 `Tm`，用于审计、debug 和后续诊断。
- ADD COLUMN 表达式是否是可证明 null-safe 的 simple base column reference。
- 该 FSE column 对应的 invalid base partition names。

规划阶段只需要持久化 invalid set。某个分区不在 invalid set 中，则认为它安全，原因只能是：

- 创建 FSE metadata 时已经被判定为 null-safe；或
- FSE metadata 创建后，该分区已经完成 refresh。

实现可以暴露 null-safe / invalid 分区计数或 trace 文本用于 debug，但不需要持久化完整 safe-partition set。

## ADD COLUMN 行为

当 `ALTER MATERIALIZED VIEW ... ADD COLUMN ...` 成功时：

1. 分析 MV add-column 表达式，识别它引用的单个 base column。
2. 对不在本期范围内的场景拒绝或沿用现有行为，包括 external table、多 base MV、非分区 MV、缺少
   partition/version metadata。
3. 为新增 MV column 创建 FSE metadata。
4. 遍历每个 base partition：
   - 如果 ADD COLUMN 表达式是 simple base column reference，并且
     `basePartition.visibleVersionTime <= baseColumn.createdTime`，则该分区 safe；
   - 否则，如果 `basePartition.visibleVersionTime > baseColumn.createdTime`，则将该 base partition 加入
     invalid set；
   - 如果表达式不是 simple base column reference，则旧有分区在 refresh 前一律保守视为 invalid。本期实现不尝试证明
     表达式或 aggregate 的 null/default 等价性。
5. 在 `mv_fast_schema_change_mode = strict` 下，只要属于本期支持范围并且 FSE metadata 初始化成功，就允许
   ADD COLUMN。这是主要行为变化：strict 可以变安全，因为后续 rewrite 会变成 column-aware 和 partition-aware。
6. 保留已有 force 行为，但 CHECKED 正确性模型不能依赖 force clear partition。

如果 MV 当前配置的是非 CHECKED rewrite consistency，也可以记录 metadata，这样后续切回 CHECKED 时仍有足够信息。
但 guard 本身只在 CHECKED 模式下生效。

## Refresh 行为

当 MV refresh 成功更新 base partition version metadata 时：

1. 识别本次 refresh 覆盖的 base partitions。
2. 对该 MV 上的每个 FSE column metadata entry，从 invalid set 中移除这些已经 refresh 的 base partitions。
3. 即使 invalid set 变空，也保留 FSE metadata entry。此时它对 rewrite 是 inert 的，但对审计和 debug 有价值。

不要通过比较已存储的 base `visibleVersionTime` 和 `Tm` 来判断“是否在 FSE 后刷新过”。可靠的证明事件是 refresh
metadata update path 本身，它说明 MV 分区已经包含新增列的值。

base partition drop 或 rename 时，应在普通 MV partition metadata 更新路径中移除陈旧 invalid entry。如果映射关系不明确，
则保持该分区 invalid，或者跳过 rewrite。

## CHECKED 改写行为

guard 应该在普通 CHECKED freshness 计算之后、MV scan operator 构造之前执行。

算法：

1. 使用现有 CHECKED timeliness arbiter 构造普通 `MvUpdateInfo`。
2. 判断查询是否实际依赖带有 FSE metadata 的 MV columns。
3. 如果查询不依赖 FSE 新增列，则原样返回普通 `MvUpdateInfo`。
4. 对每个被引用的 FSE 新增列，将它的 invalid base partitions 和查询命中的 base partitions 取交集。
5. 使用现有 MV partition / base partition 映射，将这些 invalid base partitions 转换为 MV partitions。
6. 将对应 MV partitions 注入 `MvUpdateInfo.mvToRefreshPCells`，并在 `mvPartNameToBasePCells` 中保留 base
   partition cells。
7. 让现有 scan creation 从 MV scan 侧排除这些 MV partitions。
8. 让现有 transparent compensation builder 读取被注入的 base partition cells，并构造 base-table
   compensation branch。
9. 如果无法构造 compensation，则拒绝该 MV 候选。

这样可以得到目标中的混合 plan：safe partitions 继续扫描 MV，unsafe partitions 从 base table 读取。

guard 必须是 query-column-aware 的。只使用旧列的查询，不应因为某个新增 FSE column 的 invalid partitions 而损失
普通 CHECKED 改写机会。

## 支持与不支持的 case

本期支持：

- 非聚合 MV 新增 simple base column，查询不引用它：走普通 CHECKED 改写。
- 非聚合 MV 新增 simple base column，查询引用它，旧分区是 null-safe：该分区允许 MV 改写。
- 非聚合 MV 新增 simple base column，查询引用它，分区可能已有 base 真实值但 MV 未 refresh：使用 base
  compensation，或者跳过 rewrite。
- 非聚合 MV 新增 simple base column，分区在 ADD COLUMN 后已经 refresh：允许 MV 改写。
- 聚合 MV 新增 simple base column 作为 GROUP BY 维度：旧 null-safe 分区允许改写，因为按 `c3 = NULL/default`
  分组不会改变旧聚合粒度。
- 聚合 MV 对新增 GROUP BY 维度存在 invalid partitions：使用 base compensation，或者跳过 rewrite。

refresh 前保守不支持：

- 新增表达式列，例如 `c3 + 1`。
- 新增 aggregate measure，例如 `sum(c3)` 或 `count(c3)`。
- 任何无法证明 base old rows 与 MV FSE-filled rows 之间 null/default 等价的场景。

如果查询不引用这些保守不支持的 FSE columns，仍应按普通 CHECKED 改写处理。

## 错误处理

guard 应尽量避免在查询改写阶段向用户暴露错误。优先行为是：

- 如果 scope 或映射关系不支持，跳过该 MV 候选；
- 如果存在 invalid partitions 且无法构造正确 transparent compensation，跳过该 MV 候选；
- 对无法安全追踪的 FSE 操作，在 ALTER 阶段保留错误。

debug 或 trace 输出应能标明导致 compensation 或 rewrite rejection 的 MV 名称、FSE column 和 invalid partitions。

## SQL-first 测试策略

本期优先使用端到端 SQL tests，而不是追求大量 FE unit test 分支覆盖。SQL tests 应固定正向和反向的用户可感知行为。

建议 SQL cases：

1. 非聚合 MV、simple column、`p1` / `p2` / `p3` 时间序列。
   - `p1`：base partition version time 早于 base column `c3` creation time，因此 `c3` 是 null/default safe，
     可以使用 MV。
   - `p2`：base partition 已有真实 `c3` 值，但 MV 尚未 refresh。引用 `c3` 的查询不能读取 stale MV 值；
     如果支持则走 compensation，否则跳过 rewrite。
   - `p3`：在 MV ADD COLUMN 后已经 refresh，因此可以使用 MV。
   - 跨 `p1`、`p2`、`p3` 的查询应返回正确结果；如果 plan 可断言，应能看到 safe partitions 使用 MV，
     `p2` 使用 base compensation。
2. 查询不引用 FSE 新增列。
   - invalid FSE metadata 不能阻止普通 CHECKED 改写。
3. 查询只命中 invalid partition 且引用 FSE 新增列。
   - 不能直接 rewrite 到 stale MV partition。
4. 聚合 MV 新增 simple GROUP BY 维度。
   - 旧 null-safe 分区可以 rewrite。
   - 已有真实 base 值且 MV 未 refresh 的分区不能直接 rewrite。
   - refresh 后允许 rewrite。
5. 聚合 MV 新增 aggregate measure。
   - 查询不引用新增 measure 时，可以按普通 CHECKED 改写。
   - 查询引用新增 measure 时，refresh 前保守 invalid。
   - refresh 后允许 rewrite。
6. MV refresh transition。
   - refresh 前 invalid 的分区在 refresh 后变为 safe，通过 query result 和 plan shape 验证。

如果 metadata 初始化或 refresh-time invalid-set pruning 很难通过 SQL 稳定断言，可以补充 FE unit tests。但它们不是本期的
主要验收门槛。

## 实现锚点

可能涉及的代码位置：

- `AlterMVJobExecutor`：在 `ADD COLUMN` 时初始化 FSE metadata，并在本期支持范围内放宽 strict。
- `MaterializedView` 或其 async refresh context：持久化 FSE column freshness metadata。
- `MVVersionManager`：refresh metadata 更新时，从 invalid sets 中移除已经 refresh 的 partitions。
- `MvRefreshArbiter` / `MVTimelinessArbiter`：保持普通 CHECKED freshness 逻辑不变。
- `MvRewritePreprocessor`：在构造 MV scan operator 前应用 query-aware FSE guard。
- `MvUpdateInfo`：把注入的 unsafe MV partitions 和 base partition cells 带给 transparent compensation。
- `sql/optimizer/rule/transformation/materialization/compensation` 下的 compensation 代码：复用现有 base-table
  compensation；无法构造正确 compensation 时拒绝 rewrite。

## 实现计划阶段仍需确定的问题

- 持久化 FSE metadata 的 JSON/Gson class 具体结构。
- 在 materialized view rewrite 内提取 query-column dependency 的确切位置。
- 本期是否加入 trace-only observability，还是只保留日志和测试断言。
- SQL suite 的具体位置，以及 plan 断言使用 `EXPLAIN`、trace output 还是已有 MV rewrite helpers。
