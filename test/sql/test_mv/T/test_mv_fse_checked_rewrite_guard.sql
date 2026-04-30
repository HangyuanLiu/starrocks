-- name: test_mv_fse_checked_rewrite_guard @sequential
-- Test Point:
--   1. CHECKED strict MV FSE ADD COLUMN can succeed with tracked invalid partitions.
--   2. Query not referencing the FSE column still rewrites normally.
--   3. Query referencing the FSE column reads safe partitions from MV and invalid partitions from base compensation.
--   4. Invalid-only queries fall back to base, and refreshed invalid partitions become MV-rewritable again.
--   5. Expression and aggregate-measure FSE columns are currently unsupported by rewrite.
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
[UC]refresh materialized view mv_fse_detail partition start ('2026-01-02') end ('2026-01-03') force with sync mode;

alter materialized view mv_fse_detail add column c3 as c3;
function: wait_alter_table_finish()

insert into fse_detail values ('2026-01-03', 3, 30, 300);
[UC]refresh materialized view mv_fse_detail partition start ('2026-01-03') end ('2026-01-04') with sync mode;

function: print_hit_materialized_view("select dt, k, v from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04'", "mv_fse_detail")

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt = '2026-01-01' order by dt, k", "TABLE: mv_fse_detail")
function: assert_query_contains_times("explain select dt, k, c3 from fse_detail where dt = '2026-01-01' order by dt, k", "TABLE: fse_detail", 0)

[ORDER]select dt, k, c3 from fse_detail
where dt = '2026-01-01'
order by dt, k;
-- result:
2026-01-01	1	None
-- !result

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt = '2026-01-03' order by dt, k", "TABLE: mv_fse_detail")
function: assert_query_contains_times("explain select dt, k, c3 from fse_detail where dt = '2026-01-03' order by dt, k", "TABLE: fse_detail", 0)

[ORDER]select dt, k, c3 from fse_detail
where dt = '2026-01-03'
order by dt, k;
-- result:
2026-01-03	3	300
-- !result

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", "TABLE: fse_detail")

[ORDER]select dt, k, c3 from fse_detail
where dt >= '2026-01-01' and dt < '2026-01-04'
order by dt, k;
-- result:
2026-01-01	1	None
2026-01-02	2	None
2026-01-02	22	200
2026-01-03	3	300
-- !result

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt = '2026-01-02' order by dt, k", "TABLE: fse_detail")
function: assert_query_contains_times("explain select dt, k, c3 from fse_detail where dt = '2026-01-02' order by dt, k", "TABLE: mv_fse_detail", 0)

[ORDER]select dt, k, c3 from fse_detail
where dt = '2026-01-02'
order by dt, k;
-- result:
2026-01-02	2	None
2026-01-02	22	200
-- !result

[UC]refresh materialized view mv_fse_detail partition start ('2026-01-02') end ('2026-01-03') force with sync mode;

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail")
function: assert_query_contains_times("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: fse_detail", 0)

[ORDER]select dt, k, c3 from fse_detail
where dt >= '2026-01-01' and dt < '2026-01-04'
order by dt, k;
-- result:
2026-01-01	1	None
2026-01-02	2	None
2026-01-02	22	200
2026-01-03	3	300
-- !result

alter table fse_detail add column c4 int null;
function: wait_alter_table_finish()

insert into fse_detail values ('2026-01-02', 44, 440, 400, 40);
[UC]refresh materialized view mv_fse_detail partition start ('2026-01-02') end ('2026-01-03') with sync mode;

alter materialized view mv_fse_detail add column c4_plus as c4 + 1;
function: wait_alter_table_finish()

function: assert_query_contains("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: fse_detail")
function: assert_query_contains_times("explain select dt, k, c3 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", 0)

function: assert_query_contains("explain select dt, k, c4 + 1 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: fse_detail")
function: assert_query_contains_times("explain select dt, k, c4 + 1 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", 0)

[ORDER]select dt, k, c4 + 1 from fse_detail
where dt >= '2026-01-01' and dt < '2026-01-04'
order by dt, k;
-- result:
2026-01-01	1	None
2026-01-02	2	None
2026-01-02	22	None
2026-01-02	44	41
2026-01-03	3	None
-- !result

[UC]refresh materialized view mv_fse_detail with sync mode;

function: assert_query_contains("explain select dt, k, c4 + 1 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: fse_detail")
function: assert_query_contains_times("explain select dt, k, c4 + 1 from fse_detail where dt >= '2026-01-01' and dt < '2026-01-04' order by dt, k", "TABLE: mv_fse_detail", 0)

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
[UC]refresh materialized view mv_fse_agg partition start ('2026-02-02') end ('2026-02-03') force with sync mode;

alter materialized view mv_fse_agg add column c3 as c3;
function: wait_alter_table_finish()

insert into fse_agg values ('2026-02-03', 3, 30, 300);
[UC]refresh materialized view mv_fse_agg partition start ('2026-02-03') end ('2026-02-04') with sync mode;

function: print_hit_materialized_view("select dt, k, sum(v) as sum_v from fse_agg group by dt, k", "mv_fse_agg")

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-01' group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-01' group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg", 0)

[ORDER]select dt, k, c3, sum(v) from fse_agg
where dt = '2026-02-01'
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-01	1	None	10
-- !result

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-03' group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-03' group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg", 0)

[ORDER]select dt, k, c3, sum(v) from fse_agg
where dt = '2026-02-03'
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-03	3	300	30
-- !result

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", "TABLE: fse_agg")

[ORDER]select dt, k, c3, sum(v) from fse_agg
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-01	1	None	10
2026-02-02	2	None	20
2026-02-02	22	200	220
2026-02-03	3	300	30
-- !result

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-02' group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(v) from fse_agg where dt = '2026-02-02' group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", 0)

[ORDER]select dt, k, c3, sum(v) from fse_agg
where dt = '2026-02-02'
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-02	2	None	20
2026-02-02	22	200	220
-- !result

[UC]refresh materialized view mv_fse_agg partition start ('2026-02-02') end ('2026-02-03') force with sync mode;

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg", 0)

alter table fse_agg add column c4 int null;
function: wait_alter_table_finish()

insert into fse_agg values ('2026-02-02', 23, 230, 201, 400);
[UC]refresh materialized view mv_fse_agg partition start ('2026-02-02') end ('2026-02-03') with sync mode;

alter materialized view mv_fse_agg add column sum_c4 as sum(c4);
function: wait_alter_table_finish()

function: assert_query_contains("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(v) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", 0)

function: assert_query_contains("explain select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", 0)

[ORDER]select dt, k, c3, sum(c4) from fse_agg
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-01	1	None	None
2026-02-02	2	None	None
2026-02-02	22	200	None
2026-02-02	23	201	400
2026-02-03	3	300	None
-- !result

[UC]refresh materialized view mv_fse_agg with sync mode;

function: assert_query_contains("explain select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: fse_agg")
function: assert_query_contains_times("explain select dt, k, c3, sum(c4) from fse_agg group by dt, k, c3 order by dt, k, c3", "TABLE: mv_fse_agg", 0)

[ORDER]select dt, k, c3, sum(c4) from fse_agg
group by dt, k, c3
order by dt, k, c3;
-- result:
2026-02-01	1	None	None
2026-02-02	2	None	None
2026-02-02	22	200	None
2026-02-02	23	201	400
2026-02-03	3	300	None
-- !result

drop materialized view mv_fse_agg;
drop table fse_agg;
drop materialized view mv_fse_detail;
drop table fse_detail;
drop database db_${uuid0} force;
