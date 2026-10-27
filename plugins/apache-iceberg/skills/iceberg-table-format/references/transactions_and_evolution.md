# Transactions, Evolution, Time Travel, Branches and Tags

## Problem
Without a table layer, a "table" is whatever files sit under a prefix. Crashed writers leave
half a batch visible, retries duplicate rows, concurrent read-modify-write jobs lose updates,
failed overwrites delete the old data first, and schema drift surfaces only when a reader fails.
Iceberg makes every change a snapshot commit and every structural change a metadata operation.

## ACID through snapshots and a pointer swap
1. The writer reads the current metadata (its **base**).
2. It writes data files and manifests. Nothing is visible yet.
3. It asks the catalog to swap `metadata-location` to a new metadata.json **only if the base is
   still current** (REST: `assert-ref-snapshot-id` requirement on `main`).
4. Success: the new snapshot is visible atomically. Failure: refresh, re-apply, retry.

| Failure | Plain Parquet folder | Iceberg table |
|---|---|---|
| Writer dies after 3 of 5 files | Readers see 6,000 of 10,000 rows | No commit, previous snapshot |
| Scheduler retries the crashed job | 16,000 rows, 10,000 distinct ids | One commit of 10,000 rows |
| Spark task fails mid-append | Partial files visible | Spark aborts and deletes the written files; row, snapshot, `files` and S3 object counts unchanged |
| `mode("overwrite")` hits a bad record | Old data deleted first; path holds only `_temporary`, read fails `UNABLE_TO_INFER_SCHEMA` | Previous snapshot stays current, all rows readable |
| Two read-modify-write jobs | Last writer wins, the other change silently lost | Stale commit rejected |
| Uncommitted files under `data/` | Part of the "table" | Invisible: not in `data_files`, no new snapshot |

### Optimistic concurrency in practice
```python
from pyiceberg.exceptions import CommitFailedException

a, b = catalog.load_table("db.orders"), catalog.load_table("db.orders")   # same base
a.append(batch_1)
try:
    b.append(batch_2)                 # stale base
except CommitFailedException:         # "Requirement failed: branch main has changed"
    b.refresh()
    b.append(batch_2)                 # retry on the new state
```

- PyIceberg raises `CommitFailedException` on a stale base and does **not** retry; the caller must
  `refresh()` and retry. On a table with no snapshot yet the message is
  `branch main was created concurrently`.
- Spark (Java client) retries automatically (`commit.retry.num-retries`, default 4): 4 parallel
  appends of 2,000 rows give 8,000 rows, 4 new snapshots and a linear parent chain.
- A failed commit leaves its data files behind as orphans (1 orphan after one rejected append);
  `remove_orphan_files` cleans them. The catalog pointer is unchanged.
- Readers are isolated: a PyIceberg reader that loaded the table before an append and a `DELETE`
  still scans its 1,000 rows; a reload shows the new state.
- Spark's catalog cache (`cache-enabled`, default true) can serve a stale table version in a
  long-lived session; set `spark.sql.catalog.<name>.cache-enabled=false` when other engines
  commit to the same tables.

| Situation | Approach |
|---|---|
| Many appenders on one table | Default optimistic retries |
| Hot table with frequent conflicts | Fewer, larger commits; partition-scoped writers |
| Python writers | Wrap `append` in a refresh-and-retry loop |
| Long rewrite while appends continue | Default `use-starting-sequence-number` for `rewrite_data_files` |

## Schema evolution
Columns are tracked by **field id**, not name or position, so add, rename, reorder and drop are
metadata-only.

```sql
ALTER TABLE t ADD COLUMN channel STRING;               -- old rows read NULL
ALTER TABLE t RENAME COLUMN amount TO amount_eur;      -- old files map by id
ALTER TABLE t ALTER COLUMN status FIRST;               -- reorder
ALTER TABLE t DROP COLUMN customer_id;
ALTER TABLE t ALTER COLUMN qty TYPE BIGINT;            -- safe widening only
```

- After all four DDLs the data file set is identical; `sum(amount_eur)` reads the old files.
- A plain Parquet folder read with a renamed column returns NULL for every row (matched by name).
- Evolution is explicit: an append with wrong types or an unknown column fails with
  `AnalysisException`; `ALTER COLUMN amount TYPE string` (double -> string) is rejected. After
  `ADD COLUMN currency`, 1,000 old rows read `currency` as NULL and new rows carry it.
- In a plain folder with drifted files, Spark takes the schema from one footer (which one varies
  between runs), `sum(amount)` fails with `Parquet column cannot be converted`, and
  `mergeSchema` fails with `CANNOT_MERGE_SCHEMAS`, although the writer succeeded.

## Partition evolution
```sql
ALTER TABLE t REPLACE PARTITION FIELD order_ts_month WITH days(order_ts);
ALTER TABLE t ADD PARTITION FIELD bucket(16, customer_id);
ALTER TABLE t DROP PARTITION FIELD country;
```
Old files keep spec 0, new files use spec 1; the planner applies each spec's transform to the
filter. A table with January monthly (1 file) and February daily (28 files): a Feb-10 query reads
1 file, a Jan-10 query reads the 1 monthly file, total 58,000 rows; January files are untouched.
Compact old partitions later if the coarse layout hurts.

## Time travel and rollback
```sql
SELECT count(*) FROM t VERSION AS OF 5843025671893411111;
SELECT count(*) FROM t TIMESTAMP AS OF '2025-01-01 10:00:00';
CALL lake.system.rollback_to_snapshot('db.t', 5843025671893411111);
CALL lake.system.set_current_snapshot('db.t', 5843025671893411111);
```

- Duplicate load (10,000 rows): both time-travel forms return 5,000; after rollback `main` points
  at the good snapshot and both snapshots stay in history until expiration.
- After a rollback or a branch commit the newest snapshot is not the current one. Read the head
  of `main` from `SELECT snapshot_id FROM t.refs WHERE name = 'main'`.
- Time travel does not undo a destructive schema change. After `DROP COLUMN customer_id`,
  `VERSION AS OF` an older snapshot shows the old schema and the values are readable, but a filter
  on that column fails during planning with `Cannot find field 'customer_id'`: pushed-down filters
  bind to the current schema.
- Snapshot retention (`expire_snapshots`) decides how far back time travel reaches.

## Branches, tags and write-audit-publish (WAP)
```python
spark.sql("ALTER TABLE db.t SET TBLPROPERTIES ('write.wap.enabled'='true')")
spark.sql("ALTER TABLE db.t CREATE BRANCH IF NOT EXISTS etl_run_2")
spark.conf.set("spark.wap.branch", "etl_run_2")
try:
    df.writeTo("db.t").append()                         # lands on the branch only
finally:
    spark.conf.unset("spark.wap.branch")                # never leak into later writes

audit = spark.read.option("branch", "etl_run_2").table("db.t")
if audit.groupBy("order_id").count().where("count > 1").limit(1).count() == 0:
    spark.sql("CALL lake.system.fast_forward('db.t', 'main', 'etl_run_2')")
spark.sql("ALTER TABLE db.t CREATE TAG published_2025_01_01")
```

- A buggy run on a branch leaves `main` unchanged and the audit reports `duplicate order_id`; a
  clean run publishes 6,000 rows. `t.refs` lists `main`, two branches (`BRANCH`) and one `TAG`.
- Branch = movable named reference; tag = fixed reference, can carry its own retention
  (`CREATE TAG x RETAIN 365 DAYS`).

| Need | Feature |
|---|---|
| New source attribute | `ADD COLUMN` |
| Monthly partitions too coarse | `REPLACE PARTITION FIELD` |
| Job loaded bad or duplicate data | `rollback_to_snapshot` |
| Output must pass checks before readers see it | Branch + audit + `fast_forward` |
| Month-end / regulatory state must stay queryable | Tag |
| Undo a dropped column | Re-add from a backup or old snapshot data; time travel alone is not enough |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `CommitFailedException: branch main has changed` | Stale base in PyIceberg | `refresh()` and retry |
| Spark does not see a PyIceberg commit | Catalog cache | `cache-enabled=false` or `REFRESH TABLE` |
| Rollback "did nothing" in a dashboard | Dashboard picks newest snapshot | Read `main` from `t.refs` |
| `Cannot find field` on time travel | Filter on a dropped column binds to current schema | Read without the filter, filter after load |
| Append rejected with `AnalysisException` | Schema mismatch | Evolve explicitly with `ALTER TABLE`, then append |
| Orphan Parquet files after failures | Failed commits | `remove_orphan_files` with a safe `older_than` |
| WAP writes landed on `main` | `write.wap.enabled` not set or branch conf leaked | Set the property; unset `spark.wap.branch` in `finally` |
