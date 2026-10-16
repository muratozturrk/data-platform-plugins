# bucket_join

## Problem

Two large tables are joined on the same key in many jobs (daily reports, several downstream marts).
Every job reshuffles both sides from scratch. Bucketing pays the shuffle once at write time: rows are
hash-distributed into a fixed number of bucket files per table. Later joins on the bucket column skip
the `Exchange` (and optionally the `Sort`).

## How Spark executes it

`bucketBy(n, col)` writes each task's output split into `n` files by `hash(col) % n` and records the
bucket spec in the catalog. When both sides of an equi-join on `col` are bucketed tables with
compatible bucket counts, the planner knows they are already hash-partitioned the same way. It plans
`SortMergeJoin` with no `Exchange` above either `FileScan`. If they were also written with
`sortBy(col)` and each bucket is one file, the `Sort` can be skipped too.

Plan signal (`explain()`):

```
# Good: no Exchange, scans report bucketing
SortMergeJoin [id], [id], Inner
:- Sort [id ASC]                      <- may disappear with sortBy + 1 file/bucket
:  +- FileScan parquet db.orders_b  Bucketed: true, SelectedBucketsCount: 64 out of 64
+- Sort [id ASC]
   +- FileScan parquet db.items_b   Bucketed: true, SelectedBucketsCount: 64 out of 64

# Bad: bucketing not used
+- Exchange hashpartitioning(id, 200)
   +- FileScan parquet ...  Bucketed: false (bucket column(s) not read)
```

## sparkopt rule

| Field | Value |
|---|---|
| Class | `BucketJoinCase(num_buckets=200, bucket_col="id")` |
| Priority | `MEDIUM` (50) |
| matches() | `profile.row_count > 10_000_000 and not profile.is_small` (`is_small` = `estimated_bytes <= 200 MB`) |
| Output | `ctx.add_note("bucket_join", ...)`. A note, not a transformation, because it changes table layout. |

10M rows / 200 MB are **sparkopt heuristics**. The real criterion is *repeated* joins on the same key.
The profile can't see that.

Library output (original):

```python
df.write.bucketBy(200, "id").sortBy("id").saveAsTable("db.bucketed")
spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
result = df1.join(df2, on="id", how="inner")
```

**Corrections**:
1. Only one table is bucketed. Both sides must be bucketed on the join key, and the join must read the
   bucketed tables back via `spark.table()`, not the original `df1`/`df2`.
2. `spark.sql.sources.bucketing.enabled` is `true` by default, so the `conf.set` is a no-op.
3. 200 buckets is arbitrary. Size buckets so each file is roughly 128 MB-1 GB. Use the same number
   (or multiples, with `spark.sql.bucketing.coalesceBucketsInJoin.enabled`) on both sides.

## Before / After

```python
# Before: every job reshuffles both tables
orders = spark.read.parquet("s3://lake/orders")
items = spark.read.parquet("s3://lake/order_items")
result = orders.join(items, "order_id")

# After: one-time (or per-load) bucketed write of BOTH sides
(orders.write.mode("overwrite").bucketBy(64, "order_id").sortBy("order_id")
       .format("parquet").saveAsTable("sales.orders_b"))
(items.write.mode("overwrite").bucketBy(64, "order_id").sortBy("order_id")
      .format("parquet").saveAsTable("sales.order_items_b"))

# Every subsequent job
result = spark.table("sales.orders_b").join(spark.table("sales.order_items_b"), "order_id")
```

Write tip: if the writing DataFrame has many partitions, each task writes up to `n` files, so the
file count is `tasks x n`. Run `repartition(n, "order_id")` before `bucketBy` to get one file per
bucket.

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.sources.bucketing.enabled` | `true` | Use bucket specs when planning |
| `spark.sql.sources.bucketing.autoBucketedScan.enabled` | `true` | Spark may ignore bucketing if no operator benefits |
| `spark.sql.bucketing.coalesceBucketsInJoin.enabled` | `false` | Join 64 vs 128 buckets without shuffle (ratio must be <= 4) |
| `spark.sql.legacy.bucketedTableScan.outputOrdering` | `false` | Sort elision requires one file per bucket |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AnalysisException: 'save' does not support bucketBy` | Used `.save(path)` / `.parquet(path)` | Use `.saveAsTable(name)` |
| Delta table, bucketing not applied | Delta Lake does not support `bucketBy` | Use Z-ORDER / liquid clustering, or Parquet/Hive tables |
| Exchange still present | Different bucket counts, different column, column cast in the join, or one side not bucketed | Same key + same count (or coalesce config); join on the raw column |
| Exchange present after filter/projection | Bucket column dropped/renamed before the join | Keep the bucket column as-is |
| Thousands of tiny files | `tasks x buckets` files | `repartition(n, col)` before writing |
| Hive reads the table wrong | Spark and Hive bucketing hash functions differ | Treat the tables as Spark-only |
| Gains vanish over time | Appends without bucketing / mixed writers | Always write through the same bucketed path |

## Related cases

- `sort_merge_join`: bucketing removes the exchange of an SMJ. Without bucketing, SMJ shuffles every time.
- `broadcast_join`: if one side is small, broadcast and skip bucketing.
- `skew_join`: bucketing does not fix key skew. Hot keys still land in one bucket.
- `delta_zorder` (I/O skill): the Delta-side layout alternative.
