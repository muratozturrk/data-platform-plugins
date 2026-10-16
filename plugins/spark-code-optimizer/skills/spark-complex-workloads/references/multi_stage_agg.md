# multi_stage_agg: rollup + window + pivot from one shared base

## Problem

One expensive base (scan + filter + joins) feeds several aggregation branches: a
hierarchical rollup, window analytics, a pivot. Each branch is its own action, so without
persistence Spark rebuilds the base once per branch: N scans, N sets of upstream shuffles.

## How Spark executes it

- `rollup(a, b, c)` = `Expand` (one copy of every row per grouping set, here 4x) +
  `HashAggregate`. Doing it on raw rows multiplies the shuffle volume by the number of
  grouping sets; doing it on a pre-aggregate is cheap.
- Window functions = `Exchange hashpartitioning(partition cols)` + `Sort` + `Window`.
  `rowsBetween(-29, 0)` is 30 *rows*; a 30-day window needs `rangeBetween` over a numeric
  day/second ordering.
- `pivot(col)` without values runs an extra job to collect distinct values first;
  `pivot(col, [values])` skips it.
- Persisted data is used only by actions that run *after* it is materialized and *before*
  `unpersist()`. `unpersist()` right after defining lazy branches means no branch uses it.

Plan signal: each branch's `explain()` (or the SQL tab) starting from `FileScan` + the same
upstream `Exchange` nodes = recomputation. After the fix they start from
`InMemoryTableScan`.

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `multi_stage_agg` |
| `priority` | `Priority.HIGH` (90) |
| `matches()` | `row_count > 5_000_000 and lineage.estimated_reuse_count >= 2` |
| Registration | Not in `ALL_BUILTIN_CASES` and no DSL method; select it via the CLI `--case` |

5M rows is a sparkopt heuristic: the real question is whether recomputing the base costs
more than caching it, whatever the row count.

Generated code (bundled copy): persist `base_df` (recommended level), rollup on
year/quarter/month with `fillna("ALL")`, persist the rollup at `MEMORY_ONLY`, window with
`rowsBetween(-29, 0)`, running total and daily rank, pivot on region with explicit values, then
write the three outputs and only then unpersist:

```python
OUTPUT_BASE = "s3://bucket/reports"  # hedef dizin
rollup_result.write.mode("overwrite").parquet(OUTPUT_BASE + "/rollup")
windowed_df.write.mode("overwrite").parquet(OUTPUT_BASE + "/windowed")
pivot_df.write.mode("overwrite").parquet(OUTPUT_BASE + "/pivot")

base_df.unpersist()
rollup_result.unpersist()
```

Original sparkopt unpersisted before any action (the cache was never used), used
`MEMORY_ONLY_SER` (AttributeError in PySpark) and joined the rollup with the pivot on
`category`, which the rollup does not have; the bundled copy writes before unpersisting, uses
PySpark levels and writes the outputs separately.

Remaining template bugs (fixed in `examples/multi_stage_agg.py`):

1. `fillna({"year": "ALL", ...})` on integer columns is silently ignored (fillna only fills
   type-compatible columns), and it confuses subtotal NULLs with real NULLs. Use
   `F.grouping_id()` or `F.grouping(col)`.
2. `rowsBetween(-29, 0)` labeled "30d": it is 30 rows.
3. It caches the full `base_df`, not the smallest frame the branches share (below).
4. No `count()` after persist: the first branch materializes it; fine, but two concurrent
   branches (threads, `parallelism`) would both compute the base.

## Before / After

```python
# Before (original sparkopt style): base recomputed for every branch; cache dropped before it was used
base_df.persist(StorageLevel.MEMORY_AND_DISK_SER)          # AttributeError in PySpark
rollup_result = base_df.rollup("year", "quarter", "month").agg(...)
pivot_df = base_df.groupBy("category").pivot("region", [...]).agg(...)
base_df.unpersist()
rollup_result.write.parquet(...)                            # recomputes base
```

```python
# After: cache the smallest frame every branch can be derived from
daily = (base.groupBy("category", "region", "event_date")
         .agg(F.sum("revenue").alias("revenue"), F.count("*").alias("orders"))
         .withColumn("year", F.year("event_date"))
         .withColumn("quarter", F.quarter("event_date"))
         .withColumn("month", F.month("event_date")))
daily.persist(StorageLevel.MEMORY_AND_DISK); daily.count()

rollup_df = daily.rollup("year", "quarter", "month").agg(
    F.sum("revenue").alias("total_revenue"), F.grouping_id().alias("level"))
w30 = (Window.partitionBy("category")
       .orderBy(F.datediff("event_date", F.lit("1970-01-01"))).rangeBetween(-29, 0))
pivot_df = daily.groupBy("category", "year").pivot("region", ["EU", "US", "APAC"]).agg(F.sum("revenue"))

rollup_df.write.parquet(...); windowed_df.write.parquet(...); pivot_df.write.parquet(...)
daily.unpersist()                                           # after the last action
```

Full runnable version: `examples/multi_stage_agg.py`.

### Choosing what to cache

| Branches need | Cache |
|---|---|
| Only additive measures (sum, count, min, max) at day grain or coarser | The daily pre-aggregate (often 100-1000x smaller) |
| Averages | Pre-aggregate `sum` and `count`, divide after |
| Row-level ranks, exact `countDistinct`, medians | The column-pruned raw base |
| One branch only | Nothing |

## Configs

| Config | Why |
|---|---|
| `spark.sql.adaptive.enabled` + `coalescePartitions.enabled` | Small rollup/pivot outputs get few partitions instead of 200 |
| `spark.sql.pivotMaxValues` (10000) | Ceiling for pivot without explicit values |
| `spark.sql.shuffle.partitions` | Window on a big base: size so each sorted partition fits memory |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Storage tab empty during the writes | `unpersist()` before actions, or no action yet | Materialize with `count()`, unpersist last |
| "ALL" never appears in rollup output | fillna type mismatch | `grouping_id()` |
| Rolling average wrong around gaps | `rowsBetween` counts rows, not days | `rangeBetween` on a day number |
| Window stage has a few huge tasks | Low-cardinality `partitionBy` (e.g. category) | Pre-aggregate first; check skew |
| Extra short job before the pivot | Pivot values not listed | Pass the value list |
| Cache only partly in memory | Raw base too large for `MEMORY_ONLY` | Pre-aggregate, prune columns, `MEMORY_AND_DISK` |

## Related cases

- `fanout_cache`, `storage_level`: the base reuse rule.
- `window_opt`, `partial_agg`: per-branch tuning of the window and aggregate.
- `staged_materialization`: when the base itself is a long, multi-shuffle pipeline.
