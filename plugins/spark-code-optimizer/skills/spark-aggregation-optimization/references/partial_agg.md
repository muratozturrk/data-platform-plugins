# partial_agg

## Problem

A `groupBy` over many rows shuffles every input row to the reducer that owns its key. For decomposable
aggregates (`sum`, `count`, `min`, `max`, `avg`, `first`, ...), most of that traffic is avoidable.
Each map task can combine its own rows per key first and ship one partial result per key per task.
When keys repeat a lot, shuffle volume drops by orders of magnitude. The problem cases are code that
*prevents* this combine step, and workloads where it can't help.

## How Spark executes it

For built-in aggregate functions the planner always produces a two-phase plan:

```
HashAggregate(keys=[group_col], functions=[sum(value), count(1)])            <- final
+- Exchange hashpartitioning(group_col, 200)
   +- HashAggregate(keys=[group_col], functions=[partial_sum(value), partial_count(1)])  <- map side
      +- FileScan parquet ...
```

`partial_` functions **below** the `Exchange` are the signal that map-side aggregation is working.
`ObjectHashAggregate` (for `collect_list`, `percentile_approx`, typed aggregators) and
`SortAggregate` (fallback for non-mutable buffer types) also run in two phases.

Signals that it is *not* working:

```
# repartition before groupBy: raw rows shuffled, partial agg runs after the shuffle
HashAggregate(keys=[group_col], functions=[sum(value)])
+- HashAggregate(keys=[group_col], functions=[partial_sum(value)])
   +- Exchange hashpartitioning(group_col, 200)        <- raw rows!
      +- FileScan ...

# pandas grouped-agg UDF: no partial phase at all
AggregateInPandas [group_col], [my_udaf(value)]
+- Sort [group_col ASC]
   +- Exchange hashpartitioning(group_col, 200)
```

In the Spark UI, compare the shuffle write of the map stage with its input size. If they are about
equal, nothing was combined.

## sparkopt rule

| Field | Value |
|---|---|
| Class | `PartialAggCase()` |
| Priority | `HIGH` (90) |
| matches() | `profile.row_count > 1_000_000` |
| Output | `add_hint("partial_agg", ...)` only (no configs) |

1M rows is a **sparkopt heuristic**.

Generated output (bundled copy):

```python
# PARTIAL AGGREGATION — map tarafında ön-toplama, shuffle verisi küçülür
# groupBy'dan ÖNCE repartition YAPMAYIN: ham satırları shuffle eder ve
# map-side partial aggregation'ın kazancını yok eder.
from pyspark.sql import functions as F
result = (
    df.groupBy("group_col")
    .agg(F.sum("value").alias("total"), F.count("*").alias("cnt"))
)
```

Original sparkopt prefixed the `groupBy` with `repartition("group_col")` (a shuffle of raw rows that
pushes the partial step after the shuffle) and set `spark.sql.aggregate.fastHashMap.enabled` (not a
Spark config; Spark silently accepts unknown `spark.sql.*` keys) and
`spark.sql.execution.useObjectHashAggregateExec` (already `true`); the bundled copy fixes all three.

**Corrections**:
1. The hint changes nothing by itself: a plain built-in `groupBy` already gets partial aggregation.
   The real fixes are removing an existing `repartition` before the `groupBy`, and replacing UDAFs
   with built-ins (below).
2. The "~5-10x smaller" claim in `explain()` depends entirely on rows-per-key per map task. With
   near-unique keys, partial aggregation saves nothing.

## Before / After

```python
from pyspark.sql import functions as F

# Before (original sparkopt style): extra shuffle, no map-side combine
result = df.repartition("store_id").groupBy("store_id").agg(F.sum("amount"))

# After: built-in aggregates, no repartition: partial_sum runs before the shuffle
result = df.groupBy("store_id").agg(F.sum("amount").alias("total"),
                                   F.count("*").alias("cnt"))

# Before: pandas UDAF (no partial phase)
# @F.pandas_udf("double")
# def mean_udf(v: pd.Series) -> float: return v.mean()
# df.groupBy("store_id").agg(mean_udf("amount"))
# After: built-in equivalent
result = df.groupBy("store_id").agg(F.avg("amount"))

# RDD code: reduceByKey combines map-side, groupByKey does not
pairs = df.select("store_id", "amount").rdd.map(tuple)
totals = pairs.reduceByKey(lambda a, b: a + b)      # not: pairs.groupByKey().mapValues(sum)
```

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.execution.useObjectHashAggregateExec` | `true` | Already on. Hash-based agg for object buffers. |
| `spark.sql.objectHashAggregate.sortBased.fallbackThreshold` | `128` | Keys per task before ObjectHashAggregate falls back to sort |
| `spark.sql.shuffle.partitions` | `200` | Reducer count for the final phase |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merges the small post-aggregation partitions |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Shuffle write ~ input size | Near-unique keys, or repartition before groupBy | Remove the repartition. For unique keys there is nothing to combine. |
| `AggregateInPandas` in the plan | pandas grouped-agg UDF | Rewrite with built-ins, or pre-aggregate with built-ins first |
| `SortAggregate` instead of `HashAggregate` | Buffer type not mutable/fixed-width (e.g. strings in `max` on old versions) | Usually fine. Check that the input is not pre-sorted unnecessarily. |
| `collect_list` spill/OOM | Partial agg can't shrink a list | Aggregate to scalars first, or bound the list size |
| Many distinct aggregates in one `agg` | `Expand` multiplies rows | See `approx_distinct` |
| One reducer is slow | Hot key | See `repartition_before_agg` (salted two-phase) |

## Related cases

- `repartition_before_agg`: same root issue (an extra repartition), plus the skew remedy.
- `approx_distinct`: distinct counts don't partially aggregate well in their exact form.
- `window_opt`: windows never partially aggregate. Replace with `groupBy` when possible.
- `adaptive_qe` (partition skill): coalesces post-aggregation partitions.
