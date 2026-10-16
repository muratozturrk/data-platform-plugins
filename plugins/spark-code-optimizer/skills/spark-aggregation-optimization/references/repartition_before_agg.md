# repartition_before_agg

## Problem

Aggregations can suffer from two different problems, and the sparkopt rule combines them:

1. **Bad partition sizing**: too few or uneven partitions upstream, or too many tiny ones after the
   aggregation.
2. **Hot group keys**: one key (e.g. `country = 'US'`, `status = 'OK'`, a null placeholder) holds a big
   share of the rows. The reducer that owns it becomes a straggler. This is the case when the
   aggregate can't shrink rows map-side (`collect_list`, exact distinct, UDAFs), or when it is
   computed over a window.

A `repartition` on the group keys solves neither. `groupBy` already hash-partitions by those keys, and
a hot key hashes to one partition no matter how many partitions exist.

## How Spark executes it

```python
df.repartition(400, "k").groupBy("k").agg(F.sum("v"))
```

```
HashAggregate(keys=[k], functions=[sum(v)])
+- HashAggregate(keys=[k], functions=[partial_sum(v)])
   +- Exchange hashpartitioning(k, 400)      <- raw rows shuffled; no map-side combine
      +- Scan ...
```

Compare with plain `df.groupBy("k").agg(F.sum("v"))`:

```
HashAggregate(keys=[k], functions=[sum(v)])
+- Exchange hashpartitioning(k, 200)
   +- HashAggregate(keys=[k], functions=[partial_sum(v)])   <- combine before the shuffle
```

The repartitioned version shuffles more data. The one benefit: the `Exchange` it adds is *reused*
by every later operator that needs `hashpartitioning(k)` on the same frame (several aggregations,
windows, joins on `k`) within one plan. Through `cache()` with AQE on (verified on Spark 3.5), the
cached plan's partitioning can be hidden, and Spark re-adds an `Exchange`. Check `explain()`.

Hot-key signal: Spark UI > aggregation stage > Max task duration / shuffle read far above the 75th
percentile, and `df.groupBy("k").count().orderBy(F.desc("count"))` shows a few dominant keys.

## sparkopt rule

| Field | Value |
|---|---|
| Class | `RepartitionBeforeAggCase()` |
| Priority | `MEDIUM` (50) |
| matches() | `profile.partition.is_skewed or profile.row_count > 10_000_000` (`is_skewed` = `skew_ratio > 3.0`, max/avg rows per partition) |
| Output | `add_hint("repartition_agg", ...)` |

Both thresholds are **sparkopt heuristics**. `skew_ratio` measures *input partition* imbalance. The
group-by shuffle repartitions anyway, so input skew only slows the map stage. It is not key skew.

Library output (original):

```python
result = (
    df.repartition(spark.sparkContext.defaultParallelism * 2, *group_keys)
    .groupBy(*group_keys).agg(...)
)
```

**Corrections**:
1. For a single aggregation, drop the repartition. It adds a full shuffle and disables partial
   aggregation (see `partial_agg`).
2. The comment "same keys in the same partition, less spill" describes what `groupBy` already does.
3. It does not fix key skew. Use a **two-phase salted aggregation** for decomposable aggregates
   (sum/count/min/max; avg as sum/count), or AQE + finer keys.
4. `defaultParallelism * 2` is arbitrary. Size by data: roughly shuffle bytes / 128 MB, or let AQE
   coalesce.
5. For *input* partition skew (few huge files, unsplittable gzip), use `repartition(n)` *without
   keys*, or tune `spark.sql.files.maxPartitionBytes`.

## Before / After

```python
from pyspark.sql import functions as F

# Before (library style)
out = df.repartition(400, "country").groupBy("country").agg(F.sum("amount").alias("total"))

# After, normal case: plain groupBy + AQE coalescing
out = df.groupBy("country").agg(F.sum("amount").alias("total"))

# After, hot keys: two-phase salted aggregation
N = 32
partial = (df.withColumn("_salt", (F.rand(seed=7) * N).cast("int"))
             .groupBy("country", "_salt")
             .agg(F.sum("amount").alias("s"), F.count("*").alias("c")))
out = (partial.groupBy("country")
              .agg(F.sum("s").alias("total"), F.sum("c").alias("cnt"))
              .withColumn("avg", F.col("total") / F.col("cnt")))

# After, reuse: several aggregations on the same keys share one shuffle
# (with cache() + AQE, verify in explain() that no new Exchange appears)
by_c = df.repartition("country").cache()
totals = by_c.groupBy("country").agg(F.sum("amount"))
maxes = by_c.groupBy("country").agg(F.max("amount"))
```

## Configs

| Config | Default | Notes |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Needed for runtime coalescing |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merges small post-aggregation partitions |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | Target partition size after coalescing |
| `spark.sql.shuffle.partitions` | `200` | Initial reducer count. Set high and let AQE coalesce. |
| `spark.sql.files.maxPartitionBytes` | `128MB` | Input split size (fixes input-side imbalance) |

AQE's skew-join split does **not** apply to aggregations. Hot aggregation keys need salting.

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Two `Exchange` nodes on the same key | repartition + groupBy | Remove the repartition unless the frame is reused |
| Shuffle write ~ input size | Repartition removed map-side combine | Remove it |
| One reducer straggles | Hot group key | Two-phase salted aggregation |
| Salted result wrong for `avg`/`countDistinct` | Non-decomposable aggregate in phase 1 | avg = sum/count. Distinct: dedupe `(key, col)` first, or use `approx_count_distinct`. |
| Map stage uneven, reduce stage fine | Input partition skew (files) | `repartition(n)` without keys / `maxPartitionBytes` |
| Thousands of tiny output partitions | Too many shuffle partitions, AQE off | Enable AQE coalescing |

## Related cases

- `partial_agg`: why repartitioning before aggregating hurts.
- `skew_join` (join skill): the join-side counterpart. AQE handles join skew but not aggregation skew.
- `repartition` / `coalesce` / `adaptive_qe` (partition skill): partition count sizing.
- `approx_distinct`: the skew-safe way to do distinct counts.
