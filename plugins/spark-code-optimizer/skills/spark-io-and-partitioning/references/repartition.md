# repartition: raise parallelism when partitions are too few

## Problem

A stage runs one task per partition. With 4 partitions on a 64-core cluster, 60 cores idle and each
task processes a quarter of the data; one slow task stretches the whole stage, and large partitions
spill. Common causes:

- a non-splittable source (a single `.gz` CSV/JSON file, one big file from an external system);
- `spark.read.jdbc` without `numPartitions` / `partitionColumn`;
- an earlier `coalesce(1)` or `coalesce(small)`;
- `spark.createDataFrame` from a small local list that later explodes or joins into big data.

## How Spark executes it

`repartition(n)` adds a full shuffle: every row is written to shuffle files and read back into `n` new
partitions. In the plan:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Exchange RoundRobinPartitioning(400), REPARTITION_BY_NUM, [plan_id=12]
   +- FileScan csv [...]

# repartition(n, "key")
+- Exchange hashpartitioning(key#5, 400), REPARTITION_BY_NUM, [plan_id=12]
```

Diagnosis signals:

- `df.rdd.getNumPartitions()` returns a single digit;
- Spark UI: the stage has fewer tasks than `spark.sparkContext.defaultParallelism`;
- the plan has `FileScan csv` / `json` on `.gz` files or `Scan JDBCRelation(...) [numPartitions=1]`.

`REPARTITION_BY_NUM` tells AQE not to change the count. `repartition("key")` without a number shows
`REPARTITION_BY_COL`, and AQE may coalesce it.

## sparkopt rule

```python
class RepartitionCase(OptimizationCase):
    name = "repartition"
    priority = Priority.HIGH
    def matches(self, profile): return profile.partition.is_undersized
    def apply(self, ctx):
        target = max(ctx.profile.partition.count * 4, ctx.spark_parallelism * 2)
        # -> df = df.repartition(target)
# PartitionProfile.is_undersized: count < 8
```

- **Matches:** fewer than 8 partitions (a sparkopt heuristic). It ignores data size and cluster size.
- **Generates:** `df = df.repartition(target)` with `target = max(count*4, spark_parallelism*2)`.
  `spark_parallelism` defaults to 200 in `CodegenContext`, so the target is **400** whenever
  `count < 100`, which is always the case when the rule fires.
- **Correction:** 400 partitions for a 20 MB DataFrame creates 400 tiny tasks and 400 tiny output files,
  which is the `coalesce` problem in reverse. Base the target on both cores and bytes, and skip the
  repartition entirely for small data.

## Before / After

```python
# BEFORE: one gzip file -> 1 partition -> 1 task for the whole pipeline
raw = spark.read.option("header", True).csv("s3://bucket/dump.csv.gz")
out = raw.withColumn("score", heavy_expr).groupBy("k").agg(F.sum("score"))

# AFTER: repartition once, right after the non-splittable read
import math
cores = spark.sparkContext.defaultParallelism
est_bytes = 30 * 1024**3                       # from the Spark UI "Input Size" or file listing
target = max(cores * 2, math.ceil(est_bytes / (128 * 1024**2)))
raw = spark.read.option("header", True).csv("s3://bucket/dump.csv.gz").repartition(target)
out = raw.withColumn("score", heavy_expr).groupBy("k").agg(F.sum("score"))

# JDBC: fix it at the source instead of repartitioning afterwards
spark.read.jdbc(url, "orders", column="order_id", lowerBound=1, upperBound=50_000_000,
                numPartitions=32, properties=props)
```

Use `repartition(n, "key")` when the next step groups or joins on `key`: that one shuffle can then also
serve the aggregation (no second `Exchange`). Use plain `repartition(n)` for an even round-robin spread
before row-wise heavy work.

## Configs

| Config | Default | Note |
|---|---|---|
| `spark.default.parallelism` | total executor cores | RDD operations; `defaultParallelism` reads it |
| `spark.sql.shuffle.partitions` | `200` | partitions after a DataFrame shuffle (initial count under AQE) |
| `spark.sql.files.maxPartitionBytes` | `128MB` | lower it to get more scan partitions from splittable files |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | none (uses shuffle.partitions) | high initial value + AQE coalescing |

## Gotchas / troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| repartition made the job slower | data was small; shuffle cost > gain | remove it; fewer than 8 partitions of small data is fine |
| Still one busy task after `repartition(n, "key")` | key skew: one hot key goes to one partition | round-robin `repartition(n)`, or AQE skew join / salting (`skew_join`) |
| Two `Exchange`s in a row | `repartition` followed by `groupBy` on a different key | repartition by the grouping key, or drop the repartition |
| Many tiny output files after the fix | write inherits the 400 partitions | `coalesce` before write (see `coalesce_write.md`) |
| Splittable Parquet still 1 partition | one small file or `maxPartitionBytes` large | expected for small data; otherwise lower `maxPartitionBytes` |

## Related cases

- `coalesce` / `coalesce_write`: the opposite condition (too many partitions).
- `adaptive_qe`: AQE adjusts *shuffle* partition counts but cannot split a non-splittable scan.
- `repartition_before_agg` (spark-aggregation skill): repartition by the group key before a heavy aggregation.
- `skew_join`: when the problem is one hot key rather than too few partitions.
