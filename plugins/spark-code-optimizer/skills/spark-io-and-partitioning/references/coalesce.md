# coalesce: cut task overhead when partitions are too many and too small

## Problem

Each task has a fixed cost: scheduling on the driver, deserializing the closure, opening the input or
shuffle blocks, and reporting metrics, which comes to tens of milliseconds. A stage with 20,000 partitions of
a few hundred rows each spends most of its time on that overhead, keeps the driver busy, and writes
thousands of small shuffle blocks or files. Typical causes:

- reading thousands of small files (each becomes at least part of a partition);
- `spark.sql.shuffle.partitions` set very high for a small dataset, without AQE coalescing;
- a selective `filter` that leaves the original partition count with almost-empty partitions;
- `union` of many DataFrames (partition counts add up).

## How Spark executes it

`coalesce(n)` merges existing partitions **without a shuffle**: each new partition reads several parent
partitions, preferring ones on the same executor. It is a narrow dependency, so it is fused into the
current stage and also reduces the parallelism of everything upstream until the previous shuffle.

```
== Physical Plan ==
Coalesce 200
+- *(1) Filter (isnotnull(status#7) AND (status#7 = FAILED))
   +- *(1) ColumnarToRow
      +- FileScan parquet [...]
```

Signals that you need it:

- `df.rdd.getNumPartitions()` in the thousands while the data is small;
- Spark UI: median task duration under about 100 ms, "Scheduler Delay" and "Task Deserialization Time"
  comparable to "Executor Computing Time";
- many shuffle partitions with a few KB each ("Shuffle Read Size" per task).

## sparkopt rule

```python
class CoalesceCase(OptimizationCase):
    name = "coalesce"
    priority = Priority.MEDIUM
    def matches(self, profile): return profile.partition.is_oversized
    # -> df = df.coalesce(200)
# PartitionProfile.is_oversized: count > 2000 and avg_rows < 10_000
```

- **Matches:** more than 2000 partitions averaging fewer than 10,000 rows (a sparkopt heuristic). The same
  trigger drives `coalesce_write`, so both appear together; use one.
- **Generates:** `df = df.coalesce(200)` (a fixed 200).
- **Correction:** derive `n` from data volume (about 64-256 MB per partition) and cores, and fix the
  source of the excess partitions before reaching for `coalesce`.

## Before / After

```python
# BEFORE: 30k tiny input files -> 30k partitions; selective filter keeps 0.1%
failed = spark.read.json("s3://logs/2024/*/*/*.json").filter(F.col("status") == "FAILED")
failed.groupBy("service").count().show()

# AFTER (1): pack small files into fewer scan partitions at read time
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024**2))
spark.conf.set("spark.sql.files.openCostInBytes", str(8 * 1024**2))
failed = spark.read.json("s3://logs/2024/*/*/*.json").filter(F.col("status") == "FAILED")

# AFTER (2): after a very selective filter, merge cheaply
n = max(spark.sparkContext.defaultParallelism, 1)
failed = failed.coalesce(n)
```

## coalesce vs repartition

| | `coalesce(n)` | `repartition(n)` |
|---|---|---|
| Shuffle | no | yes (full) |
| Can increase partitions | no (stays at current count) | yes |
| Balance | uneven, locality-based merge | even (round-robin or hash) |
| Effect upstream | parent stage also runs with n tasks | parent stage keeps its parallelism |
| Plan node | `Coalesce n` | `Exchange RoundRobinPartitioning(n)` |
| Use when | reducing moderately, upstream cheap | reducing drastically, upstream heavy, or skewed |

## Configs

| Config | Default | Note |
|---|---|---|
| `spark.sql.files.maxPartitionBytes` | `128MB` | larger value: fewer scan partitions |
| `spark.sql.files.openCostInBytes` | `4MB` | larger value packs more small files per partition |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE merges small *shuffle* partitions automatically |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | `false` makes AQE honour `advisoryPartitionSizeInBytes` |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | `1MB` | floor used when `parallelismFirst=true` |

## Gotchas / troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| Job slower after `coalesce` | expensive upstream now runs in n tasks | use `repartition(n)` or move `coalesce` after a shuffle |
| A few huge partitions after `coalesce` | locality-based merge is uneven | `repartition(n)` |
| `coalesce(5000)` did nothing | `coalesce` cannot increase | use `repartition` |
| Shuffle partitions still tiny with AQE on | `parallelismFirst=true` | set it to `false` and an advisory size |
| Partition count jumps back up later | a later shuffle uses `spark.sql.shuffle.partitions` | rely on AQE, or lower `shuffle.partitions` |
| Thousands of files on write | write uses the current partition count | see `coalesce_write.md` |

## Related cases

- `coalesce_write`: same trigger, applied right before `.write`.
- `repartition`: the opposite problem (too few partitions).
- `adaptive_qe`: post-shuffle coalescing removes most manual `coalesce` calls.
- `parquet_pushdown`: selective filters that prune at scan time leave fewer empty partitions.
