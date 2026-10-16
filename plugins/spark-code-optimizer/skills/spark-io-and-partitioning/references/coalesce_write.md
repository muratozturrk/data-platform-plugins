# coalesce_write: avoid the small-files problem on write

## Problem

Each write task produces at least one file per output directory it touches. A DataFrame with 5000 small
partitions writes 5000 files, and with `partitionBy("dt")` it can write up to 5000 x (distinct `dt`)
files. Small files hurt everyone downstream:

- each file costs a listing, an open and a footer read (`spark.sql.files.openCostInBytes` = 4 MB of
  "virtual" cost per file);
- object stores (S3/ABFS/GCS) throttle on request count, not on bytes;
- the metastore and the Delta log grow with file count.

The usual target is files of about 128 MB to 1 GB.

## How Spark executes it

The write runs as the last stage of the job; the number of tasks in that stage is the number of
partitions of the DataFrame being written. Signals:

- Spark UI, last stage: thousands of tasks with tiny "Output Size".
- `df.rdd.getNumPartitions()` is in the thousands.
- In `df.explain()`, `coalesce(n)` appears as `Coalesce n` **without** an `Exchange`, while
  `repartition(n)` appears as `Exchange RoundRobinPartitioning(n)`.

```
== Physical Plan ==
Coalesce 100
+- *(1) Project [...]
   +- FileScan parquet [...]
```

Because `Coalesce` is a narrow dependency, Spark fuses it into the **same stage** as everything above it,
up to the previous shuffle. `coalesce(10)` on top of an expensive projection or a UDF means that this
work also runs in only 10 tasks.

## sparkopt rule

```python
class CoalesceOnWriteCase(OptimizationCase):
    name = "coalesce_write"
    priority = Priority.MEDIUM
    def matches(self, profile): return profile.partition.is_oversized
# PartitionProfile.is_oversized: count > 2000 and avg_rows < 10_000
```

- **Matches:** more than 2000 partitions **and** fewer than 10,000 rows per partition on average
  (sparkopt heuristic). Note that it looks at the in-memory partition count of the DataFrame, not at
  files already on disk.
- **Generates:** a hint `df.coalesce(100).write.parquet("output/path/")`.
- **Correction:** 100 is hard-coded. Derive the count from bytes, and choose between `coalesce` and
  `repartition` by how expensive the upstream stage is.

## Before / After

```python
# BEFORE: 5000 shuffle partitions -> 5000 files per dt
(df.groupBy("dt", "user_id").agg(F.sum("amount").alias("amount"))
   .write.partitionBy("dt").parquet(out))

# AFTER (A): cheap upstream, just merge partitions
n = max(1, math.ceil(est_bytes / (256 * 1024**2)))
df.coalesce(n).write.mode("overwrite").parquet(out)

# AFTER (B): expensive upstream or partitionBy -> shuffle by the partition column
(df.repartition("dt")                      # one task per dt value (plus AQE coalescing)
   .write.option("maxRecordsPerFile", 5_000_000)
   .partitionBy("dt").mode("overwrite").parquet(out))
```

Pick (A) when the stage before the write is a cheap scan/projection. Pick (B) when it does heavy work,
when you write with `partitionBy`, or when partitions are skewed (`coalesce` only merges neighbours and
never splits a big one).

## Configs

| Config | Default | Use |
|---|---|---|
| `maxRecordsPerFile` (write option) / `spark.sql.files.maxRecordsPerFile` | `0` (unlimited) | cap file size when `repartition("dt")` makes one huge file per day |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | AQE merges small shuffle partitions before the write |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | target post-shuffle size (needs `parallelismFirst=false` to be honoured when coalescing) |
| `spark.databricks.delta.optimizeWrite.enabled` / Delta `optimizeWrite` | off (OSS) | Delta-native file sizing |

## Gotchas / troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| Write became much slower after `coalesce(n)` | upstream work collapsed to n tasks | use `repartition(n)` instead (an extra shuffle, but parallel) |
| Still thousands of files with `partitionBy` | files = tasks x partition values | `repartition(<partition cols>)` before `partitionBy` |
| One giant file per partition value | `repartition("dt")` puts each dt in one task | add `maxRecordsPerFile`, or spread with `repartition("dt", (F.rand() * k).cast("int"))` |
| `coalesce(1)` OOM / hours | single task for the whole dataset | never on large data; write many files and let readers handle it |
| Files small even after AQE | `parallelismFirst=true` ignores advisory size | set `spark.sql.adaptive.coalescePartitions.parallelismFirst=false` |
| Existing small files on disk | rule only affects new writes | compact: read + `repartition` + overwrite, or Delta `OPTIMIZE` |

## Related cases

- `coalesce`: same trigger, applied in the middle of a pipeline instead of before the write.
- `repartition`: the opposite problem (too few partitions).
- `delta_zorder`: Delta `OPTIMIZE` both compacts files and clusters them.
- `adaptive_qe`: post-shuffle coalescing often makes manual `coalesce` unnecessary.
