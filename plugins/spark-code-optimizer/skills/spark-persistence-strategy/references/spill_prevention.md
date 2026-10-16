# spill_prevention: data larger than memory during shuffle/sort/aggregate

## Problem

Wide operations (groupBy, join, sort, window) buffer rows per task in execution memory. When a task's
data does not fit, Spark sorts and writes it to local disk ("spill") and merges later. Spill is
not an error but it multiplies I/O and CPU (serialize, compress, write, read, merge). Heavy spill
often comes with GC pauses and, in the worst case, executor OOM.

## How Spark executes it

- Execution and storage share the unified memory region:
  `(executor heap - 300MB) * spark.memory.fraction` (default 0.6). `spark.memory.storageFraction`
  (default 0.5) is the part of it protected from eviction for cached blocks. Execution can evict
  cached blocks above that line; storage cannot evict execution.
- Per task, execution memory is roughly `unified / active tasks`. Too few shuffle partitions →
  big partitions → spill. Skewed keys → one task spills heavily.
- Cached data with a memory level competes with execution memory.

Signals:

- Spark UI → Stages → task metrics `Spill (Memory)` and `Spill (Disk)`; compare max vs median
  task (skew).
- Executor logs: `UnsafeExternalSorter: Thread ... spilling sort data of ... to disk`.
- SQL tab: `Exchange` with few partitions and large "data size" per partition; `SortMergeJoin`,
  `HashAggregate` with "spill size" metric.

## sparkopt rule

`SpillPreventionCase`, priority `HIGH`.

```python
def matches(self, profile):
    mem = profile.memory
    return not mem.fits_in_memory and profile.row_count > 5_000_000
```

`fits_in_memory` is `df_size_mb < usable_memory_mb * 0.8` with
`usable_memory_mb = executor_memory_mb * executor_count * 0.6 * 0.5`. The 5M rows and 0.8 are sparkopt
heuristics; actual spill depends on per-task partition size, not the cluster-wide total.

Generated configs: `spark.conf.set("spark.sql.adaptive.enabled", "true")`, and the launch-time
`spark.memory.fraction=0.75` / `spark.memory.storageFraction=0.4` rendered as comments:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
# Launch-time ayarlar (spark.conf.set ile DEĞİŞTİRİLEMEZ) —
# spark-submit veya SparkSession.builder ile verin:
#   --conf spark.memory.fraction=0.75
#   --conf spark.memory.storageFraction=0.4
```

Generated code: `df.repartition(max(200, int(size_mb / 128)))` (about 128 MB per partition),
`persist(StorageLevel.MEMORY_AND_DISK)` when the DataFrame feeds several aggregations, `unpersist()`
afterwards.

Corrections:

- `spark.memory.fraction` and `spark.memory.storageFraction` are **launch-time** configs: with
  `spark.conf.set` at runtime Spark 3.x rejects them (`Cannot modify the value of a Spark config`).
  The bundled renderer never emits launch-time keys as `spark.conf.set`, only as `--conf` comments
  (original sparkopt emitted `spark.conf.set`). Put them in the builder before the session starts or
  in `spark-submit --conf`. Raising `memory.fraction` to 0.75 leaves less room for user/Python
  objects; change it only with evidence.
- Storage level: original sparkopt emitted `MEMORY_AND_DISK_SER`; the bundled copy emits
  `MEMORY_AND_DISK`.
- `repartition(n)` adds its own full shuffle. Prefer making the existing shuffle produce the right
  partition count: `spark.sql.shuffle.partitions` and AQE coalescing
  (`spark.sql.adaptive.coalescePartitions.enabled`, `spark.sql.adaptive.advisoryPartitionSizeInBytes`),
  and AQE skew join for skewed keys. Use `repartition("key")` only if the result is reused by
  several aggregations on the same key.
- Persisting does not prevent shuffle spill; it only avoids recomputing the input for each
  aggregation, and the cached blocks themselves use memory.

## Before / After

Before (original sparkopt output; the bundled copy already avoids the first and third lines):

```python
spark.conf.set("spark.memory.fraction", "0.75")      # rejected at runtime
df = df.repartition(200)                              # extra shuffle
df.persist(StorageLevel.MEMORY_AND_DISK_SER)          # AttributeError
a = df.groupBy("cat").agg(F.sum("val"))
```

After:

```python
spark = (SparkSession.builder
         .config("spark.memory.fraction", "0.7")          # only if measured
         .config("spark.sql.adaptive.enabled", "true")
         .getOrCreate())
spark.conf.set("spark.sql.shuffle.partitions", str(max(200, size_mb // 128)))
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

df.persist(StorageLevel.MEMORY_AND_DISK); df.count()
df.groupBy("cat").agg(F.sum("val")).write.mode("overwrite").parquet(out_cat)
df.groupBy("region").agg(F.count("*")).write.mode("overwrite").parquet(out_region)
df.unpersist()
```

See `examples/spill_prevention.py`.

## Configs

| Config | Default | Scope / note |
|---|---|---|
| `spark.sql.shuffle.partitions` | 200 | runtime; initial shuffle partitions |
| `spark.sql.adaptive.enabled` | true (3.2+) | runtime |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | runtime |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64MB | runtime; target post-shuffle size |
| `spark.sql.adaptive.skewJoin.enabled` | true | runtime |
| `spark.executor.memory`, `spark.executor.cores` | | launch-time; memory per task = memory / cores |
| `spark.memory.fraction` / `storageFraction` | 0.6 / 0.5 | launch-time |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Spill only in a few tasks | key skew | AQE skew join, salting as fallback |
| Spill in all tasks | partitions too big | more shuffle partitions / smaller advisory size |
| Spill grows after adding `cache()` | cached blocks squeeze execution memory | `MEMORY_AND_DISK` / `DISK_ONLY`, or no cache |
| `Cannot modify the value of a Spark config` | runtime set of core config | builder / `--conf` |
| Container killed by YARN/K8s, no Java OOM | off-heap/Python memory | `spark.executor.memoryOverhead` |
| Many tiny tasks after raising partitions | AQE coalescing off | enable coalescePartitions |

## Related cases

- `storage_level`: level choice when data exceeds memory.
- `fanout_cache`: caching the input of several aggregations.
- `skew_join`, `adaptive_qe`, `repartition` (other skills): partitioning-side fixes.
