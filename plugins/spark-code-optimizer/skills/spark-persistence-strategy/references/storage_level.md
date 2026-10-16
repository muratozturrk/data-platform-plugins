# storage_level: picking a StorageLevel that exists in PySpark

## Problem

sparkopt (and its manual section 6.4) recommends `StorageLevel.MEMORY_ONLY_SER` as the "default"
and `MEMORY_AND_DISK_SER` as the "production default". In PySpark these attributes do not exist:

```
>>> from pyspark import StorageLevel
>>> StorageLevel.MEMORY_ONLY_SER
AttributeError: type object 'StorageLevel' has no attribute 'MEMORY_ONLY_SER'
```

The `_SER` levels are Scala/Java API. In Python, the "serialized vs deserialized" distinction is
meaningless for DataFrames anyway: the DataFrame cache stores compressed columnar batches.

## How Spark executes it

A `StorageLevel` is `(useDisk, useMemory, useOffHeap, deserialized, replication)`. For DataFrames,
`persist(level)` creates an `InMemoryRelation` that stores `CachedBatch`es (columnar, compressed per
column) in the block manager with that level:

- `useMemory` without `useDisk`: blocks that do not fit, or get evicted, are recomputed on read.
- `useDisk`: blocks that do not fit are written to executor local disk and read back.
- `replication=2/3`: each block also stored on other executors.

Signals: `df.storageLevel`; Spark UI Storage tab (Size in Memory / Size on Disk / Fraction Cached);
`explain()` shows `InMemoryTableScan`.

### PySpark levels (3.3+)

| PySpark level | Tuple (disk, mem, offheap, deser, repl) | When |
|---|---|---|
| `MEMORY_AND_DISK_DESER` | (T, T, F, T, 1) | `df.cache()` / `df.persist()` default for DataFrames in Spark 3.x |
| `MEMORY_AND_DISK` | (T, T, F, F, 1) | Equivalent choice for DataFrames; safe default |
| `MEMORY_AND_DISK_2` | (T, T, F, F, 2) | Expensive base, executors get preempted (spot) |
| `MEMORY_ONLY` | (F, T, F, F, 1) | Small hot data, cheap to recompute evicted parts; `RDD.cache()` default |
| `MEMORY_ONLY_2` | (F, T, F, F, 2) | Same, replicated |
| `DISK_ONLY` | (T, F, F, F, 1) | Much larger than storage memory, very expensive to recompute |
| `DISK_ONLY_2`, `DISK_ONLY_3` | (T, F, F, F, 2/3) | Same, replicated |
| `OFF_HEAP` | (T, T, T, F, 1) | Requires `spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size` |

Mapping of library names: `MEMORY_ONLY_SER → MEMORY_ONLY`, `MEMORY_AND_DISK_SER → MEMORY_AND_DISK`,
`MEMORY_ONLY_SER_2 → MEMORY_ONLY_2`, `MEMORY_AND_DISK_SER_2 → MEMORY_AND_DISK_2`.

## sparkopt rule

`StorageLevelCase`, priority `HIGH`.

```python
def matches(self, profile):
    return profile.lineage.estimated_reuse_count >= 2
```

The level comes from `MemoryProfile.recommended_storage_level` (`core/analyzer.py`):

```
total_cluster_mb = executor_memory_mb * executor_count          # default 4096 * 10
usable_memory_mb = total_cluster_mb * 0.6 * 0.5                 # default 12288 MB
fits_in_memory   = df_size_mb       < usable_memory_mb * 0.8
fits_serialized  = df_size_mb / 3.0 < usable_memory_mb * 0.8
MEMORY_ONLY (was _SER) | MEMORY_AND_DISK (was _SER) | DISK_ONLY
```

The generated comment table uses different bands (`usable*0.3`, `0.8`, `1.5`) than the property;
the persisted level is the property's. The `/3` "serialized shrink" factor and the 0.8 headroom are
sparkopt heuristics; they were meant for Kryo-serialized RDDs, not DataFrames.

Reality check on the formula: Spark's unified memory is `(executor heap - 300MB) * spark.memory.fraction`.
`spark.memory.storageFraction` (0.5) is the part of it that execution cannot evict; storage can
borrow more when execution is idle. So `usable_memory_mb` is a conservative lower bound per cluster,
and per-executor skew matters more than the cluster total.

Other corrections: the library says `spark.conf.set("spark.serializer", "...KryoSerializer")` makes
"SER levels effective". `spark.serializer` is a launch-time config (builder / `spark-submit --conf`),
and it does not change DataFrame cache size; it affects RDD shuffle/cache serialization. For
streaming it suggests `MEMORY_AND_DISK_SER_2` → use `MEMORY_AND_DISK_2` (for the static side only).

## Before / After

Before (fails in Python):

```python
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

After:

```python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()
print(df.storageLevel)        # Disk Memory Serialized 1x Replicated
...
df.unpersist()
```

Kryo, if wanted for RDD work, goes into the builder:

```python
spark = (SparkSession.builder
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .getOrCreate())
```

See `examples/storage_level.py` (includes a helper that maps legacy names safely).

## Configs

| Config | Default | Scope |
|---|---|---|
| `spark.memory.fraction` | 0.6 | launch-time |
| `spark.memory.storageFraction` | 0.5 | launch-time |
| `spark.memory.offHeap.enabled` / `.size` | false / 0 | launch-time, needed for `OFF_HEAP` |
| `spark.serializer` | JavaSerializer | launch-time |
| `spark.sql.inMemoryColumnarStorage.compressed` | true | runtime SQL conf |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AttributeError: ... MEMORY_ONLY_SER` | Scala-only name | use the mapping above |
| `Cannot modify the value of a Spark config: spark.serializer` | runtime `spark.conf.set` | set at launch |
| `OFF_HEAP` persist has no effect / errors | off-heap not enabled | enable + size at launch |
| Fraction Cached < 100% with `MEMORY_ONLY` | not enough storage memory | `MEMORY_AND_DISK` |
| New level ignored (DataFrame) / `Cannot change storage level` (RDD) | already persisted; DataFrame logs "Asked to cache already cached data" | `unpersist()` first, then persist with the new level |
| Cached size larger than estimate | estimate from row size, not columnar size | read the Storage tab |

## Related cases

- `fanout_cache`, `persistence_strategy`: use this level.
- `spill_prevention`: when nothing fits.
- `hybrid_checkpoint`: replicated levels for the static side of a streaming job.
