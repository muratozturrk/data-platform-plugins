# fanout_cache: compute once, consume N times

## Problem

One expensive DataFrame feeds several actions:

```
enriched_df -> report_A.write, report_B.count(), report_C.write, ml_features.toPandas()
```

Each action is a separate Spark job with its own plan. Without persistence each job re-reads the
sources and re-runs the joins/filters that build `enriched_df`: N actions cost N times the base work.

## How Spark executes it

Spark does not share results between actions automatically (except shuffle files that happen to be
reused within a job, and AQE stage reuse within one query). `persist()` registers the plan; the first
action materializes the partitions; every later plan containing the identical sub-plan is rewritten to
`InMemoryTableScan`.

Signals:

- Spark UI SQL tab: every query shows the same `Scan parquet ...` + `Exchange` for the base tables.
- After caching, the plans show `InMemoryTableScan`, and the Storage tab shows the DataFrame with
  "Fraction Cached" = 100% after the first full action.

## sparkopt rule

`FanOutCacheCase`, priority `HIGH`, `consumer_count=2` (constructor default). The generated comment
uses `max(consumer_count, lineage.estimated_reuse_count)`, and `explain()` no longer prints a count.
(Original sparkopt printed the fixed constructor value; the bundled copy fixes it.)

```python
def matches(self, profile):
    return profile.lineage.estimated_reuse_count >= 2
```

Generated code: build `base_df`, `base_df.persist(StorageLevel.<recommended_storage_level>)`,
N consumers, `base_df.unpersist()`. The level comes from `MemoryProfile.recommended_storage_level`
(see `storage_level.md`): `MEMORY_ONLY` if `size < usable*0.8`, `MEMORY_AND_DISK` if
`size/3 < usable*0.8`, else `DISK_ONLY`. Reuse 2 is a sparkopt heuristic: caching pays only if the
base is more expensive to compute than to write into and read from the block manager.

Corrections:

- Storage levels are PySpark names (original sparkopt emitted `MEMORY_ONLY_SER`; the bundled copy
  maps it to `MEMORY_ONLY`).
- Add `base_df.count()` right after `persist()` so the first consumer does not race to fill a
  partial cache, and so `show()`-style consumers do not leave the cache partially filled.
- The template's consumers are lazy (`report_A = base_df.groupBy(...)`), and `unpersist()` follows
  immediately, so nothing actually reads the cache. `unpersist()` must come after the last **action**.

## Before / After

Before:

```python
base = raw.join(dim, "key", "left").filter("is_valid").withColumn("x", F.col("a") * F.col("b"))
base.groupBy("region").agg(F.sum("revenue")).write.parquet(out_a)   # computes base
n = base.filter(F.col("category") == "electronics").count()        # computes base again
base.select("f1", "f2", "label").write.parquet(out_ml)              # and again
```

After:

```python
from pyspark import StorageLevel

base = (raw.join(dim, "key", "left").filter("is_valid")
        .withColumn("x", F.col("a") * F.col("b"))
        .persist(StorageLevel.MEMORY_AND_DISK))
base.count()                                    # eager materialization
try:
    base.groupBy("region").agg(F.sum("revenue")).write.mode("overwrite").parquet(out_a)
    n = base.filter(F.col("category") == "electronics").count()
    base.select("f1", "f2", "label").write.mode("overwrite").parquet(out_ml)
finally:
    base.unpersist()                            # after the last action
```

Persist only the columns consumers need (`select` before `persist`): cache size scales with width.

See `examples/fanout_cache.py`.

## Configs

| Config | Default | Note |
|---|---|---|
| `spark.sql.inMemoryColumnarStorage.compressed` | `true` | keep on |
| `spark.sql.inMemoryColumnarStorage.partitionPruning` | `true` | batch min/max pruning on cached data |
| `spark.memory.fraction` | `0.6` | launch-time only |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Cache not used by consumers | consumers built from a different DataFrame object or a different plan (column added before persist) | persist exactly the DataFrame consumers use |
| Storage tab shows < 100% cached | `MEMORY_ONLY` and not enough memory, or only `take()` ran | `MEMORY_AND_DISK`, `count()` |
| Every consumer recomputes despite persist | `unpersist()` ran before the actions | unpersist in `finally` after the last action |
| Executors evict other caches | cache too wide | `select` needed columns first |
| Slower with cache than without | base is cheap (single parquet scan + filter) | do not cache; parquet pushdown is often faster |
| Cache stale after source table update | cache is a snapshot | `unpersist()` and re-persist, or `spark.catalog.refreshTable` |

## Related cases

- `storage_level`: how the level is picked.
- `persistence_strategy`: when cache loses to a checkpoint (deep plan, loops).
- `spill_prevention`: when the base does not fit memory.
