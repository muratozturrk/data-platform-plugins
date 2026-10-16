# persistence_strategy: one persistence verdict per DataFrame

## Problem

Several sparkopt cases can fire on the same DataFrame and suggest different things: `fanout_cache`
says cache, `lineage_break` says reliable checkpoint, `iterative_loop` says localCheckpoint,
`spill_prevention` says MEMORY_AND_DISK. Applying all of them stacks persistence on top of
persistence (cache + checkpoint of the same data) and wastes memory and I/O. You need exactly one
decision: cache, localCheckpoint, reliable checkpoint, or nothing.

## How Spark executes it

- `persist()` / `cache()` registers the plan in the `CacheManager`. Nothing happens until an action.
  The first action computes partitions and stores them in the executors' block managers. Later plans
  that contain the same sub-plan are rewritten to read `InMemoryRelation`. The lineage stays: if a
  block is lost, Spark recomputes it from the source.
- `localCheckpoint()` computes the DataFrame (eager by default), stores the partitions in executor
  block managers (MEMORY_AND_DISK) and returns a new DataFrame whose plan is just
  `LogicalRDD` / `Scan ExistingRDD`. Lineage is truncated. A lost block cannot be recomputed and the
  job fails.
- `checkpoint()` writes partitions as files to `SparkContext.setCheckpointDir` and returns a
  DataFrame that reads those files. Lineage is truncated and the data survives executor loss.

Plan signals:

```
df.explain()   # InMemoryTableScan / InMemoryRelation   -> cache is used
               # Scan ExistingRDD / LogicalRDD           -> (local)checkpoint is used
df.storageLevel   # StorageLevel(True, True, False, True, 1) after cache()
```

## sparkopt rule

`PersistenceStrategyCase`, priority `CRITICAL`.

```python
def matches(self, profile):
    lin = profile.lineage
    return lin.estimated_reuse_count >= 2 or lin.has_iterative_loop or lin.depth > 10
```

`apply()` picks one branch (first match wins):

| Branch | Condition | Generated code |
|---|---|---|
| local checkpoint | `lineage.needs_local_checkpoint` = `has_iterative_loop and not has_streaming_source` | `localCheckpoint(eager=True)` + loop every 5 |
| reliable checkpoint | `lineage.needs_reliable_checkpoint` = `depth > 20 and not has_iterative_loop` | `setCheckpointDir("hdfs:///...")` + `checkpoint(eager=True)` |
| local checkpoint | `depth > 10 and estimated_reuse_count >= 2` | same template as the first branch |
| cache | `estimated_reuse_count >= 2` | `persist(StorageLevel.<memory.recommended_storage_level>)` |
| none | else (e.g. depth 11-20, reuse 1) | comment "single use, no persist" |

The authoritative resolver is `PersistenceAdvisor.decide()` (run with `--advice`). Its order is:
HYBRID (streaming + reuse >= 2) → ITERATIVE_LOCAL_CHECKPOINT → RELIABLE_CHECKPOINT (depth > 20) →
LOCAL_CHECKPOINT (depth > 10 and reuse >= 2) → CACHE (reuse >= 2) → NO_PERSIST.
The case now has the same depth > 10 + reuse >= 2 → localCheckpoint branch as the advisor (original
sparkopt lacked it; the bundled copy adds it). The remaining difference is HYBRID: the case has no
streaming branch, so follow the advisor when they differ.

The depth branch reuses the iterative template, so its comments talk about an iteration loop and
its first line, `df.localCheckpoint(eager=True)`, discards the returned DataFrame. **Correction**:
write `df = df.localCheckpoint(eager=True)` once, before the reuse; no loop is needed.

All thresholds (10, 20, reuse 2, 0.8 memory headroom) are sparkopt heuristics, not Spark rules.

Corrections applied to the generated code:

- Storage level names: `MEMORY_ONLY_SER → MEMORY_ONLY`, `MEMORY_AND_DISK_SER → MEMORY_AND_DISK`.
- `localCheckpoint()` needs no `setCheckpointDir`.
- Checkpoint cleanup uses `Path.getFileSystem()` on this app's checkpoint subdirectory, not
  `FileSystem.get()` on the shared root (wrong FS for `s3a://`, and it deletes other apps' data).

## Before / After

Before: everything persisted, nothing released.

```python
enriched = orders.join(customers, "customer_id").cache()
enriched = enriched.checkpoint()          # needs setCheckpointDir, double storage
daily = enriched.groupBy("day").count()
by_country = enriched.groupBy("country").sum("amount")
daily.write.parquet(out_daily)
by_country.write.parquet(out_country)
```

After: one decision (reuse 2, shallow plan → cache), eager, released after the last action.

```python
from pyspark import StorageLevel

enriched = orders.join(customers, "customer_id").persist(StorageLevel.MEMORY_AND_DISK)
enriched.count()                           # materialize once
enriched.groupBy("day").count().write.mode("overwrite").parquet(out_daily)
enriched.groupBy("country").sum("amount").write.mode("overwrite").parquet(out_country)
enriched.unpersist()                       # after the last action
```

See `examples/persistence_strategy.py` for a function that applies the advisor's decision.

## Configs

| Config | Default | Note |
|---|---|---|
| `spark.sql.inMemoryColumnarStorage.compressed` | `true` | DataFrame cache is columnar + compressed |
| `spark.sql.inMemoryColumnarStorage.batchSize` | `10000` | rows per column batch |
| `spark.cleaner.referenceTracking.cleanCheckpoints` | `false` | launch-time; delete checkpoint files when unreferenced |
| `spark.memory.fraction` / `spark.memory.storageFraction` | `0.6` / `0.5` | launch-time only |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `AttributeError: MEMORY_ONLY_SER` | Scala-only level name | `StorageLevel.MEMORY_ONLY` |
| Cache shows in Storage tab only partially | first action was `show()`/`take()` | materialize with `count()` |
| Second action still re-reads source | the reused DataFrame is a different object/plan (e.g. extra `withColumn` before persist) | persist the exact DataFrame that the branches use |
| `SparkException: Checkpoint directory has not been set` | reliable `checkpoint()` without dir | `spark.sparkContext.setCheckpointDir("hdfs:///...")` |
| Job fails "checkpoint block ... not found" after executor loss | `localCheckpoint()` is not fault tolerant | switch to `checkpoint()` |
| Executors OOM late in a long job | cached data never unpersisted | `unpersist()` after last action, `spark.catalog.clearCache()` |

## Related cases

- `fanout_cache`, `storage_level`: the cache branch.
- `iterative_loop`: the localCheckpoint branch.
- `lineage_break`: the reliable checkpoint branch.
- `hybrid_checkpoint`: advisor HYBRID branch for streaming.
- `spill_prevention`: when the data does not fit storage memory.
