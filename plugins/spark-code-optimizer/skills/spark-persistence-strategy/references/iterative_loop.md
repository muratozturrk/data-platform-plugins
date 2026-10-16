# iterative_loop: break the plan every N iterations

## Problem

Driver-side Python loops that rebuild a DataFrame from itself:

```python
for i in range(100):
    df = df.withColumn("rank", 0.15 + 0.85 * F.col("contrib")).join(...)
```

Every iteration wraps the previous logical plan. After 50-100 iterations:

- `explain()` takes seconds and prints thousands of lines.
- Analysis/optimization time grows with every iteration (often super-linearly), so the gap
  between Spark jobs grows even though the data is constant.
- The driver holds the whole plan tree: driver OOM, long GC pauses, `StackOverflowError` in the
  optimizer.
- If anything fails, recomputation walks back through all iterations.

`cache()` does not help: it keeps the plan and just adds an `InMemoryRelation` on top.

## How Spark executes it

DataFrames are immutable plans. `df = f(df)` builds `f(f(f(...source)))`. Catalyst analyzes and
optimizes the full tree at every action. Truncating the plan requires a checkpoint: afterwards the
plan is `Scan ExistingRDD` and the loop starts from a short tree again.

Signals:

- `len(df._jdf.queryExecution().logical().toString())` keeps growing per iteration.
- In the Spark UI, the time between consecutive jobs grows; the jobs themselves stay short.
- Driver logs show long `Optimizer` / `Analyzer` phases or GC.

## sparkopt rule

`IterativeLoopCase`, priority `CRITICAL`, `checkpoint_interval=5` (constructor default).

```python
def matches(self, profile):
    return profile.lineage.has_iterative_loop
```

The advisor (`ITERATIVE_LOCAL_CHECKPOINT`) also wins over everything except HYBRID: an iterative
loop is checked before depth.

Generated code (corrected):

- localCheckpoint the starting point, then every `checkpoint_interval` iterations.
- **Removed** `spark.sparkContext.setCheckpointDir("/tmp/sparkopt_ckpt/")`: `localCheckpoint()`
  does not use the checkpoint directory.
- **Fixed** `prev.unpersist()`: in the library, `prev` is the un-checkpointed DataFrame that was never
  persisted, so the call is a no-op. Blocks of an old local checkpoint are released by Spark's
  ContextCleaner after the driver drops the last reference; in Spark 3.x `DataFrame.unpersist()`
  does not release localCheckpoint blocks. So rebind the variable and let the old one go.
- The interval 5 and the "~3x faster than reliable checkpoint" claim are sparkopt heuristics.

## Before / After

Before:

```python
ranks = initial
for i in range(50):
    ranks = step(ranks, links)
ranks.write.parquet(out)     # plan with 50 nested joins
```

After:

```python
CHECKPOINT_INTERVAL = 5
ranks = initial.localCheckpoint(eager=True)
for i in range(50):
    ranks = step(ranks, links)
    if (i + 1) % CHECKPOINT_INTERVAL == 0:
        ranks = ranks.localCheckpoint(eager=True)   # old blocks freed when unreferenced
ranks.write.mode("overwrite").parquet(out)
```

Static inputs used in every iteration (`links` above) should be persisted once and
materialized with `count()` before the loop, then `unpersist()`ed after it.

Fault-tolerant variant for long or critical loops: replace `localCheckpoint(eager=True)` with
`checkpoint(eager=True)` after `spark.sparkContext.setCheckpointDir("hdfs:///...")`; costs a
write + read per interval but survives executor loss.

See `examples/iterative_loop.py`.

## Configs

| Config | Note |
|---|---|
| `spark.dynamicAllocation.cachedExecutorIdleTimeout` | default infinity; executors holding local-checkpoint blocks are not released. If you lower it, local checkpoints can disappear |
| `spark.cleaner.periodicGC.interval` | default 30min; how often driver GC runs so ContextCleaner frees old blocks |
| `spark.sql.optimizer.maxIterations` | do not raise to "fix" slow loops; truncate the plan |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Job fails with missing checkpoint block | executor lost; local checkpoint not recoverable | reliable `checkpoint()` every N, or smaller N plus retries |
| Storage tab keeps growing across iterations | old checkpointed DataFrames still referenced (list, dict, closure) | keep only the current reference; optional `gc.collect()` |
| Loop still slow after checkpoint | checkpoint interval too large, or non-checkpointed side input with deep plan | lower interval; checkpoint side inputs too |
| `localCheckpoint()` on streaming DataFrame errors | not supported on streaming | use `foreachBatch` |
| Each checkpoint is slow | data recomputed from source each time | persist static inputs before the loop |

## Related cases

- `lineage_break`: same idea without a loop, reliable checkpoint for depth > 20.
- `persistence_strategy`: iterative branch of the advisor.
- `fanout_cache`: caching loop-invariant inputs.
