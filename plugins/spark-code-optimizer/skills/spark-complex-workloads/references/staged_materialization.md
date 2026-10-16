# staged_materialization: where cache and checkpoint go in a long pipeline

## Problem

A batch job chains read -> join -> join -> window -> aggregate -> several writes. Without
materialization points, every write replays the full chain, a late executor failure
recomputes from the source, and very deep plans slow the optimizer. Materializing
everywhere is just as bad: each cache/checkpoint is a full write of the data.

## How Spark executes it

- Every action re-executes its whole plan, except subtrees served from cache.
- Shuffle files are reused across jobs while they exist, so a second action on the same
  DataFrame can skip stages ("skipped" in the UI). This is best-effort, not a strategy:
  shuffle files disappear with executors and do not help branched DataFrames with
  different downstream plans.
- `persist()` + action: blocks kept, plan intact, recomputed from lineage if lost.
- `localCheckpoint()`: blocks on executors, plan truncated, not recoverable.
- `checkpoint()`: reliable files under `setCheckpointDir`, plan truncated, recoverable.
  Spark does not delete them unless `spark.cleaner.referenceTracking.cleanCheckpoints=true`.
- Atomic output is the job of the output committer or the table format (Delta/Iceberg),
  never of a checkpoint.

Plan signal: count `Exchange` nodes in `df.explain()`; repeated identical subtrees in two
writes' plans (or identical stage DAGs in the UI) = recomputation; plan text thousands of
lines long = depth problem.

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `staged_materialization` |
| `priority` | `Priority.HIGH` (90) |
| `matches()` | `lineage.depth > 8 or lineage.wide_transform_count >= 3` |
| Registration | Not in `ALL_BUILTIN_CASES`, no DSL method; select it via the CLI `--case` |

All thresholds are sparkopt heuristics, and they are inconsistent across the library:
`matches()` uses depth > 8 (the code value is authoritative), the generated comments say
"DAG > 20 -> checkpoint" and the manual says > 15. `explain()` now matches the code ("DAG
derinliği > 8 veya 3+ shuffle"). Use the matches() values for the trigger and decide
checkpoint placement from cost, below.

Generated code (bundled copy): imports `F`, `Window` and `StorageLevel`; read + filter; join
`dim_A`, persist stage2; join `dim_B` + rank window, `setCheckpointDir("hdfs:///...")` and
`checkpoint(eager=True)` stage3, unpersist stage2; groupBy with `approx_count_distinct`; persist
stage4 at `MEMORY_ONLY` before a fork; `checkpoint()` both outputs "for atomic writes"; write
both; unpersist. Original sparkopt forgot the `F`/`Window` imports (NameError), used
`MEMORY_ONLY_SER` and its `explain()` printed `?`; the bundled copy fixes all three.

Remaining template bugs (fixed in `examples/staged_materialization.py`):

1. stage2 is persisted but has a single consumer (stage3): pure overhead.
2. Checkpointing the two outputs before writing them: two extra full writes of data that
   is a cheap filter over a cached frame; it gives no atomicity.
3. `setCheckpointDir` mid-pipeline: set it once at startup, from config, durable path.
4. `approx_count_distinct("user_id")` uses the default `rsd=0.05` (~5% relative standard
   deviation). Pass `rsd` explicitly when accuracy matters. (The library's
   `approx_distinct` case claims "~2%" and passes `rsd=0.02`; that is a setting, not a default.)

## Placement rules

| Point in the pipeline | Action | Why |
|---|---|---|
| Source read + cheap filters | Nothing | Re-reading parquet with pushdown is cheap |
| Frame with one consumer | Nothing | Cache only pays off with 2+ actions on it |
| Frame with 2+ consumers (fork) | `persist(MEMORY_AND_DISK)` + `count()` | Each branch would recompute it |
| After the most expensive shuffles, before risky/long work | `checkpoint(eager=True)` | Failure recovery restarts here; plan truncated |
| Iterative loop | `localCheckpoint` every N | See graph_algorithm |
| Right before a write | Nothing | Atomicity belongs to committer/table format |

## Before / After

```python
# Before (original sparkopt style)
stage2 = raw.join(dim_A, "key_a").persist(StorageLevel.MEMORY_ONLY_SER)     # 1 consumer, bad level
stage3 = stage2.join(dim_B, "key_b").withColumn("rank", F.rank().over(w)).checkpoint(eager=True)
stage4 = stage3.groupBy("category", "region").agg(...).persist(StorageLevel.MEMORY_ONLY_SER)
out_a = stage4.filter(...).checkpoint(eager=True)                          # redundant
out_b = stage4.filter(...).checkpoint(eager=True)                          # redundant
```

```python
# After
spark.sparkContext.setCheckpointDir(args.checkpoint_dir)                   # once, durable
ranked = (raw.join(dim_a, "user_id", "left").join(dim_b, "product_id")
          .withColumn("rank_in_category", F.rank().over(w))
          .checkpoint(eager=True))
summary = ranked.groupBy("category", "region").agg(
    F.sum("revenue").alias("total"),
    F.approx_count_distinct("user_id", rsd=0.05).alias("uniq_users"),
).persist(StorageLevel.MEMORY_AND_DISK)
summary.count()
summary.where("region = 'EU'").write.mode("overwrite").parquet(f"{out}/eu")
summary.where("region != 'EU'").write.mode("overwrite").parquet(f"{out}/non_eu")
summary.unpersist()
```

Full runnable version: `examples/staged_materialization.py`.

## Configs

| Config | Why |
|---|---|
| `sparkContext.setCheckpointDir(<hdfs/s3/abfs>)` | Required for `checkpoint()`; local paths break on clusters |
| `spark.cleaner.referenceTracking.cleanCheckpoints` | Delete checkpoint files when the RDD is GC'd (off by default) |
| `spark.sql.adaptive.enabled` | Default on (3.2+); rewrites joins/partitions at runtime |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Checkpoint directory has not been set` | `checkpoint()` without dir | `setCheckpointDir` at startup |
| Checkpoint bucket grows forever | Spark does not clean it | `cleanCheckpoints=true` + lifecycle rule |
| Job slower after adding checkpoints | Checkpoints on cheap or single-use frames | Keep one after the expensive shuffles |
| Both writes recompute the joins | No persist before the fork | Persist + `count()` the fork point |
| Partial output after failure | Plain parquet overwrite | Delta/Iceberg, or write to a new path and switch the table location |

## Related cases

- `lineage_break`, `fanout_cache` (persistence skill): single-rule versions.
- `multi_stage_agg`: the fork point is an aggregation base.
- `graph_algorithm`: iterative variant with localCheckpoint.
