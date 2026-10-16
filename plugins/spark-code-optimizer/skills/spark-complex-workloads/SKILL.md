---
name: spark-complex-workloads
description: "Persistence and checkpoint placement for composite PySpark workloads: ML pipelines with cross-validation, iterative graph algorithms (PageRank), rollup/window/pivot over one base, Delta MERGE incremental ETL, cached+broadcast lookups, staged multi-shuffle pipelines and streaming x static joins. Use when a job mixes several of these patterns and you must decide which DataFrame to persist, localCheckpoint or checkpoint, and why."
---

# Spark Complex Workloads

Composite jobs fail on persistence placement, not on a single join or aggregate: the
wrong frame is cached, the right one is recomputed per branch, a loop's plan grows
without bound, or a "checkpoint for atomic writes" doubles the I/O. This skill covers
the six composite cases in sparkopt's `cases/complex_cases.py` plus the manual's
streaming x batch hybrid scenario (section 7.4), with corrected PySpark 3.3+ code.

Every scenario answers three questions: **which DataFrame**, **which mechanism**
(persist / localCheckpoint / checkpoint / nothing), and **why** (fan-out, lineage
growth, failure recovery, freshness).

## When to Use

- A DataFrame feeds `randomSplit`, `CrossValidator`, or several aggregation branches.
- A Python loop rebuilds a DataFrame from its previous value (PageRank, SSSP, label
  propagation, custom iterative scoring).
- An increment is upserted into a large table each run.
- A small lookup is joined in every batch / micro-batch and changes occasionally.
- A long batch pipeline has 3+ shuffles and several outputs.
- A streaming query is enriched with a static table.

## Mechanism cheat sheet

| Mechanism | Truncates plan | Survives executor loss | Needs `setCheckpointDir` | Use for |
|---|---|---|---|---|
| `persist(StorageLevel.MEMORY_AND_DISK)` + `count()` | No | Recomputed from lineage | No | Fan-out (2+ actions) |
| `localCheckpoint(eager=True)` | Yes | No (job fails) | No | Iterative loops, every N iterations |
| `checkpoint(eager=True)` | Yes | Yes | Yes, HDFS/S3/ABFS | Expensive long pipelines, recovery points |
| writeStream `checkpointLocation` | n/a | Yes | No (own option) | Streaming offsets + state |
| Nothing | - | - | - | Single-use frames, the final write |

PySpark levels: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_AND_DISK_DESER`, `DISK_ONLY`,
`OFF_HEAP` (+ `_2`/`_3`). `df.cache()` = `MEMORY_AND_DISK_DESER` for DataFrames. There is
no `*_SER` level in Python.

## Decision table

| Symptom / plan signal | Case | Reference |
|---|---|---|
| Every CV fit starts from `FileScan` + feature-join `Exchange`; train+test != total | `ml_pipeline` | `references/ml_pipeline.md` |
| Iteration time grows each loop; repeating join+agg stack in `explain()`; driver StackOverflowError | `graph_algorithm` | `references/graph_algorithm.md` |
| Rollup, window, pivot branches each scan the source; Storage tab empty during writes | `multi_stage_agg` | `references/multi_stage_agg.md` |
| Whole table rewritten per run; `Cannot overwrite a path that is also being read from` | `incremental_etl` | `references/incremental_etl.md` |
| `BroadcastExchange` over `FileScan` in every batch; stale lookup values | `cached_broadcast` | `references/cached_broadcast.md` |
| 3+ `Exchange` nodes, two writes replaying the same stages | `staged_materialization` | `references/staged_materialization.md` |
| Stream-static join re-reads the lookup each trigger; checkpoint on `/tmp` | `hybrid_checkpoint` (manual 7.4) | `references/streaming_batch_hybrid.md` |

## Cases

All thresholds below are sparkopt heuristics, not Spark rules.

### ml_pipeline (HIGH, 90)
- **Trigger:** `has_iterative_loop or estimated_reuse_count >= 3`. DSL `.ml_pipeline(has_cv, num_folds)`.
- **Generates:** persist features/train/test, assembler + scaler + RandomForest pipeline, optional CrossValidator.
- **Persist:** `features_df` (parent of `randomSplit`, must be stable) and `train_df` (parent of every CV fold). Not `test_df` (scored once), not predictions.
- **Checkpoint:** the estimator's `checkpointInterval` (trees with `cacheNodeIds`, ALS) needs a reliable `setCheckpointDir`; you cannot localCheckpoint inside `fit()`.
```python
features_df.persist(StorageLevel.MEMORY_AND_DISK); features_df.count()
train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
train_df.persist(StorageLevel.MEMORY_AND_DISK); train_df.count()
model = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=ev, numFolds=5).fit(train_df)
```
- **Gotchas:** `CrossValidator(parallelism=k)` multiplies fold caches; unpersist after the last action. Example: `examples/ml_pipeline.py`.

### graph_algorithm (CRITICAL, 100)
- **Trigger:** `has_iterative_loop`. DSL `.graph_algorithm(algorithm, checkpoint_interval=5)`.
- **Generates:** persisted vertices/edges, PageRank loop, localCheckpoint every N, sum-based convergence.
- **Persist:** `edges` (with `out_degree`) and `vertices`, read every iteration. **localCheckpoint** `ranks` every N iterations. No checkpoint dir.
```python
ranks = vertices.withColumn("rank", F.lit(1.0 / n)).localCheckpoint(eager=True)
for i in range(max_iter):
    contribs = (ranks.join(edges, ranks.id == edges.src).groupBy(F.col("dst").alias("id"))
                .agg(F.sum(F.col("rank") / F.col("out_degree")).alias("incoming")))
    ranks = vertices.join(contribs, "id", "left").select(
        "id", ((1 - d) / n + d * F.coalesce("incoming", F.lit(0.0))).alias("rank"))
    if (i + 1) % 5 == 0:
        ranks = ranks.localCheckpoint(eager=True)   # then L1 delta vs last checkpoint
```
- **Gotchas:** library joins on wrong keys and checks `sum(score)` (near constant); reference the previous frame once per iteration or the plan doubles; `unpersist()` does not free localCheckpoint blocks. Example: `examples/graph_algorithm.py`.

### multi_stage_agg (HIGH, 90)
- **Trigger:** `row_count > 5_000_000 and estimated_reuse_count >= 2`. No DSL method.
- **Generates:** cached base, rollup with `fillna("ALL")`, windows, pivot, three writes, then unpersist.
- **Persist:** the smallest frame all branches derive from: a daily pre-aggregate when measures are additive, else the column-pruned base. `count()` it, unpersist after the writes.
```python
daily.persist(StorageLevel.MEMORY_AND_DISK); daily.count()
rollup_df = daily.rollup("year", "quarter", "month").agg(F.sum("revenue"), F.grouping_id().alias("level"))
pivot_df = daily.groupBy("category").pivot("region", ["EU", "US", "APAC"]).agg(F.sum("revenue"))
```
- **Gotchas:** `rowsBetween(-29,0)` is 30 rows not days, fillna skips int columns, and it caches the full base. (Original sparkopt also unpersisted before any action and joined rollup with pivot on a missing column; the bundled copy writes before unpersisting and writes the outputs separately.) Example: `examples/multi_stage_agg.py`.

### incremental_etl (HIGH, 90)
- **Trigger:** `estimated_reuse_count >= 2 and not has_iterative_loop` (broad, confirm it is an upsert). DSL `.incremental_etl()`.
- **Generates:** persisted full target, double full-outer-join, checkpoint, in-place overwrite.
- **Persist:** only the deduplicated increment (reused by count + MERGE). **Never** the target. No checkpoint: Delta commits atomically.
```python
(DeltaTable.forPath(spark, target).alias("t").merge(updates.alias("s"), "t.id = s.id")
   .whenMatchedDelete(condition="s.op = 'D'")
   .whenMatchedUpdate(condition="s.op != 'D' AND NOT (t.v <=> s.v)", set={"v": "s.v"})
   .whenNotMatchedInsert(condition="s.op != 'D'", values={"id": "s.id", "v": "s.v"}).execute())
```
- **Gotchas:** dedup the source by key; partition predicate in ON for pruning; "missing from increment = deleted" is snapshot logic. Example: `examples/incremental_etl.py`.

### cached_broadcast (MEDIUM, 50)
- **Trigger:** `is_small` (`estimated_bytes <= 200 MB`) `and estimated_reuse_count >= 3`. No DSL method.
- **Generates:** TTL-by-call-count `LookupManager` + broadcast join loop.
- **Persist:** the lookup snapshot, reloaded when its Delta version changes; `F.broadcast()` in each join. Cache saves the scan, not the per-query broadcast.
- **Gotchas:** 200 MB is an explicit-hint ceiling; auto threshold is `spark.sql.autoBroadcastJoinThreshold` (10MB), hard limit 8GB, and driver memory / `spark.driver.maxResultSize` / per-executor copies limit it first. Load the new snapshot before unpersisting the old. Example: `examples/cached_broadcast.py`.

### staged_materialization (HIGH, 90)
- **Trigger:** `depth > 8 or wide_transform_count >= 3` (library comments/manual also cite 15 and 20; they disagree).
- **Generates:** persist after join 1, checkpoint after join 2 + window, persist before fork, checkpoint each output "for atomicity". `F`/`Window` are imported and `explain()` states the depth > 8 / 3+ shuffle trigger (original sparkopt missed the imports and printed `?`).
- **Place:** reliable `checkpoint(eager=True)` once, after the most expensive shuffles; `persist` + `count()` at the fork; nothing on single-consumer frames or before writes.
- **Gotchas:** `setCheckpointDir` once at startup on durable storage; Spark does not clean checkpoint files; `approx_count_distinct` default `rsd=0.05` (library's approx_distinct case uses 0.02 and calls it ~2%). Example: `examples/staged_materialization.py`.

### streaming x batch hybrid (manual 7.4, case `hybrid_checkpoint`)
- **Trigger:** `has_streaming_source`. DSL `.streaming()`. `StreamBatchJoinCase` is listed in the module docstring but not implemented.
- **Place:** persist + `count()` the static lookup before `start()`; `checkpointLocation` on durable storage; nothing on the join result (localCheckpoint is not allowed on streaming plans; inside `foreachBatch` persist only if the batch feeds 2+ sinks).
- Example: `examples/streaming_batch_hybrid.py`.

## Run sparkopt

```bash
# all built-ins + the six opt-in complex cases
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --case ml_pipeline --case graph_algorithm --case multi_stage_agg \
  --case incremental_etl --case cached_broadcast --case staged_materialization \
  --advice --format text

# with a plan captured by plain df.explain() so depth/exchanges/streaming are detected;
# the built-in hybrid_checkpoint restricts the built-ins to itself, cached_broadcast is added
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --plan plan.txt \
  --case hybrid_checkpoint --case cached_broadcast --format json
```

`--case <complex>` adds an opt-in case; `--case <builtin>` restricts evaluation to the named
built-ins; both can be mixed. Use simple-mode `df.explain()` for `--plan` (`formatted` is for
humans; it double-counts operators). Each matched case in the output has a `reviewed_example`
path (relative to the plugin root, pointing at `examples/<case>.py` here; the hybrid case maps to
the persistence skill's `hybrid_checkpoint.py`) next to the generated `code` sketch. The complex
templates are sketches with placeholder names and logic errors (wrong PageRank keys and teleport
term, caching a whole MERGE target, pre-write checkpoints); the reviewed examples are the corrected
versions, so start from them.

## Anti-patterns

- `persist()` with no action before the branches, or `unpersist()` before the last action.
- Caching a large target table to "speed up" a MERGE or upsert.
- `checkpoint()` right before a write to get atomicity; use Delta/Iceberg or the committer.
- `localCheckpoint()` on a cluster with aggressive dynamic allocation scale-down.
- `setCheckpointDir("/tmp/...")` on a cluster.
- `spark.conf.set("spark.serializer", ...)` at runtime: Spark 3 rejects core confs ("Cannot modify the value of a Spark config"); set it at startup.
- Caching every intermediate "just in case": each cache is a full extra write.
- Iterating with the previous frame referenced twice per iteration (exponential plan).
- Treating sparkopt thresholds (200 MB, 5M rows, depth 8/15/20, interval 5) as Spark limits.
