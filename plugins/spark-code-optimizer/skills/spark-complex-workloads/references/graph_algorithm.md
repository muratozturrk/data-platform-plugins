# graph_algorithm: iterative PageRank / SSSP / label propagation on DataFrames

## Problem

Each iteration joins the current scores with the edge table and aggregates. DataFrames
are lazy, so after k iterations the plan of `scores` contains k joins + k aggregates.
Symptoms grow super-linearly: planning time per iteration climbs, the driver spends
seconds in the optimizer, then `StackOverflowError` or driver OOM. Any recomputation
(executor loss, or a second action on `scores`) replays the whole chain from the source.

## How Spark executes it

- No action = no execution: the loop only builds a deeper logical plan.
- `cache()` keeps blocks but does not truncate the logical plan; the analyzer still walks
  the full tree every iteration.
- `localCheckpoint(eager=True)` materializes to executor storage (memory + local disk) and
  returns a DataFrame whose plan is a single `LogicalRDD` leaf. No checkpoint dir needed.
  Trade-off: lost executor = lost blocks = job failure (the lineage was cut).
- `checkpoint(eager=True)` does the same on reliable storage (`setCheckpointDir`), slower.
- If a frame is referenced twice in the next iteration (e.g. scores used in the join and
  in a separate aggregate), the un-checkpointed plan doubles every iteration: 2^N.

Plan signal: `scores.explain()` after a few iterations shows a repeating stack of
`SortMergeJoin`/`BroadcastHashJoin` + `HashAggregate` nodes; after `localCheckpoint`
it shows `Scan ExistingRDD`. Driver logs: increasing gaps between jobs = planning cost.

## sparkopt rule

| Item | Value |
|---|---|
| `name` | `graph_algorithm` |
| `priority` | `Priority.CRITICAL` (100) |
| `matches()` | `lineage.has_iterative_loop` |
| Params | `algorithm="pagerank"`, `checkpoint_interval=5`, `max_iterations=50` |
| Registration | DSL `.graph_algorithm(algorithm, checkpoint_interval)` (forces iterative) |

`checkpoint_interval=5` is a sparkopt default, not a Spark rule; tune it so that
planning time between checkpoints stays small (3-10 is typical).

Generated code: persist vertices (`MEMORY_ONLY`) and edges (recommended level), start scores at `1/vertices.count()`, loop with a
PageRank update, every `checkpoint_interval` iterations `localCheckpoint` + check the
change in `sum(score)`.

Bugs (fixed in `examples/graph_algorithm.py`):

1. Original sparkopt used `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` (not in PySpark) and an
   unnecessary `setCheckpointDir("/tmp/graph_ckpt/")`; the bundled copy fixes both.
2. Join keys do not line up: `scores` has `id`, joined `on="src_id"`; contributions keyed
   by `dst_id`, joined `on="id"`. AnalysisException. `out_degree` is assumed to exist.
3. Start `1/N` but teleport term `(1 - d)`: mixes normalized and unnormalized PageRank.
4. `iteration % N == 0` checkpoints at iteration 0 (useless); use `(i + 1) % N == 0`.
5. Convergence on `sum(score)`: the sum is nearly constant in PageRank, so it "converges"
   immediately. Use the L1 difference between two materialized rank vectors.
6. (Original sparkopt also called `prev.unpersist()` on the un-checkpointed plan; the bundled copy
   drops it. `DataFrame.unpersist()` does not release localCheckpoint blocks anyway: drop
   references and the ContextCleaner frees them.)
7. `vertices_df.count()` inside `F.lit()` runs an extra job; compute N once.

## Before / After

```python
# Before: plan grows by one join + agg per iteration, never truncated
for i in range(50):
    contribs = ranks.join(edges, ranks.id == edges.src).groupBy("dst").agg(...)
    ranks = vertices.join(contribs, ...).select(...)
```

```python
# After
edges.persist(StorageLevel.MEMORY_AND_DISK); edges.count()
ranks = vertices.withColumn("rank", F.lit(1.0 / n)).localCheckpoint(eager=True)
last = ranks
for i in range(max_iter):
    contribs = (ranks.join(edges, ranks.id == edges.src)
                .groupBy(F.col("dst").alias("id"))
                .agg(F.sum(F.col("rank") / F.col("out_degree")).alias("incoming")))
    ranks = vertices.join(contribs, "id", "left").select(
        "id", ((1 - d) / n + d * F.coalesce("incoming", F.lit(0.0))).alias("rank"))
    if (i + 1) % 5 == 0:
        ranks = ranks.localCheckpoint(eager=True)
        delta = (ranks.alias("c").join(last.alias("p"), "id")
                 .agg(F.sum(F.abs(F.col("c.rank") - F.col("p.rank")))).first()[0])
        last = ranks
        if delta < tol:
            break
```

Full runnable version: `examples/graph_algorithm.py`.

## Configs

| Config | Why |
|---|---|
| `spark.sql.shuffle.partitions` / AQE coalescing | Small graphs: 200 default partitions per iteration is overhead; AQE coalesces |
| `spark.sql.autoBroadcastJoinThreshold` (10MB default) | Small vertex/rank frames may broadcast; edges should not |
| `spark.cleaner.periodicGC.interval` (30min default) | Lower it if old localCheckpoint blocks pile up in long loops |
| `spark.dynamicAllocation.enabled` | Executors removed by dynamic allocation take localCheckpoint blocks with them; prefer reliable `checkpoint()` or keep min executors |

## Gotchas / troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Each iteration slower than the last | Plan growth | localCheckpoint every N |
| `StackOverflowError` in the driver | Very deep plan | Smaller N; raise `-Xss` for the driver only as a stopgap |
| `Checkpoint block rdd_x not found` | Executor lost after localCheckpoint | Reliable `checkpoint()`, or disable dynamic allocation scale-down |
| Converges at the first check | Sum-based convergence | L1 / max-abs per-vertex delta |
| Memory climbs across checkpoints | Old checkpoint frames still referenced | Keep only `last` and current |
| Iteration time doubles each step | Frame referenced twice per iteration | Reference the previous scores once, or checkpoint every iteration |

For production graph work consider GraphFrames (`g.pageRank(..., maxIter)` and its
`checkpointInterval`) before hand-rolling; the persistence rules above still apply.

## Related cases

- `iterative_loop`, `lineage_break` (persistence skill): the generic lineage rules.
- `ml_pipeline`: estimator-internal iteration uses `checkpointInterval` instead.
- `staged_materialization`: reliable checkpoint when executor loss must be survivable.
