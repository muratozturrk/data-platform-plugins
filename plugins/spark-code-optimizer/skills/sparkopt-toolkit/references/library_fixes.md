# Fixes in the bundled sparkopt copy

Source: `Desktop/spark/optimized_code_generation` (sparkopt 0.2.0). The original is unchanged; the
plugin ships a fixed copy in `scripts/sparkopt/`. All 120 original tests still pass, and
`tests/test_plugin_cli.py` adds 14 regression tests.

## Fixed in code

| # | Problem in the original | Effect | Fix |
| --- | --- | --- | --- |
| 1 | Generated code used `StorageLevel.MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `*_SER_2` (45 places) | `AttributeError`: these levels exist only in Scala/Java | Mapped to `MEMORY_ONLY`, `MEMORY_AND_DISK`, `*_2`; the StorageLevel table was rewritten |
| 2 | `spark.conf.set("spark.serializer", Kryo)` advice | can't be changed at runtime, and doesn't affect DataFrame cache | removed |
| 3 | `spark.conf.set("spark.memory.fraction"/"storageFraction")` in spill_prevention | Spark 3.x rejects runtime changes of launch-time configs | the renderer emits launch-time keys as `--conf` comments |
| 4 | `setCheckpointDir()` added for `localCheckpoint` | unnecessary | removed where only localCheckpoint is used |
| 5 | `prev.unpersist()` after `localCheckpoint` on a never-persisted DF | no-op, and misleading | removed; old blocks are freed when the reference is dropped |
| 6 | `analyzer.py` / `dsl.py` imported pyspark at module level | nothing worked without Spark, tests included | lazy / `TYPE_CHECKING` imports |
| 7 | DSL `.iterative()` / `.streaming()` / `.ml_pipeline()` / `.graph_algorithm()` set unused `_force_*` attributes | profile never iterative/streaming | set the real flags |
| 8 | `.iterative()` added a second IterativeLoopCase next to the built-in | applied twice | custom case replaces the built-in with the same name |
| 9 | `with_persistence_advisor()` flag never read | no effect | `build()` adds the advisor decision |
| 10 | PersistenceStrategyCase had no "depth 10–20 + reuse → localCheckpoint" branch, unlike the advisor | case and advisor disagreed | branch added |
| 11 | `skew_join.explain()` printed salt buckets as the skew ratio; `fanout_cache` printed a fixed consumer count | wrong numbers in reports | explain() no longer prints profile values; fanout uses the reuse count |
| 12 | multi_stage_agg unpersisted before any action | cache never used | writes come before unpersist |
| 13 | staged_materialization: missing `F`/`Window` imports; explain() printed `?` | NameError / useless text | imports added, explain fixed |
| 14 | LineageInspector regexes used names Spark 3.5 doesn't print (`BroadcastNestedLoop`, `Cartesian`, `InMemoryScan`, `\bStreamingRelation\b`, `\bMicroBatch\b`, no `ObjectHashAggregate`) | BNLJ not a broadcast join, CartesianProduct not a cross join, InMemoryTableScan / StreamingRelationV2 / MicroBatchScan missed, collect_list aggregates uncounted | Spark 3.5 names: BNLJ sets broadcast + cross join, CartesianProduct cross join, InMemoryTableScan in-memory, `StreamingRelation(V2)` / `MicroBatch*` / `StreamingDataSourceV2*` streaming, ObjectHashAggregate counted |
| 15 | LineageInspector parsed the whole text of extended / `toString()` captures | logical plans double-counted depth, exchanges, joins, aggs, windows, filters, subqueries | counts come from the `== Physical Plan ==` section when present; streaming is still searched in the whole text. `formatted` still double-counts (its detail blocks are inside the physical section): use plain `df.explain()` |
| 16 | `collect_profile.py` wrote `queryExecution().toString()` | streaming never detected in collected profiles | writes `df.explain()` output; `toString()` is only the fallback |
| 17 | partial_agg emitted fake `spark.sql.aggregate.fastHashMap.enabled`, default `useObjectHashAggregateExec` and `repartition` before `groupBy` | no-op configs; repartition defeats partial aggregation | plain `groupBy` hint only |
| 18 | skew_join salting kept the right-side key and `_salt` | ambiguous columns after the join | right side drops them (run on PySpark 3.5; row count equals a plain join) |
| 19 | broadcast_join explain printed `~0 MB` without a right size | misleading text | size only when `right_table_bytes` is given |
| 20 | CLI `--case` with a built-in name; `repartition` with an unknown partition count | a built-in name did not narrow the run / `repartition` always fired on `--plan`-only runs | a built-in `--case` restricts evaluation to the named built-ins (complex names still opt in); `repartition` is excluded without `partition.count` |
| 21 | CLI output had only generated code | sketches with placeholder names | each matched case adds `reviewed_example` (path relative to the plugin root) |
| 22 | persistence_strategy local-checkpoint template called `df.localCheckpoint(eager=True)` without assigning the result | the checkpointed DataFrame was discarded, so nothing changed | `df = df.localCheckpoint(eager=True)` |

## Known limits (documented, not changed)

| Area | Limit | Where handled |
| --- | --- | --- |
| BroadcastJoinCase without a right size judges the main table's own size | can fire without any join | CLI skips it unless `join_cols` / `right_table_bytes` is given |
| Complex-case templates (ml_pipeline, graph_algorithm, incremental_etl, cached_broadcast, hybrid) | placeholder names and several logic errors (e.g. caching a whole target table, localCheckpoint on a streaming plan, PageRank teleport term) | corrected end-to-end versions in `spark-complex-workloads/examples/`; the CLI prints `reviewed_example` |
| LineageInspector depth = indent // 3; reuse from duplicate FileScan column lists | approximate | `spark-plan-analysis` skill |
| `fits_serialized` assumes ~3× compression | heuristic | `spark-persistence-strategy` skill |
| `incremental_etl` matches any reuse ≥2 non-iterative profile | broad trigger | opt-in via `--case` only |
| staged_materialization threshold (`depth > 8`) differs from the manual (15) | inconsistent docs | the code value is authoritative |
