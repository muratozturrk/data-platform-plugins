# profile.json reference

`sparkopt_cli.py --profile` reads a JSON object that mirrors sparkopt's `DataProfile` dataclasses
(`scripts/sparkopt/core/analyzer.py`). Every field is optional. Unknown keys inside `partition`,
`lineage`, `memory` or `columns` raise a `TypeError`, which catches typos.

## Top level

| Key | Type | Default | Used by |
| --- | --- | --- | --- |
| `row_count` | int | 0 | partial_agg (>1M), approx_distinct (>5M), window_opt (>500K), bucket_join / repartition_before_agg (>10M), spill_prevention (>5M), multi_stage_agg |
| `col_count` | int | 0 | header only |
| `estimated_bytes` | int | 0 | `is_small` (≤200 MB), `is_large` (≥10 GB), memory fit |
| `estimated_mb` | float | n/a | alternative to `estimated_bytes` (bytes wins if both are given) |
| `join_cols` | list[str] | [] | codegen column names; without it (and without `right_table_bytes`) `broadcast_join` is skipped |
| `agg_cols` | list[str] | [] | codegen column names |
| `right_table_bytes` | int | n/a | size of the other join side; `broadcast_join` matches when ≤ 200 MB |
| `df_name` | str | `"df"` | codegen |
| `spark_parallelism` | int | 200 | repartition/coalesce targets (`spark.sparkContext.defaultParallelism`) |

## `partition`

| Key | Meaning | Rule |
| --- | --- | --- |
| `count` | number of partitions | `< 8` → repartition; `> 2000` and `avg_rows < 10000` → coalesce / coalesce_write. Missing or 0: the CLI excludes `repartition` |
| `skew_ratio` | max rows / avg rows per partition | `> 3.0` → skew_join, repartition_before_agg |
| `avg_rows`, `min_rows`, `max_rows`, `empty_count` | partition stats | `avg_rows` is used by `is_oversized` |

Real skew needs `collect(..., full_partition_scan=True)`. The fast path assumes `skew_ratio = 1.0`.

## `lineage`

| Key | Meaning | Rule |
| --- | --- | --- |
| `depth` | DAG depth (plan indent levels) | >10 + reuse → localCheckpoint; >20 → reliable checkpoint / lineage_break; >8 → staged_materialization |
| `estimated_reuse_count` | how many actions/branches consume the DF | ≥2 → cache cases; ≥3 → ml_pipeline / cached_broadcast |
| `has_iterative_loop` | ML/graph loop | iterative_loop, graph_algorithm, advisor ITERATIVE_LOCAL_CHECKPOINT |
| `has_streaming_source` | readStream source | hybrid_checkpoint, advisor HYBRID (with reuse ≥2) |
| `wide_transform_count` | shuffles (Exchange count) | ≥3 → staged_materialization |

How to count `estimated_reuse_count` from code: count the distinct **actions** (write, count,
collect, toPandas, show) and the **branches** (separate downstream chains) that read the
DataFrame. Two writes from one chain count as 2.

With `--plan`, the missing fields `depth`, `has_streaming_source` and `wide_transform_count` are filled
from the plan, and `estimated_reuse_count` too when the plan shows a duplicate FileScan. Fields you
give explicitly are never overwritten. Depth and counts come from the `== Physical Plan ==` section
(streaming from anywhere in the text), so capture the plan with plain `df.explain()`; `formatted`
output double-counts and hides `FileScan`.

## `memory`

| Key | Default | Notes |
| --- | --- | --- |
| `executor_memory_mb` | 4096 | `spark.executor.memory` |
| `executor_count` | 10 | `spark.executor.instances` |
| `df_size_mb` | from `estimated_bytes` | override only if the cached size differs |

Usable storage memory = `executor_memory_mb × executor_count × 0.6 × 0.5` (spark.memory.fraction ×
storageFraction defaults). `recommended_storage_level`:
- `MEMORY_ONLY` when size < 80% of usable;
- `MEMORY_AND_DISK` when size/3 < 80% of usable (the ~3× columnar compression heuristic);
- `DISK_ONLY` otherwise.

## `columns`

`{"<name>": {"dtype": "long", "null_ratio": 0.02, "distinct_count": 120, "min_val": 0, "max_val": 9,
"mean_val": 4.5, "std_val": 1.2}}`. Only custom cases use these (see `examples/custom_case.py`).

## Estimating without Spark

| Field | Where to get it |
| --- | --- |
| size | storage listing (`aws s3 ls --summarize`, `hdfs dfs -du -s`) × 2–4 for Parquet decompression |
| rows | table stats (`DESCRIBE DETAIL` for Delta, `ANALYZE TABLE … COMPUTE STATISTICS`) |
| partitions | number of input files / `spark.sql.files.maxPartitionBytes` (128 MB) |
| depth | count chained transformations, joins ×5, aggs ×3 (sparkopt's own fallback heuristic) |

Mark estimated values as assumptions in the report.
