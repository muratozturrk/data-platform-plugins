# SparkOptimizer DSL

The fluent API in `scripts/sparkopt/dsl.py` needs a live SparkSession. `build()` profiles the DataFrame
(a `count()` plus a 5% sample per focus column), matches cases, applies them and renders the code.

```python
import sys; sys.path.insert(0, "<plugin>/scripts")
from sparkopt import SparkOptimizer

code, report = (
    SparkOptimizer(orders_df, spark)          # sample_fraction=0.05 by default
    .named("orders_df")
    .with_join(users_df, on=["user_id"])      # profiles users_df too → broadcast decision
    .group_by("category")
    .with_executor(memory_mb=8192, count=20)
    .reused(times=3)
    .build()
)
print(report.summary())
print(code)
```

## Methods

| Method | Effect |
| --- | --- |
| `named(name)` | variable name used in generated code |
| `with_join(right_df, on=[...])` | join columns plus right-side profile; BroadcastJoinCase uses the right size |
| `group_by(*cols)` | aggregation columns |
| `full_scan()` | counts rows per partition (real skew ratio; full scan, slow) |
| `reused(times)` | estimated reuse count |
| `lineage_depth(n)` | override the plan-derived depth |
| `with_executor(memory_mb, count)` | memory model for StorageLevel |
| `iterative(checkpoint_interval=5)` | sets the iterative flag and adds IterativeLoopCase with that interval |
| `streaming()` | sets the streaming flag and adds HybridCheckpointCase |
| `fanout(consumer_count=3)` | adds FanOutCacheCase |
| `ml_pipeline(has_cv, num_folds)` | adds MLPipelineCase (iterative) |
| `graph_algorithm(algorithm, checkpoint_interval)` | adds GraphAlgorithmCase (iterative) |
| `incremental_etl()` | adds IncrementalETLCase |
| `when_skewed(col, salt_buckets=10)` | adds SkewJoinCase for that column |
| `use_approx_distinct()`, `read_parquet()` | add those cases |
| `add_case(case)` | custom or composite case; replaces a built-in with the same `name` |
| `exclude(*names)` | skip cases |
| `with_persistence_advisor()` | adds the advisor decision to the generated code |
| `analyze_only()` | returns the DataProfile only |
| `explain()` | matched cases without codegen |
| `advice()` | the PersistenceAdvisor decision as text |
| `build()` | returns `(code, OptimizationReport)` |

`OptimizationReport` has `summary()`, `profile`, `matched_cases` and `generated_code`.

## DSL vs CLI

| | DSL | CLI |
| --- | --- | --- |
| Needs Spark | yes | no |
| Profile source | measured | JSON (measured with `collect_profile` or estimated) |
| Complex cases | via methods (ml_pipeline…) | `--case` (all six, including multi_stage_agg / cached_broadcast / staged_materialization) |
| Output | code + report object | text or JSON (+ `reviewed_example` per case) |

A common workflow: run `collect_profile.collect()` once on the cluster, then iterate offline with the CLI.

## Behaviour changes in the bundled copy

- `.iterative()`, `.streaming()`, `.ml_pipeline()` and `.graph_algorithm()` now set the profile flags. In
  the original they set unused `_force_*` attributes, so the profile was never iterative or streaming.
- `add_case()` replaces a built-in with the same name, so the iterative loop is no longer applied twice.
- `with_persistence_advisor()` affects `build()`; in the original the flag was never read.
- See [library_fixes](library_fixes.md) for the full list.
