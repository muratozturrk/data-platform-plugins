---
name: sparkopt-toolkit
description: "How to run the bundled sparkopt PySpark optimization engine: the offline CLI (profile JSON and/or explain() plan to matched cases, PersistenceAdvisor decision and generated code), collect_profile for live DataFrames, the fluent SparkOptimizer DSL, case composition (& | ~) and writing custom OptimizationCase rules. Use when running sparkopt, writing a profile.json, adding a custom Spark optimization rule, or interpreting sparkopt output."
---

# sparkopt toolkit

sparkopt is a rule engine for PySpark. It turns a **DataFrame profile** into prioritized optimization cases with ready-made code:
- profile = size, partitions, skew, lineage depth, reuse, iteration, streaming, executor memory;
- each case has `matches(profile)`, `priority`, `apply(ctx)` and `explain()`;
- persistence conflicts go to the **PersistenceAdvisor**, which makes one decision.

This plugin ships a fixed copy under `${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt/` (see [library fixes](references/library_fixes.md)) and an offline CLI.

## When to Use This Skill

- Running sparkopt on a job, with or without a SparkSession
- Writing or reviewing a `profile.json`
- Capturing a profile and a plan from a live DataFrame
- Composing cases or writing a custom rule
- Explaining why sparkopt picked (or skipped) a case

## Quick start

```bash
# 1. No Spark available: describe the DataFrame in JSON
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --advice

# 2. Add a plain df.explain() plan: depth, shuffles, reuse and streaming are derived from it
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --plan plan.txt --advice

# 3. Plan only, opt-in complex scenario, machine-readable output
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --plan plan.txt --case multi_stage_agg --format json

# 4. Only some built-ins: a built-in --case name restricts evaluation to the named built-ins
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --case broadcast_join --case skew_join

# 5. Every case with its priority
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --list-cases
```

Inside a Spark job or notebook, collect real numbers:

```python
import sys; sys.path.insert(0, "<plugin>/scripts")
from collect_profile import collect
collect(orders_df, "orders", join_cols=["user_id"], agg_cols=["category"],
        right_df=users_df, reuse_count=3)          # writes orders.profile.json + orders.plan.txt
```

`orders.plan.txt` is the `df.explain()` output (`queryExecution().toString()` only if `explain()`
fails), so streaming is detected in collected profiles. When you capture a plan yourself, use plain
`df.explain()` (simple mode); `explain(mode="formatted")` is for humans only, because its detail
blocks double every count and its scans print as `Scan parquet`.

Example profile: [examples/profile_example.json](examples/profile_example.json). Field reference: [profile_json](references/profile_json.md).

## Reading the output

| Section | Meaning |
| --- | --- |
| `profile` | Values sparkopt decided on (after plan merge); `recommended_storage_level` comes from MemoryProfile |
| `plan` | LineageInspector signals (only with `--plan`) |
| `matched_cases` | Priority order CRITICAL > HIGH > MEDIUM > LOW; each has `explain` and `reviewed_example`, a path relative to the plugin root (`skills/<skill>/examples/<case>.py`) to the corrected, reviewed version |
| `advice` | PersistenceAdvisor's single persistence decision. **It wins over the individual persistence cases.** |
| `code` | Generated sketch with placeholder names (`left_df`, `dim_df`…). Adapt it; prefer the `reviewed_example` |

Matching uses profile thresholds only and doesn't read your code. Drop cases the code doesn't need, e.g. `window_opt` when there is no window.

## Cases and where they are documented

| Cases | Skill |
| --- | --- |
| broadcast_join, skew_join, bucket_join, sort_merge_join | `spark-join-optimization` |
| partial_agg, approx_distinct, window_opt, repartition_before_agg | `spark-aggregation-optimization` |
| parquet_pushdown, coalesce_write, delta_zorder, repartition, coalesce, adaptive_qe | `spark-io-and-partitioning` |
| persistence_strategy, iterative_loop, fanout_cache, lineage_break, storage_level, hybrid_checkpoint, spill_prevention | `spark-persistence-strategy` |
| ml_pipeline, graph_algorithm, multi_stage_agg, incremental_etl, cached_broadcast, staged_materialization (opt-in via `--case`) | `spark-complex-workloads` |
| plan signals behind every case | `spark-plan-analysis` |

## DSL (needs a live SparkSession)

```python
from sparkopt import SparkOptimizer
code, report = (SparkOptimizer(df, spark)
                .with_join(users_df, on=["user_id"])
                .group_by("category")
                .with_executor(memory_mb=8192, count=20)
                .reused(times=3)
                .build())
print(report.summary()); print(code)
```
Full method list, `advice()`, `explain()` and `analyze_only()`: [dsl](references/dsl.md).

## Custom rules and composition

Subclass `OptimizationCase` and define `name`, `priority`, `matches`, `apply` and `explain`. Combine cases with `&` (and), `|` (or) and `~` (not):

```python
join_strategy = ((~SkewJoinCase() & BroadcastJoinCase()) | SkewJoinCase()) & AdaptiveCase()
```
Worked example with a test: [examples/custom_case.py](examples/custom_case.py) · [custom_cases](references/custom_cases.md).

## Gotchas

- Without `join_cols` or `right_table_bytes`, the CLI skips `broadcast_join`: the library's BroadcastJoinCase would otherwise judge the main table's own size.
- Without `partition.count` (for example `--plan` only), the CLI skips `repartition`: the count would default to 0 and always look undersized.
- `--case <builtin>` restricts the run to the named built-ins; `--case <complex>` adds an opt-in case; both can be mixed.
- Profile values beat plan-derived values. Leave `lineage.depth` out if the plan should decide.
- The thresholds (200 MB broadcast, >3 skew, >10M rows, depth 10/20) are sparkopt heuristics, not Spark limits.
- Run the tests after changing the library: `python -m pytest "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt/tests"` (pyspark not needed).
