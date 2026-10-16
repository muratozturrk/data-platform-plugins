---
name: spark-plan-analysis
description: "Read Spark query plans (df.explain(), explain(\"formatted\"), queryExecution) and map plan signals to sparkopt optimization cases. Covers what sparkopt's LineageInspector extracts (depth, Exchange count, join types, InMemoryRelation, streaming, reuse, window/agg counts, is_complex, lineage_break_needed), its measured blind spots, and how to capture a plan file for sparkopt_cli.py --plan."
---

# Spark Plan Analysis

`df.explain()` shows what Spark will actually run. sparkopt's `LineageInspector`
(`scripts/sparkopt/core/lineage_inspector.py`) turns that text into a handful of numbers with regexes,
and `sparkopt_cli.py --plan` feeds some of them into the rule engine. This skill teaches you to read
the plan yourself, explains exactly what the inspector extracts (and gets wrong), and maps each signal
to the sparkopt case it should trigger.

Every limit below was **measured on Spark 3.5.3** by running the inspector on real plan text. All
thresholds (depth 15/20, 3/5 exchanges, 5000 chars) are **sparkopt heuristics, not Spark rules**.

## When to Use

- You have `explain()` output and need to know what is expensive (shuffles, join strategy, caches).
- You want to run `sparkopt_cli.py --plan plan.txt` and must decide how to capture the plan.
- sparkopt's `plan` section (depth, exchange_count, is_complex...) looks wrong or surprising.
- You need to decide which sparkopt case (cache, checkpoint, broadcast, repartition...) a plan calls for.

## Plan Formats

| Capture | Contains | sparkopt suitability |
|---|---|---|
| `df.explain()` (= `mode="simple"`) | `== Physical Plan ==` only | **use this for `--plan`**: counts match the operators that run, and it works for streaming. `collect_profile.py` captures this |
| `df.explain(mode="extended")` / `df._jdf.queryExecution().toString()` | parsed + analyzed + optimized logical + physical | works: counts come from the `== Physical Plan ==` section only, and streaming is detected anywhere in the text. For a streaming DataFrame `toString()` holds an `AnalysisException` instead of the physical plan, so only `has_streaming` is useful. `collect_profile.py` falls back to it when `explain()` fails |
| `df.explain(mode="formatted")` | operator tree + numbered detail blocks | for humans only: the detail blocks sit inside the physical section, so every operator is still counted twice, and `FileScan` prints as `Scan parquet`, so reuse cannot be detected |
| `df.explain(mode="cost")` | optimized logical plan with `Statistics(sizeInBytes=...)` | use it to check broadcast size estimates (not for sparkopt) |
| after an action (AQE) | `isFinalPlan=true`, with `== Final Plan ==` **and** `== Initial Plan ==` | shows runtime decisions, but Exchanges and joins appear twice |

Read bottom-up: leaves (`FileScan`, `Range`, `InMemoryTableScan`) are where data enters; each `Exchange`
starts a new stage; `*(n)` marks whole-stage-codegen stage n.

## Decision Table: Signal to Case

| Plan signal | Inspector field | Consumed by sparkopt? | Case it should trigger | Reference |
|---|---|---|---|---|
| `Exchange hashpartitioning` / `rangepartitioning` / `SinglePartition` | `exchange_count` | yes, as `wide_transform_count`, but only opt-in `staged_materialization` (`>= 3`) reads it | `staged_materialization`, `partial_agg`, `repartition` | `references/shuffles_and_exchanges.md` |
| `SortMergeJoin` with a small side | `join_types` contains `sort_merge` | no | `broadcast_join` (profile `right_table_bytes`) | `references/join_strategies.md` |
| `SortMergeJoin` + one slow task | `has_sort_merge_join` | no | `skew_join` (AQE first, salting fallback) | `references/join_strategies.md` |
| `BroadcastNestedLoopJoin` / `CartesianProduct` | `has_cross_join` (BNLJ also sets `has_broadcast_join`) | no | fix the join condition / binning | `references/join_strategies.md` |
| `InMemoryRelation` / `InMemoryTableScan` | `has_in_memory_relation` | no | **suppress** new cache advice; check `unpersist()` | `references/caching_signals.md` |
| same `FileScan [cols]` twice | `estimated_reuse` | yes, as `estimated_reuse_count` when > 1 | `fanout_cache`, `persistence_strategy`, `storage_level` | `references/caching_signals.md` |
| `StreamingRelation(V2)`, `MicroBatch*`, `StreamingDataSourceV2*` | `has_streaming` | yes, as `has_streaming_source` | `hybrid_checkpoint` | `references/caching_signals.md` |
| deep tree | `depth` | yes, as `lineage.depth` | `persistence_strategy` (>10), `lineage_break` (>20) | `references/plan_complexity.md` |
| `Window` nodes | `window_count` | no | `window_opt` | `references/plan_complexity.md` |
| `HashAggregate` / `ObjectHashAggregate` pairs | `agg_count` | no | `partial_agg`, `approx_distinct`, `repartition_before_agg` | `references/plan_complexity.md` |
| text size / subqueries / depth / shuffles | `is_complex`, `lineage_break_needed` | no (printed only) | `lineage_break` / `staged_materialization`, by hand | `references/plan_complexity.md` |

Precedence: values in `--profile` always win. The plan only fills `depth`, `has_streaming_source`,
`wide_transform_count` and (if > 1) `estimated_reuse_count` when the profile leaves them unset.

## What the Inspector Gets Wrong (measured)

- **Depth = leading spaces // 3.** Lines under a left child start with `:`, so left-deep join chains
  collapse: 5 chained SMJs have a real depth of 13 and report **5**. (Original sparkopt also counted the
  logical plans of a `toString()` capture, 36 instead of 5 for a 12-step loop; the bundled copy reads the
  physical section only.)
- **Reuse is not the path.** The regex captures the `[column list]` after `FileScan <fmt>`, not the
  `Location`. Two separate `spark.read` calls of one path get new attribute ids (no match); reuse across
  separate actions (the real fan-out case) is never visible in one plan; formatted captures have no
  `FileScan`. Set `lineage.estimated_reuse_count` yourself.
- **Exchange count doubles** with `formatted` (tree + detail blocks) and with AQE final plans (final and
  initial plan both sit in the physical section), and `ReusedExchange` lines count too (the line repeats
  `Exchange hashpartitioning(...)`). `BroadcastExchange` is not counted.
- **Streaming** is detected anywhere in the text (`StreamingRelation`, `StreamingRelationV2`,
  `MicroBatch*`, `StreamingDataSourceV2*`), so `df.explain()` and `toString()` captures both work.
  Original sparkopt missed `StreamingRelationV2` in `toString()` captures; the bundled copy fixes it.
- **Operator names follow Spark 3.5:** `BroadcastNestedLoopJoin` sets `has_broadcast_join` and
  `has_cross_join`, `CartesianProduct` sets `has_cross_join`, `InMemoryTableScan` sets
  `has_in_memory_relation`, and `ObjectHashAggregate` (`collect_list`...) counts as an aggregate.
  Original sparkopt's regexes missed all four; the bundled copy fixes them.
- **Over-counting:** one scalar subquery yields `subquery_count = 4` (`Subquery` + `subquery#id`), which
  is already `is_complex`. `re.I` also counts columns named `window`/`filter`.
- **Presence, not count:** `join_types` lists strategies, not how many joins there are.
- `is_iterative` is never detected from a plan; set `has_iterative_loop` in the profile.

## Capture and Run

```python
# inside the job / notebook (see examples/capture_plan.py)
import io
from contextlib import redirect_stdout
buf = io.StringIO()
with redirect_stdout(buf):
    df.explain()                       # simple mode: use this for --plan
open("orders.plan.txt", "w", encoding="utf-8").write(buf.getvalue())
```

```bash
# Plan only (no Spark needed): plan section + cases that match the plan-derived lineage
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --plan orders.plan.txt --format json

# Plan + profile (profile values win), with the PersistenceAdvisor decision
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile orders.profile.json \
  --plan orders.plan.txt --advice

# Opt in to the shuffle-driven complex case
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --plan orders.plan.txt \
  --case staged_materialization
```

`${CLAUDE_PLUGIN_ROOT}/scripts/collect_profile.py` writes both `<name>.profile.json` and `<name>.plan.txt`
(a `df.explain()` capture; `queryExecution().toString()` only when `explain()` fails), so streaming is
detected in collected profiles too.

## Workflow

1. Capture `df.explain()` **before** the action, and the SQL-tab plan **after** it if AQE matters.
2. Run the CLI with `--plan`; read its `plan` section.
3. Sanity-check each number against the limits above (count `SortMergeJoin`/`Exchange` lines yourself).
4. Put corrected values (`lineage.depth`, `estimated_reuse_count`, `has_streaming_source`,
   `has_iterative_loop`) into the profile JSON and rerun. Profile values win.
5. Apply the cases from the decision table, using the case-specific skills
   (spark-join-optimization, spark-aggregation-optimization, spark-persistence-strategy,
   spark-io-and-partitioning, spark-complex-workloads).

## Anti-patterns

- Trusting `depth` from a left-deep join chain or `is_complex` from a single subquery.
- Feeding `explain(mode="formatted")` to the CLI and reading `exchange_count` as "number of shuffles".
- Capturing a streaming plan with `queryExecution().toString()`: `has_streaming` is found, but the
  physical section is an `AnalysisException`, so the other counts are meaningless.
- Adding `cache()` because sparkopt said so while the plan already shows `InMemoryRelation`.
- Judging the join strategy from `explain()` before execution when AQE is on (runtime SMJ to BHJ switch).
- Reading logical-plan `Join` nodes as the strategy; only the physical plan names it.
- Calling `df.explain()` in a production loop: planning a deep plan is driver work every time.
