---
name: spark-aggregation-optimization
description: "Optimize PySpark aggregations and window functions with the sparkopt aggregation cases: partial_agg, approx_distinct, window_opt and repartition_before_agg. Use when a groupBy/agg or window stage is slow, spills, OOMs, has straggler tasks, when countDistinct dominates runtime, or when explain() shows extra Exchange nodes around HashAggregate/Window."
---

# Spark Aggregation Optimization

Diagnoses and rewrites aggregation and window workloads in PySpark. The rules come from the sparkopt
library (`cases/__init__.py`, aggregation cases; manual section 5.2). Several of the library's
generated snippets work against Spark's own optimizations. The versions here are corrected, and every
correction is marked **Correction**.

All row thresholds below are **sparkopt heuristics**, not Spark rules.

## When to Use

- A `groupBy().agg()` stage shuffles a lot of data or spills.
- `countDistinct` / `COUNT(DISTINCT ...)` is the slowest part of a job, or several distinct counts
  run in one query (plan shows `Expand`).
- Window functions OOM, spill heavily, or log
  `WARN WindowExec: No Partition Defined for Window operation!`.
- Code calls `repartition(key)` right before `groupBy(key)` and you need to know if it helps.
- One aggregation task runs much longer than the others (hot group key).

## Decision Table

| Symptom / plan signal | sparkopt case | Reference |
|---|---|---|
| `Exchange` directly under the *first* `HashAggregate` (no `partial_` functions before the shuffle), or a Python/pandas UDAF | `partial_agg` | [references/partial_agg.md](references/partial_agg.md) |
| `Expand` + several `HashAggregate` levels; `count(distinct ...)` dominates | `approx_distinct` | [references/approx_distinct.md](references/approx_distinct.md) |
| `Window` node with `SinglePartition` exchange, huge window partitions, spill in `WindowExec` | `window_opt` | [references/window_opt.md](references/window_opt.md) |
| `repartition` + `groupBy` on the same keys gives 2 shuffles; or one hot group key stalls a task | `repartition_before_agg` | [references/repartition_before_agg.md](references/repartition_before_agg.md) |

## Cases

### partial_agg (priority HIGH)
- **Trigger**: `profile.row_count > 1_000_000`.
- **Generates**: a hint with a plain `groupBy` (no configs), and a comment not to repartition first:
  ```python
  result = df.groupBy("group_col").agg(F.sum("value").alias("total"), F.count("*").alias("cnt"))
  ```
  Spark *already* does map-side partial aggregation for built-in aggregates
  (`HashAggregate(partial_sum)` > `Exchange` > `HashAggregate(sum)`), so the hint is a reminder, not a
  change. Original sparkopt emitted the fake `spark.sql.aggregate.fastHashMap.enabled`, the default
  `useObjectHashAggregateExec=true` and a `repartition("group_col")` before the `groupBy` that defeats
  partial aggregation; the bundled copy fixes all three.
- **Gotchas**: partial aggregation is lost with pandas grouped-agg UDFs, `applyInPandas`, and
  `rdd.groupByKey`. Prefer built-in functions, and `reduceByKey`/`aggregateByKey` for RDDs.

### approx_distinct (priority MEDIUM)
- **Trigger**: `profile.row_count > 5_000_000`.
- **Generates**: `F.approx_count_distinct("user_id", rsd=0.02)`, described as "~2% error, 100x faster".
- **Correction**: Spark's default is `rsd=0.05` (5% relative standard deviation). The 2% only holds
  because the snippet passes `rsd=0.02`, which costs ~8x more sketch memory per group than the default.
  "100x faster" is a library claim, not a measured guarantee. `rsd` is a standard deviation, not a
  maximum error.
  ```python
  df.groupBy("country").agg(F.approx_count_distinct("user_id", rsd=0.02).alias("approx_users"))
  ```
- **Gotchas**: only use it when an approximate answer is acceptable (dashboards, not billing). Approx
  counts can't be summed across groups. For re-aggregatable sketches use `hll_sketch_agg` /
  `hll_union_agg` (Spark 3.5+).

### window_opt (priority HIGH)
- **Trigger**: `profile.row_count > 500_000`.
- **Generates**: a bounded `rangeBetween(-7*86400, 0)` frame over `unix_timestamp("ts")` instead of
  an `unboundedPreceding` frame.
- **Correction**: `WindowExec` buffers each *window partition* (all rows of one `partitionBy` key)
  whatever the frame is, and it spills past a threshold. A running frame
  (`unboundedPreceding` > `currentRow`) is computed incrementally and is cheap. The real OOM risks are
  a missing `partitionBy` (all data in one task), skewed partition keys, and whole-partition frames
  with heavy aggregates (`collect_list`). The 7-day `rangeBetween` changes the result. Use it only
  when a rolling window is what the business logic needs.
- **Gotchas**: `rangeBetween` needs one numeric/date `orderBy` expression. Reuse a single
  `WindowSpec` so several columns share one sort. For "latest row per key", `groupBy` + `max_by`
  avoids the window entirely.

### repartition_before_agg (priority MEDIUM)
- **Trigger**: `profile.partition.is_skewed` (`skew_ratio > 3.0`) **or** `profile.row_count > 10_000_000`.
- **Generates**: `df.repartition(defaultParallelism * 2, *group_keys).groupBy(*group_keys).agg(...)`.
- **Correction**: `groupBy` already hash-partitions by its keys. A repartition on the same keys right
  before it adds a full shuffle of raw rows and removes partial aggregation. It also *cannot* fix key
  skew, because a hot key still lands in one partition. Use instead:
  - plain `groupBy` + AQE coalescing (`spark.sql.adaptive.coalescePartitions.enabled`);
  - for a hot group key, a **two-phase salted aggregation** (examples/repartition_before_agg.py);
  - `repartition(n, keys)` only when the frame is reused by several aggregations/windows on those keys.

## Run sparkopt

```bash
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --case partial_agg --case approx_distinct --case window_opt --case repartition_before_agg --advice
# plan-aware, machine-readable:
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --plan plan.txt \
  --case window_opt --format json
```

`--case <builtin>` restricts the run to the named built-in cases (a complex case name adds an opt-in
case). Capture `plan.txt` with plain `df.explain()` (simple mode); `explain("formatted")` is for
reading only, because the inspector double-counts its operators. The aggregation conditions use
`row_count` and `partition.skew_ratio` from the profile (schema:
[sparkopt-toolkit/references/profile_json.md](../sparkopt-toolkit/references/profile_json.md)). These
conditions fire on almost any large table, so treat a match as "review this", not "apply this". The
plan signals in the decision table decide. Each matched case in the output carries a
`reviewed_example` path (relative to the plugin root, pointing at `examples/<case>.py` here) next to
the generated `code` sketch; prefer the reviewed example.

## Examples

- [examples/partial_agg.py](examples/partial_agg.py): plain groupBy keeps `partial_` aggregation; repartition kills it
- [examples/approx_distinct.py](examples/approx_distinct.py): exact vs approx, rsd default vs 0.02
- [examples/window_opt.py](examples/window_opt.py): running vs bounded frames, shared WindowSpec, max_by
- [examples/repartition_before_agg.py](examples/repartition_before_agg.py): two-phase salted aggregation for hot keys

Run any of them locally with `python examples/<case>.py`. They need `pyspark>=3.3`.

## Anti-Patterns

- `repartition(key)` immediately before `groupBy(key)`: two shuffles, no map-side combine.
- `Window.orderBy(...)` with no `partitionBy`. Every row goes to a single task.
- Several `countDistinct` columns in one `agg` over billions of rows when approximate is fine.
- `collect_list` / `collect_set` over unbounded window frames on large partitions.
- Using `approx_count_distinct` for financial or compliance numbers.
- Setting made-up `spark.sql.*` keys. Spark accepts unknown `spark.sql.*` keys without error, so a
  typo or invented config silently does nothing.
- `rdd.groupByKey().mapValues(sum)` instead of `reduceByKey`.
