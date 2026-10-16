# Shuffles and Exchanges: what `Exchange` means and how sparkopt counts it

## What an Exchange is

An `Exchange` node is a **stage boundary**. Every row of the child is written to local shuffle files,
partitioned by some rule, and read back over the network by the tasks of the next stage. It is the most
expensive thing in most plans: disk I/O on both sides, network transfer, serialization, and often spill.

| Plan text | Produced by | Note |
|---|---|---|
| `Exchange hashpartitioning(k#2L, 200), ENSURE_REQUIREMENTS` | `groupBy`, equi-join (SMJ/SHJ), window `partitionBy` | inserted by the planner; `200` = `spark.sql.shuffle.partitions` |
| `Exchange hashpartitioning(k#2L, 64), REPARTITION_BY_NUM` | `repartition(64, "k")` | user-requested; AQE keeps the count |
| `Exchange hashpartitioning(k#2L, 200), REPARTITION_BY_COL` | `repartition("k")` | user-requested; AQE may coalesce |
| `Exchange RoundRobinPartitioning(64), REPARTITION_BY_NUM` | `repartition(64)` | even spread, no key |
| `Exchange rangepartitioning(v#3 DESC NULLS LAST, 200)` | `orderBy` / `sort` (global) | samples the data first (extra job) |
| `Exchange SinglePartition` | global aggregate without keys, `limit` + sort, window without `partitionBy` | **everything goes to one task** |
| `BroadcastExchange HashedRelationBroadcastMode(...)` | broadcast hash join | collect to driver + ship to all executors; not a shuffle |
| `ReusedExchange [..], Exchange hashpartitioning(...)` | the same exchange appears twice (self-join, union of same subtree) | shuffle done once, read twice (still counted by sparkopt) |

A typical aggregation shows the two halves around the shuffle:

```
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[k#2L], functions=[sum(v#3)])            <- final agg, stage 2
   +- Exchange hashpartitioning(k#2L, 200), ENSURE_REQUIREMENTS, [plan_id=35]
      +- HashAggregate(keys=[k#2L], functions=[partial_sum(v#3)])  <- map-side partial agg, stage 1
         +- Project [(id#0L % 10) AS k#2L, (cast(id#0L as double) / 7.0) AS v#3]
            +- Range (0, 1000, step=1, splits=2)
```

The number of shuffles is the number of stage boundaries minus one per job. The Spark UI's "Shuffle
Read/Write" columns tell you their cost.

## What sparkopt extracts

```python
_EXCHANGE_RE = re.compile(r'\bExchange\b', re.I)
features.exchange_count = len(_EXCHANGE_RE.findall(plan))   # occurrences in the == Physical Plan == section
```

The CLI and `DataFrameAnalyzer.analyze()` copy `exchange_count` into
`lineage.wide_transform_count` (unless the profile sets it).

**Which cases read it:** among the built-in cases, none. Only the opt-in `staged_materialization` case
(`depth > 8 or wide_transform_count >= 3`) uses it. The `PlanFeatures.is_complex` (`> 3`) and
`lineage_break_needed` (`> 5`) properties use it too, but they are only printed, not consumed by a case.

## Limits of the count (measured, Spark 3.5.3)

The count is a plain word match over the physical section (the whole text if there is no
`== Physical Plan ==` header), so it still depends on **how the plan was captured**.
For one `groupBy().agg()` (exactly one real shuffle):

| Capture | `exchange_count` | Why |
|---|---|---|
| `df.explain()` (simple), before running | 1 | correct; use this for `--plan` |
| `queryExecution().toString()` (full) | 1 | only the physical section is counted |
| `df.explain(mode="formatted")` | **2** | operator appears in the tree **and** in its `(n) Exchange` detail block, both inside the physical section |
| simple, **after** an action (AQE `isFinalPlan=true`) | **2** | the physical section holds `== Final Plan ==` **and** `== Initial Plan ==` |
| full, on a **cached** DataFrame | 1 | the logical plans' `InMemoryRelation` copies are skipped (original sparkopt counted them and reported 2) |

Other blind spots:

- `BroadcastExchange` is **not** counted (no word boundary inside the name), so broadcast cost is
  invisible to this signal.
- `ReusedExchange` **is** counted: its line reads
  `ReusedExchange [k#18L, sum#26], Exchange hashpartitioning(k#2L, 200), ...` and the embedded
  `Exchange` matches. A self-join of one aggregated DataFrame (one real shuffle, reused) measured
  `exchange_count = 4` after the action: final plan Exchange + ReusedExchange + two Initial Plan Exchanges
  (all four are in the AQE physical section).
- Exchanges inside a cached subtree (under `InMemoryRelation`) are counted even though they will not
  run again once the cache is materialized.
- Exchanges inside scalar subqueries are counted (they do run, as a separate job).
- `Exchange SinglePartition` counts the same as a 2000-way hash shuffle, although it is far more
  dangerous: it funnels the data into one task.
- A 5-way left-deep sort-merge join chain gives `exchange_count = 6`, which is above the
  `lineage_break_needed` threshold of 5. That doesn't mean the lineage must be broken; it just means
  there are six shuffles.

**Rule of thumb:** capture with `df.explain()` (simple) **before** the action, or count only the
`== Final Plan ==` section. Then `exchange_count` is the number of shuffles.

## Signal to case

| Plan signal | Interpretation | sparkopt case | Skill |
|---|---|---|---|
| `Exchange hashpartitioning` under a `HashAggregate` with high-cardinality keys | big map-side shuffle | `partial_agg` (row_count > 1M), `repartition_before_agg` | spark-aggregation-optimization |
| `Exchange` + `Sort` on both sides of `SortMergeJoin` | shuffle join | `broadcast_join` if a side is small, `bucket_join` if repeated | spark-join-optimization |
| Several `Exchange`s on the same key back to back | redundant shuffles | `repartition` by that key once / bucketing | spark-io-and-partitioning |
| `exchange_count >= 3` in one pipeline | many stage boundaries | `staged_materialization` (opt-in `--case`) | spark-complex-workloads |
| `Exchange SinglePartition` under a `Window` | window without `partitionBy` | `window_opt` | spark-aggregation-optimization |
| `Exchange rangepartitioning` for an `orderBy` before a write | global sort | drop it, or `sortWithinPartitions` | spark-io-and-partitioning |
| Many shuffle partitions of a few KB (Spark UI) | 200 partitions for small data | `adaptive_qe` (`parallelismFirst=false`), `coalesce` | spark-io-and-partitioning |
| One shuffle partition much larger than others | key skew | `skew_join` (AQE skew join first) | spark-join-optimization |

## Quick checks

```python
plan = df._jdf.queryExecution().executedPlan().toString()     # physical only, current state
shuffles = [l.strip() for l in plan.splitlines() if l.strip().lstrip("+-: ").startswith("Exchange")]
single = [l for l in shuffles if "SinglePartition" in l]
print(len(shuffles), "shuffles;", len(single), "single-partition")
```

After the action, the Spark UI's SQL tab shows each Exchange with "shuffle bytes written", "records
written" and the partition sizes AQE used. That is the ground truth that the text heuristic only
approximates.
