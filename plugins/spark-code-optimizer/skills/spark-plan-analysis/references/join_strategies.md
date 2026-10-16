# Join strategies in the plan: BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin, nested loops

## What the plan shows

Logical plans only say `Join Inner, (a#1 = b#5)`; the **physical** plan names the strategy Spark
picked. Each strategy has a recognizable shape:

```
# Broadcast hash join: no Exchange on either side; the small side goes through BroadcastExchange
+- BroadcastHashJoin [k#1L], [k#9L], Inner, BuildRight, false
   :- FileScan parquet [id#0L,k#1L,v#2] ...
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]),false), [plan_id=41]
      +- FileScan parquet [k#9L,sid#8L] ...

# Sort-merge join: an Exchange hashpartitioning + Sort on BOTH sides
+- SortMergeJoin [id#0L], [id#12L], Inner
   :- Sort [id#0L ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=55]
   :     +- Filter isnotnull(id#0L)
   :        +- FileScan parquet [...]
   +- Sort [id#12L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(id#12L, 200), ENSURE_REQUIREMENTS, [plan_id=56]
         +- ...

# Shuffled hash join: Exchange on both sides, no Sort; builds a hash map per partition
+- ShuffledHashJoin [k#1L], [k#9L], Inner, BuildRight

# Broadcast nested loop join: non-equi or cross join with a broadcastable side
+- BroadcastNestedLoopJoin BuildRight, Inner, (v#2 > cast(id#8L as double))

# Cartesian product: cross join with no broadcastable side (prints CartesianProduct)
+- CartesianProduct
```

Things to read off a join node:

| Text | Meaning |
|---|---|
| `BuildRight` / `BuildLeft` | which side is hashed / broadcast (must be the small one) |
| `Inner`, `LeftOuter`, `LeftSemi`, `LeftAnti`, `FullOuter` | join type; broadcast cannot build the preserved side of an outer join (no BuildLeft for LeftOuter) |
| `isnotnull(k)` filters under the join | Spark adds them for inner equi-joins: null keys are dropped early |
| `ReusedExchange` on one side | self-join where the shuffle of one side is reused (no extra shuffle) |
| `Exchange ... (k, 200)` on both sides | sort-merge/shuffled hash, shuffle partition count 200 |

## Strategy selection (Spark 3.x)

1. Hints win: `broadcast` > `merge` > `shuffle_hash` > `shuffle_replicate_nl`
   (`F.broadcast(df)`, `df.hint("broadcast")`, `/*+ BROADCAST(t) */`).
2. Equi-join with a side whose estimated size is below `spark.sql.autoBroadcastJoinThreshold` (10 MB
   default): BroadcastHashJoin.
3. Otherwise SortMergeJoin (`spark.sql.join.preferSortMergeJoin=true`), or ShuffledHashJoin when that
   flag is false and one side is much smaller.
4. Non-equi join: BroadcastNestedLoopJoin if one side is broadcastable, else CartesianProduct (cross) or
   BNLJ on the smaller side.
5. AQE (3.2+) can turn a planned SortMergeJoin into a BroadcastHashJoin at runtime
   (`spark.sql.adaptive.autoBroadcastJoinThreshold`). `explain()` before the action shows only the
   initial plan.

Broadcast tables are collected to the driver and shipped to every executor. Spark refuses relations of
8 GB or more, and in practice anything above a few hundred MB pressures driver and executor memory.

## What sparkopt's LineageInspector extracts

```python
_SMJ_RE   = re.compile(r'\bSortMergeJoin\b', re.I)
_BHJ_RE   = re.compile(r'\bBroadcastHashJoin\b|\bBroadcastNestedLoopJoin\b', re.I)
_SHJ_RE   = re.compile(r'\bShuffledHashJoin\b', re.I)
_CROSS_RE = re.compile(r'\bBroadcastNestedLoopJoin\b|\bCartesianProduct\b', re.I)
# searched in the == Physical Plan == section only
# join_types: "broadcast" / "sort_merge" / "shuffled_hash" (presence only, not counts)
```

Limits of the heuristic:

- **Presence, not count.** Five sort-merge joins and one look identical (`join_types=["sort_merge"]`).
  Count `SortMergeJoin` lines yourself when it matters.
- A BNLJ sets both `has_broadcast_join` (so `join_types` contains `broadcast`) and `has_cross_join`;
  `CartesianProduct` sets `has_cross_join`. Original sparkopt's `\bBroadcastNestedLoop\b` and
  `\bCartesian\b` never matched these Spark 3.5 names; the bundled copy fixes it.
- `has_cross_join` is also true for a legitimate non-equi join (range join, `a.ts BETWEEN b.start AND
  b.end`), which runs as BNLJ. It does not mean "someone forgot the join key".
- No side/size information: the inspector does not know which side is built, or how big it is.
- AQE final-plan text includes the initial plan, so a runtime SMJ to BHJ switch reports **both**
  `broadcast` and `sort_merge`.
- **Nothing consumes these fields.** No sparkopt case reads `join_types`, `has_*_join` or
  `has_cross_join`; the CLI only prints them in the `plan` section. The join cases are driven by profile
  values (`right_table_bytes`, `partition.skew_ratio`, `row_count`). Map the signals by hand.

## Signal to case

| Plan signal | Check | sparkopt case | Skill |
|---|---|---|---|
| `SortMergeJoin` with one side small after filters | real size of that side (Spark UI, `df.count()` x row width) | `broadcast_join` (profile `right_table_bytes` <= 200 MB, a sparkopt ceiling) | spark-join-optimization |
| `SortMergeJoin`, one task much slower in the join stage | shuffle read skew in the Spark UI | `skew_join` (profile `partition.skew_ratio > 3`); AQE skew join first, salting fallback | spark-join-optimization |
| `SortMergeJoin` on the same key in many jobs, both sides large | repeated `Exchange hashpartitioning(key)` on the same tables | `bucket_join` (row_count > 10M and not small) | spark-join-optimization |
| `SortMergeJoin` on two large tables, no skew | expected | `sort_merge_join` (`is_large`) | spark-join-optimization |
| `BroadcastHashJoin` with a huge build side | broadcast size / driver OOM | remove the hint, lower the threshold | spark-join-optimization |
| `BroadcastNestedLoopJoin` / `CartesianProduct` | is it intended? | none; add an equi-join key, or bucket the range (binning) | n/a |
| `BroadcastHashJoin` on a small table reused by several joins | same broadcast built repeatedly | `cached_broadcast` (opt-in: `is_small` and reuse >= 3) | spark-complex-workloads |

## Quick checks

```python
plan = df._jdf.queryExecution().executedPlan().toString()
smj = plan.count("SortMergeJoin")
bhj = plan.count("BroadcastHashJoin")
bnlj = plan.count("BroadcastNestedLoopJoin")
cart = plan.count("CartesianProduct")
```
