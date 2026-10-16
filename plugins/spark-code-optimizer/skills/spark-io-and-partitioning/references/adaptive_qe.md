# adaptive_qe: Adaptive Query Execution (Spark 3.x)

## Problem

A static plan is chosen before any data is read, from estimates that are often wrong. The consequences:

- `spark.sql.shuffle.partitions=200` is too many for small stages and too few for big ones;
- a join planned as sort-merge although one side turns out to be a few MB after filtering;
- one skewed shuffle partition makes a single task run for an hour.

AQE re-optimizes the plan **at every shuffle boundary** using real statistics of the materialized
shuffle (query stage), then plans the next stage.

## How Spark executes it

With `spark.sql.adaptive.enabled=true` the physical plan root becomes `AdaptiveSparkPlan`. Before
execution (`df.explain()` without running) you see the initial plan:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[k#1], functions=[sum(v#2)])
   +- Exchange hashpartitioning(k#1, 200), ENSURE_REQUIREMENTS, [plan_id=20]
      +- HashAggregate(keys=[k#1], functions=[partial_sum(v#2)])
         +- FileScan parquet [k#1,v#2] ...
```

After an action, the SQL tab / `explain` on the executed DataFrame shows `isFinalPlan=true` with
`ShuffleQueryStage`, `AQEShuffleRead coalesced` (3.2+; `CustomShuffleReader` in 3.0/3.1),
`AQEShuffleRead ... skewed`, or a `SortMergeJoin` that became `BroadcastHashJoin`. Final-plan text
contains **both** "Final Plan" and "Initial Plan", so operator counts appear twice (see the
spark-plan-analysis skill).

The three runtime features:

1. **Coalescing post-shuffle partitions:** merges small shuffle partitions up to a target size.
2. **Switching join strategy:** converts sort-merge to broadcast hash join when a side is small at runtime
   (`spark.sql.adaptive.autoBroadcastJoinThreshold`, 3.2+, defaults to `autoBroadcastJoinThreshold`).
3. **Skew join:** splits a partition larger than `skewedPartitionFactor` x median **and** larger than
   `skewedPartitionThresholdInBytes` into sub-partitions, replicating the other side.

## sparkopt rule

```python
class AdaptiveCase(OptimizationCase):
    name = "adaptive_qe"
    priority = Priority.CRITICAL
    def matches(self, profile): return True
    def apply(self, ctx):
        ctx.add_config("spark.sql.adaptive.enabled", "true")
        ctx.add_config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        ctx.add_config("spark.sql.adaptive.skewJoin.enabled", "true")
        ctx.add_config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
```

- **Matches:** always (CRITICAL, sorted first).
- **Generates:** the four configs above plus the same as `spark.conf.set` calls (`134217728` = 128 MiB).
- **Corrections:**
  1. AQE is **on by default since Spark 3.2**, and `coalescePartitions.enabled` and `skewJoin.enabled`
     default to `true`. On 3.2+ the three flags are no-ops; on 3.0/3.1 they are needed.
  2. In 3.2+, `spark.sql.adaptive.coalescePartitions.parallelismFirst` defaults to `true`, which makes
     coalescing **ignore `advisoryPartitionSizeInBytes`** and use only `minPartitionSize` (1 MB). Set it to
     `false` for the 128m target to apply. The advisory size is still used for skew splitting.
  3. These are runtime SQL configs, so `spark.conf.set` works (unlike `spark.serializer`).
  4. Prefer AQE skew handling before manual salting. Salting is the fallback for non-equi joins, joins
     where AQE does not split (e.g. the skewed side is the build side it cannot split, or there is no
     shuffle), and RDD code.

## Before / After

```python
# BEFORE (Spark 3.1, AQE off): hand-tuned per job
spark.conf.set("spark.sql.shuffle.partitions", "37")

# AFTER: start high, let AQE coalesce to ~128 MB partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "2000")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

## Configs (Spark 3.2-3.5 defaults)

| Config | Default | Note |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | master switch |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | merge small shuffle partitions |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | `false` = honour advisory size |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | `64MB` | target size; sparkopt uses 128m |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | none | initial shuffle partitions under AQE |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | split skewed partitions in SMJ/SHJ |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | `5` | skew = factor x median ... |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | `256MB` | ... and above this size |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | same as `autoBroadcastJoinThreshold` (10MB) | runtime SMJ to BHJ |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | avoid a re-shuffle after the BHJ switch |

## Gotchas / troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| No `AdaptiveSparkPlan` in plan | AQE off, or no exchange in the query | enable it; nothing to adapt without a shuffle |
| Streaming query not adapted | AQE does not apply to Structured Streaming | tune `shuffle.partitions` before the first run (it is fixed by the checkpoint) |
| Still 200 small partitions after a write | `parallelismFirst=true` / no shuffle before write | set `parallelismFirst=false`; see `coalesce_write.md` |
| `repartition(n)` count unchanged | `REPARTITION_BY_NUM` is respected by AQE | expected; drop the explicit n to let AQE decide |
| Skew not split | below factor/threshold, unsupported join type, or broadcast side | lower thresholds, or fall back to salting (`skew_join`) |
| `explain()` shows the "wrong" join | only the initial plan is shown before execution | check the SQL tab after the action |
| Operator counts doubled in parsed plan | final plan text includes the initial plan | count only the "Final Plan" section |

## Related cases

- `skew_join`: AQE skew join first, salting second.
- `broadcast_join`: static `F.broadcast` / `hint("broadcast")` vs AQE's runtime switch.
- `coalesce`, `coalesce_write`: manual merging that AQE often makes unnecessary.
- `repartition`: AQE cannot split a non-splittable scan; that still needs `repartition`.
