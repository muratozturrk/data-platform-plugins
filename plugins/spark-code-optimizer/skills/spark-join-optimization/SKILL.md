---
name: spark-join-optimization
description: "Optimize PySpark joins with the sparkopt join cases: broadcast_join, skew_join (AQE skew join first, salting as fallback), bucket_join and sort_merge_join. Use when a join is slow, shuffles too much, has straggler tasks, OOMs on broadcast, or when explain() shows SortMergeJoin/BroadcastHashJoin/Exchange decisions you want to change."
---

# Spark Join Optimization

Picks the right physical join strategy for a PySpark job and generates corrected code for it.
The rules come from the sparkopt library (`cases/__init__.py`, join cases; manual section 5.1).
The library's generated snippets contain a few mistakes. The versions here are corrected, and every
correction is marked **Correction**.

All size thresholds below are **sparkopt heuristics**, not Spark rules, unless they are named as a
Spark config.

## When to Use

- A join stage has one or a few tasks that run far longer than the rest (skew).
- `explain()` shows `SortMergeJoin` with an `Exchange` on both sides and one side is small.
- A broadcast fails with `OutOfMemoryError`, `SparkException: Cannot broadcast the table that is larger than 8GB`, or broadcast timeout.
- The same two large tables are joined on the same key over and over (bucketing candidate).
- Large x large joins where you need to reason about shuffle partition counts.

## Decision Table

| Symptom / plan signal | sparkopt case | Reference |
|---|---|---|
| One side small; plan shows `SortMergeJoin` + 2x `Exchange hashpartitioning` | `broadcast_join` | [references/broadcast_join.md](references/broadcast_join.md) |
| Straggler tasks in the join stage; Spark UI max task time >> median; spill on a few tasks | `skew_join` | [references/skew_join.md](references/skew_join.md) |
| Same large tables joined repeatedly on the same key; `Exchange` re-runs every job | `bucket_join` | [references/bucket_join.md](references/bucket_join.md) |
| Both sides >= 10 GB; `SortMergeJoin` is correct, partition count/size is the issue | `sort_merge_join` | [references/sort_merge_join.md](references/sort_merge_join.md) |

Order of preference: broadcast (no shuffle) > bucketed (no shuffle at read) > sort-merge with AQE
(skew split + coalesce) > manual salting.

## Cases

### broadcast_join (priority HIGH)
- **Trigger**: `right_table_bytes or profile.estimated_bytes <= 200 * 1024**2` (200 MB). If no right
  table size is given, sparkopt tests the *profiled* DataFrame's size, so profile the small side.
- **Generates**: a hint `large_df.join(F.broadcast(small_df), on=join_keys, how="inner")`. The CLI
  only evaluates it when the profile has `join_cols` or `right_table_bytes`.
- **Corrected sketch**:
  ```python
  result = large_df.join(F.broadcast(small_df), "customer_id", "inner")
  # or: large_df.join(small_df.hint("broadcast"), "customer_id")
  ```
- **Gotchas**: Spark's automatic limit is `spark.sql.autoBroadcastJoinThreshold` (default 10 MB).
  200 MB is sparkopt's ceiling for an explicit hint. **Correction**: the library suggests raising the
  auto threshold to 200 MB globally. Don't. Hint the specific join instead. The table is collected to
  the driver and copied to every executor, and the in-memory size is often 2-5x the Parquet size.
  Spark refuses to broadcast more than 8 GB. Can't broadcast the preserved side of an outer join
  (a left join can only broadcast the right side).

### skew_join (priority HIGH)
- **Trigger**: `profile.partition.is_skewed`, i.e. `skew_ratio = max_rows / avg_rows > 3.0` across
  partitions.
- **Generates**: random salting with `salt_buckets=10`. The left side gets `_salt = rand()*N` and the
  right side is cross-joined with `range(N)`.
- **Correction**: try AQE first (Spark 3.2+ enables AQE by default):
  ```python
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  # a partition is skewed if > skewedPartitionFactor (5) x median AND > 256MB
  spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
  ```
  Use salting only when AQE can't help: non-equi joins, a skewed side that AQE can't split (for example
  the outer side of a full outer join), a stage with no shuffle, or RDD code.
- **Salting code**: the bundled template drops the right-side key and `_salt` before joining on
  `key_s`, so there are no ambiguous columns (run on PySpark 3.5, same row count as a plain join).
  Original sparkopt kept both copies; the bundled copy fixes it. It still salts *every* key, so the
  right side grows N times: **Correction**: salt only the hot keys (examples/skew_join.py).
- **Gotchas**: `skew_ratio` is partition-row skew, not key skew, so check the key distribution
  (`groupBy(key).count().orderBy(desc)`). (Original sparkopt's `explain()` printed `salt_buckets` as the
  skew ratio; the bundled copy no longer prints profile values there.)

### bucket_join (priority MEDIUM)
- **Trigger**: `profile.row_count > 10_000_000 and not profile.is_small` (is_small = <= 200 MB).
- **Generates**: `df.write.bucketBy(200, "id").sortBy("id").saveAsTable(...)` for one table.
- **Correction**: bucket *both* tables on the join key with the same bucket count, and read them back
  with `spark.table()`. A plain `join` right after the write gains nothing.
  `spark.sql.sources.bucketing.enabled` is already `true` by default.
- **Gotchas**: only Hive/Spark catalog tables (`saveAsTable`). Plain path writes and Delta Lake don't
  support it. It pays off only when the tables are joined many times. Rewriting costs one full shuffle.

### sort_merge_join (priority LOW)
- **Trigger**: `profile.is_large`, i.e. `estimated_bytes >= 10 GB` (`large_table_threshold_bytes`).
- **Generates**: `repartition(200, key).sortWithinPartitions(key)` on both sides before the join.
- **Correction**: SMJ is already Spark's default for large x large equi-joins, and Spark adds the
  exchange and sort itself. Manual `repartition` + `sortWithinPartitions` only helps when the
  repartitioned frame is reused by several joins/aggregations on the same key. Otherwise tune
  `spark.sql.shuffle.partitions` and let AQE coalesce. Don't hardcode 200.

## Run sparkopt

```bash
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --case broadcast_join --case skew_join --case bucket_join --case sort_merge_join --advice
# with a physical plan captured from plain df.explain():
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json --plan plan.txt \
  --case skew_join --format json
```

`--case <builtin>` restricts the run to those built-in cases (a complex case name adds an opt-in case).
Capture the plan with plain `df.explain()` (simple mode) and save the output to `plan.txt`;
`explain("formatted")` is for reading only (it double-counts operators), and `mode="cost"` is for
checking size statistics by eye. Profile JSON schema:
[sparkopt-toolkit/references/profile_json.md](../sparkopt-toolkit/references/profile_json.md). The case
conditions use `row_count`, `estimated_bytes`, `partition.skew_ratio`, and `right_table_bytes` /
`join_cols` for `broadcast_join`.

Each matched case in the output has a `reviewed_example` (a path relative to the plugin root, for
example `skills/spark-join-optimization/examples/skew_join.py`) next to the generated `code` sketch.
Prefer the reviewed example; the sketch uses placeholder names.

Read sparkopt's output critically. The matches() conditions are size heuristics. A matched
`broadcast_join` and a matched `sort_merge_join` can't both apply to the same join, so pick using the
decision table and the plan.

## Examples

- [examples/broadcast_join.py](examples/broadcast_join.py): hint vs auto threshold, plan check
- [examples/skew_join.py](examples/skew_join.py): AQE skew join, then targeted salting
- [examples/bucket_join.py](examples/bucket_join.py): bucket both sides, verify no Exchange
- [examples/sort_merge_join.py](examples/sort_merge_join.py): SMJ with AQE, reuse-only pre-partition

Run any of them locally with `python examples/<case>.py`. They need `pyspark>=3.3`.

## Anti-Patterns

- Setting `spark.sql.autoBroadcastJoinThreshold` to hundreds of MB globally. Every future join
  becomes a broadcast candidate, and driver OOMs follow.
- Broadcasting a table whose size comes from stale or missing statistics (`ANALYZE TABLE` first, or
  measure it).
- Salting every key with a large N, so the right side grows N times. Salt only the hot keys.
- Salting while AQE skew join is on and already working. That does the same work twice.
- `repartition(200, key)` before every join "to help SMJ". It adds a shuffle when the plan already
  has one.
- Bucketing tables that are joined once, or bucketing on a column different from the join key.
- Writing `spark.conf.set("spark.serializer", ...)` at runtime. It only takes effect in the session
  builder / `--conf`, and it does not change DataFrame join shuffle formats (Tungsten rows).
