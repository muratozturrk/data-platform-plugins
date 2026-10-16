---
name: spark-io-and-partitioning
description: "Tune PySpark read/write I/O and partition counts with sparkopt's I/O and partition cases: parquet_pushdown, coalesce_write, delta_zorder, repartition, coalesce, adaptive_qe. Use when a Spark job reads too much data, writes thousands of small files, runs with idle executors, has too many tiny tasks, or needs AQE settings."
---

# Spark I/O and Partitioning

This skill covers the six sparkopt cases that control **how much data Spark reads**, **how many files
it writes**, and **how many partitions (tasks) each stage gets**. It explains each sparkopt rule (the
exact `matches()` condition from `cases/__init__.py`), fixes the generated snippets where they are wrong
or naive, and gives the `explain()` signal that confirms the problem.

Thresholds such as "fewer than 8 partitions", "more than 2000 partitions averaging under 10k rows" or
"10 GB is large" are **sparkopt heuristics, not Spark rules**. Treat them as prompts to look at the plan
and the Spark UI, and don't apply them blindly.

## When to Use

- A scan reads every column or every file even though the query needs a few (no `PushedFilters`,
  wide `ReadSchema`, no `PartitionFilters`).
- An output directory holds thousands of files of a few KB to a few MB each.
- A Delta table is large, queried by selective filters on a non-partition column, and scans most files.
- The Spark UI shows a stage with 1-7 tasks while dozens of cores sit idle.
- A stage has thousands of tasks that each finish in milliseconds (scheduling overhead dominates).
- You are on Spark 3.x and unsure whether AQE is on and tuned.

## Decision Table

| Symptom / plan signal | sparkopt case | Reference |
|---|---|---|
| `FileScan parquet` with `PushedFilters: []` or a `ReadSchema` far wider than the `select` | `parquet_pushdown` | `references/parquet_pushdown.md` |
| Output has many small files; write stage has more than 2000 tasks of under 10k rows | `coalesce_write` | `references/coalesce_write.md` |
| Delta table of 10 GB or more (sparkopt estimate), selective filters, `numFilesSkipped` low | `delta_zorder` | `references/delta_zorder.md` |
| `df.rdd.getNumPartitions() < 8`, a stage with fewer tasks than cores | `repartition` | `references/repartition.md` |
| More than 2000 partitions averaging under 10k rows, task time shorter than scheduling time | `coalesce` | `references/coalesce.md` |
| `AdaptiveSparkPlan isFinalPlan=false` missing, `spark.sql.shuffle.partitions` hand-tuned per job | `adaptive_qe` | `references/adaptive_qe.md` |

`coalesce_write` and `coalesce` share the same trigger (`partition.is_oversized`), so sparkopt emits both
together. Apply one: `coalesce` for the in-pipeline DataFrame, `coalesce_write` right before `.write`.

## The Cases

### parquet_pushdown (HIGH, always matches)
- **Trigger:** `matches()` returns `True` for every profile.
- **Generates:** configs `spark.sql.parquet.filterPushdown=true` and
  `spark.sql.parquet.columnarReaderBatchSize=4096` (both are already Spark's defaults, so they are a no-op
  unless someone turned them off), plus a `select(...).filter(...)` hint.
- **Corrected sketch:**
  ```python
  df = (spark.read.parquet(path)
        .select("event_date", "user_id", "amount")          # column pruning
        .filter(F.col("event_date") >= F.lit("2024-01-01").cast("date")))  # pushed filter
  df.explain()  # look for PushedFilters / PartitionFilters / ReadSchema
  ```
- **Gotchas:** operation order does not matter (Catalyst pushes the filter down anyway). What blocks
  pushdown is a Python UDF, a function wrapped around the column (`F.year(c) == 2024`, `F.lower(c)`),
  or a cast on the column side. Filters on partition columns appear as `PartitionFilters`, not
  `PushedFilters`.

### coalesce_write (MEDIUM)
- **Trigger:** `profile.partition.is_oversized`, i.e. `count > 2000 and avg_rows < 10_000`.
- **Generates:** `df.coalesce(100).write.parquet("output/path/")` (100 is hard-coded).
- **Corrected sketch:** size the file count from data volume (about 128 MB to 1 GB per file) and use
  `repartition` when the upstream stage is expensive, because `coalesce` lowers the parallelism of the
  whole upstream stage:
  ```python
  n_files = max(1, math.ceil(est_bytes / (256 * 1024**2)))
  df.repartition(n_files).write.mode("overwrite").parquet(out)
  ```
- **Gotchas:** with `partitionBy("dt")` the file count is tasks x distinct `dt`. Use
  `repartition("dt")` (optionally with `maxRecordsPerFile`) so that each task writes one directory.

### delta_zorder (LOW)
- **Trigger:** `profile.is_large`, i.e. `estimated_bytes >= 10 GiB`. sparkopt estimates the bytes as rows
  x fixed per-type widths (string = 32 B), so this is a rough figure.
- **Generates:** a note with `DeltaTable.forPath(...).optimize().executeZOrderBy("event_date", "user_id")`.
  The default columns are placeholders.
- **Corrected sketch:**
  ```python
  DeltaTable.forPath(spark, path).optimize().where("dt >= '2024-01-01'").executeZOrderBy("user_id")
  ```
- **Gotchas:** you **cannot Z-order by a partition column**. Delta raises an error, and `event_date`,
  the library default, is often the partition column. Pick 1-4 high-cardinality filter columns that have
  statistics (the first 32 columns by default). On Delta 3.1+ / Databricks, prefer liquid clustering
  (`CLUSTER BY`). The library's "80%+ data skipping" claim is not guaranteed; it depends on the data and
  the queries.

### repartition (HIGH)
- **Trigger:** `profile.partition.is_undersized`, i.e. `count < 8`. Data size is ignored.
- **Generates:** `df.repartition(max(count * 4, spark_parallelism * 2))`. `spark_parallelism` defaults
  to 200 in `CodegenContext`, so the result is usually **400**, even for a 50 MB DataFrame.
- **Corrected sketch:**
  ```python
  cores = spark.sparkContext.defaultParallelism
  target = max(cores * 2, math.ceil(est_bytes / (128 * 1024**2)))
  df = df.repartition(target, "group_key")   # or repartition(target) for round-robin
  ```
- **Gotchas:** `repartition` is a full shuffle (`Exchange RoundRobinPartitioning(n)` or
  `Exchange hashpartitioning(key, n)`). A small DataFrame with one partition is fine and needs no fix.
  A gzip/CSV source that cannot be split is the usual cause of a single partition.

### coalesce (MEDIUM)
- **Trigger:** `profile.partition.is_oversized` (`count > 2000 and avg_rows < 10_000`).
- **Generates:** `df = df.coalesce(200)`.
- **Corrected sketch:** fix the source of the partitions first. For many small input files, tune
  `spark.sql.files.maxPartitionBytes` and `spark.sql.files.openCostInBytes`. For shuffles, let AQE
  coalesce. Use `coalesce(n)` only as a last, cheap step.
- **Gotchas:** `coalesce` never increases partitions, merges partitions unevenly (by locality), and pulls
  the reduced parallelism up into the parent stage. No `Exchange` appears in the plan; you see
  `Coalesce 200`.

### adaptive_qe (CRITICAL, always matches)
- **Trigger:** `matches()` returns `True`.
- **Generates:** `spark.sql.adaptive.enabled`, `.coalescePartitions.enabled`, `.skewJoin.enabled` = true,
  `advisoryPartitionSizeInBytes` = 128m.
- **Corrected sketch:**
  ```python
  spark.conf.set("spark.sql.adaptive.enabled", "true")                     # default since 3.2
  spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")  # honour advisory size
  spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
  ```
- **Gotchas:** the three enable flags are already `true` by default on Spark 3.2+. In 3.2+,
  `coalescePartitions.parallelismFirst=true` (the default) **ignores** `advisoryPartitionSizeInBytes` when
  coalescing, so the library's 128m setting has no effect there unless you set `parallelismFirst=false`.
  AQE needs an `Exchange` to act on, does nothing for streaming queries, and does not coalesce an explicit
  `repartition(n)`. Prefer AQE skew handling over manual salting.

## Run sparkopt

All six are **built-in** cases: they are evaluated on every run unless you narrow the run. `--case <builtin>`
(repeatable) restricts evaluation to the named built-ins, `--case <complex>` adds an opt-in complex case, and
the two can be mixed. Use `--exclude` (repeatable) to drop the ones you don't want.

```bash
# Minimal profile that exercises the partition rules
cat > profile.json <<'EOF'
{"row_count": 12000000, "estimated_bytes": 21474836480,
 "partition": {"count": 4, "avg_rows": 3000000}, "spark_parallelism": 64}
EOF
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json

# Not on Delta: drop the Z-order note; JSON for tooling
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --exclude delta_zorder --exclude coalesce_write --format json

# Only the partition-count rules
python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --profile profile.json \
  --case repartition --case coalesce --case coalesce_write

python "${CLAUDE_PLUGIN_ROOT}/scripts/sparkopt_cli.py" --list-cases   # names + builtin/opt-in
```

Without `partition.count` (for example a `--plan`-only run) the CLI excludes `repartition`, because the
unknown count defaults to 0 and would always look undersized; `coalesce*` never fire either, and a
`--plan`-only run has no size, so neither does `delta_zorder`. (Original sparkopt has no such guard;
the bundled CLI adds it.) Ignore partition advice unless the
profile has a real `partition` block. Each matched case in the output carries a `reviewed_example` path
(relative to the plugin root, pointing at `examples/<case>.py` here) next to the generated `code` sketch.
The profile fields that drive these cases are `partition.count`, `partition.avg_rows`, `estimated_bytes`
and `spark_parallelism` (the repartition target). On a live job,
`${CLAUDE_PLUGIN_ROOT}/scripts/collect_profile.py` writes them for you. Otherwise take them from
`df.rdd.getNumPartitions()`, `df.count()` and the Spark UI "Input Size" rather than guessing. Review the
generated snippets against the corrections above before pasting them.

## Examples

`examples/<case>.py`: one runnable PySpark 3.3+ script per case, each with a `__main__` demo that uses
local data under a temp directory.

## Anti-patterns

- `df.coalesce(1).write...` on a large DataFrame: the whole upstream stage runs on one core.
- `repartition(400)` on a tiny DataFrame because "fewer than 8 partitions" fired.
- Setting `spark.sql.shuffle.partitions` per query by hand while AQE is on; set a high initial value and
  let AQE coalesce.
- Z-ordering by the partition column, or by more than 4 columns.
- Reading Parquet and filtering inside a Python UDF, which disables pushdown.
- `partitionBy` on a high-cardinality column (one directory per user_id).
- Calling `df.rdd.getNumPartitions()` or `count()` inside loops just to "check": each call is a job.
