---
name: pyspark-standards
description: PySpark coding standard based on the Palantir PySpark Style Guide: column expressions, naming of DataFrames and expressions, chaining, schemas, UDF avoidance, joins, performance and testing. Use when writing or reviewing Spark jobs or transform plugins.
---

# PySpark Standards

Baseline: Palantir PySpark Style Guide (adopted), plus Python standards.

## Core Rules
| # | Rule |
|---|---|
| 1 | `from pyspark.sql import functions as F, types as T`; column refs with `F.col("name")` (or bare strings in Spark 3+), never `df.name` / `df["name"]` across DataFrames |
| 2 | Name every intermediate DataFrame by its state (`orders_raw`, `orders_deduped`, `orders_enriched`); never `df1/df2`; never reassign `df` repeatedly across a long method |
| 3 | Break complex expressions into named boolean/column variables (`is_delivered = F.col(...) == "Delivered"`) before use in `when`/`filter` |
| 4 | Method chaining in parentheses, one method per line; `select` explicit columns early to narrow; `withColumn` for ≤ 3 columns else a single `select` with expressions |
| 5 | Explicit schemas on reads of semi-structured data (`T.StructType`); never infer in production; cast at the boundary (staging), not downstream |
| 6 | No Python UDFs when a built-in or `pandas_udf` exists; UDFs isolated in one module with unit tests and documented performance cost |
| 7 | Joins: specify `how`, join keys by name list to avoid duplicate columns, broadcast small dims explicitly, deduplicate before join, assert grain after join |
| 8 | No actions (`count`, `collect`, `show`) in library code; no actions inside loops; `cache` only with a comment stating why and `unpersist` |
| 9 | Partitioning and repartition decisions commented with the reason; write with explicit `mode` and `partitionBy` |
| 10 | Configuration (paths, options) injected; no `spark.conf.set` inside transforms; SparkSession obtained once at the entry point |
| 11 | Each transform step is a pure function `DataFrame -> DataFrame` with a docstring stating input/output grain; pipelines compose steps with `transform()` |
| 12 | Tests: small in-memory DataFrames via `chispa`/`pytest-spark`; schema equality and row equality assertions; property tests for parsers |
| 13 | Logging via the platform logger with `tenant_id`, `run_id`; no `print` |

## Anti-patterns
- `df = df.withColumn(...)` repeated 20 times
- `.toPandas()` on unbounded data
- `F.udf` wrapping `datetime.strptime`
- `select("*")` after joins (duplicate column names)
- String-built SQL with f-strings and user input

Source: Palantir PySpark Style Guide (https://github.com/palantir/pyspark-style-guide); Spark docs on performance tuning; chispa (https://github.com/MrPowers/chispa).
