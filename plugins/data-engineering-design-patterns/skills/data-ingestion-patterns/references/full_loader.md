# Full Loader

Category: data loading. One-liner: every run takes the **complete** dataset and replaces the previous copy.

## Problem
Building the Silver layer, a transformation needs device attributes from an external provider. The dataset
changes only a few times a week, stays under ~1M rows, and **has no attribute that tells which rows changed
since the last ingestion** (no `updated_at`, no version, no CDC). Without a delta column you cannot
load "what's new"; you must take everything. The same shape shows up when bootstrapping a database or
generating a *reference dataset* (lookup tables, dimension snapshots, country/currency lists).

## Solution / Mechanics
The lack of a last-updated value makes the Full Loader the natural answer. Two flavours:
- **EL (extract-load, "passthrough job")** for homogeneous stores: use the stores' native export/import
  commands, no transformation, e.g. `aws s3 sync s3://input-bucket s3://output-bucket --delete`
  (`--delete` removes objects absent from the source, so the target mirrors it).
- **ETL** for heterogeneous stores: a thin transformation adapts formats between extract and load, usually in a
  data processing framework with native connectors (Spark JDBC/JSON readers, Delta writer).

Step by step (the safe version):
1. Extract the whole dataset with an explicit schema (no inference drift between runs).
2. Validate: non-empty, plausible size versus the currently exposed version.
3. Load into a location consumers do not read yet (new table version, staging table, Delta transaction).
4. Expose atomically: a table-format commit, or a switch of the *single data exposition abstraction*.
5. Keep N previous versions for rollback; clean older ones.

```
          run N (devices_20240102_1)                         consumers
 provider ──extract──► COPY into new table ──validate──┐        │
                                                       ▼        ▼
     devices_20240101_1  ◄── view "devices" ──(CREATE OR REPLACE VIEW)──► devices_20240102_1
     (kept for rollback)        single data exposition abstraction
```

With Delta Lake/Iceberg/BigQuery the "switch" is built in: an overwrite is one transaction, readers see the old
snapshot until commit, and time travel gives the rollback.

## Consequences & trade-offs
- **Data volume**. Compute needs follow the full dataset size, not the change size. Slowly growing
  data is fine on static infrastructure; a dataset that doubles overnight makes the job slower or fails on fixed
  hardware. Mitigate with auto-scaling of the processing layer.
- **Data consistency**. A *drop-and-insert* exposes a window where consumers read partial or no
  data, and a crash in between leaves the table empty. Mitigations: transactions; if the store has none, a
  single data exposition abstraction (view) over hidden technical tables (see Proxy).
- **Rollback**. Overwriting destroys the previous version. You need it when the provider ships a
  broken export: use a format with time travel (Delta, Iceberg, BigQuery) or keep versioned tables yourself.
- Every run re-transfers unchanged rows: cost and source load are proportional to size × frequency.

## Key design decisions
| Decision | Options | Guidance |
|---|---|---|
| Exposure mechanism | table-format overwrite · view switch · RENAME swap in a transaction | Table format first; view switch for DBs without versioning |
| Version identifier | logical date · run id · logical date + try number | Immutable and unique per attempt, so retries never reuse a half-loaded table |
| Validation | min rows · max shrink vs current · schema check | Fail before exposure; a full overwrite can replace good data with an empty export |
| Rollback depth | time travel retention · N versioned tables | At least the time it takes to detect a bad provider export |
| Schema | explicit schema · inference | Explicit; `overwriteSchema=false` so schema change is a deliberate migration |
| Frequency | per provider change cadence | No point loading hourly what changes weekly |

## When to use / when not
| Situation | Use Full Loader? | Alternative |
|---|---|---|
| Small/medium dataset, no change-tracking column | Yes | - |
| Reference/dimension snapshot, DB bootstrap | Yes | - |
| Source updates and deletes rows freely, no soft deletes | Yes (deletes come for free) | CDC if latency matters |
| Large, continuously growing, append-mostly data | No | Incremental Loader |
| Need every intermediate change or seconds-level latency | No | Change Data Capture |
| Exact copy of prod data in another environment | Partly | Passthrough Replicator (preserves metadata) |
| Copy must remove PII | No | Transformation Replicator |

## Common mistakes / troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| Dashboards empty for minutes every night | `TRUNCATE` + `INSERT` (or JDBC `mode("overwrite")`) on the live table | Load a versioned/staging table and switch the view, or use a table format |
| Table empty after a failed run | Drop happened, insert crashed, no transaction | Never drop the exposed table before the new one is validated |
| Yesterday's good data overwritten by an empty file | No pre-publish validation | Min-rows and max-shrink guard (`check_publishable`) before the write |
| Job time doubled, then OOM | Dataset growth on fixed cluster size | Auto-scaling; or switch to Incremental Loader if a delta column appears |
| `CREATE OR REPLACE VIEW` fails | New table has different columns/types than the view | Create versioned tables `LIKE` a template; schema changes need a migration |
| Rollback impossible | Overwrite without versions; VACUUM removed old files | Keep N versions; VACUUM retention ≥ rollback window |
| Database storage keeps growing | Every run leaves a `devices_<version>` table behind | Retention task dropping all but the last N versions (never the exposed one) |
| S3 target flattened or stale objects kept | `aws s3 sync` without `--delete` or with wrong prefixes | Mirror the same prefix on both sides and use `--delete` |

## Implementation notes per stack

### Spark + Delta Lake
`examples/full_loader/spark/full_loader_delta.py` loads a JSON export with an explicit schema, with guards and rollback:
```python
input_data = spark.read.schema(args.schema).json(args.input)
input_data = input_data.cache()
new_count = input_data.count()
check_publishable(new_count, current_count(spark, args.table_path), args.min_rows, args.max_shrink)
(input_data.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "false").save(args.table_path))
# rollback: RESTORE TABLE delta.`{path}` TO VERSION AS OF {n}
```
The overwrite is one Delta commit, so readers never see an empty table.

### Airflow + PostgreSQL (no native versioning)
`examples/full_loader/airflow/full_loader_versioned_view_dag.py` implements the versioned-table + view switch:
```python
hook.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true)", path)
...
hook.run(f"CREATE OR REPLACE VIEW {VIEW} AS SELECT * FROM {table}")
```
Tasks: retrieve → create `devices_<ds_nodash>_<try>` (`LIKE devices_template`) → COPY → validate against the
currently exposed view → switch view → drop all but the last 3 versions.

### Kafka
- `examples/full_loader/kafka/full_loader_production.json`: JDBC source connector with `"mode": "bulk"`,
  which re-publishes the whole table every `poll.interval.ms`. Consumers must treat each poll as a new
  snapshot (or use a compacted topic keyed by primary key so the latest snapshot per key wins).
- `examples/full_loader/kafka/full_loader_initial_full_load.py`: one-shot bootstrap of a topic from a table,
  keyed by primary key, `acks="all"`. Typical first step before switching to CDC for ongoing changes.

## Related patterns
- **Incremental Loader** - the alternative once the dataset is large and has a delta column or time partitions.
- **Change Data Capture** - captures deletes and every change with low latency; often bootstrapped by a full load.
- **Passthrough Replicator** - same EL mechanics, but the goal is an exact copy with metadata preserved.
- **Proxy** - the single data exposition abstraction (view) used for the atomic switch.
- **Readiness Marker** - tells consumers the new full snapshot is complete when there is no atomic switch.
