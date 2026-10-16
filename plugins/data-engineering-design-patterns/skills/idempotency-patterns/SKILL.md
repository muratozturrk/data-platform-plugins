---
name: idempotency-patterns
description: "Idempotency design patterns: Fast Metadata Cleaner, Data Overwrite, Merger, Stateful Merger, Keyed Idempotency, Transactional Writer and Proxy, with Spark, Kafka and Airflow examples. Use when retries, replays or backfills create duplicates or partial data, when choosing between TRUNCATE/DROP, INSERT OVERWRITE, replaceWhere, MERGE/UPSERT, deterministic keys, transactions or write-once tables behind a view, or when making a pipeline safe to re-run."
---

# Idempotency Design Patterns

Error-management patterns make jobs retry automatically, and every retry or backfill can replay writes that already succeeded. Duplicates are the *best* case, because they can at least be removed. The worst case is output whose duplicates cannot even be recognised. An idempotent pipeline produces the same output no matter how many times it runs on the same input, like `abs(abs(-1)) == abs(-1)`. When duplicates cannot be avoided (for example, a non-transactional broker), it at least makes them identifiable. The patterns are grouped by what you can do to the dataset:
- **overwrite** it whole (Fast Metadata Cleaner, Data Overwrite);
- **update** it from increments (Merger, Stateful Merger);
- rely on **database** guarantees (Keyed Idempotency, Transactional Writer);
- keep it **immutable** (Proxy).

## When to Use This Skill
- A retried Airflow task or Spark job wrote rows twice, or left partial output visible.
- Backfills append duplicates, or show consumers data that never existed at that point in time.
- A `DELETE`-then-`INSERT` idempotency step keeps getting slower as the table grows.
- You must choose between TRUNCATE/DROP, `INSERT OVERWRITE`, Delta `replaceWhere`, dynamic partition overwrite, `MERGE`, `ON CONFLICT`, Kafka transactions or a view swap.
- You are designing deterministic keys for sessions, CDC rows or Kafka records.
- Legal or audit requires keeping every dataset version while consumers see only the latest.
- You are reviewing whether a pipeline is "safe to re-run".

## Pattern Selection Guide
| Problem / symptom | Pattern | Reference |
|---|---|---|
| Full or partitioned reload; DELETE is too slow; store supports TRUNCATE/DROP | Fast Metadata Cleaner | [references/fast_metadata_cleaner.md](references/fast_metadata_cleaner.md) |
| Full or partition reload on an object store or lakehouse; no metadata ops | Data Overwrite | [references/data_overwrite.md](references/data_overwrite.md) |
| Only increments (CDC, changed rows) are available; stable identity key | Merger | [references/merger.md](references/merger.md) |
| Merger works, but backfills must restore the table to its past state first | Stateful Merger | [references/stateful_merger.md](references/stateful_merger.md) |
| Key-value or keyed store; streaming retries; sessions or IDs must be stable | Keyed Idempotency | [references/keyed_idempotency.md](references/keyed_idempotency.md) |
| Consumers must never see partial output (spot nodes, multi-table writes) | Transactional Writer | [references/transactional_writer.md](references/transactional_writer.md) |
| Every version must be kept (write-once); expose only the latest | Proxy | [references/proxy.md](references/proxy.md) |

Rules of thumb:
- Full data available → overwrite family (metadata ops first, data ops otherwise).
- Increments only → Merger, plus Stateful Merger if backfill consistency matters.
- No orchestrator (streaming) → database family.
- Cannot mutate → Proxy.

## Patterns

### Fast Metadata Cleaner
- **What**: split the dataset into per-unit tables or partitions (for example weekly) behind a union view, and clean a unit with `TRUNCATE` or `DROP` + `CREATE`. These are metadata operations with no row scan.
- **When**: large, growing tables where DELETE-based idempotency degrades, on stores that have metadata operations (warehouse, RDBMS, lakehouse).
- **Mechanic**:
  1. Route on the execution date (Monday or Jan 1 starts a new unit).
  2. Create or truncate the unit table.
  3. Refresh the view.
  4. Load.
- **Gotchas**:
  - The backfill granularity equals the unit, so fixing one day means replaying from the week's table-creation step.
  - Partition and table quotas (BigQuery 4,000 partitions, Redshift 200k tables) call for a freezing step.
  - Remove a table from the view before you DROP it.
- Examples: Airflow + PostgreSQL weekly tables (TRUNCATE or DROP flow), Spark + Delta weekly tables behind a view, Kafka weekly topics behind a regex subscription.
- Links: [reference](references/fast_metadata_cleaner.md) · [examples](examples/fast_metadata_cleaner/)

### Data Overwrite
- **What**: replace the dataset or partition through the data layer:
  - Spark `mode("overwrite")`;
  - Delta `replaceWhere`;
  - `partitionOverwriteMode=dynamic`;
  - `INSERT OVERWRITE`;
  - `bq load --replace`.
- **When**: no metadata layer exists (object stores), or the full partition is recomputed.
- **Mechanic**: scope the overwrite to the affected partitions. Table formats commit it atomically as a new log version.
- **Gotchas**:
  - A bare `mode=overwrite` drops *all* partitions.
  - A dynamic overwrite of an empty DataFrame wipes data, so guard `count() > 0`.
  - A save mode on raw files is not transactional.
  - Old blocks stay on disk until VACUUM.
- Links: [reference](references/data_overwrite.md) · [examples](examples/data_overwrite/)

### Merger
- **What**: `MERGE`/UPSERT increments into the target on an immutable unique key.
- **When**: CDC or incremental feeds where full replacement is impossible.
- **Mechanic**:
  - not matched → insert;
  - matched → update;
  - deletes only as soft deletes (`is_deleted`).
  - Guard INSERT with `is_deleted = false`, so the first run never inserts rows that are already deleted.
- **Gotchas**:
  - A non-unique or mutable key creates duplicates on replay.
  - Dedup the increment per key before MERGE.
  - A backfill starts from the *latest* state, so consumers temporarily see resurrected rows.
- Examples: Spark + Delta CDC MERGE with soft deletes (main), Kafka CDC topic into a PostgreSQL 15 MERGE (offsets committed after the DB commit), Airflow single SQL task (`CREATE TEMPORARY TABLE ... LIKE`, `COPY`, `MERGE`).
- Links: [reference](references/merger.md) · [examples](examples/merger/)

### Stateful Merger
- **What**: a Merger plus a `versions` state table (execution_time → table version), so a backfill restores the table before merging.
- **When**: the dataset is incremental, on versioned storage (Delta, Iceberg), and consumers demand point-in-time consistency during backfills.
- **Mechanic**:
  - no previous version → TRUNCATE;
  - previous version < last MERGE version → `RESTORE` to `current_execution_version - 1`;
  - after the MERGE, upsert the new version.
- **Gotchas**:
  - VACUUM retention bounds how far back you can backfill.
  - Compaction and OPTIMIZE create versions, hence the `-1` rule.
  - Serialise runs. Without versioning, rebuild from a history table instead.
- Examples: Spark + Delta `versions` table with restore/merge/update-state steps (main), Airflow DAG running those three steps, Kafka time slice into a PostgreSQL history table for stores without versioning.
- Links: [reference](references/stateful_merger.md) · [examples](examples/stateful_merger/)

### Keyed Idempotency
- **What**: derive the same key for the same logical record on every run, so that a key-based store overwrites instead of duplicating.
- **When**: streaming jobs, key-value stores (Cassandra, ScyllaDB, HBase, Redis), file or partition naming.
- **Mechanic**: build keys from **immutable** attributes only:
  - broker append time (`LogAppendTime`) or ingestion time, not event time (late data shifts "first event");
  - `sha256(concat_ws('|', ...))` of natural IDs;
  - output file, partition or table names derived only from the logical date.
- **Gotchas**:
  - An RDBMS errors on a duplicate PK, so use MERGE or `ON CONFLICT`.
  - Kafka dedups only through asynchronous compaction.
  - Compaction or retention deleting the source event changes the key.
  - Never put run IDs, `now()` or Python's salted `hash()` in keys.
- Examples: Spark `applyInPandasWithState` sessions keyed on first append time into ScyllaDB/Cassandra, Kafka sessions to a compacted topic, Airflow date-named files plus `ON CONFLICT` upserts.
- Links: [reference](references/keyed_idempotency.md) · [examples](examples/keyed_idempotency/)

### Transactional Writer
- **What**: begin → write (private) → commit or rollback. Readers see all of the output or none of it.
- **When**: preemptible clusters, multi-table or multi-topic outputs, consumers that read right after the write.
- **Mechanic**:
  - a job-level transaction: one Delta commit per job (plus `txnAppId`/`txnVersion` so a retried job is skipped), SQL `BEGIN ... COMMIT` around several MERGEs, Kafka `init/begin/commit/abort_transaction`;
  - or a local, task-level transaction, which is weaker under retries.
- **Gotchas**:
  - Idempotency is limited to one transaction: backfills and job restarts write again.
  - Readers need `read_committed`.
  - Spark has no transactional Kafka sink (use Flink or native clients).
  - kafka-python lacks transactions.
  - The Flink `transaction.timeout.ms` must exceed the checkpoint duration.
  - Delta has no multi-table transactions; use a warehouse or RDBMS transaction for those.
- Links: [reference](references/transactional_writer.md) · [examples](examples/transactional_writer/)

### Proxy
- **What**: each run writes a new write-once table or prefix (run-start-time suffix, revoked grants, WORM locks). A view, alias or manifest points to the latest.
- **When**: immutability or legal retention of every version, with a single stable entry point for consumers.
- **Mechanic**:
  1. Load into `devices_internal_<run_start>`.
  2. Revoke writes.
  3. `CREATE OR REPLACE VIEW`.
  - On Delta, Iceberg or BigQuery, native table versions can play this role.
- **Gotchas**:
  - Immutability needs enforcement in the infrastructure (object locks, grants), not only in the DAG.
  - Without views, manifests make reads cumbersome.
  - A loader role that can DROP breaks the guarantee.
- Examples: Airflow + PostgreSQL view over write-once tables, Spark view/manifest/versioned modes, Kafka snapshot topics behind a compacted pointer topic.
- Links: [reference](references/proxy.md) · [examples](examples/proxy/)

## Cross-cutting Best Practices
- Decide the **idempotency granularity** (row key, partition, table, run) up front. It is also your backfill granularity.
- Prefer metadata operations to data operations, and data operations to application logic: TRUNCATE/DROP > overwrite > MERGE > custom dedup.
- Keys and table names come from immutable inputs (natural IDs, append time, logical date). Proxy is the exception: it deliberately uses the run start time.
- Guard destructive writes: empty-output check, dry-run, `overwriteSchema=false`, and validate before commit or repoint.
- Serialise runs that share a target (`max_active_runs=1`). Fire downstream triggers only after a verified commit.
- Mind time-travel and VACUUM retention. It bounds both rollback and Stateful Merger backfills.
- Transactions and producer idempotence cover a single attempt. Combine them with key- or overwrite-based idempotency to make re-runs safe.
- Keep secrets out of code (`os.environ[...]`). The examples read `PG_PASSWORD`.
