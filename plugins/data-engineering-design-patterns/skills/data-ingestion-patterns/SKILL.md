---
name: data-ingestion-patterns
description: "Data ingestion design patterns: Full Loader, Incremental Loader, Change Data Capture (CDC), Passthrough Replicator, Transformation Replicator, Compactor, Readiness Marker and External Trigger, with Spark, Kafka and Airflow examples. Use when designing or reviewing how data is loaded, replicated or synced into a lake/warehouse/topic: full refresh vs incremental/watermark loads, Debezium/WAL CDC or Delta Change Data Feed, exact prod-to-staging/dev replication, prod-to-staging replication with PII removal, small files and OPTIMIZE/VACUUM/log compaction, _SUCCESS/readiness flags and sensors, or event-driven (S3/SQS/Kafka/webhook) pipeline triggers."
---

# Data Ingestion Patterns

Ingestion is the first stage of almost every data system, and it fails in quiet ways: consumers read incomplete
datasets, deletes go missing, a lake slows down under millions of tiny files, or jobs start before the data
exists. This skill covers eight ingestion patterns. **Full** and **incremental** loads acquire all or part
of a dataset. **Replication** copies data with or without transformation (privacy). **Compaction** fixes the
small-files problem. **Readiness** and **event-driven** triggering decide *when* ingestion should start. Each
pattern has a dense reference and runnable-ish Spark / Kafka / Airflow samples.

## When to Use This Skill
- Choosing between full refresh, watermark-based incremental load and CDC for a source table.
- Building or reviewing a loader that must never expose an empty or partial table to readers.
- Designing watermark/state handling, look-back buffers, backfills, or handling hard deletes.
- Setting up Debezium/Kafka Connect, Delta Change Data Feed, or a CDC consumer into Delta Lake.
- Copying prod data to dev/staging as is, mirroring Kafka topics, or replicating with PII dropped/masked.
- Diagnosing slow queries from small files; configuring OPTIMIZE, Z-ORDER, VACUUM or Kafka log compaction.
- Coordinating producer/consumer pipelines (`_SUCCESS`, `_READY`, readiness tables, Airflow sensors).
- Replacing fixed schedules with event-driven triggers (S3 events, SQS, Kafka, webhooks, Airflow REST API).

## Pattern Selection Guide
| Problem / symptom | Pattern | Reference |
|---|---|---|
| Small or slowly changing dataset, no `updated_at`, source updates/deletes freely | Full Loader | [full_loader.md](references/full_loader.md) |
| Table is empty or partial while the reload runs | Full Loader (stage + atomic swap / view / table format) | [full_loader.md](references/full_loader.md) |
| Large, growing dataset; only new rows or partitions should be loaded | Incremental Loader | [incremental_loader.md](references/incremental_loader.md) |
| Backfill turns an incremental job into a giant full load | Incremental Loader (bounded windows) | [incremental_loader.md](references/incremental_loader.md) |
| Need hard deletes, every change, or seconds-level latency from an OLTP DB | Change Data Capture | [change_data_capture.md](references/change_data_capture.md) |
| Need an exact copy in another environment (non-replayable loader, Kafka topic mirror) | Passthrough Replicator | [passthrough_replicator.md](references/passthrough_replicator.md) |
| Prod data needed in staging for tests, but PII must not leave production | Transformation Replicator | [transformation_replicator.md](references/transformation_replicator.md) |
| Jobs spend most time listing/opening files; thousands of tiny files | Compactor | [compactor.md](references/compactor.md) |
| Topic must keep only the latest value per key | Compactor (Kafka log compaction) | [compactor.md](references/compactor.md) |
| Consumers read incomplete data because they start on a fixed time | Readiness Marker | [readiness_marker.md](references/readiness_marker.md) |
| Data arrives unpredictably; scheduled runs waste compute or read stale data | External Trigger | [external_trigger.md](references/external_trigger.md) |

## Patterns

### Full Loader
Reload the whole dataset each run: EL with native commands (`aws s3 sync --delete`, `COPY`) between homogeneous
stores, thin ETL between heterogeneous ones. Use for reference/lookup data without a change-tracking column.
Key mechanic: never drop-and-insert the exposed table. Overwrite a table format (Delta/Iceberg commit, time
travel for rollback) or load `devices_<version>` and switch a view (`CREATE OR REPLACE VIEW`).
- Gotcha: TRUNCATE + INSERT (or JDBC `mode("overwrite")`) shows readers an empty table; a crash leaves it empty.
- Gotcha: an empty or shrunken export must abort the run before the overwrite.
- Gotcha: compute grows with data; plan auto-scaling and keep previous versions for rollback.
Reference: [references/full_loader.md](references/full_loader.md) · Examples: [examples/full_loader/](examples/full_loader/)

### Incremental Loader
Load only the new part: either a **delta column** (ingestion time; bounded window from the orchestrator or a
remembered watermark) or **time partitions** resolved from the execution date. Use for large, growing datasets.
Key mechanic: bounded window `[start, end)` or partition `date={{ ds }}` → extract → overwrite/MERGE that unit
→ (watermark saved **last**). Partition-based: sensor on the next partition first.
- Gotcha: hard deletes are invisible; use soft deletes, insert-only tables or CDC.
- Gotcha: unbounded windows turn backfills into full loads; event-time delta columns miss late data.
- Gotcha: never cap a run with a row `LIMIT` before taking `max(delta)` as the new watermark.
Reference: [references/incremental_loader.md](references/incremental_loader.md) · Examples: [examples/incremental_loader/](examples/incremental_loader/)

### Change Data Capture
Stream every committed INSERT/UPDATE/DELETE from the database commit log (WAL/binlog) via Debezium + Kafka
Connect, or Delta Lake Change Data Feed (`readChangeFeed`). Use for low latency and physical deletes.
Key mechanic: envelope with `op`, `before`, `after`, `source.lsn`; consumers dedupe per key by LSN and apply MERGE/DELETE.
- Gotcha: needs DB/ops setup (logical replication, slots); a stalled consumer grows the WAL until the disk fills.
- Gotcha: may only capture changes after start; combine with a snapshot/Full Loader for history.
- Gotcha: it's data in motion now (joins may not match *yet*); unwrap metadata and handle tombstones.
Reference: [references/change_data_capture.md](references/change_data_capture.md) · Examples: [examples/change_data_capture/](examples/change_data_capture/)

### Passthrough Replicator
Copy data as is, preserving metadata (keys, order, headers, table logs). Use for prod → dev/staging when the
original loader isn't idempotent (third-party API), or for topic/bucket mirroring.
Key mechanic: simplest possible copy (S3 replication, MirrorMaker 2, file copy; else `read.text` → `write.text`), **pushed** by the data-owning environment.
- Gotcha: any interpretation (JSON parsing, casts) risks altering data; Spark doesn't keep file names/counts.
- Gotcha: copying Parquet without `_delta_log`, or Kafka records without headers/partition order, breaks the copy.
- Gotcha: push still consumes shared resources (subnet IPs, slots); PII → Transformation Replicator.
Reference: [references/passthrough_replicator.md](references/passthrough_replicator.md) · Examples: [examples/passthrough_replicator/](examples/passthrough_replicator/)

### Transformation Replicator
Replicate production data to a lower environment (realistic tests) with a transformation between read and
write that removes or alters PII. Use when synthetic data can't reproduce provider quality issues.
Key mechanic: PII contract per column → data reduction (`drop()`, `SELECT * EXCEPT`) or column/row
transformation (`expr`, hash, mask, mapping function); fail on unclassified columns; verify no leak.
- Gotcha: typed schemas over JSON/CSV silently break dates; keep untouched columns as strings.
- Gotcha: desynchronisation: new PII columns appear; drive rules from catalog/data-contract tags.
- Gotcha: more risk than passthrough; keep keys/headers/partitions on Kafka, quarantine what can't be mapped.
Reference: [references/transformation_replicator.md](references/transformation_replicator.md) · Examples: [examples/transformation_replicator/](examples/transformation_replicator/)

### Compactor
Merge many small files into fewer large ones (~128 MB-1 GB) without changing data: Delta `OPTIMIZE`
(+ `ZORDER`), Iceberg `rewrite_data_files`, Hudi MoR compaction, custom bin-packing for raw Parquet; Kafka
log compaction keeps the latest record per key.
Key mechanic: transactional rewrite commit (readers see old or new snapshot), followed by a cleaning step (`VACUUM`).
- Gotcha: cost vs freshness trade-off; schedule off-peak and only on recent/unhealthy partitions.
- Gotcha: VACUUM retention below 7 days breaks running readers and removes time travel.
- Gotcha: Kafka compaction is nondeterministic and deletes old versions, unlike file compaction.
Reference: [references/compactor.md](references/compactor.md) · Examples: [examples/compactor/](examples/compactor/)

### Readiness Marker
Signal that a dataset is complete: `_SUCCESS`/`_READY` flag file, Delta commit, DB readiness row, Kafka
readiness event, or the convention "partition N is ready when N+1 starts". Consumers pull/wait on it.
Key mechanic: marker written as the very last step, atomically (temp + rename / upsert), carrying row count, batch id and schema hash.
- Gotcha: it's a convention, not enforced; document and communicate it to consumers.
- Gotcha: late data in closed event-time partitions; keep partitions immutable or share mutability rules.
- Gotcha: Airflow sensors with `mode="reschedule"` and generous timeouts; delete the marker before reprocessing.
Reference: [references/readiness_marker.md](references/readiness_marker.md) · Examples: [examples/readiness_marker/](examples/readiness_marker/)

### External Trigger
Start pipelines from events instead of a clock: subscribe → react (filter/validate) → trigger the orchestrator
(e.g. AWS Lambda → Airflow REST API with `conf`) or a job directly. Pull pollers are valid but wasteful.
Key mechanic: push semantics, DAGs with `schedule=None` reading `dag_run.conf`, rich trigger metadata.
- Gotcha: at-least-once notifications; dedupe triggers and make jobs idempotent.
- Gotcha: a bare "ping" loses context; pass event envelope, event/processing time, trigger version.
- Gotcha: never lose events: bounded retries, DLQ, acknowledge only after the job commits.
Reference: [references/external_trigger.md](references/external_trigger.md) · Examples: [examples/external_trigger/](examples/external_trigger/)

## Cross-cutting Best Practices
- **Never expose in-progress data.** Stage then swap, transactional table formats, markers written last.
- **Validate before you publish.** Source count > 0, staging vs source counts, quarantine rate thresholds.
- **Persist progress after the write.** Watermarks, offsets, trigger status and SQS deletes come after the commit.
- **Make reruns safe.** Immutable execution-time parameters (`{{ ds }}`), bounded windows, MERGE / partition overwrite.
- **Copy, don't reinterpret.** Replication uses byte/text copies and keeps metadata (`_delta_log`, Kafka headers/order).
- **Prefer push and native features.** Infra replication, `_SUCCESS` files, Delta CDF/OPTIMIZE before custom code.
- **Watch the side effects.** Replication slots, small files, VACUUM retention, sensor slots (`reschedule`).
- **No hardcoded secrets.** Env vars / secret stores; Kafka Connect config providers (`${env:...}`).
