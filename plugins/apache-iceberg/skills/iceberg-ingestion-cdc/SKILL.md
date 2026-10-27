---
name: iceberg-ingestion-cdc
description: "Design and debug ingestion into Apache Iceberg tables: delivery guarantees (exactly-once vs at-least-once), idempotent consumers, DLQ and schema drift, batch vs micro-batch vs streaming, the write path (append, overwritePartitions, MERGE INTO, copy-on-write vs merge-on-read, position/equality deletes, distribution-mode, commit retries) and Kafka + Spark Structured Streaming CDC with Debezium envelopes. Use when duplicates appear after a streaming restart, foreachBatch replays, CommitFailedException or ValidationException show up, MERGE fails on multiple matching source rows, a batch rerun double-loads data, small files pile up from frequent commits, or you must choose between Spark, Flink, Kafka Connect, Redpanda, Tableflow, Fivetran or Airbyte."
---

# Iceberg Ingestion and CDC

Ingestion design starts from requirements (freshness, throughput, delivery guarantee, schema change,
team capacity), not from a tool. Iceberg is an analytical sink: every engine commits in bounded
units, so "streaming" into Iceberg is controlled micro-batching, and commits faster than about 5
seconds are aggressive. Every write is one atomic snapshot published by a compare-and-swap on the
catalog's metadata pointer; concurrent writers are reconciled by optimistic retries plus
validation, not locks. Exactly-once needs a replayable source, tracked offsets and an idempotent
atomic commit together.

## When to Use This Skill

- Building Kafka -> Iceberg pipelines with Spark Structured Streaming (native sink or `foreachBatch`).
- Duplicates after a restart/replay, or proving a pipeline is exactly-once.
- Applying CDC (Debezium `before`/`after`/`op`/`lsn`, tombstones) with `MERGE INTO`.
- Choosing batch vs micro-batch (`availableNow`) vs streaming (`processingTime`, Flink) and commit intervals.
- Choosing copy-on-write vs merge-on-read, write distribution mode, fanout writers.
- Multiple writers on one table: `CommitFailedException`, `ValidationException`, lost updates.
- Handling malformed records (fail-fast, `try_cast`, dead-letter table) and source schema drift.
- Picking a tool per source (Spark, Flink, Kafka Connect, NiFi, Fivetran, Airbyte, Qlik, Redpanda,
  Tableflow, Glue/ADF/Dataflow/Firehose) or running a requirements review.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| No duplicates after a crash/replay | Native Iceberg streaming sink, or idempotent `MERGE` in `foreachBatch` | [ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) |
| Bad records must not stop the stream | Hybrid: fail-fast keys, `try_cast` optionals, DLQ table keyed by Kafka coordinates | [ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) |
| New source fields silently missing | Explicit drift check + `ALTER TABLE ADD COLUMNS` as STRING | [ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) |
| Batch rerun must be idempotent | Pin source `snapshot-id` + `overwritePartitions()` | [ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) |
| Upserts, SCD, corrections | `MERGE INTO`, source reduced to one row per key | [iceberg_write_path.md](references/iceberg_write_path.md) |
| Frequent small row changes | Merge-on-read (`write.merge/update/delete.mode`) + compaction | [iceberg_write_path.md](references/iceberg_write_path.md) |
| Too many files per partition | `distribution-mode=hash` | [iceberg_write_path.md](references/iceberg_write_path.md) |
| Concurrent writers fail or lose data | Retries >= writer concurrency; validated, disjoint overwrites | [iceberg_write_path.md](references/iceberg_write_path.md) |
| Keep an Iceberg copy of an OLTP table in sync | Debezium -> Kafka -> latest-per-key -> LSN-guarded MERGE on MOR table | [streaming_cdc.md](references/streaming_cdc.md) |
| Choose tools / model / commit interval | Requirements-first rules and evaluator | [ingestion_tool_selection.md](references/ingestion_tool_selection.md) |

## Delivery guarantees and idempotency

- **Mechanic**: Spark writes `offsets/N`, the sink commits, Spark writes `commits/N`. A crash between
  the sink commit and `commits/N` re-runs batch N with the same offsets.
- The native sink (`writeStream.format("iceberg")`) stores `spark.sql.streaming.queryId`/`epochId` in
  the snapshot summary and skips an epoch it already committed: after a forced replay, 1,500 rows stay
  1,500. A plain `foreachBatch` + `append()` doubles the replayed 1,000 rows; a `MERGE ... WHEN NOT
  MATCHED THEN INSERT *` handler keeps 1,000.
- Non-replayable sources (webhooks) cap the guarantee at at-least-once: dedupe on a business key.
- **Gotchas**: `foreachBatch` runs in a cloned session with its own catalog instance; set
  `cache-enabled=false` on the catalog or the driver reads a stale table (0 rows). Producers should use
  `enable.idempotence=true`.
- Ref: [references/ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) · Code: [examples/ingestion_lakehouse/reliable_ingestion/](examples/ingestion_lakehouse/reliable_ingestion/)

## Bad records and schema drift

- Parse payloads as `map<string,string>`, cast to the current table schema (table = validation contract).
- Fail-fast on keys, amounts, event time; `try_cast` -> NULL for optional fields; route the rest to a DLQ
  Iceberg table written by `MERGE` on `(topic, partition, offset)` so replays add no duplicate rejects.
- A fixed `from_json` schema drops new fields with no error. Detect `map_keys` not in the table, add as
  `STRING`, widen later on purpose. Old rows read NULL; no rewrite (columns are tracked by field ID).
- Ref: [references/ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) · Code: [examples/ingestion_lakehouse/reliable_ingestion/](examples/ingestion_lakehouse/reliable_ingestion/)

## Batch, micro-batch, streaming

| Model | Spark form | Use for |
|---|---|---|
| Batch | `spark.read.option("snapshot-id", s)...writeTo(t).overwritePartitions()` | Backfills, daily loads, reconciliations |
| Micro-batch | `trigger(availableNow=True)` run by a scheduler | Polled sources, minutes of freshness |
| Streaming | `trigger(processingTime="10 seconds")` or Flink | Seconds of freshness, stateful logic |

- Rerun with `append` doubles a day (1,600 for 800); `overwritePartitions` stays at 800. Unpinned rerun
  after 50 late rows loads 850, pinned rerun 800.
- `maxOffsetsPerTrigger` is a soft cap split across Kafka partitions: 2,000 records, cap 1,000 -> 3
  batches; cap 100 -> 21 snapshots and 84 files (23.8 rows/file) vs 12 files (166.7 rows/file).
- Idle `processingTime` triggers commit nothing; 2 s trigger gave 1.9-2.6 s produce-to-visible.
- Ref: [references/ingestion_models_and_reliability.md](references/ingestion_models_and_reliability.md) · Code: [examples/ingestion_lakehouse/ingestion_models/](examples/ingestion_lakehouse/ingestion_models/)

## The write path: COW, MOR, MERGE, distribution

- Snapshot operations: append -> `append`; overwrite / COW MERGE / MOR MERGE -> `overwrite`; whole-file
  delete -> `delete`. A job failing midway (row 5,000 of 10,000) leaves snapshots and rows unchanged.
- COW MERGE reports `deleted-data-files`; MOR MERGE reports `added-position-deletes` and leaves base files.
  Spark writes position deletes; Flink writes equality deletes (cheap to write, costly to read/compact).
- `MERGE` allows at most one source row per target row: reduce to the latest change per key first.
- `distribution-mode=hash`: 15 files for 15 partitions; `none`: 60 (4 tasks x 15), with or without
  `fanout-enabled`. Hash costs a shuffle.
- Ref: [references/iceberg_write_path.md](references/iceberg_write_path.md) · Code: [examples/ingestion_lakehouse/iceberg_writes/](examples/ingestion_lakehouse/iceberg_writes/)

## Commit protocol and concurrent writers

- A lost compare-and-swap raises `CommitFailedException` internally; the client refreshes, re-applies,
  validates, retries. Stale appenders are simply rebased.
- 8 writers racing: `commit.retry.num-retries=0` -> 1 commits, 7 fail; default 4 -> 0-3 failures per run;
  20 -> all 8 commit. Size retries to real writer concurrency.
- An overwrite without `validateFromSnapshot(base)` + `validateNoConflictingData()` silently deletes a file
  another writer just appended to the same partition. With validation: `ValidationException`; disjoint
  partitions still succeed.
- Ref: [references/iceberg_write_path.md](references/iceberg_write_path.md) · Code: [examples/ingestion_lakehouse/iceberg_writes/](examples/ingestion_lakehouse/iceberg_writes/)

## Kafka + Spark CDC

- Kafka key = primary key (per-key order); store the source `_lsn` in the table; MOR table bucketed on the key.
- Per micro-batch: drop tombstones -> `row_number()` latest per id by LSN -> one MERGE with
  `WHEN MATCHED AND s._lsn > t._lsn` guards, `DELETE` for `op='d'`, `INSERT` only when `op <> 'd'`.
- 1,737 records (237 tombstones) in 6 micro-batches converge to 159 live source rows; replays and late
  older events change nothing; 4 MERGE snapshots, 0 data-file rewrites, 12 position delete files.
- **Gotcha**: the LSN guard cannot protect a hard-deleted key from an older late event; use soft deletes.
- Ref: [references/streaming_cdc.md](references/streaming_cdc.md) · Code: [examples/ingestion_lakehouse/kafka_spark_cdc/](examples/ingestion_lakehouse/kafka_spark_cdc/)

## Tools and the requirements evaluator

- Spark: batch, MERGE, compaction. Flink: event time, low latency, CDC at scale. Kafka-native (Redpanda,
  Tableflow, Kafka Connect sink): topic -> table without a job; Tableflow is append-only. Fivetran/Airbyte:
  small teams, SaaS. NiFi 2.x has no Iceberg processor.
- Freshness rules: < 5 s -> OLTP/stream state; <= 60 s streaming; <= 1 h micro-batch; else batch.
- `evaluate(Requirements(...))` returns model, commit interval, tools, write pattern, guarantee,
  guardrails and warnings; pure Python, runs in CI.
- Ref: [references/ingestion_tool_selection.md](references/ingestion_tool_selection.md) · Code: [examples/ingestion_lakehouse/requirements_evaluator/](examples/ingestion_lakehouse/requirements_evaluator/)

## Best Practices

1. Write the requirements down first: peak and sustained throughput, freshness per use case, guarantee,
   schema-change rate, who operates it.
2. Prefer the native Iceberg sink; when `foreachBatch` is needed, make the batch function idempotent.
3. Keep checkpoints on durable storage, one directory per query; never share one between queries.
4. Bound every batch input (snapshot ID, file list, offsets) and record it with the run.
5. Commit no more often than freshness requires; pair every streaming table with compaction and snapshot
   expiration (delete-file compaction for MOR/CDC).
6. Give concurrent writers disjoint scopes, validated overwrites and retries sized to their count.
7. Emit metrics per batch: records, rejects, new fields, retries, commit status, lag.
8. Use snapshot summaries and metadata tables (`snapshots`, `files`) to verify write behavior instead
   of scanning data.
