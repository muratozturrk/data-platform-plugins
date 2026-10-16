---
name: data-integration-patterns
description: Streaming data integration design patterns - API Gateway, Zero-ETL Synchronizer, Hybrid Source, Sidecar, Partial State Writer, Streaming Reprocessor and Batch Reprocessor - with Spark Structured Streaming, Kafka and Airflow examples. Use when ingesting events over HTTP into Kafka, syncing Kafka topics to Iceberg/Delta lakehouse tables without pipelines (Tableflow, Bufstream, Iceberg topics), backfilling a streaming job from historical data before switching to real time, combining multiple streams or writing to multiple sinks, emitting partial/early session results, or reprocessing/replaying streaming data after a bug (checkpoint rewind, offset reset, lakehouse-based reprocessing).
---

# Data Integration Patterns (Streaming)

Real-time use cases (AI, fraud, personalization) cannot wait for batch latency, so data
must flow continuously. Streaming changes the dataset as well as the clock: batch data is
**bounded**, while streaming data is **unbounded** and arrives in small increments. The
patterns in this skill cover the lakehouse-centred streaming lifecycle, using
sessionization as the running example:
**ingest** (API Gateway, Zero-ETL Synchronizer),
**generate value** (Hybrid Source, Sidecar, Partial State Writer), and
**recover from mistakes** (Streaming Reprocessor, Batch Reprocessor).
They build on earlier building blocks: CDC and replication (`data-ingestion-patterns`), fan-in/fan-out (`data-flow-patterns`),
idempotency (`idempotency-patterns`), and decorators (`data-value-patterns`).

## When to Use This Skill
- Applications must send events to Kafka without coupling to the broker or dropping data during outages.
- You want to remove "copy topic to lakehouse" jobs and replace them with a managed or broker-native sync.
- A new streaming job needs a historical backfill before it processes live data.
- One streaming job must read several sources or write several sinks consistently.
- Consumers need in-progress (partial) state before windows or sessions close.
- A buggy release produced wrong results and the data must be reprocessed, from the topic or from the lakehouse copy.
- Designing checkpoint, savepoint and offset-reset procedures for releases.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|-------------------|---------|-----------|
| Producers write directly to Kafka; broker outages drop events; malformed payloads reach consumers | API Gateway | [references/api_gateway.md](references/api_gateway.md) |
| Small team maintains jobs whose only purpose is copying topics to lakehouse tables | Zero-ETL Synchronizer | [references/zero_etl_synchronizer.md](references/zero_etl_synchronizer.md) |
| Streaming job must first process historical data, then switch to real time | Hybrid Source | [references/hybrid_source.md](references/hybrid_source.md) |
| One job must combine several streams and/or write to several stores | Sidecar | [references/sidecar.md](references/sidecar.md) |
| Consumers need session/window state before it is final | Partial State Writer | [references/partial_state_writer.md](references/partial_state_writer.md) |
| Bug in a release; source data still in the topic | Streaming Reprocessor | [references/streaming_reprocessor.md](references/streaming_reprocessor.md) |
| Bug discovered after topic retention expired, or only a subset must be fixed | Batch Reprocessor | [references/batch_reprocessor.md](references/batch_reprocessor.md) |

## Patterns

### API Gateway (ingestion)
An intermediary service between producers and Kafka. It needs connectivity (HTTP/gRPC),
fault tolerance (**dead-letter storage + replay pipeline** or a **local buffer**),
elastic scalability, availability behind DNS and a load balancer, and **schema + semantic
validation** (shift-left).
- Gotchas: do not move all business logic into the gateway (scalability, and rules that
  change data break consumers); application security (DDoS, auth); producers still need
  local buffering; replayed records arrive late and may be duplicated.
- See [reference](references/api_gateway.md) and [examples](examples/api_gateway/).

### Zero-ETL Synchronizer (ingestion)
The broker or vendor syncs topics into Iceberg/Delta tables (Aiven Iceberg topics, Bufstream,
Confluent Tableflow). You configure the location, permissions, catalog and sync tuning. You
write no code.
- Gotchas: **not zero effort** (compaction, snapshot expiration, freshness monitoring);
  **zero transform** (transform afterwards into a *new* table, with the Transformation
  Replicator); time travel is not an immutability guarantee.
- See [reference](references/zero_etl_synchronizer.md) and [examples](examples/zero_etl_synchronizer/).

### Hybrid Source (data value)
Backfill from historical sources, then continue on the real-time source. Define **closed
boundaries** (offsets, timestamps, runs), so the job neither gets stuck on an active
historical source nor interleaves duplicates. Use a single job (Flink `HybridSource`) or
orchestrated jobs (historical, then fire-and-forget real time).
- Gotchas: harmonize the schemas; overlap requires idempotency; process history incrementally
  (OOM, I/O); business filters are not boundaries; size the compute so the job catches up.
- See [reference](references/hybrid_source.md) and [examples](examples/hybrid_source/).

### Sidecar (data value)
Several inputs (union after schema harmonization) and/or several outputs (shared
transformation, fan-out writes) in one streaming job. It covers Aligned/Unaligned Fan-In,
Parallel Split and Exclusive Choice, for I/O only.
- Gotchas: schema harmonization before the union; slow sinks slow the stream (consider a
  storage-based fan-out); no cross-store transactions (use idempotent writes such as Delta
  `txnAppId`/`txnVersion`, or CDC); `persist()` in `foreachBatch`.
- See [reference](references/sidecar.md) and [examples](examples/sidecar/).

### Partial State Writer (data value)
Emit intermediate state with an `is_final` flag. The frequency is a business choice
(per update, every N updates, every T). Flink uses triggers (`ContinuousEventTimeTrigger`).
Spark emits from the stateful function: partial on input, final on timeout or expired timer.
- Gotchas: partial is not final (late data, consumer awareness); higher throughput; needs
  **upserts** (MERGE or a compacted topic) on a **stable session id**; latency-critical
  consumers may embed the logic.
- See [reference](references/partial_state_writer.md) and [examples](examples/partial_state_writer/).

### Streaming Reprocessor (reprocessing)
Rewind the streaming job: find the start (source offset or time, or a checkpoint), then pass
the start position with a fresh checkpoint, restore a valid checkpoint into a new location, or
clean newer checkpoints (riskiest).
- Gotchas: duplicates (idempotent sinks); a fresh checkpoint loses state; all-or-nothing on
  logs; human error (back up and automate); works only within retention. Savepoint at every release.
- See [reference](references/streaming_reprocessor.md) and [examples](examples/streaming_reprocessor/).

### Batch Reprocessor (reprocessing)
Reprocess from the lakehouse copy with the **same business logic, decoupled from I/O**. Mark
the output's origin (Wrapper or Metadata Decorator). Run it via CI/CD, the orchestrator, or both.
- Gotchas: shared code drifts (use Factory, Adapter, Template); arbitrary state such as sessions
  needs retention, approximation, or **reconstruction from adjacent partitions**.
- See [reference](references/batch_reprocessor.md) and [examples](examples/batch_reprocessor/).

## Cross-cutting Best Practices
- Design every sink for **idempotency**. Gateways, hybrid sources and reprocessors all
  re-deliver data.
- Use **technical boundaries** (offsets, versions, ingestion time) for switches and
  reprocessing ranges, never business filters.
- Harmonize schemas at the edge of the job, once, before the shared logic.
- Keep business logic in pure functions over DataFrames or records. It enables reprocessing,
  backfills and testing.
- Mark data origin (`api` / `replay` / `historical` / `reprocessing`) in headers or wrappers.
- Keep a raw lakehouse copy of important topics. It turns an expired retention into a
  recoverable situation.
- Make checkpoints and savepoints part of the release process. Back up before any manual
  checkpoint operation.
- Monitor everything that "needs no code": fallback backlogs, sync freshness, consumer lag
  (see data-observability-patterns).
