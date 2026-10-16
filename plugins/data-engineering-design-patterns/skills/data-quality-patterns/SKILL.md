---
name: data-quality-patterns
description: Data quality design patterns (Audit-Write-Audit-Publish / Write-Audit-Publish, Constraints Enforcer, Schema Compatibility Enforcer, Schema Migrator, Offline Observer, Online Observer) for Spark, Kafka and Airflow pipelines. Use when adding data validation or audit steps, quality gates before publishing, NOT NULL/CHECK constraints, Schema Registry compatibility modes, safe column rename/type changes, breaking schema changes, data profiling, data quality monitoring, null-rate or volume drift detection, or consumer lag observation.
---

# Data Quality Patterns

Trust is the most important property of a dataset, and poor quality (incomplete, inaccurate, inconsistent data) is the fastest way to lose it. This skill covers three families: **quality enforcement** (stop bad data before consumers see it), **schema consistency** (stop producers from breaking consumers, and evolve schemas safely when you must), and **quality observation** (keep watching the data so today's rules stay relevant tomorrow). Enforcement only guarantees there are no issues *for the rules you wrote*; observation is how you discover the rules you are missing.

## When to Use This Skill

- A job published wrong numbers (e.g. unique visitors dropped 50%) and nobody noticed until a business decision was made on it.
- You need a quality gate between "written" and "visible to consumers" (WAP / AWAP, staging tables, branch-and-publish).
- Required fields randomly arrive as NULL and you want the storage layer to reject them.
- Upstream teams remove or rename fields and your jobs fail.
- You must rename a column, change a type, or restructure a message without breaking consumers.
- You want null rates, value distributions, volume and lag tracked over time, offline or inline with the pipeline.

## Pattern Selection Guide

| Problem / symptom | Pattern | Reference |
|---|---|---|
| Bad input or wrong transformation output reaches consumers | Audit-Write-Audit-Publish | [references/audit_write_audit_publish.md](references/audit_write_audit_publish.md) |
| Want declarative NOT NULL / CHECK / FK / type rules without validation code in the job | Constraints Enforcer | [references/constraints_enforcer.md](references/constraints_enforcer.md) |
| Producers make breaking schema changes (drop required field, type change) | Schema Compatibility Enforcer | [references/schema_compatibility_enforcer.md](references/schema_compatibility_enforcer.md) |
| You need a rename / type change / removal but consumers must keep working | Schema Migrator | [references/schema_migrator.md](references/schema_migrator.md) |
| Want quality metrics and profiles without slowing or risking the main pipeline | Offline Observer | [references/offline_observer.md](references/offline_observer.md) |
| Offline observation detects issues too late; need insight right after data generation | Online Observer | [references/online_observer.md](references/online_observer.md) |

## Patterns

### Audit-Write-Audit-Publish (AWAP)
Evolution of Netflix's Write-Audit-Publish: audit the **input** (cheap checks: format, size, schema/header), transform, write to a **staging** area, audit the **output** (data checks: nulls, volume, distinctness), then publish. Outcomes are not only "fail": you can **dispatch** (valid rows to output, invalid to an errors table) or run a **nonblocking audit** that publishes and annotates issues. Streaming variants: window-based (audit buffered records per processing window) or staging-based (stream to staging, a second job audits and promotes).
- Gotchas: same rule in both audits has different intent (provider vs your logic); put expensive rules in the most exhaustive place (output audit). Rules go stale — pair with an observer. A failed audit may be real success (viral traffic spike) — alert instead of fail for volume rules. Streaming audits add window latency.
- [Reference](references/audit_write_audit_publish.md) · [Examples](examples/audit_write_audit_publish/)

### Constraints Enforcer
Delegate validation to the storage/serialization layer: type, nullability, value (`CHECK`), and integrity (FK) constraints. Delta Lake `NOT NULL` + `ADD CONSTRAINT ... CHECK`, PostgreSQL DDL, Avro/Protobuf types, Protobuf + protovalidate for value rules. Informative for consumers, interactive for producers.
- Gotchas: all-or-nothing transactions (one bad row rejects the batch) and databases stop at the first error. Constraints are producer-oriented — consumers may still need stricter filters. Coverage is limited (table formats have no FK) — complete with AWAP rules.
- [Reference](references/constraints_enforcer.md) · [Examples](examples/constraints_enforcer/)

### Schema Compatibility Enforcer
Validate schema *changes*, not values. Three modes: external service/library (Kafka Schema Registry, Avro SchemaValidator), implicit with inserts (Delta Lake rejects mismatched writes unless `mergeSchema`), event-driven DDL (PostgreSQL event triggers, or withhold `ALTER TABLE`). Compatibility levels: backward (new reader reads old data), forward (old reader reads new data), full, each transitive or not.
- Gotchas: the producer pays a registry round trip. Renames become add-new + deprecate-old. Transitive modes compare against all versions, not just the previous one. Never use `NONE` as "enforcement". A DDL event trigger also blocks your own approved migrations: allowlist them (`SET LOCAL dp.schema_migration = 'contract'` + `dp_schema_migrator` role).
- [Reference](references/schema_compatibility_enforcer.md) · [Examples](examples/schema_compatibility_enforcer/)

### Schema Migrator
Breaking changes done safely: add the new (renamed/retyped) field, agree on a transition deadline, emit **both** old and new during the grace period, then publish a version without the old field. Removal = announce a removal period, then drop. Requires **non-transitive** compatibility.
- Gotchas: grace period increases storage/network/IO and widens schemas (Protobuf field overhead, Delta stats on first 32 columns only). Removal is impossible if a consumer depends on the field and you have no substitute. Use lineage (Fine-Grained Tracker) to know who reads what. The contract step (drop) must pass the DDL guard through its allowlist, on a dedicated migrator connection.
- [Reference](references/schema_migrator.md) · [Examples](examples/schema_migrator/)

### Offline Observer
A separate observation job, on its own schedule, that computes metrics (row counts, nulls per column, distributions, unprocessed new fields, consumer lag, data profiles via ydata-profiling) on already-produced data and stores them in a monitoring layer. Never blocks the producer.
- Gotchas: insight may arrive after consumers already used bad data. Running less often means bigger batches to observe; sampling saves compute but misses signals. Record the observed scope (first/last row id, offsets) so reruns are idempotent.
- [Reference](references/offline_observer.md) · [Examples](examples/offline_observer/)

### Online Observer
Same observation logic, but **inside** the data generation pipeline, right after transform/load — via Local Sequencer (observe after load, sees what consumers see) or Parallel Split (observe concurrently, faster but may miss load-time issues). In streaming, observation lives in the job (Spark accumulators, inline metrics).
- Gotchas: adds latency to the pipeline; an observer failure can fail the job — isolate with an `all_done` final task in Airflow or try/except in streaming. Parallel Split can observe a dataset that differs from the loaded one.
- [Reference](references/online_observer.md) · [Examples](examples/online_observer/)

## Cross-cutting Best Practices

- Layer defenses: unit tests (logic) → Constraints Enforcer (storage) → AWAP (pipeline) → Schema Compatibility Enforcer (contract) → Observers (drift).
- Audits block; observers don't. Decide per rule whether a violation should fail, dispatch, annotate, or alert.
- Collect *all* violations before failing — one error message listing every broken rule saves round trips.
- Keep audit/observation scope deterministic (partition, row-id range, offset range) so reruns give identical results.
- Feed observer findings back into enforcement rules; review rules when schemas evolve.
- Store quality metrics as data (a monitoring table), not only in logs, so you can trend and alert on them.
