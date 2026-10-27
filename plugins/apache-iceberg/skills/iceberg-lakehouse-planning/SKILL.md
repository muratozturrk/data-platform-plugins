---
name: iceberg-lakehouse-planning
description: "Plan and stand up an Apache Iceberg lakehouse: a hands-on first stack (PostgreSQL -> Spark JDBC ingestion -> Iceberg tables in a Nessie catalog on S3 -> Dremio SQL, time travel and metadata pruning -> BI dashboards over curated views via Arrow Flight), migration preparation (stakeholder interviews, automated platform audit with pg_stat_statements, testable requirements, acceptance probes, architecture guide, road show, add_files vs CTAS pilot migration) and storage selection (block vs object storage, Hadoop vs REST catalog commits, Parquet range reads, S3 compatibility checks, Object Lock/versioning/SSE governance, vendor scorecard). Use when building a first Iceberg lakehouse, wiring Spark and Dremio to the same catalog, planning a move from a warehouse or Hive lake to Iceberg, writing or validating platform requirements, choosing S3/MinIO/Ceph/HDFS/ADLS storage, or debugging gc.enabled PURGE errors, AT SNAPSHOT parse errors, add_files purge data loss or SSE checksum failures."
---

# Iceberg Lakehouse Planning

Covers the three things that happen before an Iceberg platform runs in production: seeing every
layer work end to end on one machine, preparing the move (audit, requirements, acceptance checks,
architecture plan, first migration wave), and choosing the storage layer underneath. The examples are
runnable Docker Compose stacks with pytest suites; every number quoted here was measured on them.

## When to Use This Skill

- Building a first lakehouse: PostgreSQL source, Spark ingestion, Nessie catalog, S3 storage, Dremio, Superset.
- Connecting several engines (Spark, Dremio, PyIceberg) to one catalog and proving they see one table state.
- Explaining or measuring metadata pruning, snapshots and time travel to stakeholders.
- Defining dashboards as SQL over curated views (semantic layer) instead of per-chart metrics.
- Starting an Iceberg migration: who to interview, what to audit, how to turn findings into requirements.
- Making requirements testable and gating them in CI (traceability, coverage, product-name warnings).
- Ordering migration waves and choosing `add_files`, CTAS, `migrate` or `snapshot` with a safe fallback.
- Selecting storage: block vs object, S3 compatibility of on-prem stores, encryption, WORM, cost.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| See all layers work together, locally | Compose stack: PostgreSQL + Spark + Nessie + S3 + Dremio | [first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) |
| Copy an operational table into Iceberg | JDBC read + `writeTo().createOrReplace()` or high-water-mark append | [first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) |
| Query the Spark table elsewhere, no copy | Dremio Nessie source on the same catalog and bucket | [first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) |
| Show that filters skip files | Range-sorted vs random layout, files read from the job profile | [first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) |
| Dashboards with one metric definition | Views in the catalog + chart = (dimension, metric) over Arrow Flight | [first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) |
| Know what the legacy platform really does | Automated audit: catalog, `pg_stat_statements`, object listing + interviews | [migration_planning.md](references/migration_planning.md) |
| Requirements that filter vendors | Outcome-focused, testable, traced, prioritized YAML + validator | [migration_planning.md](references/migration_planning.md) |
| Evaluate a candidate platform | Acceptance probes (probe / metric / manual) with evidence | [migration_planning.md](references/migration_planning.md) |
| Plan and sell the architecture | Decisions with `satisfies` links, generated guide, role-specific road show | [migration_architecture_pilot.md](references/migration_architecture_pilot.md) |
| First migration wave | `add_files` for Parquet, CTAS for the rest, fingerprint verification | [migration_architecture_pilot.md](references/migration_architecture_pilot.md) |
| Storage requirements with evidence | S3 probe: p99, concurrency, checksums, consistency, SSE | [storage_layer_selection.md](references/storage_layer_selection.md) |
| Block vs object, which catalog | Rename vs pointer-swap commits, request counts | [storage_layer_selection.md](references/storage_layer_selection.md) |
| Is this store "S3-compatible" enough | 12-check suite + same Iceberg workload on each store | [storage_layer_selection.md](references/storage_layer_selection.md) |
| Security/integrity features that really work | Feature matrix + Iceberg reads/writes/purge under each feature | [storage_layer_selection.md](references/storage_layer_selection.md) |
| Defend the storage choice | Constraint-first weighted scorecard, then a proof of concept | [storage_layer_selection.md](references/storage_layer_selection.md) |

## First Lakehouse (Spark -> Iceberg -> Dremio -> BI)

- **Mechanics**: Spark (`SparkCatalog` + `catalog-impl=org.apache.iceberg.nessie.NessieCatalog`,
  `S3FileIO`, path-style access) writes Parquet + metadata to `s3://warehouse/<ns>/<table>_<uuid>/` and
  commits the new `metadata.json` to Nessie. Dremio reads the same table through a Nessie source. No
  export, no copy: the object count is unchanged after Dremio queries, snapshot ids match.
- **Loads**: full reload with `createOrReplace()` (one more snapshot, same row count) or an incremental
  append with a high-water mark pushed into PostgreSQL (2 new rows -> 2 appended, rerun 0). CDC when
  rows change.
- **Pruning**: `sales_amount = 35` reads 1 of 6 files on a range-sorted table, 6 of 6 on a randomly
  distributed one. Clustering at write time is what makes min/max stats useful.
- **Gotchas**: Nessie tables have `gc.enabled=false`, so `DROP TABLE ... PURGE` fails; Dremio needs
  `AT SNAPSHOT '<id>'` quoted; Dremio views land in Nessie as `ICEBERG_VIEW` and are invisible to
  Spark's `SHOW TABLES`; `VARCHAR(n)` loses its length.
- Ref: [references/first_lakehouse_spark_dremio.md](references/first_lakehouse_spark_dremio.md) · Code: [examples/first_lakehouse_spark_dremio/](examples/first_lakehouse_spark_dremio/)

## Migration Preparation (audit -> requirements -> probes -> plan -> pilot)

- **Audit**: interview data engineers, analysts/data scientists, compliance/security, business, and
  IT (start there); collect facts automatically (`pg_class` + `pg_depend` filter, `pg_stat_statements`
  with an excluded marker, object listing with format, partition keys, PII columns). Findings get stable
  IDs (`F-ROW-FORMATS`, `F-NO-PRIMARY-KEY`, `F-PII`, `F-HOT-DATASETS`, ...).
- **Requirements**: one outcome per statement, per component (storage, ingestion, catalog, federation,
  consumption), priority bucket, traces, and an acceptance (`probe`, `metric/op/target` or `manual`).
  The validator rejects untestable or untraced entries and unaddressed high-severity findings.
- **Probes**: S3 API, REST catalog with Spark and PyIceberg seeing the same snapshot, deterministic MERGE
  (dedup + `event_ts` guard + replay), identifier fields, Python access. Schema rollback restores data
  only and `ADD COLUMN` makes no snapshot, so user attribution must come from catalog audit events.
- **Plan**: decisions with `satisfies` links make uncovered requirements visible; waves by access share,
  sensitivity (PII last) and complexity; road-show sessions per audience refine the proposal.
- **Pilot**: `add_files` registers Parquet in place (0 bytes written); CTAS converts CSV/JSON/JDBC.
  Never `PURGE` an `add_files` table: it deletes the legacy files.
- Ref: [references/migration_planning.md](references/migration_planning.md), [references/migration_architecture_pilot.md](references/migration_architecture_pilot.md) · Code: [examples/migration_planning/](examples/migration_planning/)

## Storage Layer Selection

- **Requirements**: performance (tail, not mean: p99 grew 9.8x at 16 clients while throughput grew
  1.4x), security (anonymous 403, SSE with a real key source), integrity (BadDigest on bad MD5,
  read/list-after-write), cost and operations (scorecard only).
- **Block vs object**: Hadoop catalog on `s3a://` turns rename into COPY + DELETE (65 requests per
  append); REST catalog + S3FileIO commits by pointer swap (13 requests). Use object storage + a REST,
  Glue or Nessie catalog for new platforms.
- **Standards**: Parquet footer via suffix range GET, projection reads ~10% of a 10-column file;
  manifest pruning cut a scan from 27 to 4 ranged GETs. "S3-compatible" stores differ in namespace
  semantics and SSE ETags: test the exact operations S3FileIO uses.
- **Governance**: storage identities hold under Iceberg (read-only key cannot commit); versioning
  recovers a deleted data file; compliance Object Lock keeps every version after PURGE; S3FileIO write
  tags + soft delete hand deletion to lifecycle rules; SSE-C leaves the REST server's `metadata.json`
  unencrypted.
- **Selection**: must-haves filter, weights rank, unprobed features count as absent; converge on one
  platform and confirm with a proof of concept.
- Ref: [references/storage_layer_selection.md](references/storage_layer_selection.md) · Code: [examples/storage_layer_selection/](examples/storage_layer_selection/)

## Best Practices

1. Start from requirements, never from a vendor; write outcomes, not product names.
2. Prove claims with executable checks that return evidence (snapshot ids, row counts, request counts).
3. Compare against ground truth from the source system, not constants.
4. Point every engine at one catalog and one bucket; let the catalog's atomic pointer swap publish commits.
5. Pin image and library versions; `latest` changes under you.
6. Keep laptop shortcuts (in-memory catalog, no auth, no TLS, static keys) out of shared environments.
7. Treat an Iceberg table as data + metadata + catalog pointer in backup, retention and purge design.
8. Measure percentiles and request counts on the storage side; averages and client logs hide the cost.
9. Plan fallback per migration strategy before the first wave, and test it.
