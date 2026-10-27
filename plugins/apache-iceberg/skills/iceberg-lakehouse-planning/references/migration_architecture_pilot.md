# Migration Architecture Plan, Road Show and Pilot Wave

Scope: turning validated requirements (see [migration_planning.md](migration_planning.md)) into a
requirement-traced architecture guide, presenting it per audience, and migrating the first wave
with a verified, tested fallback.

## Problem
A plan presented as a directive invites resistance; a plan whose choices are not tied to requirements
cannot be defended; a first migration wave without a tested fallback can destroy the legacy data it
was meant to protect. Implementation starts with alignment, not infrastructure changes.

## 1. Architecture guide

Build it with platform engineers, data architects, compliance officers and analytics leaders, not the
data team alone. Contents: data-flow diagrams and topologies, storage tiering, dataset map with
regulatory classification, lineage mappings, requirement traceability, open items, staged migration
plan, fallback procedures for catalog or engine outages, change management for retiring legacy ETL.

Reference choices for the Hamerliva Bank scenario:

| Component | Choice | Answers |
|---|---|---|
| Storage | Hybrid: Amazon S3 for analytics, on-prem S3-compatible clusters in EU and US (air-gapped, encrypted at rest) | Residency, S3 APIs |
| Ingestion | Spark + Airflow batch; Flink + Kafka Connect for append-heavy streams; corrections in controlled batch jobs | Dual-mode ingestion, deterministic updates |
| Catalog | Apache Polaris: REST spec, RBAC, IdP integration, on-prem and cloud | Interoperability, governance |
| Federation | Dremio: semantic layer, Arrow-based execution, REST and Arrow Flight | Federated access, shared metrics |
| Consumption | Tableau over ODBC; Python/PyIceberg for data science | 5 s dashboards, Python access |

Keep decisions as data so coverage is computed, not remembered:

```yaml
- id: D-CATALOG
  component: catalog
  source: scenario
  choice: Apache Polaris (Iceberg REST catalog) with RBAC and the corporate identity provider
  rationale: REST spec for engine interoperability, one catalog across on-prem and cloud
  satisfies: [CAT-01, CAT-02, CAT-03]
```

`uncovered(reqs, decisions)` returns requirements no decision satisfies: on the reference set only
ING-04 (derived from the audit, absent from the original plan); removing D-CATALOG adds CAT-01..03.
Failed acceptance checks (CAT-02) are listed as open items next to uncovered requirements. The
generated guide has 6 sections, a `flowchart TD` with 5 component subgraphs and 18 traceability rows.

## 2. Migration waves

Order datasets by access frequency, sensitivity and transformation complexity:

```python
if pii_columns:               wave = 3   # needs access controls and residency first
elif read_share >= 0.25:      wave = 1   # hot dashboards front-loaded
elif strategy == "add_files": wave = 1   # metadata-only move
else:                         wave = 2
complexity = base + int(semi_structured) + int(has_primary_key is False)
```

Result on the reference estate: wave 1 = daily positions (48% of reads), exposure view (32%),
Parquet trade history (`add_files`); wave 2 = four rewrite datasets; wave 3 = clients and compliance
records (PII). The thresholds are assumptions to tune per estate.

## 3. Road show

One session per audience (compliance, engineering, analysts/data science, business, IT). Restate audit
goals, constraints and business motivation first; present the guide as a proposal; let attendees
annotate it; appoint volunteer transition liaisons per team. Briefings are generated per role
(compliance: 6 requirements incl. STO-01, excluding CON-04; ING-04 shown as OPEN to data engineering).

| Raised by | Request | Plan change |
|---|---|---|
| Analytics | Early access to curated datasets | Waves reprioritized, most-used dashboards first |
| Compliance | Tighter control over metadata audits | Catalog log export and monitoring, SIEM integration |
| IT operations | Hybrid storage cost | Tiered storage, capacity-alert SLAs, lifecycle policies |

## 4. Pilot migration

| Strategy | Use when | Trade-off | Fallback |
|---|---|---|---|
| `add_files` | Parquet already, Hive layout matches the target spec | No copy; table shares files with the legacy lake | DROP TABLE without PURGE |
| CTAS rewrite | CSV/JSON, JDBC tables, layout change | Full copy; clean file ownership | Drop the copy; source stays system of record |
| `migrate` | Hive table becomes Iceberg in place | Needs a Hive-backed session catalog | Restore from the backup table |
| `snapshot` | Trial copy of a Hive table | Needs a Hive-backed session catalog | Drop the snapshot, never purge |
| Recreate view | Logic in warehouse views | Semantic layer / Iceberg views | Keep the old view until sign-off |

```sql
CREATE TABLE lake.ns.trades (...) USING iceberg PARTITIONED BY (trade_date);
CALL lake.system.add_files(table => 'ns.trades',
                           source_table => '`parquet`.`s3a://legacy-lake/trades_history`');
```

Verify each table with row count plus an order-independent hash, on source and target:
`df.select(F.xxhash64(*sorted(df.columns)).alias("h")).agg(F.count("*"), F.sum("h"))`.

Measured: `add_files` registered 20 files / 10,000 rows, all outside the table location, 0 bytes
written, 5 partitions, 1 snapshot. CTAS wrote 5-20 files inside the table location. CSV 1,000, JSON
900 and JDBC 310 / 50,000 rows all verified with `PARQUET` data files.

## Gotchas

- `DROP TABLE ... PURGE` on an `add_files` table deletes the legacy Parquet files (4 files -> 0).
  Without PURGE they stay readable (2,000 rows). In-place fallback must never purge.
- `add_files` writes staging manifests through Hadoop FileIO: map `spark.hadoop.fs.s3.impl` to
  `org.apache.hadoop.fs.s3a.S3AFileSystem` (and ship `hadoop-aws`), or it fails with
  "Failed to get file system for path: s3://...".
- `migrate` with Spark's in-memory session catalog fails: "Cannot migrate a table from a non-Iceberg
  Spark Session Catalog"; `snapshot` fails similarly ("Unable to get partition spec for table").
- With a REST catalog as default, `CREATE TABLE default.x USING parquet LOCATION 's3a://legacy/...'`
  creates an Iceberg table and writes `metadata.json` into the legacy folder. Address
  `spark_catalog.default.x` explicitly for non-Iceberg tables.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Legacy files vanished after rollback | PURGE on in-place table | DROP without PURGE; restore from versioning/backup |
| `migrate` / `snapshot` error | No Hive-backed session catalog | Use `add_files` or CTAS, or configure `SparkSessionCatalog` on HMS |
| `add_files` "Failed to get file system" | s3:// not mapped for Hadoop FileIO | `fs.s3.impl=S3AFileSystem` + hadoop-aws jar |
| Requirement silently unaddressed | Decisions not linked | `satisfies` lists + `uncovered()` in CI |
| Stakeholders push back on the plan | Presented as a directive | Proposal + per-role session + liaisons |
