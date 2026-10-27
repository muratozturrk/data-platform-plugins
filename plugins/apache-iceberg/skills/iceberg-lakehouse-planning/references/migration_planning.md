# Preparing a Migration to Iceberg: Audit, Requirements, Acceptance Probes

Scope: the work before implementation. Audit the platform (people and technology), turn findings
into outcome-focused requirements, and make them executable acceptance checks. The architecture guide,
road show and pilot wave continue in [migration_architecture_pilot.md](migration_architecture_pilot.md).

## Problem
Teams pick tools from feature lists and vendor marketing, and build "a solution optimized for someone
else's problem". Compliance constraints arrive late and stall the project; migrations fail because
nobody knew which datasets were hot, sensitive or keyless.

## Sequence
`audit (interviews + technology) -> requirements per component -> prioritize -> acceptance probes ->
architecture guide (proposal) -> road show -> refine -> implementation in waves`.

## 1. Audit

**Stakeholder discovery** (interview all five groups; start with IT/infrastructure, who meet
integration, scale and cost constraints first):

| Group | Ask about |
|---|---|
| Data engineers | Where pipelines fail, manual workarounds, slow tools/formats, schema-change effort |
| Analysts / data scientists | Time to data, trust, private copies (shadow systems = low trust) |
| Compliance / security | Mandatory access control, lineage, encryption, residency, audit support |
| Business / product owners | Blocked use cases, definition of success (speed, cost, new products) |
| IT / infrastructure | Storage, compute, network limits; cloud/hybrid/on-prem; egress, residency, maintenance |

**Technology audit**: storage systems and their regulatory boundaries; file-format mix (CSV/JSON vs
Parquet); ETL/ELT tools; metadata and catalogs (Hive Metastore, Glue, custom DBs); query and BI tools.
Look for interdependencies (ingestion choices affect metadata, catalog choices shape governance).
Keep the report neutral: problems and open questions, no solutions. Audit for decisions, not for an
exhaustive inventory.

**Automate the facts** (repeatable, comparable across runs), interview for pain and strategy:

```sql
/* platform-audit */
SELECT n.nspname, c.relname, c.relkind, pg_total_relation_size(c.oid),
       EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = c.oid AND k.contype = 'p') AS has_pk
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v')
  AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
  AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e');  -- skip extension objects

/* platform-audit */
SELECT query, calls, total_exec_time, rows FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
  AND query NOT LIKE '%platform-audit%' ORDER BY calls DESC;
```

- Enable `shared_preload_libraries=pg_stat_statements`; attribute calls to `schema.table` tokens;
  statements starting with INSERT/UPDATE/DELETE/MERGE/COPY/TRUNCATE are writes. Constants are
  normalized (`$1`).
- Without the `pg_depend.deptype = 'e'` filter the `pg_stat_statements` views appear as two extra
  datasets. Without the comment marker the auditor's own `count(*)` queries inflate read counts on
  every run; with it, a second audit reports identical counts.
- Object store: one dataset per top-level prefix; format share by bytes; Hive keys from `k=v` path
  segments; schema from the Parquet footer or the first CSV/JSON line; PII by column-name regex
  (`email|iban|full_name|phone|ssn|...`). At scale use `pg_class.reltuples` and ranged footer reads.

**Finding rules** (stable IDs so requirements can trace to them):

| ID | Domain | Severity | Rule |
|---|---|---|---|
| F-ROW-FORMATS | storage | high | Main format CSV/JSON: convert to Parquet-backed Iceberg |
| F-PARQUET-IN-PLACE | storage | low | Parquet datasets: register in place with `add_files` |
| F-SMALL-FILES | storage | medium | >= 4 files and average < 32 MiB: plan compaction |
| F-NO-PRIMARY-KEY | processing | high | Table without PK blocks upserts and dedup |
| F-SEMI-STRUCTURED | processing | medium | json/jsonb payloads need parsing |
| F-PII | governance | high | Personal data: residency, RBAC, audit trail |
| F-WAREHOUSE-VIEWS | consumption | medium | Business logic in warehouse views (semantic drift) |
| F-HOT-DATASETS | consumption | medium | Datasets with >= 25% of reads |

On the reference estate the rules produce 8 findings across all 4 domains; reads were positions 30,
view 20, trades 10, clients 3, and the staging table had 6 writes (5 INSERT + 1 DELETE).

## 2. Requirements

Outcome-focused, testable, technology-neutral, grouped by the five components, prioritized
(critical / nice-to-have / future-facing), traced to findings.

| Component | Define | Example requirement |
|---|---|---|
| Storage | Compliance, performance, cost, ops; cloud/on-prem/hybrid | "All PII collected in the EU must be stored within EU data centers" |
| Ingestion | Sources, batch vs streaming, freshness, schema evolution, late data, idempotency | "Streaming data must be queryable within 5 seconds of an event arriving" |
| Catalog | REST spec interoperability, RBAC/ABAC, time travel/rollback, self-hosted vs managed, HA | "The catalog must support RBAC integration with enterprise identity systems" |
| Federation | Coverage, semantic consistency, pushdown/caching, JDBC/ODBC/REST/Flight/ADBC | "Federated access across Iceberg, JDBC databases and object stores" |
| Consumption | Interfaces, secure remote and cross-region access | "Users can securely access data from notebooks outside the corporate network" |

```yaml
- id: ING-04
  component: ingestion
  source: lab
  statement: Every table that receives upserts must declare a unique row identifier in the table schema.
  priority: critical
  stakeholders: [data_engineering]
  traces: [F-NO-PRIMARY-KEY]
  acceptance: {probe: identifier_fields}      # or {metric, op, target, unit} or {manual: ...}
```

Validator rules (gate the YAML in CI): unique id; known component/priority/stakeholder; acceptance is a
`probe`, `metric+op+target` or `manual`; at least one trace; every trace exists; every high-severity
finding and every stakeholder finding is addressed; every component has a requirement; product names in
a statement are a **warning** (an existing estate can be a fixed constraint). On the Hamerliva set:
18 requirements (4/4/3/3/4 per component), 0 errors, warnings exactly for ING-02 (Spark, Airflow),
CAT-03 (Active Directory), FED-01 (HDFS); 13 critical, 4 nice-to-have, 1 future-facing.

Rewrite "Use Polaris for the catalog" as "The catalog must support the Iceberg REST Catalog
specification and RBAC integrated with the corporate identity provider."

## 3. Acceptance probes

| Requirement type | Verification |
|---|---|
| Protocol / API capability (S3, REST spec, Python access) | Automated probe on each candidate |
| SLO under load ("dashboards within 5 s") | Metric from a benchmark on representative data |
| Organizational / cross-system (IdP, semantic layer) | Manual acceptance script with an owner |

Deterministic upsert (dedup inside the batch + guard against older events + replay-safe):

```sql
MERGE INTO trades t
USING (SELECT trade_id, status, event_ts FROM (
         SELECT *, row_number() OVER (PARTITION BY trade_id ORDER BY event_ts DESC, status) rn
         FROM batch) WHERE rn = 1) s
ON t.trade_id = s.trade_id
WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

Facts the probes establish (Iceberg 1.8.1, REST fixture, PyIceberg 0.7.1):
- REST catalog: `/v1/config` has `defaults` and `overrides`; Spark and PyIceberg report the same
  snapshot id for one table (direct interoperability check).
- `rollback_to_snapshot` restores data only: 1 row after rollback, but the later column
  (`risk_score`) stays in the current schema; both schemas remain in metadata.
- `ALTER TABLE ... ADD COLUMN` creates 0 snapshots, so there is no commit summary to carry a user.
  Data commits can carry `.option("snapshot-property.changed-by", "alice")`. "User attribution for all
  changes" therefore needs catalog audit events, not table metadata.
- `ALTER TABLE t SET IDENTIFIER FIELDS client_id` rejects a nullable column ("not a required field"):
  keyless legacy tables need a NOT NULL key first.
- Result on the Hamerliva set: 6 probe, 3 metric, 9 manual; CAT-02 fails; STO-03 and ING-02 not measured.

Row- and column-level security is enforced by the query engine, not the catalog: a catalog with policy
modeling but no engine enforcement misses the goal.

## Next step

Component decisions, the architecture guide, the road show and the first migration wave:
[migration_architecture_pilot.md](migration_architecture_pilot.md).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Audit read counts grow every run | Auditor queries recorded | Comment marker + `NOT LIKE '%platform-audit%'` |
| Extra "datasets" named `pg_stat_statements*` | Extension-owned objects | `pg_depend.deptype = 'e'` filter |
| Requirement set passes but a high finding has no owner | Missing coverage check | Validate traces against audit findings |
| `SET IDENTIFIER FIELDS` fails | Nullable column | Add NOT NULL key, backfill, then declare |
