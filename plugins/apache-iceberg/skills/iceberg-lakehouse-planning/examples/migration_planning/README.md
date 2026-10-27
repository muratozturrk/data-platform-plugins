# Migration planning: audit -> requirements -> acceptance probes -> architecture guide -> pilot

A "legacy" platform (PostgreSQL 16 warehouse with `pg_stat_statements` + a Hive-style file lake on S3)
next to the target lakehouse (Spark 3.5.5 + Iceberg 1.8.1, Iceberg REST catalog fixture 1.8.1,
SeaweedFS, PyIceberg 0.7.1). The Hamerliva Bank scenario provides interview findings, requirements and
component decisions; everything else is generated deterministically so every number is assertable.

| File | Shows |
|---|---|
| `config/legacy-warehouse.sql` | 50,000 trades (PK), 5,000 clients (PII, no PK), 310 positions, a view, 2,000 jsonb payloads |
| `common/legacy_platform.py` | Lake seeder (20 Parquet files Hive-partitioned by `trade_date`, CSV, JSON lines, PII CSV) and a replayable query workload |
| `common/lakehouse.py` | Spark on the REST catalog + S3FileIO; S3A for files outside Iceberg; `fs.s3.impl` mapped to S3A for `add_files` |
| `platform_audit/audit.py` | Catalog inventory (`pg_class`, PK, PII by column name, jsonb), `pg_stat_statements` attribution, object-store inventory, 8 finding rules |
| `platform_audit/interviews.yaml` | Five stakeholder findings `S-ENG ... S-IT` that requirements trace to |
| `requirements_validation/requirements.yaml` | 18 requirements (16 scenario + 2 audit-derived) with priority, stakeholders, traces, acceptance |
| `requirements_validation/requirements.py` | `validate()`: testable, traced, component coverage, unaddressed high-severity findings, product-name warnings |
| `acceptance_probes/probes.py` | Executable probes: S3 API, REST catalog seen by Spark + PyIceberg, schema rollback/attribution, deterministic MERGE, identifier fields, Python access |
| `architecture_plan/decisions.yaml` | Component decisions with `satisfies` links |
| `architecture_plan/plan.py` | Migration waves (access, sensitivity, complexity), traceability, open items, Mermaid data flow, role briefings |
| `architecture_plan/pilot_migration.py` | `add_files` in place vs CTAS rewrite, verified by row count + `xxhash64` fingerprint |
| `architecture_plan/test_pilot_migration.py` | `DROP TABLE ... PURGE` deletes the legacy files of an `add_files` table; `migrate` needs an Iceberg session catalog |

## Prerequisites

- Docker with Compose v2, about 3 GB free RAM.
- `tabulario/spark-iceberg` (tested at Spark 3.5.5 / Iceberg 1.8.1 / PyIceberg 0.7.1; pin the tag when
  reproducing) plus `hadoop-aws` 3.3.4 and `aws-java-sdk-bundle` 1.12.262 added by the Dockerfile;
  `apache/iceberg-rest-fixture:1.8.1` (keep it equal to the Spark runtime version); `postgres:16-alpine`.

## Run

```bash
docker compose -p iceberg-migration-planning up -d --build
MSYS_NO_PATHCONV=1 docker compose -p iceberg-migration-planning exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-migration-planning down -v
```

`MSYS_NO_PATHCONV=1` is only needed on Git Bash for Windows. 29 tests, about 2 minutes on a fresh stack.
The session fixture in `conftest.py` seeds the lake, resets `pg_stat_statements`, replays the workload
and runs the audit once; all modules build on that report.

## Fixes

- Removed source references from comments, docstrings and YAML headers; Spark app names are
  `migration-planning` / `migration-planning-tests`; compose project `iceberg-migration-planning`;
  lab folders renamed (`platform_audit`, `requirements_validation`, `acceptance_probes`,
  `architecture_plan`) and imports/`pytest.ini` updated.
- `requirements.yaml`: section-number `source` values became `source: scenario` (`lab` unchanged); the test now
  counts `source == "scenario"`. `decisions.yaml`: `source: scenario` / `road-show` / `lab`.
- Audit rules test renamed to `test_findings_encode_the_audit_rules`.
- Credentials moved to environment variables with marked local dev defaults: S3 keys in compose
  (`${AWS_ACCESS_KEY_ID:-admin}`), PostgreSQL (`${PG_USER:-auditor}` / `${PG_PASSWORD:-auditor}` for the
  server, `PG_DSN` and the JDBC options), PyIceberg S3 keys in `acceptance_probes/probes.py`.
- PostgreSQL healthcheck uses `$$POSTGRES_USER`.
- `config/s3.json` holds the local-only `admin` / `password` SeaweedFS identity; change it together
  with the `AWS_*` variables.
- No Iceberg API changes were needed (`add_files`, `migrate`, `rollback_to_snapshot`,
  `SET IDENTIFIER FIELDS`, PyIceberg `load_catalog`/`scan` are current for Iceberg 1.8 / PyIceberg 0.7).
