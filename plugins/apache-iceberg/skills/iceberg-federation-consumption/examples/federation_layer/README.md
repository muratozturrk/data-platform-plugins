# Federation layer examples (Trino + Iceberg REST + PostgreSQL)

A single-node Trino federates an Iceberg lakehouse (REST catalog + SeaweedFS S3) with an
operational PostgreSQL database. Six test modules share one environment.

| File | Shows |
|---|---|
| `federated_queries/federated.py` | Iceberg + PostgreSQL join through Trino, and the same answer computed point-to-point (PyIceberg + psycopg + Python join) |
| `federated_queries/test_federated_queries.py` | Correctness vs native clients, OLTP change visible at once, new source table queryable at once, source load per query from `pg_stat_user_tables` |
| `federation_requirements/pushdown.py` | Reads pushdown (filter, projection, TopN, aggregation, join) from `EXPLAIN (TYPE DISTRIBUTED)` plus `processedRows` |
| `federation_requirements/semantic_layer.py` | Versioned business definitions deployed as `SECURITY DEFINER` views stored as Iceberg views |
| `federation_requirements/rest_client.py` | Minimal Trino client protocol client (`POST /v1/statement`, follow `nextUri`) |
| `federation_requirements/test_*.py` | Pushdown limits, one definition for BI/notebook/app, central change propagation, row filter + column mask, denied raw access |
| `trino_engine/engine_inspect.py`, `test_trino_engine.py` | Catalogs as connectors, stages and splits, `REPLICATED` vs `PARTITIONED` joins, dynamic filtering on the Iceberg scan, Trino INSERT/MERGE/`optimize`, `system.query` passthrough |
| `workload_isolation/workloads.py`, `test_workload_isolation.py` | Resource-group selectors by user, source and client tag; queueing and `QUERY_QUEUE_FULL` in the ad hoc group while dashboards run |
| `platform_decision/decision.py`, `test_platform_decision.py` | Dremio/Trino deployment options, hard-constraint exclusion, weighted and explained scoring, connector coverage of the running cluster |
| `federation_alternatives/acceleration.py`, `test_federation_alternatives.py` | Live federation vs CTAS acceleration table: source load, staleness, refresh, storage-level vs database pushdown, copies without row filters |
| `trino/` | `config.properties`, `jvm.config`, `catalog/{lake,pg,tpch}.properties`, `rules.json` (file-based access control), `resource-groups.json` |
| `postgres/init.sql` | Operational schema: `customers`, `promotions`, `fx_rates`, `order_returns` |
| `common/federation.py`, `common/seed.py` | Persona connections, query stats, EXPLAIN parsing, PostgreSQL helpers; Iceberg `orders` table seeded by Trino CTAS from TPC-H |
| `conftest.py` | Seeds the lakehouse once per session; writes measured values to `measurements.json` |

## Prerequisites

- Docker with Compose v2, about 4.5 GB free memory (Trino is capped at 3 GB, 2 GB heap).
- Images: `trinodb/trino:483`, `apache/iceberg-rest-fixture:1.8.1`, `postgres:16-alpine`, `chrislusf/seaweedfs`.
- Runner image: Python 3.11, pytest 8.3.4, trino 0.336.0, psycopg 3.2.9, requests 2.34.2, pyiceberg 0.9.1.
- No Spark: Trino creates and writes the Iceberg tables itself.

## How to run

```bash
cd examples/federation_layer
docker compose -p iceberg-federation-layer up -d --build
docker compose -p iceberg-federation-layer exec runner python -m pytest -v
docker compose -p iceberg-federation-layer down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. No ports are
published; use `docker compose -p iceberg-federation-layer exec trino trino` for an interactive CLI.

Notes:
- Users come from the `X-Trino-User` header (no authenticator). Authorization is real
  (`trino/rules.json`); production needs OAuth2, LDAP, Kerberos or another authenticator.
- `CREATE MATERIALIZED VIEW` is not supported by Trino on an Iceberg REST catalog, so acceleration
  uses `CREATE OR REPLACE TABLE ... AS`.
- Dremio is not part of the stack; Trino counterparts stand in (dynamic filtering for runtime
  filtering, CTAS tables for reflections, resource groups for engines).
- `config/s3.json` holds local-only SeaweedFS dev credentials (`admin`/`password`). Change them
  together with `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` before using the stack anywhere shared.

## Fixes

- Folders and imports renamed to topic names (`federated_queries`, `federation_requirements`,
  `trino_engine`, `workload_isolation`, `platform_decision`, `federation_alternatives`); `pytest.ini`
  testpaths updated.
- Compose project renamed to `iceberg-federation-layer`; the default Trino client `source` is now
  `lakehouse-labs`; resource-group test tags use the prefix `wl-`.
- Hardcoded secrets removed from `trino/catalog/lake.properties` and `pg.properties`: they use Trino's
  `${ENV:AWS_ACCESS_KEY_ID}`, `${ENV:AWS_SECRET_ACCESS_KEY}`, `${ENV:PG_USER}`, `${ENV:PG_PASSWORD}`
  substitution, and the `trino` service gets those variables from compose.
- `docker-compose.yml` credentials are `${VAR:-default}` local dev defaults (`AWS_*`, `POSTGRES_USER`,
  `POSTGRES_PASSWORD`, `PG_DSN`); the PostgreSQL healthcheck uses `$${POSTGRES_USER}`.
- Python `os.getenv(...)` credential defaults are marked `# local dev default`.
- Decision tool: removed the citation field from `Option` and from exclusion messages;
  scenario ids are plain names and the scenario test is now `test_reference_scenarios`.
- Trino Iceberg connector properties were checked and are current for Trino 483:
  `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri`, `iceberg.rest-catalog.warehouse`,
  `fs.native-s3.enabled=true`, `s3.endpoint`, `s3.region`, `s3.path-style-access`, `s3.aws-access-key`,
  `s3.aws-secret-key`. No change needed.
