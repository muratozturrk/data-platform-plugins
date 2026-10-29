# Polaris Catalog Federation Examples

Runnable pytest suites that put Apache Polaris 1.8.0 in front of three real external Iceberg REST
catalogs. All 14 tests passed on a fresh stack (about 19 s of test time).

| Test package | What it proves |
|---|---|
| `federation/` | A Polaris `EXTERNAL` catalog over an Iceberg REST catalog: remote tables listed and read through Polaris, remote changes visible without any sync, writes through Polaris committed in the remote, RBAC enforced but only with catalog-level grants, overlapping catalog locations rejected, no data access without a vending-capable storage config, deleting the Polaris entity leaves the remote untouched |
| `migration/` | Gradual migration: federate first, then cut over by registering each table's current `metadata.json` in an internal Polaris catalog (0 objects copied), and the split history you get if the legacy writer keeps running |
| `gravitino/` | Gravitino's Iceberg REST server federated next to an internal catalog (regional metadata, one Polaris identity); `remoteCatalogName` must be `default_catalog` |
| `lakekeeper/` | Lakekeeper soft deletion and undrop, remote signing instead of vended keys, and the Polaris 1.8.0 -> Lakekeeper 0.13.8 federation failing with HTTP 500 |

Not runnable here: Nessie (Docker Hub only carries 0.76.6, which has no Iceberg REST endpoint)
and AWS Glue (needs an AWS account).

## Stack

| Service | Image (version measured) | Role |
|---|---|---|
| `polaris` | `apache/polaris:latest` (1.8.0), federation flags on | Central catalog |
| `rest` | `apache/iceberg-rest-fixture:1.8.1` | External catalog (partner / legacy system) |
| `gravitino` | `apache/gravitino-iceberg-rest:latest` (1.3.0) | External catalog (EU region) |
| `lakekeeper` + `lakekeeper-db` + `lakekeeper-migrate` | `vakamo/lakekeeper:latest` (0.13.8), `postgres:16-alpine` | External catalog with soft deletion |
| `seaweedfs` + `create-bucket` | `chrislusf/seaweedfs` | S3 + STS (`config/s3.json`, `config/iam.json`) for all catalogs |
| `runner` | `python:3.11-slim` + PyIceberg 0.9.1, s3fs 2025.3.0, pytest 8.3.4 | Test client |

## How to Run

```bash
docker compose -p polaris-federation up -d --build
docker compose -p polaris-federation exec runner python -m pytest -v
docker compose -p polaris-federation down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. No host ports or
container names are used, so the stack can run next to others.

## Layout

```
docker-compose.yml     Polaris (federation flags), three remote catalogs, SeaweedFS
config/                SeaweedFS identities (s3.json) and the STS role Polaris assumes (iam.json)
common/polaris.py      Management API + Iceberg REST client for Polaris (Admin, CatalogClient, Principal)
common/remotes.py      clients for the remote catalogs, federate() / unfederate(), lakekeeper_setup()
conftest.py            admin / root / uid / measure fixtures, seed_table()
federation/            test_federation.py
migration/             test_migration.py
gravitino/             test_gravitino.py
lakekeeper/            test_lakekeeper.py
runner/Dockerfile      test runner image
```

## Notes

- Federation is switched on in `docker-compose.yml` with `polaris.features."ENABLE_CATALOG_FEDERATION"`,
  `SUPPORTED_CATALOG_CONNECTION_TYPES` and `SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES`.
- `unfederate()` removes only the Polaris catalog entity. Never empty a federated catalog through
  Polaris to delete it: dropping its tables and namespaces deletes them in the remote catalog.
- Lakekeeper runs without authentication (lab only) and is bootstrapped by the tests.
- Each run uses unique namespace and catalog names, because remote catalogs keep state while the stack runs.
- `measurements.json` is written at the end of a run with the measured values.
