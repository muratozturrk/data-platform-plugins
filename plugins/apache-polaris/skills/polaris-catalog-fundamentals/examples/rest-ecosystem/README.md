# Polaris REST Ecosystem and Roadmap Check

Runnable pytest suite with two parts: several engines sharing one Polaris table through the
Iceberg REST API, and a check of the Polaris roadmap items (as announced in mid-2025) against the
Polaris 1.8.0 release.

| Test package | What it proves |
|---|---|
| `rest_ecosystem/` | Trino (Iceberg REST connector, vended credentials, no S3 keys) reads the PyIceberg-written `eco.orders` (100 rows, sum 5625.0); a Trino CTAS is loadable by PyIceberg (50 rows); Polars and DuckDB return the same numbers; a Trino INSERT is visible to PyIceberg and Polars (101 rows, 2 snapshots) |
| `roadmap_check/` | Generic tables are shipped (create 200, load, list, drop 204, then 404; not visible through the Iceberg API; same name as an Iceberg table 409); the policy store is shipped with 4 `system.*` types, inheritance and table-level override, optimistic versioning; the roadmap names `system.compaction`, `system.snapshot_retention`, `system.row_access_policy` and custom types are rejected (400); `S3_COMPATIBLE` storage type (400) and volumes (404) do not exist |

## Stack

| Service | Image / version | Role |
|---|---|---|
| `polaris` | `apache/polaris:latest` (1.8.0 when tested) | Catalog `lake` on S3 with STS vending |
| `trino` | `trinodb/trino:476` | Iceberg REST connector on Polaris (`config/trino/lake.properties`) |
| `seaweedfs` + `create-bucket` | `chrislusf/seaweedfs` | S3 + STS |
| `runner` | `python:3.11-slim` + PyIceberg 0.9.1, Polars 1.31.0, DuckDB 1.3.2 | Tests; talks to Trino over its HTTP statement protocol |

## How to run

```bash
docker compose -p polaris-rest-ecosystem up -d --build
docker compose -p polaris-rest-ecosystem exec runner python -m pytest -v
docker compose -p polaris-rest-ecosystem down -v
```

On Git Bash for Windows prefix the `exec` line with `MSYS_NO_PATHCONV=1`. The suite needs a fresh
stack: the session fixture creates the catalog `lake` with a fixed name, and
`test_policy_inheritance_override_and_versions` reuses a policy created by `test_policy_types`, so
run the whole suite in file order.

## Layout

```
docker-compose.yml               polaris, trino, seaweedfs, create-bucket, runner
runner/Dockerfile                Python test image
config/s3.json, iam.json         SeaweedFS identities and STS role
config/trino/lake.properties     Trino Iceberg REST catalog on Polaris with vended credentials
common/polaris.py                Polaris management API + Iceberg REST client
conftest.py                      catalog lake + eco.orders, trino() HTTP helper, measure / claims fixtures
pytest.ini                       test paths
rest_ecosystem/                  test_rest_ecosystem.py
roadmap_check/                   test_roadmap_check.py
```

The `measure` fixture writes the observed values to `measurements.json` at the end of a run.
