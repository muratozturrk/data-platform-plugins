# Polaris Catalog Basics

Runnable pytest suite that shows what Apache Polaris does as an Iceberg REST catalog: it holds the
pointer to each table's current `metadata.json`, rejects stale commits, writes metadata on the
server, vends table-scoped storage credentials, and serves several catalogs behind one endpoint.
The Apache Iceberg REST reference server and a PyIceberg `SqlCatalog` (JDBC-style) run next to it
as controls.

| Test package | What it proves |
|---|---|
| `metadata_pointer/` | Every commit writes a new immutable `metadata.json` and Polaris points at the last one (4 commits -> 4 files, 3 `metadata-log` entries); the manifest list read with vended keys lets a reader skip a manifest by partition; the engine holds no static S3 keys (vended key starts with `ASIA`) |
| `catalog_mechanics/` | All clients resolve the same current `metadata.json`; a raw REST commit with a stale `assert-ref-snapshot-id` gets 409 `CommitFailedException`; PyIceberg 0.9.1 surfaces the conflict instead of retrying; 6 concurrent writers with an application retry loop lose no rows |
| `catalog_options/` | A JDBC-style catalog needs long-lived bucket keys in every engine and fails to commit with read-only keys; Polaris needs only a catalog credential, writes the metadata itself, and limits a principal to one namespace (403 on the other, `AccessDenied` on its files) |
| `rest_spec/` | The same PyIceberg `RestCatalog` code runs unchanged on Polaris and on the reference server; PyIceberg's default OAuth scope is rejected by Polaris (`invalid_scope`); `/v1/config` endpoint lists differ (Polaris 38, reference 26); the `warehouse` parameter selects a Polaris catalog and its URL prefix; both servers write metadata for a plain-HTTP commit |

## Stack

| Service | Image / version | Role |
|---|---|---|
| `polaris` | `apache/polaris:latest` (1.8.0 when tested) | REST catalog with RBAC and credential vending |
| `rest` | `apache/iceberg-rest-fixture:1.8.1` | Minimal REST catalog: no auth, no vending, one catalog |
| `seaweedfs` + `create-bucket` | `chrislusf/seaweedfs` | S3 + STS; identities `admin` and read-only `reader` (`config/s3.json`), STS role `lake-vendor` (`config/iam.json`) |
| `runner` | `python:3.11-slim` + PyIceberg 0.9.1 (sql-sqlite), fastavro 1.10.0, pytest 8.3.4 | Test client; no AWS keys in its environment |

## How to run

```bash
docker compose -p polaris-catalog-basics up -d --build
docker compose -p polaris-catalog-basics exec runner python -m pytest -v
docker compose -p polaris-catalog-basics down -v
```

On Git Bash for Windows prefix the `exec` line with `MSYS_NO_PATHCONV=1`. Use a fresh stack
(`down -v` before re-running) because Polaris state persists between runs of the same stack.

## Layout

```
docker-compose.yml        polaris, rest, seaweedfs, create-bucket, runner
runner/Dockerfile         Python test image
config/s3.json, iam.json  SeaweedFS identities and the STS role Polaris assumes
common/polaris.py         Polaris management API + Iceberg REST client, vended-credential helpers
conftest.py               admin / root / uid / measure fixtures
pytest.ini                test paths
metadata_pointer/         anatomy.py (reads metadata layers with vended keys), test_metadata_pointer.py
catalog_mechanics/        test_catalog_mechanics.py
catalog_options/          test_catalog_options.py
rest_spec/                test_rest_spec.py
```

The `measure` fixture writes the observed values to `measurements.json` at the end of a run.
