# Snowflake on Apache Polaris: Polaris-side tests (Snowflake simulated)

Snowflake cannot run locally, so the tests do not execute any Snowflake SQL (external volume,
catalog integration, `CREATE ICEBERG TABLE`, `REFRESH`). They test the Polaris side of the
integration instead: the principal a Snowflake catalog integration uses, its roles, privileges and
OAuth scopes, the REST calls the integration makes, and the production settings of a Polaris that
Snowflake can reach. A Snowflake stand-in uses raw REST calls plus PyIceberg 0.9.1 with vended
credentials.

| Test package | What it proves |
|---|---|
| `service_principal/` | The Polaris CLI steps for a `snowflake_service` principal. A multi-privilege `privileges ... catalog grant` line exits with code 2. Without the two role assignments, token requests succeed but every call returns 401/403. `PRINCIPAL_ROLE: ALL` (with a space) returns 400 `invalid_scope`. `TABLE_LIST` + `TABLE_READ_DATA` can list and load tables and read data, but cannot list namespaces (that needs `NAMESPACE_LIST`) or write. A loaded table stays a pointer until `refresh()` |
| `production_settings/` | A server without bootstrap credentials logs the root credentials. The token broker type and lifetime are visible in the JWT (RS256/3600 by default, HS256/1800 with `symmetric-key` + `PT30M`). The credential-rotation flag blocks nothing on 1.8.0 |

## Stack

| Service | Image | Role |
|---|---|---|
| `polaris` | `apache/polaris:latest` (1.8.0 when tested) | The Polaris that "Snowflake" connects to: S3 storage with STS vending |
| `polaris-dev` | `apache/polaris:latest` | No bootstrap credentials (dev mode) |
| `polaris-prod` | `apache/polaris:latest` | Rotation flag, `symmetric-key` token broker, `PT30M` token lifetime |
| `seaweedfs` + `create-bucket` | `chrislusf/seaweedfs` | S3 and STS (`config/s3.json`, `config/iam.json`) |
| `runner` | `python:3.11-slim` + requests, PyIceberg 0.9.1, `apache-polaris` 1.8.0 (CLI), Docker SDK | Test runner. Reads the `polaris-dev` logs through `/var/run/docker.sock` |

## How to run

```bash
docker compose -p polaris-snowflake up -d --build
docker compose -p polaris-snowflake exec runner python -m pytest -v
docker compose -p polaris-snowflake down -v
```

The project name must be `polaris-snowflake`, because `production_settings/` looks up the
`polaris-dev` container by its compose project label. On Git Bash for Windows, prefix
`docker compose exec` with `MSYS_NO_PATHCONV=1`. The suite needs a fresh stack (Polaris state is in
memory and the principal names are fixed). The tests write the values they measure to `measurements.json`.

## Layout

```
docker-compose.yml       three Polaris servers, SeaweedFS, runner
runner/Dockerfile        Python runner with the polaris CLI
config/                  SeaweedFS S3 identities and STS role
common/polaris.py        management + Iceberg REST clients (Admin, Principal, CatalogClient)
conftest.py              cli() runs './polaris ...' lines with connection options; seed_table()
service_principal/ production_settings/
```
