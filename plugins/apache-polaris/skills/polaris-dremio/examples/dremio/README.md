# Dremio and Apache Polaris: runnable tests

Dremio needs Enterprise 26+ for the Iceberg REST Catalog and Snowflake Open Catalog sources that
attach Polaris. The only Dremio image on Docker Hub is Dremio OSS 26.0.5, and it rejects both source
types (`connecting/`). So the Dremio SQL here runs on Dremio OSS against a Nessie catalog source
named `lake` on the same S3 bucket. It stands in for a source named `PolarisCatalog`. Dremio's
output is then registered in Polaris (`registerTable`) and read back through Polaris with vended
credentials.

| Test package | What it proves |
|---|---|
| `connecting/` | OSS answers 400 `An invalid value was found: RESTCATALOG` / `SNOWFLAKEOPENCATALOG`, even with the `plugins.restcatalog.*` support options on. Against Polaris, an Iceberg REST client works with static `fs.s3a`-style keys and with vended credentials. Without the vending header, the load-table response has no storage keys |
| `queries_and_metadata/` | Names must be fully qualified (`source.ns.table`). A federated join with a CSV in another source works. Six metadata functions are covered (`clustering_information` fails on an unclustered table), plus `AT SNAPSHOT` / `AT TIMESTAMP` |
| `ctas_and_copy_into/` | Empty `CREATE TABLE`, CTAS, five COPY INTO forms (a double-backslash REGEX loads 0 rows, options must follow `FILE_FORMAT`, the transformation form is a syntax error), `sys.copy_errors_history` + `copy_errors()`. A CTAS table registered in Polaris needs `polaris.config.allow.unstructured.table.location=true` |
| `maintenance/` | `VACUUM TABLE` is not supported for the Nessie source. Four `OPTIMIZE` forms with file and manifest counts. `SHOW TBLPROPERTIES` returns no rows on OSS |

## Stack

| Service | Image | Role |
|---|---|---|
| `dremio` | `dremio/dremio-oss:26.0.5` | Query engine (heap and direct memory capped at 2 GB each) |
| `nessie` | `projectnessie/nessie:0.76.6` | Catalog behind the Dremio source `lake` |
| `polaris` | `apache/polaris:latest` (1.8.0 when tested) | Catalog `my_catalog` (S3, STS vending, unstructured table locations allowed) |
| `seaweedfs` + `create-buckets` | `chrislusf/seaweedfs` | S3 and STS, buckets `warehouse` and `landing` |
| `runner` | `python:3.11-slim` + requests, boto3, PyIceberg 0.9.1 | Test runner (Dremio REST API for setup and SQL) |

## How to run

```bash
docker compose -p polaris-dremio up -d --build
docker compose -p polaris-dremio exec runner python -m pytest -v
docker compose -p polaris-dremio down -v
```

On Git Bash for Windows, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. The suite needs a
fresh stack because the packages run in order and build on each other's tables. Dremio takes 1 to 2
minutes to start, and the session fixture waits for it. The tests write the values they measure to
`measurements.json`.

## Layout

```
docker-compose.yml       Dremio OSS, Nessie, Polaris, SeaweedFS, runner
runner/Dockerfile        Python runner
config/                  SeaweedFS S3 identities and STS role
common/dremio.py         Dremio REST client (first user, sources, SQL jobs)
common/polaris.py        Polaris management + Iceberg REST clients
conftest.py              sources lake (Nessie) and landing (S3), Polaris catalog, with_lab_sources(), run_sql()
connecting/ queries_and_metadata/ ctas_and_copy_into/ maintenance/
```
