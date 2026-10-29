# Spark on Apache Polaris: runnable tests

pytest suites that run Spark 3.5.5 with Iceberg 1.5.2 against Polaris. Two Polaris servers:

- `polaris`: the main track. It runs the `1.1.0-incubating-SNAPSHOT` image with FILE storage on a
  shared volume, and `polaris_setup/bootstrap.py` creates the catalog, the principal and the roles.
- `polaris-latest`: Polaris 1.8.0 with S3 storage and STS credential vending, used by the S3 tests.

| Test package | What it proves |
|---|---|
| `connecting/` | The minimal `SparkCatalog` + `RESTCatalog` settings (uri, warehouse, credential, scope) work unchanged. FILE tables need directories that both Polaris and Spark can write (`table_setup.py`). On S3 the client needs `header.X-Iceberg-Access-Delegation=vended-credentials`: without it the write fails in the AWS SDK. The 1.1.0 image drops `endpoint`/`stsEndpoint`/`pathStyleAccess`, which 1.8.0 keeps |
| `dataframe/` | `writeTo().partitionedBy().create()`, `spark.table` + filter, MERGE through a temp view, DELETE, `append()`, `.snapshots`/`.history`. Python date strings become a STRING column |
| `sparksql/` | CREATE/INSERT/UPDATE/DELETE/MERGE and time travel. String literals into a DATE column are refused (`CANNOT_SAFELY_CAST`), and placeholder `VERSION AS OF`/`TIMESTAMP AS OF` values fail |
| `streaming/` | An incremental read fails on overwrite snapshots until the skip options are set, and then silently skips MERGE inserts. A streaming write needs `lit` imported and a matching schema. `toTable` creates a missing table unpartitioned. Fanout is not required on Iceberg 1.5.2. 240 rows ended up in 192 files |
| `maintenance/` | `rewrite_data_files`, `expire_snapshots`, `rewrite_manifests` through `CALL polaris.system.*` on the streaming table |

The packages share one Spark session and run in order (`streaming/` reads the table `dataframe/` wrote).

## Stack

| Service | Image | Role |
|---|---|---|
| `polaris` | `apache/polaris:1.1.0-incubating-SNAPSHOT` | FILE storage, root `root_user` / `my_secret_id`, URI `http://polaris:8181/api/catalog` |
| `polaris-latest` | `apache/polaris:latest` (1.8.0 when tested) | S3 storage with STS credential vending |
| `spark` | `apache/spark:3.5.5-python3` + pytest, requests | Test runner. `spark.jars.packages` downloads iceberg-spark-runtime 1.5.2 and hadoop-aws 3.4.0 |
| `seaweedfs` + `create-bucket` | `chrislusf/seaweedfs` | S3 and STS (`config/s3.json`, `config/iam.json`), bucket `warehouse` |
| `data-perms` | `busybox:1.36` | Makes the shared `/data` volume writable |

## How to run

```bash
docker compose -p polaris-spark up -d --build
docker compose -p polaris-spark exec spark python3 -m pytest -v
docker compose -p polaris-spark down -v
```

On Git Bash for Windows, prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. The first run downloads
571 MB of jars, 530 MB of which is the AWS SDK v2 bundle 2.23.19 that hadoop-aws pulls in. The suite
needs a fresh stack because `bootstrap.py` is not idempotent and Polaris keeps its state in memory.
The tests write the values they measure to `measurements.json`.

## Layout

```
docker-compose.yml          services above
spark/Dockerfile            Spark image + pytest
config/                     SeaweedFS S3 identities and STS role
polaris_setup/              bootstrap.py (catalog, principal, roles, grant), icebergdata/table_setup.py
common/spark_session.py     polaris_spark(): the REST catalog SparkConf; add_vended_s3_catalog()
common/admin.py             root management calls: S3 catalog with STS vending
conftest.py                 bootstrap, table_setup, shared Spark session, measure()
connecting/ dataframe/ sparksql/ streaming/ maintenance/
```
