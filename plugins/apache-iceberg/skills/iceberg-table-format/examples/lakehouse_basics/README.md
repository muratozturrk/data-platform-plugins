# Lakehouse basics examples

Why plain lakes fail, how Iceberg plans from metadata, and one table shared by three engines.

| File | Shows |
|---|---|
| `row_vs_columnar/layout_io.py`, `test_row_vs_columnar.py` | Bytes read by an aggregate and by a point lookup in a row layout (CSV + key index) vs Parquet: 42.9x less for the aggregate, 256 B vs 3.66 MB for the lookup |
| `plain_data_lake/plain_lake.py`, `test_plain_data_lake.py` | Plain Parquet dir: half a batch visible, failed overwrite destroys data, schema drift breaks readers. Iceberg twin of each: uncommitted files invisible, old snapshot survives, drift rejected and evolved explicitly |
| `hive_vs_iceberg_planning/metadata_planning.py`, `test_hive_vs_iceberg_planning.py` | Hive-style LIST planning (31 LIST calls) vs PyIceberg planning from 2 Avro metadata files (instrumented `py-io-impl` FileIO), hidden partitioning, rename by column id, snapshot isolation, 4 concurrent appends |
| `multi_engine_lakehouse/engines.py`, `test_multi_engine_lakehouse.py` | Spark, PyIceberg and DuckDB on one REST catalog: same results, same files, zero copies, writes visible in both directions |
| `common/lakehouse.py` | Spark + PyIceberg wired to the same REST catalog, S3 helpers |
| `common/orders.py` | Deterministic synthetic orders with wide text columns |
| `conftest.py`, `pytest.ini` | Session Spark, one namespace per test module |
| `docker-compose.yml`, `Dockerfile`, `config/s3.json` | Spark + Iceberg REST catalog + SeaweedFS (S3 API) |

## Prerequisites
- Docker with Compose v2, about 6 GB RAM for the stack.
- Images: `tabulario/spark-iceberg` (tests expect Spark 3.5.5, Iceberg 1.8.1, PyIceberg 0.7.1,
  pyarrow 17.0.0, DuckDB 1.2.1), `apache/iceberg-rest-fixture:1.8.1`, `chrislusf/seaweedfs`.
- The REST fixture version must match the Spark Iceberg runtime. If `tabulario/spark-iceberg:latest`
  moves to another Iceberg version, pin a matching tag or the fixture version.

## How to run
```bash
docker compose -p iceberg-lakehouse-basics up -d --build
docker compose -p iceberg-lakehouse-basics exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-lakehouse-basics down -v
```
On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. No host ports are
published, so several example stacks can run side by side.

## Environment notes
- The plain data lake lives on the container's local file system (the image has no `hadoop-aws`
  jar). On S3 the plain-lake picture is worse: no atomic rename.
- Spark sets `cache-enabled=false` on the catalog so it sees PyIceberg commits immediately.
- `config/s3.json` holds local-only dev credentials (`admin` / `password`) for SeaweedFS. Change
  them together with `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (compose reads them from the
  environment, defaulting to the same dev values). Never reuse them outside a local stack.

## Fixes
- Hardcoded credentials in `docker-compose.yml` replaced by `${AWS_ACCESS_KEY_ID:-admin}`,
  `${AWS_SECRET_ACCESS_KEY:-password}`, `${AWS_REGION:-us-east-1}`; Python `os.getenv` defaults
  marked `# local dev default`.
- Lab folders renamed to topic names (`row_vs_columnar`, `plain_data_lake`,
  `hive_vs_iceberg_planning`, `multi_engine_lakehouse`); imports, the `py-io-impl` class path and
  `pytest.ini` testpaths updated; `test_iceberg_table_format.py` -> `test_hive_vs_iceberg_planning.py`.
- Spark app names and compose project name made generic; source references removed from docstrings.
- No Iceberg API changes were needed: all calls match Iceberg 1.8.1 / PyIceberg 0.7.1.
