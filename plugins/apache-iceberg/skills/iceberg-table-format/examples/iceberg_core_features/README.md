# Iceberg core features examples

Table format vs folder of files, the metadata tree walked by hand, evolution, time travel,
hidden partitioning, write-audit-publish, and the open five-layer lakehouse.

| File | Shows |
|---|---|
| `files_vs_table/file_lake.py` | Plain Parquet "dataset" on S3: crash mid-write, LIST-and-read, last-writer-wins overwrite |
| `files_vs_table/iceberg_writes.py` | Spark append that fails in one task, concurrent appends, Parquet footer stats |
| `files_vs_table/test_files_vs_table.py` | Data files are plain Parquet; the `data/` folder is not the table (35,000 vs 15,000 rows); failed job changes nothing; stale PyIceberg writer rejected then retried |
| `metadata_tree/metadata_tree.py` | Catalog pointer -> metadata.json -> manifest list -> manifests with `fastavro`; binary bound decoding; hand-written two-level planner |
| `metadata_tree/test_metadata_tree.py` | Hand planner = PyIceberg `plan_files()` = Spark scan metrics (5 manifests, 4 skipped; 3 files skipped, 1 read) |
| `evolution_time_travel_wap/features.py` | `rollback_to_snapshot`, WAP branch context manager, audit, `fast_forward` publish |
| `evolution_time_travel_wap/test_evolution_time_travel_wap.py` | Schema and partition evolution without rewrites, time travel after `DROP COLUMN`, hidden partitioning vs Hive layout (1 vs 31 files), rollback, branches and tags |
| `open_lakehouse/components.py`, `test_open_lakehouse.py` | Spark/PyIceberg/DuckDB interop, commit = pointer swap, `register_table` after drop without purge, DuckDB federation, one probe per layer |
| `common/lakehouse.py` | Spark + PyIceberg on one REST catalog, raw S3 and REST helpers, `scan_metrics()` |
| `common/orders.py` | Orders DDL (`days(order_ts)`, v2) and deterministic Spark/Arrow generators |

## Prerequisites
- Docker with Compose v2, about 6 GB RAM.
- Images: `tabulario/spark-iceberg` (tests expect Spark 3.5.5 + Iceberg 1.8.1),
  `apache/iceberg-rest-fixture:1.8.1`, `chrislusf/seaweedfs`. The Dockerfile adds PyIceberg 0.9.1,
  fastavro 1.10.0, boto3, pytest 8.3.4; the base image ships pyarrow 17.0.0 and DuckDB 1.2.1.
- Keep the REST fixture and the Spark Iceberg runtime on the same version; pin the Spark image
  tag if `latest` moves.

## How to run
```bash
docker compose -p iceberg-core-features up -d --build
docker compose -p iceberg-core-features exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-core-features down -v
```
On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

## Environment notes
- AQE is disabled so `resultDataFiles`, `skippedDataFiles` and `numFiles` stay on the scan nodes.
- Spark `TIMESTAMP` is Iceberg `timestamptz`; Arrow data from PyIceberg is `timestamp[us, tz=UTC]`.
- The Hive-style comparison table is written to a local path (no `hadoop-aws` in the image).
- `config/s3.json` holds local-only dev credentials for SeaweedFS; change them together with the
  `AWS_*` environment variables read by compose.

## Fixes
- Hardcoded credentials in `docker-compose.yml` replaced by `${AWS_ACCESS_KEY_ID:-admin}`-style
  variables; Python `os.getenv` defaults marked `# local dev default`.
- Lab folders renamed (`files_vs_table`, `metadata_tree`, `evolution_time_travel_wap`,
  `open_lakehouse`), test modules renamed accordingly, imports and `pytest.ini` testpaths updated.
- `Layer` example field renamed to `Layer.typical_examples`; the Hive-style temp path, Spark app
  names and the compose project name made generic; source references removed from docstrings.
- No Iceberg API changes were needed: procedures (`rollback_to_snapshot`, `fast_forward`), WAP
  settings and PyIceberg calls match Iceberg 1.8.1 / PyIceberg 0.9.1.
