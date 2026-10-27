# Table maintenance examples

One local lakehouse (Spark + Iceberg REST catalog + SeaweedFS S3) with four lab packages that degrade
a clickstream table (`event_id, user_id, session_id, event_type, country, amount, event_ts`,
partitioned by `days(event_ts)`) and repair it with Iceberg's Spark procedures.

| File | Shows |
|---|---|
| `common/lakehouse.py` | Spark session wired to the REST catalog + S3FileIO (`http-client.apache.max-connections=500`), boto3 listing/reading of physical objects |
| `common/clickstream.py` | Deterministic event generator, `micro_batch_ingest` (one small commit per batch) |
| `suboptimal_files/workloads.py` | Micro-batch table builder, COW/MOR switch (`write.update/delete/merge.mode`), sparse refund `MERGE` |
| `suboptimal_files/diagnose.py` | `diagnose()`: small-file ratio, manifests, snapshots, `metadata.json` versions, delete files from metadata tables only; `files_touched_by_lookup()` colocation check via `readable_metrics` bounds; snapshot-summary counters |
| `suboptimal_files/test_suboptimal_files.py` | 80 small files / 20 snapshots from 20 commits; >90% files touched by a point lookup vs <=2 of 8 when clustered; COW vs MOR commit counters; partition- vs file-scoped v2 deletes; v3 deletion vectors |
| `compaction/compaction.py` | `CompactionPolicy` -> `rewrite_data_files` SQL (binpack/sort/zorder, `where`, options), `rewrite_position_delete_files` purge, `rewrite_manifests`, closed-partition filter, content fingerprint |
| `compaction/test_compaction.py` | 96 files -> 3; MOR deletes materialized and dangling deletes purged; sort compaction restores pruning; open partition untouched; manifests consolidated |
| `retention_gdpr/retention.py` | `expire_snapshots` (args or table properties), retention properties, tags with `RETAIN`, `remove_orphan_files` with `file_list_view` |
| `retention_gdpr/gdpr_erasure.py` | Erasure workflow: blocking tags -> DELETE -> compaction -> expire -> orphan cleanup -> Parquet scan of storage as evidence |
| `retention_gdpr/test_retention_gdpr.py` | `retain_last`, property-driven expiry, end of time travel, tag pinning, orphan cleanup, 24 h guard, COW and MOR erasure, blocking tag |
| `maintenance_planner/health.py` | `collect_health()`: per-partition small/delete files, manifests, snapshots, tag ages; `as_metrics()` gauges |
| `maintenance_planner/planner.py` | Pure `plan_maintenance()` rules and `execute()` in safe order (compact -> manifests -> expire -> review tags) |
| `maintenance_planner/test_health_planner.py` | 6 pure rule tests + an end-to-end monitor/plan/execute loop that converges |

## Prerequisites

- Docker with Compose v2, about 4 GB RAM for the Spark container.
- Images: `tabulario/spark-iceberg:latest` (Spark 3.5.5 + Iceberg 1.8.1 at the time of testing),
  `apache/iceberg-rest-fixture:1.8.1`, `chrislusf/seaweedfs`. The REST fixture version must match the
  Iceberg runtime in the Spark image; if `latest` moves to a newer Iceberg, bump the fixture tag too.
- Python deps inside the image: `pytest==8.3.4`, `boto3==1.35.99` (installed by the `Dockerfile`).
- `config/s3.json` and the compose defaults hold **local-only dev credentials** (`admin` / `password`).
  Change them (or export `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` and edit `s3.json` to match)
  before using the stack anywhere but a laptop.

## How to run

```bash
docker compose -p iceberg-table-maintenance up -d --build
docker compose -p iceberg-table-maintenance exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
# one package only:
docker compose -p iceberg-table-maintenance exec -w /home/iceberg/labs spark-iceberg python -m pytest retention_gdpr -v
docker compose -p iceberg-table-maintenance down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. 28 tests, about 5 minutes.
UIs: Spark `http://localhost:4040` (while a session runs), SeaweedFS master `http://localhost:9333`.

Environment notes:
- The REST fixture's default in-memory SQLite is per connection, so concurrent commits (compaction with
  partial progress) fail. `CATALOG_URI` points to a file instead.
- The default S3 HTTP pool (50) is exhausted by maintenance procedures that open many files; the
  session raises it to 500.
- Do not name a table `manifests`, `files`, `snapshots`, ...: `db.manifests` is parsed as the metadata
  table of table `db`.

## Fixes

- Lab folders renamed to topic names (`suboptimal_files`, `compaction`, `retention_gdpr`,
  `maintenance_planner`); all imports and `pytest.ini` `testpaths` updated.
- Spark app names renamed (`iceberg-maintenance`, `maintenance-tests`).
- `docker-compose.yml`: hardcoded S3 credentials replaced by `${AWS_ACCESS_KEY_ID:-admin}` /
  `${AWS_SECRET_ACCESS_KEY:-password}`; fixed `container_name`s removed so the stack can run under its own
  `-p` project name next to other stacks.
- `common/lakehouse.py`: boto3 credential defaults marked `# local dev default`.
- No Iceberg API fixes were needed: every procedure (`rewrite_data_files`, `rewrite_position_delete_files`,
  `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files` with `file_list_view`), its arguments,
  result columns and the table properties used match current Iceberg.
- Known limitation kept as is: `refs_containing()` in `gdpr_erasure.py` checks tags only; a non-main
  branch that still holds the subject blocks erasure the same way and must be reviewed separately.
