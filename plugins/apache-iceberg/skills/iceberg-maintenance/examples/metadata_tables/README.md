# Metadata tables examples

One local lakehouse (Spark + Iceberg REST catalog + SeaweedFS S3 + PyIceberg) that builds a small,
deterministic `orders (id, region, amount, order_date)` history once per test session, as a v2 and a
v3 table, and checks every Spark metadata table against it.

Shared history (`common/history.py`): S1 append ids 1-30 (EU, US, APAC) -> S2 append ids 31-50 ->
S3 MOR `DELETE` ids 1, 2, 31 -> S4 `overwritePartitions` APAC -> tag `audit_s4` ->
`ADD PARTITION FIELD months(order_date)` -> S5 append (spec 1) -> branch `dev` -> S6 append on `dev`
-> `expire_snapshots(snapshot_ids => array(S1, S2))`.

| File | Shows |
|---|---|
| `common/history.py` | The shared history above; `current_snapshot()` from `refs` |
| `common/metaquery.py` | Query library: `change_audit`, `protected_snapshot_ids`, `file_lifecycle`, `file_size_distribution`, `delete_file_accumulation`, `partition_skew`, `manifest_stats`, `snapshot_growth`, `compaction_trigger`, `snapshots_since`, `health_report` |
| `common/reference_queries.py` | Commonly quoted metadata queries with status `as-is` / `fixed` and the corrected SQL |
| `querying/inventory.py` | `DESCRIBE`-based schema dump, row counts with optional `VERSION AS OF`, PyIceberg `table.inspect` counts |
| `querying/test_querying.py` | Exact row counts of all 16 tables, frozen 1.8.1 schemas, v3 columns, time-travel support matrix, PyIceberg parity |
| `history_refs/test_history_refs.py` | `history` = main lineage, `snapshots` summaries, change audit across refs, rollback, metadata log and its cap, refs pinning snapshots through expiration |
| `manifests_partitions/test_manifests_partitions.py` | `manifests` content/spec/origin, `all_manifests` reference semantics, `partitions` per spec, skew |
| `files_deletes/test_files_deletes.py` | `files` = data + delete files, `all_data_files` duplicates, `position_deletes` resolved to row ids, entries lifecycle, v3 Puffin DVs, replaced deletes until expiration |
| `health_monitoring/test_health_monitoring.py` | Small-file trigger (naive vs `content = 0`), snapshot frequency, growth from summaries, health gauges before/after maintenance, refs blocking cleanup |

## Prerequisites

- Docker with Compose v2, about 4 GB RAM for the Spark container.
- Images: `tabulario/spark-iceberg:latest` (Spark 3.5.5 + Iceberg 1.8.1 at the time of testing),
  `apache/iceberg-rest-fixture:1.8.1`, `chrislusf/seaweedfs`. Keep the REST fixture version equal to the
  Iceberg runtime of the Spark image.
- Python deps inside the image: `pytest==8.3.4`, `pyiceberg[pyarrow]==0.9.1` (installed by the `Dockerfile`).
- `config/s3.json` and the compose defaults hold **local-only dev credentials** (`admin` / `password`);
  change them together before using the stack outside a laptop.

## How to run

```bash
docker compose -p iceberg-metadata-tables up -d --build
docker compose -p iceberg-metadata-tables exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-metadata-tables down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. 58 tests, about 3 minutes.
No container names and no published ports, so it runs next to other local stacks.

## Fixes

- Lab folders renamed to topic names (`querying`, `history_refs`, `manifests_partitions`,
  `files_deletes`, `health_monitoring`); imports and `pytest.ini` `testpaths` updated.
- The query catalog module was renamed to `common/reference_queries.py` (`ReferenceQuery`,
  `REFERENCE_QUERIES`); its keys are now descriptive (`small-files`, `entries-join-snapshots`, ...) and
  the numeric section field became `topic`, which the parametrized tests filter on. Test functions were
  renamed accordingly (`test_reference_queries_run_as_printed`,
  `test_reference_queries_files_and_deletes`, `test_naive_small_file_trigger_counts_delete_files`,
  `test_snapshot_frequency_query`), and the documented-columns constant became `DOCUMENTED_COLUMNS`.
- Spark app name, shared namespace (`metadata_shared`) and compose project name renamed.
- `docker-compose.yml`: S3 credentials read from `${AWS_ACCESS_KEY_ID:-admin}` /
  `${AWS_SECRET_ACCESS_KEY:-password}`; PyIceberg credential defaults in `querying/inventory.py` marked
  `# local dev default`.
- No Iceberg API fixes were needed: metadata table names and columns, `VERSION AS OF`, branch/tag DDL,
  `rollback_to_snapshot`, `expire_snapshots(snapshot_ids => ...)`, `rewrite_data_files`,
  `rewrite_position_delete_files` and PyIceberg `table.inspect.*` match current Iceberg / PyIceberg 0.9.
