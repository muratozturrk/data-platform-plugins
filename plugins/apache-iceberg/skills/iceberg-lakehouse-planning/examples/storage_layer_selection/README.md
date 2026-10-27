# Storage layer selection: probes, object semantics, standards, governance, scorecard

One engine container, two S3 implementations (SeaweedFS 4.48 with Prometheus request counters,
RustFS 1.0.3), one Iceberg REST catalog fixture per store, and a Docker volume that stands in for block
storage. Nine Spark catalogs (`common/lakehouse.py`) differ only in storage, FileIO or governance
properties, so every difference in the results comes from the storage layer.

| File | Shows |
|---|---|
| `common/lakehouse.py` | Catalogs `lake`, `rust`, `lake_hadoop` (HadoopFileIO), `analyst` (read-only key), `lake_softdelete`, `lake_ssec`, `rust_sse`, `local` and `s3a` (Hadoop catalogs); synthetic `events_df` |
| `common/s3.py` | boto3 / `pyarrow.fs` clients per store; `count_requests()` = server-side S3 request delta from SeaweedFS metrics |
| `storage_requirements_probe/probe.py` | p50/p95/p99 latency, concurrency speedup vs tail inflation, bad Content-MD5, ETag = MD5, read/list-after-write, anonymous access, SSE-S3 |
| `storage_requirements_probe/requirements.py` | `StorageRequirements` as data, `evaluate()` -> findings, `gaps()` per store |
| `block_vs_object/object_semantics.py` | Prefix "rename" = COPY + DELETE per object (with simulated crash), paginated listing, small vs large object reads |
| `block_vs_object/iceberg_compare.py` | Same table on local disk (Hadoop catalog) and on S3 (REST catalog); metadata file names per commit protocol |
| `storage_standards/parquet_ranges.py` | `S3RangeFile`: a seekable file whose reads are range GETs, driven by pyarrow (footer, projection, row-group pruning) |
| `storage_standards/s3_compat.py` | 12 S3 checks: 8 required by S3FileIO, 4 "AWS-like" (conditional writes, flat namespace) |
| `storage_governance/governance.py` | Feature matrix with 4 statuses (enforced / accepted / not supported / breaks writes): policy, versioning, Object Lock, lifecycle, SSE-S3, SSE-C, tagging, default encryption |
| `storage_governance/test_governance.py` | Storage identities under Iceberg, versioned recovery, Object Lock vs PURGE, S3FileIO tags + soft delete, SSE-C coverage, SSE-S3 ETag failure |
| `storage_scorecard/scorecard.py` | 10 vendor rows, 16 scenarios, must-have filter + weighted score - penalties, measured profiles, raw capacity per durability scheme |

## Prerequisites

- Docker with Compose v2, about 4 GB free RAM.
- `tabulario/spark-iceberg` (tested at Spark 3.5.5 / Iceberg 1.8.1 / pyarrow 17.0.0; pin the tag when
  reproducing) + `hadoop-aws` 3.3.4 / `aws-java-sdk-bundle` 1.12.262; `apache/iceberg-rest-fixture:1.8.1`
  (twice); `chrislusf/seaweedfs` (tested 4.48); `rustfs/rustfs` (tested 1.0.3; pin it as well).

## Run

```bash
docker compose -p iceberg-storage-selection up -d --build
docker compose -p iceberg-storage-selection exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-storage-selection down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. 56 tests, about 3.5
minutes. Measured values are printed in the "measurements (local stack, not benchmarks)" section at the
end of the pytest output: compare them with each other, never with vendor benchmarks.

## Environment notes

- Request counts come from `SeaweedFS_s3_request_total{bucket,code,type}` on `:9324`; HEAD is counted
  as GET, ranged GETs appear with status 206.
- Every SeaweedFS bucket is a collection with its own volumes: with 100 volume slots the tests ran out
  after ~20 buckets and PUTs failed with HTTP 500, hence `-volume.max=500 -master.volumeSizeLimitMB=64`.
- RustFS accepts SSE-S3 only with `RUSTFS_SSE_S3_MASTER_KEY` (or a KMS). SeaweedFS has no SSE-S3 key
  source here: SSE-S3 fails and default bucket encryption breaks every later PUT.

## Fixes

- Removed source references from comments and docstrings; Spark app names `storage-selection` /
  `storage-selection-tests`; compose project `iceberg-storage-selection`; lab folders renamed
  (`storage_requirements_probe`, `block_vs_object`, `storage_standards`, `storage_governance`,
  `storage_scorecard`) and imports/`pytest.ini` updated. Test prefixes/namespaces renamed
  (`probe-`, `cmp_`, `commit_`, `select-`, block dir `/data/block/objsem`).
- `scorecard.py`: `Scenario.section` (numeric section ids) became `Scenario.category`
  (`performance` / `security` / `integrity` / `cost_ops`); the reference pick fields became `picks` /
  `weaker`; tests renamed to `test_scorecard_shortlists_the_reference_vendors`,
  `test_vendors_named_weaker_rank_below_the_picks`, `test_vendor_table_matches_the_vendor_matrix`.
- Credentials and keys moved to environment variables with marked local dev defaults: S3 keys,
  RustFS keys and SSE-S3 master key in compose, `analyst` key (`ANALYST_ACCESS_KEY` /
  `ANALYST_SECRET_KEY`) and `rnd_team` key (`RND_ACCESS_KEY` / `RND_SECRET_KEY`).
- `config/s3.json` keeps three local-only identities (`admin`, `analyst`, `rnd`) and the SSE-C key in
  `common/lakehouse.py` is a fixed test value; change both together with the env vars, and use a KMS and
  an identity provider in any real deployment.
- No Iceberg API changes were needed (`s3.delete-enabled`, `s3.delete.tags.*`, `s3.write.tags.*`,
  `s3.sse.type/key/md5`, `expire_snapshots`, `rewrite_data_files` are current for Iceberg 1.8).
