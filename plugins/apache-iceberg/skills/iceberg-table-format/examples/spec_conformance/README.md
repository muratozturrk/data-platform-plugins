# Iceberg specification examples

Every module reads the raw files Spark wrote (metadata.json, Avro manifest lists and manifests,
Parquet, Puffin) and checks them against the [table spec](https://iceberg.apache.org/spec/) and the
[Puffin spec](https://iceberg.apache.org/puffin-spec/).

| File | Shows |
|---|---|
| `conformance/spec_checker.py` | Conformance checker: required fields per version (rules as data), Avro field ids, id references, snapshot and sequence rules, manifest counts, partition summaries, column bounds vs Parquet data |
| `conformance/test_conformance.py` | v1/v2 pass (922 / 1,128 checks); v3 from Iceberg 1.8.1 misses only row lineage; string bound truncation; 9 injected metadata breaks and 3 Avro field-id breaks detected |
| `format_versions/versions.py`, `test_format_versions.py` | Same workload at v1/v2/v3: metadata keys, field ids added per version, position deletes vs deletion vectors, v3 defaults and types in 1.8.1 |
| `snapshots/snapshots.py`, `test_snapshots.py` | Immutable metadata chain, sequence-number inheritance, manifest reuse, compaction data vs file sequence number, optimistic concurrency conflict + retry + orphan |
| `rest_catalog/test_rest_catalog.py` | `/v1/config` endpoints, `loadTable` = metadata file, scan-planning endpoint absent in the 1.8.1 fixture |
| `puffin/puffin.py`, `test_puffin.py` | Puffin footer parser, theta-sketch NDV decoder, deletion-vector decoder (roaring bitmap + CRC-32), corruption detection |
| `compatibility/compat.py`, `test_compatibility.py` | In-place upgrade v1 -> v2 -> v3, downgrade refusal, reader version gate, PyIceberg 0.9.1 read/append matrix |
| `common/lakehouse.py` | Spark + PyIceberg on one REST catalog, raw S3/REST/Avro helpers |
| `common/workload.py` | Order table workload at any format version (several types, 2 transforms, sort order, evolution, delete) |
| `conftest.py` | `measure` fixture: measured values printed in a `measurements` section of the pytest summary |

## Prerequisites
- Docker with Compose v2, about 6 GB RAM.
- Images: `tabulario/spark-iceberg` (tests expect Spark 3.5.5 + Iceberg 1.8.1, Python 3.10),
  `apache/iceberg-rest-fixture:1.8.1` (pinned: newer fixtures reject v3 commits from 1.8.1 clients),
  `chrislusf/seaweedfs`. The Dockerfile adds fastavro 1.10.0, pyroaring 1.0.0, zstandard 0.23.0,
  PyIceberg 0.9.1, boto3, pytest 8.3.4.
- Several assertions document Iceberg 1.8.1 behavior (no row lineage, v3 defaults dropped,
  PyIceberg 0.9.1 v3 limits). On newer versions those tests are expected to change; pin the Spark
  image tag if `latest` moves.

## How to run
```bash
docker compose -p iceberg-spec-conformance up -d --build
docker compose -p iceberg-spec-conformance exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-spec-conformance down -v
```
On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

## Environment notes
- `http-client.apache.max-connections=500`: the default pool (50) is too small for v3 deletion
  vector writes.
- `config/s3.json` holds local-only dev credentials for SeaweedFS; change them together with the
  `AWS_*` environment variables read by compose.

## Fixes
- Hardcoded credentials in `docker-compose.yml` replaced by `${AWS_ACCESS_KEY_ID:-admin}`-style
  variables; Python `os.getenv` defaults marked `# local dev default`.
- Lab folders renamed (`conformance`, `format_versions`, `snapshots`, `rest_catalog`, `puffin`,
  `compatibility`); cross-module imports (`compatibility` uses `conformance` and `snapshots`) and
  `pytest.ini` testpaths updated.
- Spark app names and compose project name made generic; comments pointing to other material
  replaced with the concrete procedure (`remove_orphan_files`) or topic.
- No Iceberg API changes were needed: `compute_table_stats`, `rewrite_data_files` and the
  `format-version` property match Iceberg 1.8.1.
