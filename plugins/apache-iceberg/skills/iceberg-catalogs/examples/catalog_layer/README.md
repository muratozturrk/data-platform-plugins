# Catalog layer lab environment

One engine container (Spark + PyIceberg + pytest) and three catalog services (Iceberg REST
reference server, Apache Polaris, Project Nessie) plus a Hadoop catalog, all on one SeaweedFS S3
store with STS. Six lab folders share this environment.

| File | Shows |
|---|---|
| `common/lakehouse.py` | Spark session with 4 catalogs (`rest`, `polaris`, `nessie`, `hadoop`), PyIceberg `RestCatalog`/`SqlCatalog`, S3 helpers |
| `common/iceberg_http.py` | Plain-HTTP Iceberg REST client: config, namespaces, tables, commits with requirements, transactions, register |
| `common/polaris.py` | Polaris management API client (catalogs, principals, roles, grants) and PyIceberg-on-Polaris with vended credentials |
| `catalog_pointer/` | 5-column JDBC catalog entry, pointer swap per commit, name -> data-file resolution, stale writer rejected then retried, older-pointer view |
| `requirements_probe/` | LoadTable latency, 4-writer contention without lost commits, capability probe, must/preference scorecard |
| `rest_spec/` | Same REST tests on two servers: handshake, lifecycle, server-side commit, 409 on stale `assert-current-schema-id` / `assert-ref-snapshot-id`, all-or-nothing transaction |
| `catalog_options/` | Same SQL on Hadoop/REST/Polaris/Nessie, where each keeps the pointer, `register_table` migration, split-brain, Nessie commit log |
| `catalog_scenarios/` | Eight selection scenarios on the scorecard; Nessie zero-copy stress-test branch and atomic two-table merge |
| `access_control/` | Polaris RBAC (reader/writer roles), table- and mode-scoped vended S3 keys, revoke on next request, no-STS failure |
| `config/s3.json`, `config/iam.json` | SeaweedFS static identity and STS role `lake-vendor` that Polaris assumes |

## Prerequisites

- Docker with Compose v2, ~4 GB free RAM.
- Images: `tabulario/spark-iceberg` (Spark 3.5.5, Iceberg 1.8.1), `apache/iceberg-rest-fixture:1.8.1`,
  `apache/polaris:latest` (tested as 1.8.0), `projectnessie/nessie:0.76.6`, `chrislusf/seaweedfs` (tested 4.48).
- Installed in the image: PyIceberg 0.9.1 (`sql-sqlite`, `pyarrow`), pytest 8.3.4, boto3.
- Credentials are local development values. Override with env vars `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, `POLARIS_ROOT_CLIENT_ID`, `POLARIS_ROOT_CLIENT_SECRET`; if you change the
  S3 keys, change `config/s3.json` to match. `config/s3.json` and the STS `signingKey` in
  `config/iam.json` hold local-only dev values - replace them before any shared use.

## How to run

```bash
docker compose -p iceberg-catalog-layer up -d --build
docker compose -p iceberg-catalog-layer exec -w /home/iceberg/labs spark-iceberg python -m pytest -v
docker compose -p iceberg-catalog-layer down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. Measured values
(latencies, conflict counts, credential matrix) print in the `measurements` section at the end.
Expected: 51 tests pass in about 2-3 minutes.

## Environment notes

- Polaris and Nessie keep state in memory: restarting them resets catalogs, principals and branches.
- Polaris vending needs STS: SeaweedFS implements `AssumeRole` with session policies when started with
  `-s3.iam.config`; Polaris is pointed at it with `stsEndpoint`.
- Iceberg 1.8.1 Spark executors call Polaris' credential-refresh endpoint without the OAuth token, so
  the Spark config sets `client.refresh-credentials-enabled=false`.
- Catalog defaults the labs change: `catalog_admin` gets `CATALOG_MANAGE_CONTENT`,
  `polaris.config.drop-with-purge.enabled=true`, and for the migration catalog
  `polaris.config.allow.unstructured.table.location=true`.
- `NessieCatalog` sets `gc.enabled=false`; Nessie tables are dropped without `PURGE`.

## Fixes

- Lab folders renamed to topic names (`catalog_pointer`, `requirements_probe`, `rest_spec`,
  `catalog_options`, `catalog_scenarios`, `access_control`); all imports and `pytest.ini` `testpaths` updated.
- Scenario identifiers changed from numbered sections to keys (`hms_migration`, `aws_fintech`,
  `healthcare_residency`, `saas_simplicity`, `multicloud_federation`, `stress_test_clones`,
  `legacy_query_federation`, `python_first`); `Scenario` fields renamed to
  `key/expected_outcome/rejected`; test functions renamed accordingly
  (`test_scorecard_reproduces_the_expected_outcome`, `test_python_first_scenario_picks_a_deprecated_catalog`).
- Deprecation warning text no longer cites a source section; requirement `why` strings use topic words.
- Polaris catalog and namespace names (`probe_lab`, `rest_spec`, `options_*`, `acl_*`), Spark app names
  (`lakehouse-tests`, `lakehouse-catalogs`), Hadoop warehouse path (`/tmp/hadoop-warehouse`) and the
  compose project name (`iceberg-catalog-layer`) renamed.
- Secrets: compose credentials now `${VAR:-default}`; Polaris bootstrap credentials derived from
  `POLARIS_ROOT_CLIENT_ID/SECRET`; Python defaults marked `# local dev default`; STS `signingKey` in
  `config/iam.json` replaced with a placeholder value (base64 of `local-dev-sts-signing-key-CHANGE-ME!`).
- `projectnessie/nessie:latest` pinned to `0.76.6` (the version `latest` resolved to).
