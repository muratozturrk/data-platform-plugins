# External Catalog Products as Polaris Sees Them

What matters when one of these catalogs sits behind a Polaris `EXTERNAL` catalog. Product
features in general (Nessie branching, catalog selection): `iceberg-catalogs` skill of the
apache-iceberg plugin. Version status as measured with Polaris 1.8.0:

| Catalog | Version tested | Federation through Polaris | Note |
|---|---|---|---|
| Iceberg REST reference server | 1.8.1 | Works (read, write, no sync delay) | `remoteCatalogName: default` |
| Apache Gravitino Iceberg REST | 1.3.0 | Works | `remoteCatalogName: default_catalog`; needs a credential provider |
| Lakekeeper | 0.13.8 | **Fails** (500 on load) | Brotli responses Polaris cannot decode |
| Project Nessie | not run | Not tested | Needs a release with an Iceberg REST endpoint |
| AWS Glue (Iceberg REST) | not run | Not tested | Needs an AWS account |

In every case grants on the federated catalog work only at catalog level (measured on the
reference server; the same entity model applies to the others).

## Apache Gravitino

A federated metadata system for data and AI assets, deployable per region, with a unified
metadata model and direct (not periodically synchronized) metadata management. It ships a
standalone Iceberg REST server.

Pattern: one Gravitino per region (local control of regional metadata) plus Polaris on top as
the global access point, next to internal Polaris catalogs.

```python
federate(admin, "eu_gravitino", "http://gravitino:9001/iceberg", "default_catalog", "s3://warehouse/gravitino/")
eu = root.pyiceberg("eu_gravitino").load_table(f"{ns}.orders").scan().to_arrow()   # 3 rows via Gravitino
na = root.pyiceberg("na_lakehouse").load_table("sales.orders").scan().to_arrow()   # 2 rows, internal
```

Gravitino server settings used (environment of `apache/gravitino-iceberg-rest`):

| Variable | Value |
|---|---|
| `GRAVITINO_ICEBERG_REST_CATALOG_BACKEND` / `_URI` | `jdbc` / `jdbc:sqlite:file:/tmp/gravitino_catalog.db` |
| `GRAVITINO_ICEBERG_REST_WAREHOUSE` | `s3://warehouse/gravitino/` |
| `GRAVITINO_ICEBERG_REST_IO_IMPL` | `org.apache.iceberg.aws.s3.S3FileIO` |
| `GRAVITINO_ICEBERG_REST_S3_*` | access key, secret, endpoint, region, path-style |
| `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS` | `s3-secret-key` |

Measured:

- `remoteCatalogName: default` is accepted at create (201) but every load fails with
  `NoSuchWarehouseException: Couldn't find Iceberg configuration for catalog default`; the
  standalone server's catalog is `default_catalog`.
- Without a credential provider Gravitino 1.3.0 refuses to create tables.
- A write through Polaris is committed by Gravitino: a direct Gravitino read sees ids 1-4, and
  Polaris and Gravitino report the same `metadata-location`.

## Lakekeeper

An Iceberg REST catalog in Rust: single binary, projects containing warehouses, fine-grained
access control, built-in authentication support, soft deletion, change events.

Soft deletion is configured per warehouse (`delete-profile`):

```python
body = {"warehouse-name": "partner_wh", "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {"type": "s3", "bucket": "warehouse", "key-prefix": "lakekeeper",
                            "endpoint": S3_ENDPOINT, "region": "us-east-1", "path-style-access": True,
                            "sts-enabled": False, "flavor": "s3-compat"},
        "storage-credential": {"type": "s3", "credential-type": "access-key",
                               "aws-access-key-id": "admin", "aws-secret-access-key": "password"},
        "delete-profile": {"type": "soft", "expiration-seconds": 3600}}
requests.post(f"{LAKEKEEPER_URI}/management/v1/warehouse", json=body)
# undrop: GET .../warehouse/{wid}/deleted-tabulars, then
requests.post(f".../warehouse/{wid}/deleted-tabulars/undrop", json={"targets": [{"type": "table", "id": tid}]})
```

Measured with 0.13.8:

- A dropped table is listed under `deleted-tabulars` with an expiration 1 h after deletion;
  undrop returns 204 and the table comes back with its 3 rows.
- Data access uses **remote signing**, not vended keys: the table config contains
  `s3.remote-signing-enabled=true`, `s3.signer=S3V4RestSigner`, `py-io-impl`, and no access key.
  PyIceberg then switches to fsspec and needs `s3fs`.
- Federation: the external catalog is created (201, `uri` = `http://lakekeeper:8181/catalog`,
  `remoteCatalogName` = warehouse name), but loading a table through Polaris returns 500. The
  Polaris log shows `java.lang.UnsatisfiedLinkError` in the brotli decoder: Lakekeeper replies
  brotli-compressed and the Polaris 1.8.0 image cannot decode it. Lakekeeper itself stays healthy
  (direct read: 2 rows). The test asserts this failure so a version change is noticed.

## Project Nessie

"Git for data": catalog-level branches, tags, commits and multi-table transactions. Pattern with
Polaris: ingestion and validation on a Nessie branch, merge into `main`, analysts query `main`
through a Polaris external catalog with central RBAC. Federation needs Nessie's Iceberg REST
endpoint, which only newer Nessie releases provide; older servers speak only the Nessie API.
Not verified here: Docker Hub carries Nessie only up to 0.76.6 (no Iceberg REST endpoint), and
newer images are published on ghcr.io / quay.io.

## AWS Glue

AWS's managed catalog, with an Iceberg REST interface, integrated with Athena, Redshift Spectrum,
EMR and Glue ETL, secured with IAM. Pattern: AWS-centric teams keep Glue; teams outside AWS reach
the same tables through a Polaris external catalog under Polaris governance. Not tested (needs an
AWS account); expect the same catalog-level grant limit as for other external catalogs.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `Couldn't find Iceberg configuration for catalog default` | Gravitino standalone catalog is `default_catalog` | `remoteCatalogName: default_catalog` |
| Gravitino refuses table creation | No credential provider | `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS=s3-secret-key` |
| 500 through Polaris, `UnsatisfiedLinkError` brotli | Lakekeeper 0.13.8 + Polaris 1.8.0 | Access Lakekeeper directly or retest with newer versions |
| PyIceberg fails reading Lakekeeper tables | Remote signing needs fsspec | Install `s3fs` |
| Nessie federation impossible | Release without Iceberg REST endpoint | Use a Nessie release that serves Iceberg REST |

## Related

- [federation-mechanics.md](federation-mechanics.md), [gradual-migration.md](gradual-migration.md).
- `iceberg-catalogs` skill (apache-iceberg plugin): catalog options and Nessie branching.
