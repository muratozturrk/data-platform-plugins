---
name: nessie-iceberg-rest-catalog
description: Nessie Catalog, the Iceberg REST endpoint of Nessie: warehouses and object-store configuration (S3-compatible ObjectScale, Ceph RGW, MinIO AIStor; GCS/ADLS), S3 request signing versus vended session credentials, per-reference REST prefixes, OAuth2 against Keycloak, client configuration for Spark/Flink/PyIceberg/ClickHouse/Trino, and the security properties of each mode. Use when configuring Nessie Catalog or any engine via Iceberg REST.
---

# Nessie Catalog (Iceberg REST)

## Core Concepts
- Endpoint: `https://nessie.platform.svc/iceberg/` (default reference) or `/iceberg/{ref}/` (branch or `ref@hash`); `GET /iceberg/v1/config` returns catalog defaults pushed to clients.
- **Warehouses** (`nessie.catalog.warehouses.<name>.location=s3://bucket/path`, `nessie.catalog.default-warehouse`): each warehouse maps to a bucket/prefix and an object-store profile.
- **Object-store profiles** (`nessie.catalog.service.s3.default-options.*` and per-bucket `nessie.catalog.service.s3.buckets.<name>.*`): endpoint, region, path-style, credentials (access key via secret manager or `${ENV}`), `request-signing-enabled`, `assume-role`/session-credential settings, `auth-type`. GCS/ADLS equivalents exist.
- **Two security modes**:
  - *S3 request signing* (`CATALOG_S3_SIGN`): the client never holds credentials; each request is signed by Nessie; Spark/Flink/PyIceberg support it; fail-closed on snapshot lookup errors since 0.108.1.
  - *Vended credentials*: Nessie returns short-lived session credentials scoped to the table location (read-only ARN fix in 0.108.2); required for Trino (`iceberg.rest-catalog.vended-credentials-enabled=true`), which cannot sign.
  Both require the object store to support STS/assume-role (ObjectScale and Ceph RGW STS; AIStor STS) — verify on each store in staging.
- Authentication: OAuth2 client credentials against Keycloak (`oauth2-server-uri`, `credential=client:secret`, `scope=catalog sign`); engines use service principals per institution; Trino supplies a token (`iceberg.rest-catalog.oauth2.token`) because it cannot configure the token endpoint.
- Table metadata: Nessie writes `metadata.json` under the warehouse; Nessie Catalog can serve metadata without the client reading it from storage.

## Client configuration
```properties
# Spark
spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nessie.type=rest
spark.sql.catalog.nessie.uri=https://nessie.platform.svc/iceberg/t-acme/
spark.sql.catalog.nessie.oauth2-server-uri=https://keycloak.platform.svc/realms/platform/protocol/openid-connect/token
spark.sql.catalog.nessie.credential=${SPARK_NESSIE_CLIENT}:${SPARK_NESSIE_SECRET}
spark.sql.catalog.nessie.scope=catalog sign
spark.sql.extensions=org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```
```properties
# Trino (REST path; native nessie type is the platform default — see engine-integrations)
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://nessie.platform.svc/iceberg/t-acme/
iceberg.rest-catalog.vended-credentials-enabled=true
iceberg.rest-catalog.oauth2.token=${ENV:NESSIE_T_ACME_TOKEN}
fs.native-s3.enabled=true
s3.endpoint=https://objectscale.hkm.internal
```
ClickHouse: `CREATE DATABASE ... ENGINE = DataLakeCatalog SETTINGS catalog_type='rest', warehouse=..., auth_header=...` against the same endpoint; PyIceberg: `type: rest`, `uri`, `credential`, `scope`.

## Patterns
- One warehouse per institution bucket/prefix (`s3://hkm-lake/t-acme/`), referenced from the institution branch; Nessie-side bucket options carry the institution's STS role.
- Same catalog name in every engine (`nessie` or `t_acme_lake`) so tables are visible everywhere.
- Object-store portability: identical Nessie warehouse config tested against ObjectScale, Ceph RGW and AIStor in CI (platform S3-conformance suite).

## Anti-patterns
- Static shared S3 keys inside engine configs while Nessie Catalog could sign/vend
- Mixing `nessie` native type and `rest` type for the same catalog name on one engine
- Pointing Trino REST at `/api/v2` (native API) instead of `/iceberg/`

Sources: Nessie docs — Iceberg REST / Nessie Catalog configuration (warehouses, S3/GCS/ADLS options, signing, assume-role), "Getting started with Nessie and Iceberg REST", "Trino with Iceberg REST"; release notes 0.108.1–0.108.2; ClickHouse Nessie catalog guide; Trino Iceberg connector docs.
