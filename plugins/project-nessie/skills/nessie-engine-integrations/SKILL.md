---
name: nessie-engine-integrations
description: Connecting the platform's engines to Nessie: Trino (native nessie catalog type vs Iceberg REST, branch references, limitations), Spark (Nessie Spark SQL extensions, branch/tag DDL, version pinning), Flink (Nessie catalog, streaming writes to branches), ClickHouse (DataLakeCatalog REST), dbt-trino and PyIceberg; consistent catalog naming and version compatibility matrix. Use when configuring or troubleshooting any engine against Nessie.
---

# Engine Integrations

## Trino (platform SQL layer)
- **Native** `iceberg.catalog.type=nessie` (platform default): `iceberg.nessie-catalog.uri=https://nessie.platform.svc/api/v2`, `iceberg.nessie-catalog.ref=t-acme` (branch per institution; or `main` + namespaces), `iceberg.nessie-catalog.default-warehouse-dir=s3://hkm-lake/t-acme/`, `iceberg.nessie-catalog.authentication.type=BEARER` + `authentication.token=${ENV:…}` (service principal token from Keycloak, refreshed by the control plane), S3 via `fs.native-s3.enabled` with per-institution credentials from OpenBao. Supports reads/writes, `SHOW BRANCHES`/`SHOW TAGS` style metadata where available, `FOR VERSION AS OF`.
- **REST** `iceberg.catalog.type=rest` against `/iceberg/{ref}/` with `vended-credentials-enabled=true` and `oauth2.token`: use when vending must replace static S3 keys; S3-compatible stores only; watch for warehouse-path/credential resolution differences ("Cannot obtain metadata") and test per Trino version.
- Pick one type per catalog name; Trino Gateway routes institutions to catalogs by client tags; Ranger policies reference the catalog name.

## Spark
`org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:<nessie-version>` + `iceberg-spark-runtime` + `iceberg-aws-bundle` (versions pinned together); `spark.sql.extensions=NessieSparkSessionExtensions,IcebergSparkSessionExtensions`; catalog `type=rest` (Nessie Catalog with signing) or `catalog-impl=org.apache.iceberg.nessie.NessieCatalog` (native, `ref`, `uri=/api/v2`). SQL: `CREATE BRANCH wap_x IN nessie FROM main`, `USE REFERENCE wap_x IN nessie`, `MERGE BRANCH wap_x INTO main IN nessie`, `DROP BRANCH`, `CREATE TAG rel_20261105 IN nessie`; `table@branch` and `table@branch#hash` addressing.

## Flink
Iceberg Flink connector with Nessie catalog (`catalog-impl=org.apache.iceberg.nessie.NessieCatalog`, `uri`, `ref`, `warehouse`, `authentication.type=BEARER`) or REST (`type=rest`, `uri=/iceberg/{ref}/`, OAuth2 credential); streaming upserts write to the institution branch; checkpoint-aligned commits; promotion to `main` by the WAP job.

## ClickHouse
`CREATE DATABASE t_acme_lake ENGINE = DataLakeCatalog('https://nessie.platform.svc/iceberg/t-acme/') SETTINGS catalog_type='rest', warehouse='t_acme', auth_header='Bearer …', storage_endpoint='https://objectscale.hkm.internal'` — read access to Iceberg tables for serving-layer loads (`INSERT INTO … SELECT`); Iceberg tables only.

## dbt (dbt-trino) and PyIceberg
dbt-trino targets the Trino catalog (`catalog: t_acme_lake`) — branch selection happens in Trino's catalog config; for branch-per-run WAP use a second Trino catalog bound to the WAP branch or Spark. PyIceberg: `catalog: { type: rest, uri: …/iceberg/t-acme/, credential, scope: "catalog sign" }` for notebooks and the control plane's metadata jobs.

## Compatibility matrix (maintain in repo)
| Component | Version pin | Notes |
|---|---|---|
| Nessie | 0.108.x | release notes per upgrade |
| Iceberg | 1.11.x | Nessie docs reference `iceberg-spark-runtime-3.5_2.12:1.11.0` |
| Trino | 484 | nessie + rest types |
| Spark | 3.5 (Scala 2.12) | extensions artifact must match Nessie version |
| Flink | 1.20/2.x | Iceberg Flink runtime matching Iceberg |
| ClickHouse | 25.x+ | DataLakeCatalog REST |

## Anti-patterns
- Different catalog names per engine for the same Nessie reference (tables "missing")
- Extensions version drift between Spark and Nessie
- Engines committing to `main`
- Trino REST without vending (no credentials to read files)

Sources: Nessie docs — Trino, Spark, Flink, Iceberg REST client guides, "Getting started" (artifact versions); Trino Iceberg connector docs (nessie and rest catalog types); Apache Iceberg Nessie catalog docs; ClickHouse Nessie catalog guide; dbt-trino docs; nsalexamy lakehouse-on-Kubernetes walkthrough.
