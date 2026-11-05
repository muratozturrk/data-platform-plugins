---
name: nessie-cli-api-and-migration
description: Operating Nessie through its interfaces: nessie-cli (interactive/scripted reference and content management), REST API v2 (references, commits, diff, content), Python client, export/import with nessie-quarkus-cli, and migrating tables from Apache Polaris, Hive Metastore or another REST catalog into Nessie (register_table, metadata-location import, cut-over, rollback). Use when automating Nessie from the control plane or planning a migration.
---

# CLI, API and Migration

## nessie-cli
`java -jar nessie-cli-<version>.jar` (or container) → `CONNECT TO https://nessie.platform.svc/api/v2 USING "nessie.authentication.type"="BEARER" "nessie.authentication.token"="…"`; commands: `LIST REFERENCES`, `CREATE BRANCH wap_x FROM main`, `USE wap_x`, `LIST CONTENTS`, `SHOW LOG`, `MERGE BRANCH wap_x INTO main`, `ASSIGN BRANCH main TO <hash>`, `DROP TAG …`, `CREATE NAMESPACE …`; scriptable with `--command-file`. Use for operators; the control plane uses the API.

## REST API v2 (`/api/v2`)
- References: `GET /trees`, `POST /trees` (create branch/tag from ref@hash), `PUT /trees/{ref}` (assign), `DELETE /trees/{ref}`.
- Commits: `POST /trees/{branch}/history/commit` with operations (`Put`/`Delete`/`Unchanged`) and expected hash; `GET /trees/{ref}/history` (log), `GET /trees/{ref}/diff/{other}`.
- Content: `GET /trees/{ref}/contents/{key}`, `GET /trees/{ref}/entries` (filter by namespace/type).
- Merge/transplant: `POST /trees/{branch}/history/merge`, `/transplant`.
- Repository config: `GET/POST /config/repository`.
Clients: Java (`nessie-client`), Python (`pynessie`-style or REST via httpx in the control plane), Go in community tools.

## Export / import
`nessie-quarkus-cli export --target-file repo.zip` and `import --target-file repo.zip` (per version store); use for version-store migration (e.g. ROCKSDB → JDBC), environment cloning (prod → staging with anonymised data pointers), and DR evidence.

## Migrating from Apache Polaris (or HMS / other REST catalogs)
1. Inventory: list namespaces/tables and current `metadata_location` per table via the Polaris REST API (`apache-polaris` plugin: `polaris-rest-api` skill); map Polaris catalogs/namespaces → Nessie branches/namespaces; map principal roles → Keycloak groups/CEL rules + Ranger.
2. Freeze writes on the source (Polaris catalog read-only grants) during cut-over; snapshot ids recorded.
3. Register in Nessie: Spark `CALL nessie.system.register_table(table => 'ns.t', metadata_file => 's3://…/metadata/00012-….metadata.json')` or Trino `CALL t_acme_lake.system.register_table(...)` on the target branch; or write `Put` operations with `IcebergTable` content (metadata location + ids) via the REST API in bulk; files are not moved.
4. Validate: row counts/snapshot ids per table equal on both catalogs; Trino/Spark/ClickHouse read via Nessie; Ranger policies applied; lineage re-pointed in OpenMetadata.
5. Cut over engines (catalog configs, Superset/dbt profiles via the control plane), keep Polaris read-only for the rollback window, then decommission.
6. Rollback: engines back to Polaris configs; Nessie branch kept for retry.
Cross-catalog tooling such as `mc table migrate --source-type nessie|rest` (AIStor) shows the same register-by-metadata-location pattern in reverse.

## Anti-patterns
- Copying metadata JSON files instead of registering locations (breaks snapshot lineage)
- Migrating with writers active on the source
- Scripts using `/api/v1` (v2 is current)
- Control plane calling the UI/CLI instead of the API

Sources: Nessie docs — Tools (nessie-cli, nessie-quarkus-cli export/import), REST API v2 reference (OpenAPI), Client configuration; Iceberg `register_table` procedure docs (Spark, Trino); Apache Polaris REST API docs; MinIO AIStor `mc table migrate` blog (cross-catalog migration pattern).
