# Catalog Landscape: Catalog Types and Why REST / Polaris

## Concept

Where the "current metadata" pointer lives decides consistency, how much logic sits in every
client, and who controls data access. This reference keeps the generic comparison short (full
version in the `iceberg-catalogs` skill of the apache-iceberg plugin) and focuses on what changes
when the catalog is Polaris.

## Catalog types

| Type | Examples | Pointer stored in | Assessment |
|---|---|---|---|
| File-system | Hadoop catalog | `version-hint.text` next to the table | Fine for tests; update may not be atomic on object stores, no locking |
| Service | Hive Metastore | Relational database behind Hive | Consistent, but heavy to scale and operate |
| Service | JDBC catalog | Any relational database | Lightweight, consistent through database locking |
| Service | Nessie | Versioned key-value store | Git-like branches, tags and commits across tables |
| Service | AWS Glue | Managed AWS service | Serverless, tied to AWS |
| REST | Iceberg REST reference server, Nessie, Gravitino, Polaris | Server's own store | One client per language, logic on the server |

## The three challenges of many catalog types

| Challenge | Client-side catalog (JDBC-style) | Polaris (REST + vending) |
|---|---|---|
| Client-side complexity | Each catalog needs its own client in Java, Python, Rust, Go | One REST client per language for every compliant catalog |
| Configuration | Catalog credentials + storage config + storage credentials in every engine | Catalog credential only |
| Authorization | Metadata permissions in the catalog, data permissions in storage: two systems | Polaris authorizes metadata and vends table-scoped data keys |

### Measured (PyIceberg 0.9.1 `SqlCatalog` vs Polaris 1.8.0)

| Test | JDBC-style `SqlCatalog` | Polaris |
|---|---|---|
| Engine configuration | 5 properties, 2 secrets (`s3.access-key-id`, `s3.secret-access-key`) | 4 properties, 1 secret (`credential`), no `s3.*` keys |
| Property change with read-only storage keys | Fails: client must upload `00002-...metadata.json` itself (`AccessDenied`) | Succeeds from a client with no storage keys; server writes the file |
| Reach of one key pair | 2 of 2 tables readable | Principal sees `finance` only; `hr` is 403 via catalog and `AccessDenied` via its keys |

```python
jdbc_conf = {"uri": "sqlite:///catalog.db", "warehouse": "s3://warehouse/jdbc_like",
             "s3.endpoint": S3_ENDPOINT,
             "s3.access-key-id": "admin", "s3.secret-access-key": "password"}   # long-lived bucket keys
rest_conf = {"uri": "http://polaris:8181/api/catalog", "warehouse": catalog,
             "credential": f"{client_id}:{client_secret}",                     # catalog identity only
             "header.X-Iceberg-Access-Delegation": "vended-credentials"}
```

(For a live connection to Polaris also set `scope`; the example only counts secrets.)

## One client, two REST servers

The same `RestCatalog` workload (create namespace, create table, append, add column, read) ran
unchanged against Polaris and `apache/iceberg-rest-fixture:1.8.1`; both returned 3 rows, columns
`id, city, region`, 1 snapshot. Only the connection properties differed:

| Server | Properties |
|---|---|
| Polaris | `uri`, `warehouse`, `credential`, `scope`, `header.X-Iceberg-Access-Delegation` |
| Reference server | `uri`, `s3.endpoint`, `s3.path-style-access`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key` |

| Server | Strength | Limitation (measured) |
|---|---|---|
| Iceberg REST reference server | Minimal, spec-only, easy to run | No auth, no vending (engines need storage keys), one catalog (`warehouse` ignored) |
| Apache Polaris | Auth, RBAC, vending, many catalogs, extra APIs | Clients must request a `PRINCIPAL_ROLE:` scope |

Endpoint discovery via `/v1/config`: Polaris advertised 38 endpoints, the reference server 26.
Polaris-only entries: policy CRUD and mappings, `applicable-policies`, generic tables,
`register-view`, OpenLineage ingestion (`POST /api/openlineage/v1/lineage[/batch]`). Both servers
wrote a new `metadata.json` for a plain-HTTP property commit (server-side logic is spec behaviour).

## Choosing

| Need | Choice | Reason |
|---|---|---|
| Local tests, single user | File-system or SQLite-backed catalog | No service to run |
| Existing Hive estate | Hive catalog, then migrate | Compatibility |
| Branches across tables | Nessie (optionally federated under Polaris) | Git-like semantics |
| Several engines and teams with access control | Polaris with vending | One identity per engine, no bucket keys |
| Keep other catalogs but govern centrally | Polaris external catalogs | See the `polaris-catalog-federation` skill |

Other open REST implementations: Nessie, Apache Gravitino. Tabular was the first (commercial)
REST catalog and closed after the Databricks acquisition.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| Anyone with bucket keys reads every table | Catalog without its own access model | Use a catalog that vends; revoke static keys |
| Read-only users cannot even set a property | Client-side catalog writes metadata itself | Server-side (REST) catalog |
| Same code fails on Polaris but works on the fixture | Missing `scope`, or relying on storage keys | Add `scope=PRINCIPAL_ROLE:ALL`, enable vending |
| Engine bypasses Polaris governance | Direct storage access | Polaris governs only engines that go through it |

## Related

- [polaris_overview.md](polaris_overview.md), [rest_catalog_and_credential_vending.md](rest_catalog_and_credential_vending.md).
- Example tests: `catalog_options/`, `rest_spec/` in [../examples/catalog-basics/README.md](../examples/catalog-basics/README.md).
