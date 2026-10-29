# Polaris Overview: What It Is and Where It Sits

## Concept

Apache Polaris is an open-source implementation of the Apache Iceberg REST catalog specification,
donated to the Apache Software Foundation by Snowflake, with contributors from Dremio, AWS, Google,
Microsoft, Confluent, LanceDB and others. It sits between query engines and object storage:

```
Spark / Trino / Dremio / Snowflake / Flink / PyIceberg / Polars / DuckDB
        |  Iceberg REST API (+ OAuth2 token, scope PRINCIPAL_ROLE:...)
        v
   Apache Polaris  --- RBAC: principals -> principal roles -> catalog roles -> grants
        |  writes metadata.json, swaps the pointer, vends table-scoped STS keys
        v
   Object storage (S3 / S3-compatible / ADLS / GCS)
```

A lakehouse catalog is not an enterprise data catalog (Collibra, Alation). It serves engines, not
people: for each table name it says which `metadata.json` is current, and it serializes commits.
Generic Iceberg background (metadata tree, snapshots, manifests) is in the `iceberg-table-format`
skill of the apache-iceberg plugin; generic catalog theory is in its `iceberg-catalogs` skill.

## What Polaris adds on top of the REST spec

| Feature | What it addresses | Observed in 1.8.0 |
|---|---|---|
| Multiple catalogs in one server | Catalogs per team / sensitivity without separate servers | `warehouse=<catalog>` in `/v1/config` returns a distinct `prefix` per catalog |
| Credential vending | Engines hold no bucket keys | `X-Iceberg-Access-Delegation: vended-credentials` returns temporary `ASIA...` keys scoped to one table |
| RBAC in the catalog | Same rules for every engine | Principal limited to `finance` namespace gets 403 on `hr`, and its vended keys get `AccessDenied` on `hr` files |
| External catalogs | Other REST catalogs visible under Polaris RBAC | See the `polaris-catalog-federation` skill |
| Extra APIs | Generic tables, policies, `register-view`, OpenLineage ingestion | 15 endpoints advertised by Polaris but not by the Iceberg reference server |

## Two APIs, one server

| API | Base path | Used for |
|---|---|---|
| Management API v1 | `/api/management/v1` | Catalogs, principals, principal roles, catalog roles, grants |
| Iceberg REST catalog API | `/api/catalog/v1/{prefix}` | Namespaces, tables, views, commits (the Iceberg spec) |
| Polaris catalog extensions | `/api/catalog/polaris/v1/{catalog}` | Generic tables, policies, applicable policies |
| OAuth2 token endpoint | `/api/catalog/v1/oauth/tokens` | Client-credentials grant for a principal |
| Health (management port) | `:8182/q/health/ready` | Readiness probe in the example stacks |

## Worked example: creating a vending catalog (from `common/polaris.py`)

```python
storage = {"storageType": "S3", "allowedLocations": [base_location],
           "region": "us-east-1", "endpoint": S3_ENDPOINT, "pathStyleAccess": True,
           "roleArn": "arn:aws:iam::000000000000:role/lake-vendor", "stsEndpoint": S3_ENDPOINT}
props = {"default-base-location": base_location,
         "polaris.config.drop-with-purge.enabled": "true"}
POST /api/management/v1/catalogs
  {"catalog": {"name": name, "type": "INTERNAL", "properties": props, "storageConfigInfo": storage}}

# give the bootstrap admin content rights in the new catalog
PUT /api/management/v1/catalogs/{name}/catalog-roles/catalog_admin/grants
  {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
```

`stsUnavailable: true` instead of `roleArn` + `stsEndpoint` creates a catalog that does not vend.
Polaris itself writes metadata with its own storage keys (`AWS_ACCESS_KEY_ID` in the Polaris
container); engines never see them.

## Connecting an engine (PyIceberg, no storage keys)

```python
RestCatalog("prod", **{
    "uri": "http://polaris:8181/api/catalog", "warehouse": "<catalog name>",
    "credential": f"{client_id}:{client_secret}",
    "oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
    "scope": "PRINCIPAL_ROLE:ALL",
    "header.X-Iceberg-Access-Delegation": "vended-credentials",
    "s3.endpoint": S3_ENDPOINT, "s3.path-style-access": "true", "s3.region": "us-east-1"})
```

The `s3.endpoint`/`path-style` keys are only needed for S3-compatible stores; there are no
`s3.access-key-id`/`s3.secret-access-key` entries.

## Managed Polaris

| Option | Who operates it | Extras (not portable) |
|---|---|---|
| Polaris OSS | You (persistence, upgrades, keys, monitoring, maintenance) | None |
| Dremio Catalog | Vendor, or Dremio Enterprise on your infrastructure | Automatic compaction, vacuuming, clustering |
| Snowflake Open Catalog | Vendor | Snowflake RBAC and storage credential management, catalog sync |

Not testable locally; the REST interface stays portable, the vendor extras do not move with the
catalog. On self-managed Polaris you schedule maintenance yourself (see the `iceberg-maintenance`
skill of the apache-iceberg plugin). Details: `polaris-snowflake`, `polaris-dremio` skills.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `invalid_scope: The scope is invalid` on connect | PyIceberg default scope `catalog` | `scope=PRINCIPAL_ROLE:ALL` or `PRINCIPAL_ROLE:<role>` |
| Engine can read every table's files | Bucket keys handed to engines | Remove static keys; enable vending; let Polaris be the only path |
| New catalog, root still gets 403 on content | Bootstrap `catalog_admin` lacks content privilege | Grant `CATALOG_MANAGE_CONTENT` to `catalog_admin` |
| Drop catalog fails | Catalog still has namespaces, tables or custom roles | Drop content (purge) and custom catalog roles first |
| Two teams' tables in one catalog with ad hoc grants | Not using multi-catalog | One catalog per governance boundary, addressed by `warehouse` |

## Related

- [rest_catalog_and_credential_vending.md](rest_catalog_and_credential_vending.md) - handshake, commits, vending.
- [catalog_landscape.md](catalog_landscape.md) - catalog types and why REST/Polaris.
- [polaris_roadmap.md](polaris_roadmap.md) - generic tables, policies, unreleased items.
- Example stack: [../examples/catalog-basics/README.md](../examples/catalog-basics/README.md).
