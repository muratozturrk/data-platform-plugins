# Connecting Dremio to Polaris

## Concept

Dremio has two connectors that reach Polaris:

| Source type | For | API type name | Edition |
|---|---|---|---|
| Iceberg REST Catalog | Polaris OSS, or any Iceberg REST catalog | `RESTCATALOG` | Enterprise 26+ / Cloud |
| Snowflake Open Catalog | Hosted Polaris (HTTPS only) | `SNOWFLAKEOPENCATALOG` | Enterprise 26+ / Cloud |

```
Dremio source ──token (credential client_id:secret, scope)──► Polaris
              ──list namespaces/tables, load table (warehouse = catalog name)──►
              ◄── metadata (+ table-scoped temporary keys if "Use vended credentials")
              ──read/write data files──► object storage
```

## Iceberg REST Catalog source settings

| UI field | Key | Value for Polaris OSS |
|---|---|---|
| Name | `name` | e.g. `PolarisCatalog`, which becomes the first part of every table name |
| Endpoint URI | `restEndpointUri` | `http(s)://<polaris-host>:8181/api/catalog` |
| Use vended credentials | checkbox | On, if the Polaris catalog has an STS role |
| Catalog Properties | `propertyList` | `warehouse` = Polaris catalog name. `scope` = `PRINCIPAL_ROLE:ALL` or `PRINCIPAL_ROLE:<role>` |
| | | Only with vending off: `fs.s3a.aws.credentials.provider` = `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` |
| Catalog Credentials | `secretPropertyList` | `credential` = `<client_id>:<client_secret>` |
| | | Only with vending off: `fs.s3a.access.key`, `fs.s3a.secret.key` |

The Snowflake Open Catalog source takes the endpoint, the client id and secret, allowed
namespaces, and the same storage options: S3 keys, S3 assumed role (`fs.s3a.assumed.role.arn`),
Azure OAuth, or GCS.

## Storage access modes

| Mode | Dremio holds | Use when |
|---|---|---|
| Vended credentials | Nothing long-lived | Polaris can assume a storage role (recommended) |
| Static `fs.s3a` keys | Bucket keys with all their permissions | The catalog cannot vend, or a client does not support it |
| Assumed role (Open Catalog source) | Its own role | Cross-account access without keys |

Measured against Polaris 1.8.0, with an Iceberg REST client standing in for Dremio:

```python
props = {"uri": "http://polaris:8181/api/catalog", "warehouse": "my_catalog",
         "credential": f"{cid}:{secret}", "scope": "PRINCIPAL_ROLE:ALL",
         "s3.endpoint": "http://seaweedfs:8333", "s3.path-style-access": "true", "s3.region": "us-east-1"}
static = props | {"s3.access-key-id": "admin", "s3.secret-access-key": "password"}
vended = props | {"header.X-Iceberg-Access-Delegation": "vended-credentials"}
```

Both wrote and read 3 rows. Only `static` needed keys in the engine. A load-table response
without the vending header contained no `s3.*key*` entries.

## Edition check (Dremio OSS 26.0.5)

```python
dremio.sql('ALTER SYSTEM SET "plugins.restcatalog.enabled" = true')
dremio.sql('ALTER SYSTEM SET "plugins.restcatalog.mutable.enabled" = true')
r = session.post(f"{url}/api/v3/catalog", json={"entityType": "source", "name": "polaris",
                                                "type": "RESTCATALOG", "config": rest_source})
# 400 {"errorMessage": "An invalid value was found: RESTCATALOG"}   (same for SNOWFLAKEOPENCATALOG)
```

The OSS image contains the REST catalog plugin classes but registers no source type for them.

## Dremio OSS fallback: registerTable

If Dremio OSS must produce tables that other engines read through Polaris:

1. Dremio writes to a catalog it supports (Nessie in the tests) on the same object storage.
2. Take the table's current `metadataLocation` from that catalog.
3. Register it in Polaris:

```python
c = root.catalog("my_catalog")
c.create_namespace("dremio")
c.c("POST", "/namespaces/dremio/register",
    json={"name": "high_value_orders", "metadata-location": location})
root.pyiceberg("my_catalog").load_table("dremio.high_value_orders").scan().to_arrow()  # 7 rows
```

The Polaris catalog must allow the foreign location:

```python
admin.create_catalog("my_catalog", "s3://warehouse/polaris",
                     allowed=["s3://warehouse/polaris", "s3://warehouse/nessie"],
                     properties={"polaris.config.allow.unstructured.table.location": "true"})
```

With that flag off, registration failed with 403 `Invalid locations`, and a namespace with a
custom `location` failed with 400. A second catalog rooted at `s3://warehouse/nessie` is not a
way around it, because Polaris rejects catalogs whose locations overlap. A registered table is a
snapshot of the pointer: later Dremio commits do not reach Polaris unless you register again or
the writer switches to Polaris. With Enterprise and a REST source, a CTAS commits to Polaris directly.

## RBAC interplay

- The source principal is the ceiling. Dremio users can never see more than its Polaris grants allow.
- Read-only sources: `TABLE_LIST`, `TABLE_READ_DATA` and `NAMESPACE_LIST` on a catalog role
  assigned to the source's principal role. Vended keys are then read-only too.
- Write-capable sources (CTAS, COPY INTO, OPTIMIZE): add `CATALOG_MANAGE_CONTENT` (or
  finer-grained write privileges) on that source's own principal.
- Dremio row and column policies apply only to queries through Dremio. Spark or Snowflake reading
  the same Polaris table bypass them.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| 400 `An invalid value was found: RESTCATALOG` | Dremio OSS | Enterprise 26+/Cloud, or the registerTable path |
| Open Catalog source fails against a local Polaris | HTTP endpoint | HTTPS, or use the generic REST source |
| Reads fail with storage access errors | Vending off and no `fs.s3a` keys, or the catalog cannot vend | Turn vending on and fix the catalog's storage role, or set static keys |
| Dremio can read tables it should not | Static keys or an over-privileged source principal | Vending plus a dedicated reader principal |
| Register returns 403 `Invalid locations` | Table outside the catalog's allowed/structured locations | `allowedLocations` + `polaris.config.allow.unstructured.table.location=true` |
| Catalog create rejected | Location overlaps an existing catalog | Separate prefixes per catalog |

## Related

- [queries-and-metadata.md](queries-and-metadata.md), [ingestion-and-maintenance.md](ingestion-and-maintenance.md)
- The `iceberg-federation-consumption` skill of the apache-iceberg plugin covers Dremio as a federation layer
- The `polaris-catalog-federation` skill of this plugin covers registering and migrating tables between catalogs
