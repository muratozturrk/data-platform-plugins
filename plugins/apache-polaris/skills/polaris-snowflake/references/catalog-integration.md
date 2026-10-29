# Snowflake Catalog Integration with Polaris

> None of the Snowflake SQL on this page ran in the tests, because there was no Snowflake account.
> The syntax follows Snowflake's documented objects. The Polaris-side behavior it depends on (OAuth,
> list/load, read-only vended reads, refresh semantics) was tested on Polaris 1.8.0 with an Iceberg
> REST client standing in for Snowflake.

## Concept

```
Polaris principal snowflake_service ──► catalog integration (REST URI, OAuth, scopes)
Object storage with Iceberg files    ──► external volume (base URL, IAM role)
                         both ──► CREATE ICEBERG TABLE ... CATALOG_TABLE_NAME = 'db.events'
                                    └► SELECT / time travel; REFRESH to pick up new commits
```

The integration makes the same calls as any Iceberg REST client:

1. `POST /api/catalog/v1/oauth/tokens` with the client credentials and `OAUTH_ALLOWED_SCOPES`
2. list namespaces and tables (discovery functions)
3. load the table (registration and refresh)

Snowflake then reads metadata and Parquet files with the external volume's IAM role. In the
tests, the stand-in read with Polaris-vended read-only credentials instead, which is the
strictest equivalent.

## Object reference

| Object / statement | Key parameters | Notes |
|---|---|---|
| `CREATE EXTERNAL VOLUME` | `STORAGE_PROVIDER = 'S3'`, `STORAGE_BASE_URL`, `STORAGE_AWS_ROLE_ARN` | No dots in bucket names, same region as Snowflake |
| `CREATE CATALOG INTEGRATION` | `CATALOG_SOURCE = POLARIS`, `TABLE_FORMAT = ICEBERG`, optional `CATALOG_NAMESPACE` | Leave out `CATALOG_NAMESPACE` and use full table paths |
| `REST_CONFIG (...)` | `CATALOG_URI = 'https://<host>/api/catalog'`, `CATALOG_NAME = '<polaris catalog>'` | Must be reachable from Snowflake over HTTPS |
| `REST_AUTHENTICATION (...)` | `TYPE = OAUTH`, `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET`, `OAUTH_ALLOWED_SCOPES` | Scopes such as `'PRINCIPAL_ROLE:snowflake_reader'`, no spaces |
| `GRANT USAGE ON INTEGRATION ... TO ROLE ...` | Snowflake role | Snowflake-side RBAC |
| `CREATE ICEBERG TABLE` | `CATALOG`, `EXTERNAL_VOLUME`, `CATALOG_TABLE_NAME` | Registers a pointer, copies no data |
| `ALTER ICEBERG TABLE ... REFRESH` | none | Reloads metadata from Polaris |
| `AUTO_REFRESH = TRUE` | table option | Background refresh, with lag |
| `SYSTEM$LIST_NAMESPACES_FROM_CATALOG` | integration | Needs Polaris `NAMESPACE_LIST` |
| `SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG` | integration | Needs Polaris `TABLE_LIST` |

Time travel on an externally managed table has been described as `AT (OFFSET => <snapshot-id>)`.
That form was not verified. In other Snowflake contexts, `OFFSET` is a time offset in seconds, so
check the current documentation for snapshot-based reads.

## Worked example: Spark writes, Snowflake reads

```sql
-- once, as a Snowflake admin (not run)
CREATE CATALOG INTEGRATION polaris_catalog_int
  CATALOG_SOURCE = POLARIS TABLE_FORMAT = ICEBERG
  REST_CONFIG = (CATALOG_URI = 'https://polaris.example.com/api/catalog' CATALOG_NAME = 'prod_catalog')
  REST_AUTHENTICATION = (TYPE = OAUTH OAUTH_CLIENT_ID = '...' OAUTH_CLIENT_SECRET = '...'
                         OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:snowflake_reader'));
SELECT SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG('polaris_catalog_int');
CREATE OR REPLACE ICEBERG TABLE events_external
  CATALOG = 'POLARIS_CATALOG_INT' EXTERNAL_VOLUME = 'MY_ICEBERG_EXT_VOLUME'
  CATALOG_TABLE_NAME = 'db.events';
SELECT COUNT(*) FROM events_external;        -- sees snapshot S1
-- Spark appends to prod_catalog.db.events  -> snapshot S2 in Polaris
SELECT COUNT(*) FROM events_external;        -- still S1
ALTER ICEBERG TABLE events_external REFRESH;
SELECT COUNT(*) FROM events_external;        -- S2
```

The same sequence with the stand-in reader principal (tested):

```python
registered = reader.load_table("db.events")            # like CREATE ICEBERG TABLE
first = registered.current_snapshot().snapshot_id
writer.load_table("db.events").append(two_rows)        # another engine commits
registered.scan().to_arrow().num_rows                  # 3  (not refreshed)
registered.refresh()                                   # like ALTER ... REFRESH
registered.scan().to_arrow().num_rows                  # 5
registered.scan(snapshot_id=first).to_arrow().num_rows # 3  (time travel)
```

## Table kinds in Snowflake

| Aspect | Native table | Snowflake-managed Iceberg | Externally managed (Polaris-backed) |
|---|---|---|---|
| Storage | Snowflake | Your bucket, Parquet | Your bucket, Parquet |
| Writes from Snowflake | Yes | Yes | No |
| Other engines | No | Through `CATALOG_SYNC` to Open Catalog | Yes, read and write via Polaris |
| Snowflake Time Travel, cloning | Yes | Not covered here | No, Iceberg snapshots instead |
| Maintenance (compaction, expiry) | Snowflake | Snowflake | You (Spark/Dremio procedures) |

- Use **externally managed** when other engines produce the data and Snowflake consumes it.
- Use **Snowflake-managed Iceberg + `CATALOG_SYNC`** when Snowflake produces the data and other
  engines consume it.
- Use **native** when Snowflake features and heavy DML matter more than interoperability.

## Open Catalog

Snowflake Open Catalog is hosted Polaris. A service connection is a principal with a client
id/secret and a principal role, the same model as the principals tested here. Only the URI
(a Snowflake domain) and the place where credentials are created (the Open Catalog UI) change.
Open Catalog was described as GA on AWS only at one point, so check the current regions. None of
this was run against Open Catalog itself.

## Pitfalls

| Symptom | Likely cause | Fix |
|---|---|---|
| Integration cannot connect | Polaris not reachable from Snowflake over HTTPS | Public endpoint, allowlist from `SYSTEM$ALLOWLIST()`, or private connectivity |
| Token request rejected (400 `invalid_scope`) | Scope written with a space (`PRINCIPAL_ROLE: ALL`) | `PRINCIPAL_ROLE:ALL` |
| 401 on first use | Scope names a principal role that is not assigned | `principal-roles grant --principal ... <role>` |
| Table not found at registration | Principal cannot list or load it (403 in Polaris) | Check privileges and assignments, see [service-principal](service-principal.md) |
| Namespace listing fails | `NAMESPACE_LIST` missing | Grant it to the catalog role |
| New rows missing | Pointer not refreshed | `ALTER ICEBERG TABLE ... REFRESH` or `AUTO_REFRESH` |
| DML refused | Externally managed tables are read-only | Snowflake-managed Iceberg table for Snowflake writes |
| Wrong or missing rows on MOR tables | Delete files possibly unsupported | Compact before exposing, check release notes |

## Related

- [service-principal.md](service-principal.md), [production-settings.md](production-settings.md)
- The `iceberg-federation-consumption` skill of the apache-iceberg plugin covers engines that share one catalog
