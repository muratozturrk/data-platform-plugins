# A Dedicated Read-Only Polaris Principal for Snowflake

## Concept

The catalog integration stores a Polaris principal's client id and secret inside a Snowflake
object. Give Snowflake its own principal with read-only privileges. That limits the damage if the
credentials leak, makes Snowflake's access auditable on its own, and lets you revoke it in one
step. The authorization chain in Polaris:

```
principal snowflake_service
  └─ assigned principal role  snowflake_reader              (principal-roles grant)
       └─ assigned catalog role catalog_reader on prod_catalog (catalog-roles grant)
            ├─ TABLE_LIST        list tables
            ├─ TABLE_READ_DATA   load table + read-only vended credentials
            └─ NAMESPACE_LIST    list namespaces (discovery)
```

If either assignment is missing, the principal holds nothing, even though tokens are still issued.

## Privilege map (measured on Polaris 1.8.0)

| Snowflake operation | Polaris call | Privilege needed |
|---|---|---|
| `SYSTEM$LIST_NAMESPACES_FROM_CATALOG` | list namespaces | `NAMESPACE_LIST` |
| `SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG` | list tables | `TABLE_LIST` |
| `CREATE ICEBERG TABLE` / `REFRESH` | load table | `TABLE_READ_DATA` |
| Reading data files | vended credentials | `TABLE_READ_DATA` (keys are read-only) |
| Any write | commit + write-capable keys | not granted. The append failed at the S3 upload |

## CLI reference (PyPI `apache-polaris` 1.8.0)

Connection options for every command: `polaris --host <h> --port 8181 --client-id <root-id> --client-secret <root-secret> ...`

```bash
polaris principals create snowflake_service          # JSON with clientId / clientSecret
polaris principal-roles create snowflake_reader
polaris catalog-roles create --catalog prod_catalog catalog_reader
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_LIST
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader TABLE_READ_DATA
polaris privileges catalog grant --catalog prod_catalog --catalog-role catalog_reader NAMESPACE_LIST
polaris principal-roles grant --principal snowflake_service snowflake_reader
polaris catalog-roles grant --catalog prod_catalog --principal-role snowflake_reader catalog_reader
```

A common wrong form,
`polaris privileges --catalog prod_catalog --catalog-role catalog_reader catalog grant TABLE_LIST TABLE_READ_DATA`,
exits with code 2 (`invalid choice: 'catalog_reader'`). The subcommand comes first, the options
after it, and there is one privilege per command.

## Scopes for `OAUTH_ALLOWED_SCOPES`

| Scope | Token | Effect |
|---|---|---|
| `PRINCIPAL_ROLE:ALL` | 200 | All assigned principal roles. Listed `events` |
| `PRINCIPAL_ROLE:snowflake_reader` | 200 | Only that role. Listed `events` |
| `PRINCIPAL_ROLE: ALL` (space) | 400 `invalid_scope` | No token |
| `PRINCIPAL_ROLE:snowflake_reader` before the role is assigned | 200, then 401 on first use | Token issued for a role the principal does not hold |

Prefer the narrow scope. If someone later assigns more roles to the principal, a token scoped to
`snowflake_reader` still carries only that role.

## Worked example: verifying what the integration will see

```python
from common.polaris import Principal, token_response
p = Principal("snowflake_service", client_id, client_secret)
assert token_response(p.client_id, p.client_secret, "PRINCIPAL_ROLE:snowflake_reader").status_code == 200
c = p.catalog("prod_catalog", scope="PRINCIPAL_ROLE:snowflake_reader")
c.list_tables("db")                    # ['events']    -> SYSTEM$LIST_ICEBERG_TABLES_FROM_CATALOG
c.load_table("db", "events")           # ok            -> CREATE ICEBERG TABLE / REFRESH
c.list_namespaces()                    # 403 without NAMESPACE_LIST, ['db'] with it
engine = p.pyiceberg("prod_catalog", scope="PRINCIPAL_ROLE:snowflake_reader")
engine.load_table("db.events").scan().to_arrow().num_rows   # 3, read with vended keys
engine.load_table("db.events").append(rows)                 # fails at the S3 upload (read-only keys)
```

Run this check before you hand the credentials to a Snowflake admin. If it fails here, Snowflake
fails the same way, only with less helpful errors.

## Measured sequence

| Step | Result |
|---|---|
| The three `create` commands | exit 0, 0, 0 |
| Multi-privilege grant line | exit 2 |
| Fixed grants, no assignments: narrow-scope token | 200, then 401 on first call |
| Fixed grants, no assignments: `ALL` scope list tables | 403 |
| After both assignments | list tables ok, load ok, 3 rows read, list namespaces 403, append fails at S3 |
| After `NAMESPACE_LIST` | list namespaces returns `['db']` |

## Writer side

The tables Snowflake reads are written by other engines. In the tests, root's `catalog_admin` role
alone could not create tables with vended credentials, so the writer's catalog role was also
granted `CATALOG_MANAGE_CONTENT`. Keep writers and Snowflake on separate principals.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| CLI exit code 2 on grant | Wrong argument order, several privileges | `privileges catalog grant --catalog C --catalog-role R PRIV`, once per privilege |
| 401 right after token | Narrow scope, role not assigned | `principal-roles grant` |
| 403 on list/load | Catalog role not assigned to principal role, or privilege missing | `catalog-roles grant`, grant the privilege |
| Namespace discovery 403 | `NAMESPACE_LIST` missing | Grant it |
| Snowflake can write when it should not | Shared principal with write privileges | Dedicated read-only principal |
| Leaked secret | Credentials sit in a Snowflake object | Rotate/reset the principal, then update the integration |

## Related

- [catalog-integration.md](catalog-integration.md), [production-settings.md](production-settings.md)
- The `polaris-configuration-and-cli` and `polaris-access-control` skills of this plugin cover the full CLI and privilege model
