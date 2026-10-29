# Privileges Reference

## Concept

A catalog role is a named set of grants inside one catalog. A grant is one privilege on one
securable: the catalog, a namespace, a table or a view. Privileges are grouped by object family;
data privileges are separate from metadata privileges, and they decide what storage credentials
Polaris vends.

## The 25 Documented Privileges

| Family | Privileges |
|---|---|
| Table | `TABLE_CREATE`, `TABLE_DROP`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_WRITE_PROPERTIES`, `TABLE_READ_DATA`, `TABLE_WRITE_DATA`, `TABLE_FULL_METADATA` |
| View | `VIEW_CREATE`, `VIEW_DROP`, `VIEW_LIST`, `VIEW_READ_PROPERTIES`, `VIEW_WRITE_PROPERTIES`, `VIEW_FULL_METADATA` |
| Namespace | `NAMESPACE_CREATE`, `NAMESPACE_DROP`, `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `NAMESPACE_WRITE_PROPERTIES`, `NAMESPACE_FULL_METADATA` |
| Catalog | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_CONTENT`, `CATALOG_MANAGE_METADATA`, `CATALOG_READ_PROPERTIES`, `CATALOG_WRITE_PROPERTIES` |

Polaris 1.8.0 accepts more than these, for example `TABLE_MANAGE_STRUCTURE`,
`TABLE_REMOVE_SNAPSHOTS`, `TABLE_UPGRADE_FORMAT_VERSION` and policy privileges (`POLICY_CREATE`,
`CATALOG_ATTACH_POLICY`). They were accepted but not measured.

## Measured Privilege x Operation Matrix

A probe principal held a catalog role with exactly one privilege, granted at catalog level, and
tried 20 operations. Data operations used only the vended keys, so "allowed" means bytes were
really read or written. Without any catalog role, 0 of 20 operations were allowed.

| Privilege | Allowed operations |
|---|---|
| `TABLE_LIST` | table_list |
| `TABLE_READ_PROPERTIES` | table_list, table_load |
| `TABLE_WRITE_PROPERTIES` | table_list, table_load, table_write_props |
| `TABLE_CREATE` | table_list, table_create |
| `TABLE_DROP` | table_drop |
| `TABLE_READ_DATA` | table_list, table_load, data_read |
| `TABLE_WRITE_DATA` | table_list, table_load, table_write_props, data_read, data_write |
| `TABLE_FULL_METADATA` | table_list, table_load, table_write_props, table_create, table_drop |
| `VIEW_LIST` | view_list |
| `VIEW_READ_PROPERTIES`, `VIEW_WRITE_PROPERTIES` | view_list, view_load |
| `VIEW_CREATE` | view_list, view_create |
| `VIEW_DROP` | view_drop |
| `VIEW_FULL_METADATA` | view_list, view_load, view_create, view_drop |
| `NAMESPACE_LIST` | namespace_list |
| `NAMESPACE_READ_PROPERTIES` | namespace_list, namespace_read |
| `NAMESPACE_WRITE_PROPERTIES` | namespace_list, namespace_read, namespace_write |
| `NAMESPACE_CREATE` | namespace_list, namespace_create |
| `NAMESPACE_DROP` | namespace_drop |
| `NAMESPACE_FULL_METADATA` | all 5 namespace operations |
| `CATALOG_READ_PROPERTIES` | catalog_read |
| `CATALOG_WRITE_PROPERTIES` | catalog_read, catalog_write |
| `CATALOG_MANAGE_METADATA` | all 16 metadata operations; no data, no grants |
| `CATALOG_MANAGE_CONTENT` | all 18 content operations including data; no grants |
| `CATALOG_MANAGE_ACCESS` | grant_manage only |

Only HTTP 2xx or 403 were observed. The matrix is the oracle of
`test_privilege_matrix_matches_measured_reference`: a Polaris upgrade that changes privilege
semantics fails that test.

Implication runs only inside a family: `TABLE_WRITE_DATA` includes `TABLE_READ_DATA` and table
property writes, but no table privilege implies a namespace or view privilege.

## Documented Claims Checked

| Common description | Result |
|---|---|
| `TABLE_FULL_METADATA` includes all table privileges except READ/WRITE_DATA | Holds |
| `TABLE_READ_DATA` vends read-only, `TABLE_WRITE_DATA` read+write credentials | Holds |
| `CATALOG_MANAGE_CONTENT` encompasses MANAGE_METADATA, *_FULL_METADATA, READ/WRITE_DATA, catalog properties | Holds (exact union); no grant management |
| `CATALOG_MANAGE_ACCESS` grants/revokes and assigns catalog roles | Holds; no content access at all |
| `NAMESPACE_LIST` lists tables and views in a namespace | Does not hold: namespaces only; tables need `TABLE_LIST`, views `VIEW_LIST` |
| `CATALOG_MANAGE_METADATA` gives full control over metadata and roles | Partly: all metadata operations, no grants, no data |
| `CATALOG_READ_PROPERTIES` lists catalogs and reads their properties | Partly: reads properties; the catalog is not included in `GET /catalogs` |

## Scope and Inheritance

| Grant on | Reaches (measured) |
|---|---|
| Catalog | Every namespace, table and view in it |
| Namespace `sales` | `sales.orders`, nested `sales.emea.orders`, tables created later; not `hr.salaries` |
| Table `sales.orders` | That table only; not `sales.emea.orders` |

Grant bodies:

```json
{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}
{"grant": {"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"}}
{"grant": {"type": "table", "namespace": ["sales"], "tableName": "orders", "privilege": "TABLE_READ_DATA"}}
```

Add with `PUT /catalogs/{c}/catalog-roles/{r}/grants`, remove with `POST` on the same path.
Read access to three tables took 3 table grants but 1 namespace grant, and the namespace grant
also covered a table created afterwards. A revoke applied to the very next request in the same
session; grants from two catalog roles held by one principal add up.

## Persona Templates

| Persona | Grants |
|---|---|
| Analyst | `TABLE_READ_DATA` (+ `NAMESPACE_LIST`, `TABLE_LIST`, `VIEW_LIST`, `VIEW_READ_PROPERTIES`) on namespaces |
| Data engineer | `TABLE_WRITE_DATA`, `TABLE_CREATE`, `NAMESPACE_CREATE` (+ list privileges) on namespaces |
| Schema owner without data access | `TABLE_FULL_METADATA` or `CATALOG_MANAGE_METADATA` |
| Catalog admin | `CATALOG_MANAGE_CONTENT` + `CATALOG_MANAGE_ACCESS` |

## Vended Credentials and Table Creation

The matrix calls the REST API without asking for credentials. When a client sends
`X-Iceberg-Access-Delegation: vended-credentials` (Spark, PyIceberg, Trino and Dremio do), creating
a table also needs a data privilege, because Polaris must vend keys for the new location.

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Users see namespaces but no tables | Only `NAMESPACE_LIST` granted | Add `TABLE_LIST` / `VIEW_LIST` |
| Table loads but scan gets `AccessDenied` | Metadata privilege without `TABLE_READ_DATA` | Grant the data privilege |
| Write fails with read-only keys | `TABLE_READ_DATA` only | `TABLE_WRITE_DATA` |
| Catalog admin cannot grant | `CATALOG_MANAGE_CONTENT` has no grant management | Add `CATALOG_MANAGE_ACCESS` |
| New table not readable by analysts | Table-level grants | Grant on the namespace |
| Revoke with `DELETE .../grants` returns 405 | Revoke is `POST` on the grants resource | See the `polaris-rest-api` skill |

## Related

- [access_model.md](access_model.md), [roles_as_code_and_review.md](roles_as_code_and_review.md)
- `polaris-rest-api` skill: grant endpoints and status codes.
