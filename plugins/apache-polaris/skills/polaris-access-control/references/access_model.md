# Polaris Access Model: Catalogs, Roles and Scopes

## Concept

Polaris security is role-based access control in three layers:

```
principal (who)  --assigned-->  principal role (global job function)
principal role   --granted-->   catalog role (one catalog)
catalog role     --holds-->     privileges on catalog / namespace / table / view
```

Every request carries a token for exactly one principal. Polaris resolves the principal roles
active in that token, the catalog roles they are granted in the target catalog, and the union of
those catalog roles' privileges on the target object or one of its parents. If a data privilege
applies and the client asked for `X-Iceberg-Access-Delegation: vended-credentials`, Polaris also
assumes the catalog's storage role with a session policy scoped to that table's location and to
read or read+write.

## Catalogs

A catalog is the unit of isolation. One Polaris server (realm) hosts many catalogs, each with:

| Part | Meaning |
|---|---|
| Namespace tree | Tables and views live in nested namespaces |
| Catalog roles | Created per catalog; names mean nothing outside it |
| Storage config | `storageType`, `allowedLocations`, role ARN for vending |
| `catalog_admin` | Created automatically, bound to the `service_admin` principal role |

Measured behaviour (Polaris 1.8.0):

| Check | Result |
|---|---|
| Same `campaigns.events` in two catalogs | Two independent tables, different metadata locations |
| New catalog's roles | Exactly `catalog_admin`, bound to `service_admin` |
| `catalog_admin` default grants | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA` |
| Root creates a table with vended credentials before granting content | 403 until `CATALOG_MANAGE_CONTENT` |
| Reader of catalog A lists/loads in catalog B | 403 |
| Vended keys of a catalog-A table read a catalog-B file | `AccessDenied` at the storage layer |
| Table location outside `allowedLocations` | 403 `Invalid locations` (also for root) |
| Catalog base location outside its own allowed list | 400 |

Design rules:

| Condition | Choice |
|---|---|
| Teams with separate data and admins | One catalog per team |
| Data shared by several teams | One shared catalog plus private catalogs |
| Regulated data (PII, financial) | Separate catalog on its own bucket or prefix |
| Few users, one domain | One catalog, a namespace per area |

Storage separation is a second wall: vended keys are scoped to a table location, so a wrong grant
in one catalog still cannot reach another catalog's bucket.

## Principal Roles

A principal role is global. It groups catalog roles from one or more catalogs and is assigned to
any number of principals; a principal may hold several principal roles.

```
dana  -> eng_engineer   -> engineering: eng_contributor (write)
dana  -> comp_viewer    -> compliance:  comp_reader     (read)
sam, lee, kim -> data_scientist -> engineering: eng_reader (read)
```

Measured:

| Test | Result |
|---|---|
| One principal role, 3 members; revoke its catalog role once | All 3 denied |
| One principal, write on engineering + read on compliance | Both work; append on compliance denied |
| Same catalog-role name in two catalogs | 0 shared grants |

## Token Scopes

The OAuth2 `scope` decides which principal roles are active for a session:

| Scope | Active roles |
|---|---|
| `PRINCIPAL_ROLE:ALL` | Every principal role the principal holds |
| `PRINCIPAL_ROLE:<name>` | Only that role |
| Missing | 400 `invalid_scope` from the token endpoint |

```python
viewer_only = dana.pyiceberg("compliance", scope="PRINCIPAL_ROLE:comp_viewer")
viewer_only.load_table("audits.events").scan().to_arrow()      # works
eng = dana.pyiceberg("engineering", scope="PRINCIPAL_ROLE:comp_viewer")
eng.load_table("pipelines.events")                              # 403: role not active
```

A scope naming a role the principal does not hold (e.g. `service_admin`) still yields a token,
but every call with it is 401 (management API) or `UnauthorizedError` (PyIceberg). There is no
escalation path through the scope.

Use a single-role scope to run a job with less than its owner's access, for example an engineer's
scheduled export that should only read.

## Use-Case Blueprints

| Use case | Catalogs | Catalog roles | Principal roles |
|---|---|---|---|
| Multi-tenant | One per team | `<team>_admin`, `<team>_analyst` | `<team>_data_engineer`, `<team>_analyst` |
| Cross-team collaboration | Shared catalog + private catalogs | `shared_contributor`, `shared_viewer` | Engineers: contributor + team admin; analysts: viewer |
| Sensitive data | `sensitive_data`, `public_data` | `sensitive_data_admin`, `sensitive_data_viewer` | Compliance roles get viewer only |
| Multi-cloud | One per cloud or storage type | `cloud_storage_admin`, `cloud_storage_viewer` | `<cloud>_admin`, `<cloud>_viewer` |

All four are combined in the model under `examples/access-control/roles_as_code/rbac_model.yaml`.

## Worked Example: Isolation Proven at the Storage Layer

```python
def test_vended_credentials_never_reach_another_catalogs_storage(units, root):
    mkt, fin = units
    m = CatalogClient(mkt, root.client_id, root.client_secret)
    f = CatalogClient(fin, root.client_id, root.client_secret)
    s3_mkt = vended_s3(m.load_table("campaigns", "events"))
    bucket, key = split_uri(f.load_table("ledger", "events")["metadata-location"])
    with pytest.raises(ClientError) as e:
        s3_mkt.get_object(Bucket=bucket, Key=key)
    assert e.value.response["Error"]["Code"] in ("AccessDenied", "403")
```

Even root's vended keys for a marketing table cannot read a finance file, so isolation does not
depend on the engine behaving.

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Engine gets 403 creating a table in a new catalog | `catalog_admin` has no `CATALOG_MANAGE_CONTENT` | Grant it, or use a dedicated writer role |
| 403 `Invalid locations` | Table location outside `allowedLocations` | Use the catalog base location or extend the list |
| 400 creating a catalog | Base location outside its own `allowedLocations` | Make the base location one of them |
| Principal sees nothing in a catalog it "has" | Principal role not granted a catalog role there, or token scoped to another role | Check both assignments and the scope |
| 401 with a fresh token | Scope names a role the principal does not hold | Use `PRINCIPAL_ROLE:ALL` or a held role |
| Same role name behaves differently per catalog | Catalog roles are catalog-local | Read grants per catalog |

## Related

- [principals.md](principals.md), [privileges_reference.md](privileges_reference.md), [roles_as_code_and_review.md](roles_as_code_and_review.md)
- `polaris-catalog-fundamentals` skill: credential vending and the REST handshake.
- `polaris-deployment` skill: storage configuration and STS for vending.
