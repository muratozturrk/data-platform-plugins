# Catalog-Based Access Control

Scope: the catalog as the policy point for every engine: authentication, RBAC on catalogs /
namespaces / tables, and short-lived storage credentials vended per table and access mode.
Examples use Apache Polaris; Snowflake Open Catalog and the Dremio catalog are Polaris-based,
Lakekeeper uses OpenFGA, Glue uses IAM + Lake Formation, Gravitino and Unity have their own RBAC.

## Problem
Earlier data lakes duplicated permissions across bucket policies, engine roles and user
credentials and had to keep them in sync. Engines held long-lived bucket keys that could read
every table. With catalog-based control, one policy point serves every engine, users never need
direct storage access, and storage becomes an implementation detail rather than the boundary.

| Mechanism | What it does |
|---|---|
| Authentication | Users/engines present an identity (Polaris: OAuth2 client credentials per principal) |
| RBAC | Catalog checks list/read/write/create/drop/alter per request |
| Credential vending | Scoped, short-lived storage credentials per table and operation |
| Engine / storage controls | Transitional: fine-grained policies, or stores without STS |

## Polaris RBAC model

```
principal (etl_svc)     -> principal role (data_engineer) -> catalog role (sales_writer) -> grants on namespace sales
principal (bi_analyst)  -> principal role (analyst)       -> catalog role (sales_reader) -> grants on namespace sales
```

- **Principal**: service identity; `POST /principals` returns `clientId`/`clientSecret` once.
- **Principal role**: assigned to principals; spans catalogs.
- **Catalog role**: lives in one catalog; holds privileges; assigned to principal roles.
- **Grant**: `{"type": "catalog"|"namespace"|"table", "namespace": [...], "privilege": ...}`.

| Role | Privileges (namespace `sales`) |
|---|---|
| Reader | `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` |
| Writer | Reader + `TABLE_CREATE`, `TABLE_WRITE_PROPERTIES`, `TABLE_WRITE_DATA`, `TABLE_DROP` |
| Catalog admin | `CATALOG_MANAGE_CONTENT` (+ `CATALOG_MANAGE_ACCESS` for grants) |

Provisioning through the management API (`/api/management/v1`):

```python
admin.create_catalog("sales_cat")                                   # POST /catalogs
cid, secret = admin.create_principal("etl_svc")                     # POST /principals
admin.create_principal_role("data_engineer")                        # POST /principal-roles
admin.assign_principal_role("etl_svc", "data_engineer")             # PUT /principals/etl_svc/principal-roles
admin.create_catalog_role("sales_cat", "sales_writer")              # POST /catalogs/sales_cat/catalog-roles
for p in WRITER:                                                    # PUT .../catalog-roles/sales_writer/grants
    admin.grant("sales_cat", "sales_writer",
                {"type": "namespace", "namespace": ["sales"], "privilege": p})
admin.assign_catalog_role("data_engineer", "sales_cat", "sales_writer")
# PUT /principal-roles/data_engineer/catalog-roles/sales_cat
```

Namespace grants need the namespace to exist first.

Catalog with vending (S3 + STS):

```json
{"catalog": {"name": "sales_cat", "type": "INTERNAL",
  "properties": {"default-base-location": "s3://warehouse/polaris/sales_cat"},
  "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://warehouse/polaris/sales_cat"],
    "roleArn": "arn:aws:iam::<account-id>:role/<vendor-role>", "region": "us-east-1"}}}
```

For S3-compatible stores add `endpoint`, `stsEndpoint`, `pathStyleAccess: true`; for storage without
STS set `stsUnavailable: true` (then no vending). The role Polaris assumes may have broad bucket
access: Polaris narrows every vended key with a session policy.

## Vending flow

```
Engine (no keys) -> LoadTable sales.orders, header X-Iceberg-Access-Delegation: vended-credentials
Polaris          -> checks grants (TABLE_READ_DATA? TABLE_WRITE_DATA?)
Polaris -> STS   AssumeRole + session policy (table prefix; read or read/write)
Polaris -> Engine metadata + config/storage-credentials {s3.access-key-id, s3.secret-access-key,
                  s3.session-token, s3.session-token-expires-at-ms}
Engine -> S3     reads/writes files with the scoped key
```

Engine side (PyIceberg, no storage keys in the environment):

```python
RestCatalog("sales", uri="http://polaris:8181/api/catalog", warehouse="sales_cat",
            credential=f"{cid}:{secret}", scope="PRINCIPAL_ROLE:ALL",
            **{"oauth2-server-uri": "http://polaris:8181/api/catalog/v1/oauth/tokens",
               "header.X-Iceberg-Access-Delegation": "vended-credentials"})
```

## Measured behaviour (Polaris 1.8.0, S3-compatible store with STS)

| Check | Result |
|---|---|
| Analyst read `sales.orders` files with keys vended for `sales.orders` | allowed |
| Analyst write under `sales.orders` | denied (read-only session policy) |
| Analyst read `hr.salaries` files with `sales.orders` keys | denied (prefix-scoped) |
| Engineer write under `sales.orders` | allowed |
| Engineer read `hr.salaries` files | denied |
| Key type / lifetime | Temporary `ASIA...` key, ~3,597 s left of 3,600 |

- Analyst `append`: fails at **storage** (`ACCESS_DENIED` on the data file upload) before any commit.
- Analyst metadata-only commit (`set-properties`): refused by the **catalog**, 403 "not authorized for
  op SET_TABLE_PROPERTIES". Create and drop: `ForbiddenError`. Two independent layers enforce one grant.
- Analyst LoadTable `hr.salaries`: 403 "Principal ... is not authorized".
- Engineer appends through an engine with no static keys: 3 -> 5 rows.
- Revoking the catalog role: same OAuth token gets 200, then 403, then 200 after re-grant -
  authorization is evaluated per request, not baked into the token.

## Strict defaults to know

- The bootstrap `catalog_admin` catalog role cannot create tables with vended credentials until it
  receives `CATALOG_MANAGE_CONTENT`.
- `DROP TABLE ... PURGE` is off until `polaris.config.drop-with-purge.enabled=true` (catalog property).
- Tables registered outside allowed locations need `polaris.config.allow.unstructured.table.location=true`.
- Spark executors on Iceberg 1.8.1 call Polaris' credential-refresh endpoint without the OAuth token
  and fail "Not authorized"; workaround `spark.sql.catalog.<name>.client.refresh-credentials-enabled=false`
  (jobs must then finish within the vended lifetime, ~1 h).

## When to use what

| Condition | Choice | Reason |
|---|---|---|
| Catalog and storage support vending | All access through the catalog; no bucket keys in engines | One policy point, short-lived scoped keys |
| Storage without STS / catalog without vending | Keep storage-layer policies too | Catalog cannot issue scoped keys |
| Row/column security needed now | Engine/platform policies (e.g. Dremio row filters, column masking) | Not standardized in Iceberg/Polaris yet |
| Many engines and teams | Namespace-level grants per domain | Coarse-grained control is what catalogs support consistently |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| 400 "Credential vending was requested ... but no credentials are available" | Catalog has `stsUnavailable` or no role | Configure `roleArn` + STS, or drop the delegation header and use storage policies |
| 403 on LoadTable | Missing `TABLE_READ_PROPERTIES` / `TABLE_READ_DATA` | Grant on namespace or table to the catalog role |
| `ACCESS_DENIED` on write with vended keys | Principal only has read privileges | Grant `TABLE_WRITE_DATA` |
| 401 from executors mid-job | Credential refresh without token | Disable refresh or upgrade the client |
| Grant call fails for a namespace | Namespace does not exist yet | Create namespace first |
| Lost the client secret | Shown only once | Rotate credentials for the principal |
