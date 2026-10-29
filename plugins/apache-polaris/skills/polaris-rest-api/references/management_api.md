# Polaris Management API

## Concept

The Management API (`/api/management/v1`) is Polaris-specific. It manages the entities the
Iceberg REST spec does not know: catalogs and their storage, principals, principal roles,
catalog roles, grants and the assignments between them. All calls need a bearer token of a
principal with the right privilege (root / `service_admin` for most). Every entity has an
`entityVersion` used for optimistic locking.

## Catalogs

| Operation | Call | Measured |
|---|---|---|
| List | `GET /catalogs` | 200, `{"catalogs": [...]}` (filtered to what the caller may see) |
| Create | `POST /catalogs` | 201; response is the catalog object (not wrapped in `{"catalog": ...}`) |
| Get | `GET /catalogs/{name}` | 200, includes `storageConfigInfo` |
| Update | `PUT /catalogs/{name}` | 200, `entityVersion` + 1 |
| Delete | `DELETE /catalogs/{name}` | 204; only when empty (no tables, namespaces, custom roles) |

Create body (S3 with STS vending):

```json
{"catalog": {"type": "INTERNAL", "name": "finance",
  "properties": {"default-base-location": "s3://lake/finance"},
  "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://lake/finance"],
                        "region": "us-east-1", "roleArn": "arn:aws:iam::<account>:role/<vendor-role>",
                        "endpoint": "http://seaweedfs:8333", "stsEndpoint": "http://seaweedfs:8333",
                        "pathStyleAccess": true}}}
```

`endpoint`, `stsEndpoint` and `pathStyleAccess` are for S3-compatible stores; for AWS S3 leave
them out. `"stsUnavailable": true` (instead of a role) creates a catalog that cannot vend.

Update body:

```json
{"currentEntityVersion": 1, "properties": {"default-base-location": "s3://lake/finance", "owner": "finance-platform"}}
```

Measured failures: create without `storageConfigInfo` is 400 `storageConfigInfo: must not be
null`; an update moving `default-base-location` outside `allowedLocations` is 400 `not within any
of the allowed locations`.

## Principals

| Operation | Call | Measured |
|---|---|---|
| List | `GET /principals` | 200, `{"principals": [...]}`, never secrets |
| Create | `POST /principals` `{"principal": {"name", "properties"}}` | 201, `credentials.clientId` + `clientSecret` (once) |
| Get / update / delete | `GET`, `PUT` (with `currentEntityVersion`), `DELETE /principals/{name}` | 200 / 200 / 204 |
| Rotate | `POST /principals/{name}/rotate` | 200 for the principal itself; 403 for an admin |
| Reset | `POST /principals/{name}/reset` `{}` | 200 for an admin; old secret 401 |

A `clientId` in the create body is ignored (requested `analyst321`, got a generated 16-hex id).
With `credentialRotationRequired: true` the new principal got a token but every call was 403 until
it rotated its own secret in one 1.8.0 stack; another stack saw no blocking, so verify on your image.

## Roles, Grants and Assignments

| Operation | Call | Status |
|---|---|---|
| Create catalog role | `POST /catalogs/{c}/catalog-roles` `{"catalogRole": {"name"}}` | 201 |
| List catalog roles | `GET /catalogs/{c}/catalog-roles` | 200 |
| Delete catalog role | `DELETE /catalogs/{c}/catalog-roles/{cr}` | 204 |
| Create principal role | `POST /principal-roles` `{"principalRole": {"name"}}` | 201 |
| List / get principal role | `GET /principal-roles`, `GET /principal-roles/{pr}` | 200 |
| Update principal role | `PUT /principal-roles/{pr}` with `currentEntityVersion` | 200; stale version 409 |
| Delete principal role | `DELETE /principal-roles/{pr}` | 204 |
| Add grant | `PUT /catalogs/{c}/catalog-roles/{cr}/grants` `{"grant": {...}}` | 201 |
| Revoke grant | `POST /catalogs/{c}/catalog-roles/{cr}/grants` `{"grant": {...}}` | 201, grant removed |
| List grants | `GET /catalogs/{c}/catalog-roles/{cr}/grants` | 200 |
| Catalog role -> principal role | `PUT /principal-roles/{pr}/catalog-roles/{c}` `{"catalogRole": {"name"}}` | 201; 404 if a role does not exist |
| List catalog roles of a principal role | `GET /principal-roles/{pr}/catalog-roles/{c}` | 200 |
| Remove that binding | `DELETE /principal-roles/{pr}/catalog-roles/{c}/{cr}` | 204 |
| Principal role -> principal | `PUT /principals/{p}/principal-roles` `{"principalRole": {"name"}}` | 201; 404 if missing |
| List roles of a principal | `GET /principals/{p}/principal-roles` | 200 |
| List principals of a role | `GET /principal-roles/{pr}/principals` | 200 |
| Remove from principal | `DELETE /principals/{p}/principal-roles/{pr}` | 204 |

Revoke variants measured: `DELETE .../grants` 405, `POST .../grants?cascade=true` 501 Not
Implemented, `POST .../grants` 201 and the grant list is empty afterwards.

Grant body shapes:

```json
{"grant": {"type": "catalog",   "privilege": "CATALOG_MANAGE_CONTENT"}}
{"grant": {"type": "namespace", "namespace": ["sales"], "privilege": "TABLE_READ_DATA"}}
{"grant": {"type": "table",     "namespace": ["sales"], "tableName": "orders", "privilege": "TABLE_READ_DATA"}}
```

## Optimistic Locking

```python
role = s.get(f"{MGMT}/principal-roles/report_viewer").json()
s.put(f"{MGMT}/principal-roles/report_viewer",
      json={"currentEntityVersion": role["entityVersion"], "properties": {"access_scope": "write_enabled"}})
```

Sending version 2 for a role at version 1 returned 409 `currentEntityVersion '1', expected '2'`.

## Worked Example: Provision and Tear Down

Order matters both ways. Provision: catalog, catalog role, grants, principal role, binding,
principal, membership. Tear down: memberships, bindings, roles, then principal and catalog (the
catalog only once it is empty).

```python
created = Call("POST", f"{MGMT}/catalogs", fix.create_catalog(CAT, f"s3://warehouse/{CAT}")).send(bearer)
assert created.status_code == 201
grant = {"grant": {"type": "catalog", "privilege": "TABLE_READ_DATA"}}
Call("PUT",  f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants", grant).send(bearer)   # 201
Call("POST", f"{MGMT}/catalogs/{CAT}/catalog-roles/viewer/grants", grant).send(bearer)   # revoke, 201
```

## Status Codes

| Status | Typical cause |
|---|---|
| 400 | Missing required field, location outside `allowedLocations` |
| 401 | Bad secret, deleted principal, token for a role not held |
| 403 | Missing privilege, admin calling `/rotate`, rotation required |
| 404 | Entity (often a role) does not exist yet |
| 405 | `DELETE` on the grants resource |
| 409 | Stale `currentEntityVersion`, dropping a non-empty namespace |
| 501 | `?cascade=true` on revoke |

## Pitfalls

| Symptom | Fix |
|---|---|
| Script works once, then 409 on update | Read the entity first; never hard-code versions |
| Catalog delete fails | Drop tables, namespaces and custom catalog roles first |
| Response shape differs from a tutorial | Responses are the entity itself; lists are `{"<entities>": [...]}` |
| Secret not in `GET /principals/{name}` | Shown only at create/reset/rotate |

## Related

- [catalog_api.md](catalog_api.md), [tokens_and_scopes.md](tokens_and_scopes.md)
- `polaris-access-control` skill: what the privileges mean.
- `polaris-configuration-and-cli` skill: the `polaris` CLI over this API.
