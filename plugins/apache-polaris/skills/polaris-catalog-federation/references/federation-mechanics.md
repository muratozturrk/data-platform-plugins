# Federation Mechanics: Polaris EXTERNAL Catalogs

## Concept

An external catalog in Polaris is a catalog entity of type `EXTERNAL` whose namespaces and tables
live in another Iceberg REST catalog. Polaris forwards namespace and table operations to that
remote catalog (catalog federation) and adds its own three layers on top:

- **Authentication**: engines authenticate once against Polaris (OAuth2 client credentials);
  Polaris authenticates to the remote with the parameters stored in the catalog entity.
- **RBAC**: Polaris checks the caller's catalog roles before forwarding.
- **Credential vending**: Polaris issues table-scoped temporary storage keys for remote tables,
  from the storage configuration of the external catalog.

```
Engine --loadTable (OAuth token, X-Iceberg-Access-Delegation: vended-credentials)--> Polaris
Polaris: RBAC check (catalog-level grants)
Polaris --GET config?warehouse=<remoteCatalogName>, loadTable--> remote REST catalog
Polaris --metadata + table-scoped temporary keys--> Engine --read/write files--> object storage
Engine --commit--> Polaris --commit--> remote (remote writes the new metadata.json)
```

Typical reasons: tables that must stay owned by another team or system (partner data), a
stepping stone for migration (see [gradual-migration.md](gradual-migration.md)), keeping a
specialized catalog (Nessie branches for ingestion), regional catalogs for regulatory separation
behind one access point. The generic Iceberg REST protocol is covered by the `iceberg-catalogs`
skill of the apache-iceberg plugin.

## Enabling Federation (Polaris 1.8.0)

Federation is off by default. The server needs three feature flags (environment variables or
`application.properties`):

```yaml
polaris.features."ENABLE_CATALOG_FEDERATION": "true"
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES": '["ICEBERG_REST"]'
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES": '["OAUTH","BEARER","IMPLICIT"]'
```

## API Reference: Create an External Catalog

`POST /api/management/v1/catalogs`:

```json
{"catalog": {
  "name": "fed_partner",
  "type": "EXTERNAL",
  "properties": {"default-base-location": "s3://warehouse/rest/"},
  "storageConfigInfo": {
    "storageType": "S3", "allowedLocations": ["s3://warehouse/rest/"], "region": "us-east-1",
    "endpoint": "http://seaweedfs:8333", "pathStyleAccess": true,
    "roleArn": "arn:aws:iam::000000000000:role/lake-vendor", "stsEndpoint": "http://seaweedfs:8333"},
  "connectionConfigInfo": {
    "connectionType": "ICEBERG_REST",
    "uri": "http://rest:8181",
    "remoteCatalogName": "default",
    "authenticationParameters": {"authenticationType": "IMPLICIT"}}}}
```

| Field | Meaning |
|---|---|
| `type: EXTERNAL` | Content lives in the remote; Polaris proxies |
| `connectionConfigInfo.connectionType` | `ICEBERG_REST` (must be in `SUPPORTED_CATALOG_CONNECTION_TYPES`) |
| `uri` | Base URI of the remote REST catalog (Lakekeeper: `.../catalog`, Gravitino: `.../iceberg`) |
| `remoteCatalogName` | Sent by Polaris as the `warehouse` parameter of the remote's `GET /v1/config` |
| `authenticationParameters.authenticationType` | `IMPLICIT`, `OAUTH`, `BEARER` (must be in the supported list); only `IMPLICIT` was exercised in the examples |
| `storageConfigInfo` | What Polaris vends from; with `roleArn` + `stsEndpoint` it can vend, with `stsUnavailable: true` it cannot |

After creation the examples grant `CATALOG_MANAGE_CONTENT` to the catalog's built-in
`catalog_admin` role so root can use it; the creating principal gets no data access implicitly.

## Behavior Measured on Polaris 1.8.0

| Behavior | Result |
|---|---|
| List and read remote tables through Polaris | Namespace and `sales` listed; 3 rows read with vended keys |
| Remote changes (new table, new append) | Visible on the next request, no sync job |
| Writes through Polaris | Real commits in the remote: 5 rows, 3 snapshots there; a namespace created through Polaris exists in the remote |
| RBAC before any grant | 403 |
| Namespace-level grant on a federated catalog | 404 `Namespace ... not found` (remote namespaces are not Polaris entities) |
| Catalog-level `TABLE_LIST` + `TABLE_READ_DATA` | Read works; append fails at storage (`OSError`, read-only vended keys) |
| Second catalog on the same location | 400 `One or more of its locations overlaps with an existing catalog` |
| Storage config with `stsUnavailable: true` | Metadata loads; with vending requested 400 `... but no credentials are available` |
| `DELETE /catalogs/{name}` with a non-empty remote | 204; remote keeps all 5 rows |

## Worked Example: Read-Only Partner Analysts

```python
federate(admin, "fed_partner", "http://rest:8181", "default", "s3://warehouse/rest/")
p = admin.create_principal("partner_analyst")
admin.create_principal_role("partner_analyst")
admin.assign_principal_role(p.name, "partner_analyst")
admin.create_catalog_role("fed_partner", "partner_reader")
for priv in ("TABLE_LIST", "TABLE_READ_DATA"):          # catalog level only
    admin.grant("fed_partner", "partner_reader", {"type": "catalog", "privilege": priv})
admin.assign_catalog_role("partner_analyst", "fed_partner", "partner_reader")
p.pyiceberg("fed_partner").load_table("partner_x.sales").scan().to_arrow()   # works
```

`federate()` and `unfederate()` live in `examples/federation/common/remotes.py`.

## Removing a Federation Safely

`unfederate()` sends only `DELETE /api/management/v1/catalogs/{name}`. Do not run a generic
"drop all namespaces and tables, then the catalog" cleanup on an external catalog: every drop is
forwarded, so it deletes the tables in the remote catalog.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `EXTERNAL` catalog with a connection not accepted (exact error not measured) | Federation flags not set | Set the three `polaris.features` above |
| 404 `Namespace ... not found` on a grant | Grant scoped below catalog level | Grant at catalog level, or migrate into an internal catalog for fine-grained grants |
| 400 `no credentials are available` | External catalog cannot vend (`stsUnavailable`) | Give it a role ARN + STS endpoint that cover the remote's storage |
| 400 `... overlaps with an existing catalog` | Two catalogs over one prefix | Separate prefixes; remove the federation before registering tables internally |
| Created (201) but every load fails | Wrong `remoteCatalogName` or an incompatible remote | Check what the remote expects as `warehouse`; see [external-catalog-products.md](external-catalog-products.md) |
| Remote tables disappeared | Catalog emptied through Polaris before deletion | Delete only the Polaris entity |
| Remote outage shows up as Polaris errors | Federation is a live proxy | Monitor the remote; keep federation for read-mostly use |

## Related

- [gradual-migration.md](gradual-migration.md): federate first, register later.
- [external-catalog-products.md](external-catalog-products.md): Nessie, Gravitino, Lakekeeper, Glue.
- `polaris-configuration-and-cli` skill: feature flags in general.
- `polaris-deployment` skill: S3/STS storage configuration for vending.
