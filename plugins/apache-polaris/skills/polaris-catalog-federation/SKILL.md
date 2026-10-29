---
name: polaris-catalog-federation
description: "Apache Polaris external catalogs and catalog federation - EXTERNAL catalogs with connectionConfigInfo (ICEBERG_REST, uri, remoteCatalogName, IMPLICIT/OAUTH/BEARER authentication), the ENABLE_CATALOG_FEDERATION feature flags, pass-through reads and writes with no sync delay, Polaris RBAC on remote data (catalog-level grants only), credential vending for remote tables, safe removal, gradual migration (federate first, then zero-copy register into an internal catalog, stop legacy writers), and how Nessie, Apache Gravitino, Lakekeeper and AWS Glue behave behind Polaris 1.8.0. Use when exposing another Iceberg REST catalog through Polaris, planning a migration into Polaris, or debugging 404 'Namespace not found' on grants, 'no credentials are available', 'overlaps with an existing catalog', Gravitino 'Couldn't find Iceberg configuration for catalog default' or 500 errors federating Lakekeeper."
---

# Polaris Catalog Federation

Polaris can expose other Iceberg REST catalogs as `EXTERNAL` catalogs. Engines then use one
endpoint and one identity for internal and external tables; Polaris forwards each namespace and
table call to the remote catalog and adds its own authentication, RBAC and credential vending.
This skill covers how federation works and where it stops (measured on Polaris 1.8.0), how to
use it for a gradual migration into Polaris, and what to expect from Gravitino, Lakekeeper,
Nessie and AWS Glue behind Polaris. The generic Iceberg REST protocol, `register_table` and
catalog selection are in the `iceberg-catalogs` skill of the apache-iceberg plugin.

## When to Use This Skill

- Partner or team-owned tables must stay in their own REST catalog but be governed centrally.
- Moving from a legacy catalog to Polaris without changing every engine and every table at once.
- Regional catalogs (one per jurisdiction) behind one global access point.
- Grants on a federated catalog fail, vended credentials are missing, or the external catalog is
  created but every load fails.
- Deciding between federation and migrating tables into an internal Polaris catalog.

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Expose a remote REST catalog in Polaris | `POST /catalogs` with `type: EXTERNAL` + `connectionConfigInfo` | [federation-mechanics.md](references/federation-mechanics.md) |
| Turn federation on | `ENABLE_CATALOG_FEDERATION`, supported connection and auth types | [federation-mechanics.md](references/federation-mechanics.md) |
| Govern who reads remote data | Catalog roles with catalog-level grants | [federation-mechanics.md](references/federation-mechanics.md) |
| Per-namespace or per-table grants on that data | Migrate into an internal catalog | [gradual-migration.md](references/gradual-migration.md) |
| Move tables into Polaris with no data copy | Federate, freeze, register current `metadata.json`, stop legacy writers | [gradual-migration.md](references/gradual-migration.md) |
| Regional metadata under local control | Gravitino per region, `remoteCatalogName: default_catalog` | [external-catalog-products.md](references/external-catalog-products.md) |
| Undo accidental drops in the remote | Lakekeeper soft deletion (not via Polaris in 1.8.0) | [external-catalog-products.md](references/external-catalog-products.md) |
| Branch-based ingestion behind Polaris | Nessie with an Iceberg REST endpoint (not verified) | [external-catalog-products.md](references/external-catalog-products.md) |
| AWS teams on Glue, others on Polaris | Glue Iceberg REST as external catalog (not tested) | [external-catalog-products.md](references/external-catalog-products.md) |

## Federation Mechanics

**What it is.** A Polaris catalog entity whose content lives elsewhere. Polaris sends
`GET /v1/config?warehouse=<remoteCatalogName>` to the remote, then proxies loads and commits.
Commits are real commits in the remote; remote changes are visible on the next request.

**Server flags** (off by default):

```yaml
polaris.features."ENABLE_CATALOG_FEDERATION": "true"
polaris.features."SUPPORTED_CATALOG_CONNECTION_TYPES": '["ICEBERG_REST"]'
polaris.features."SUPPORTED_EXTERNAL_CATALOG_AUTHENTICATION_TYPES": '["OAUTH","BEARER","IMPLICIT"]'
```

**Create** (`POST /api/management/v1/catalogs`):

```python
body = {"catalog": {"name": name, "type": "EXTERNAL",
    "properties": {"default-base-location": location},
    "storageConfigInfo": {"storageType": "S3", "allowedLocations": [location], "region": "us-east-1",
                          "endpoint": S3_ENDPOINT, "pathStyleAccess": True,
                          "roleArn": VENDOR_ROLE_ARN, "stsEndpoint": S3_ENDPOINT},
    "connectionConfigInfo": {"connectionType": "ICEBERG_REST", "uri": uri,
                             "remoteCatalogName": remote_catalog,
                             "authenticationParameters": {"authenticationType": "IMPLICIT"}}}}
```

**Gotchas (measured on 1.8.0)**

- Namespace-level grants on a federated catalog fail with 404 `Namespace ... not found`: remote
  namespaces are not Polaris entities. Catalog-level grants (`TABLE_LIST`, `TABLE_READ_DATA`) work.
- Without a vending-capable storage config (`stsUnavailable: true`) metadata loads, but a load
  with `X-Iceberg-Access-Delegation: vended-credentials` returns 400 `no credentials are available`.
- A second catalog over the same prefix is rejected: 400 `... overlaps with an existing catalog`.
- `DELETE /catalogs/{name}` returns 204 although the remote is not empty and leaves the remote
  untouched. Emptying the catalog through Polaris first would delete the remote tables.

## Gradual Migration

**What it is.** Users switch to Polaris first (federation), tables follow (registration into an
internal catalog), then legacy writers stop.

```python
pointers = {t: legacy_cat.load_table(f"{ns}.{t}").metadata_location for t in tables}
unfederate(admin, fed)                                   # entity only; locations may not overlap
admin.create_catalog(target, f"{LOCATION}{ns}", allowed=[LOCATION],
                     properties={"polaris.config.allow.unstructured.table.location": "true"})
for t, loc in pointers.items():
    rc.c("POST", f"/namespaces/{ns}/register", json={"name": t, "metadata-location": loc})
```

**Gotchas**

- Measured: same row counts before and after; 10 storage objects before and after (zero copy).
- Registered tables outside Polaris' own namespace layout need
  `polaris.config.allow.unstructured.table.location=true` on the target catalog.
- A legacy writer left running splits history: Polaris got id 14 only, the legacy catalog id 99 only.
- Drop Polaris entries with `purgeRequested=false` while files are shared.

## External Catalog Products

| Catalog | Behind Polaris 1.8.0 | Key setting |
|---|---|---|
| Iceberg REST reference 1.8.1 | Works | `remoteCatalogName: default` |
| Gravitino 1.3.0 | Works, writes committed by Gravitino, same `metadata-location` on both sides | `remoteCatalogName: default_catalog`; `GRAVITINO_ICEBERG_REST_CREDENTIAL_PROVIDERS` |
| Lakekeeper 0.13.8 | Create 201, load 500 (brotli `UnsatisfiedLinkError` in Polaris) | Soft deletion + remote signing work directly |
| Nessie | Not verified; needs a release with Iceberg REST | Docker Hub tops out at 0.76.6 |
| AWS Glue | Not tested (AWS account) | Iceberg REST endpoint, IAM |

## Run the Examples

`examples/federation/` holds the runnable suite (Polaris + reference REST server + Gravitino +
Lakekeeper + SeaweedFS S3/STS; 14 tests):

```bash
cd examples/federation
docker compose -p polaris-federation up -d --build
docker compose -p polaris-federation exec runner python -m pytest -v
docker compose -p polaris-federation down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. See
[examples/federation/README.md](examples/federation/README.md) for what each test package proves.

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Cleaning up a federated catalog with a generic drop-all helper | Drops are forwarded: remote tables are deleted | Delete only the Polaris entity |
| Promising per-namespace grants on federated data | Grants below catalog level fail (404) | Catalog-level grants, or migrate the tables |
| Federating without a vending storage config | Engines get metadata but no data access | `roleArn` + `stsEndpoint` covering the remote's storage |
| Re-pointing engines but leaving the legacy writer on | Split history, silent divergence | Freeze and retire the legacy catalog |
| Assuming any REST catalog federates | Lakekeeper 0.13.8 fails, Gravitino needs the right name | Test the exact version pair first |
| Registering with `purge=true` cleanup while files are shared | Deletes data the other catalog still uses | `purgeRequested=false` |

## References

- [references/federation-mechanics.md](references/federation-mechanics.md): flags, API fields, measured behavior, RBAC and vending limits, safe removal.
- [references/gradual-migration.md](references/gradual-migration.md): phases, zero-copy register, split history, cleanup rules.
- [references/external-catalog-products.md](references/external-catalog-products.md): Gravitino, Lakekeeper, Nessie, AWS Glue specifics.
- Related skills: `polaris-deployment` (storage/STS, running Polaris), `polaris-configuration-and-cli` (feature flags, CLI), `iceberg-catalogs` (apache-iceberg plugin).
