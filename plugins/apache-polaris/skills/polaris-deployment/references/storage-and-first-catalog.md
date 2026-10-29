# Storage Integration and the First Catalog

## Concept

Every Polaris catalog carries a `storageConfigInfo`: which storage type, which locations it may
use (`allowedLocations`), and how Polaris obtains credentials. Polaris itself writes
`metadata.json`; engines write data files. With credential vending, Polaris assumes a role
through STS and hands engines short-lived keys scoped to one table location (read or read+write),
so engines never hold long-lived storage keys. Vending in depth (engine headers, scopes):
`polaris-catalog-fundamentals` skill of this plugin.

## Storage Types

| Type | Default on 1.8.0 | Notes |
|---|---|---|
| `S3` (incl. S3-compatible) | Allowed | `roleArn` + optional `stsEndpoint`, `endpoint`, `pathStyleAccess` |
| `GCS`, `AZURE` | Allowed | Default `SUPPORTED_CATALOG_STORAGE_TYPES` is `S3, GCS, AZURE` |
| `FILE` | **Refused**: 400 `Unsupported storage type: FILE` | Needs `ALLOW_INSECURE_STORAGE_TYPES=true` and FILE in `SUPPORTED_CATALOG_STORAGE_TYPES` |

Enabling FILE for a quickstart:

```yaml
polaris.features."ALLOW_INSECURE_STORAGE_TYPES": "true"
polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES": '["FILE","S3","GCS","AZURE"]'
polaris.features."DROP_WITH_PURGE_ENABLED": "true"
```

## S3-Compatible Storage with STS (SeaweedFS in the examples)

```json
{"storageType": "S3", "allowedLocations": ["s3://warehouse/sales/"], "region": "us-east-1",
 "endpoint": "http://seaweedfs:8333", "pathStyleAccess": true,
 "roleArn": "arn:aws:iam::000000000000:role/lake-vendor", "stsEndpoint": "http://seaweedfs:8333"}
```

| Field | Meaning |
|---|---|
| `roleArn` | Role Polaris assumes to vend table-scoped keys |
| `stsEndpoint` | STS endpoint of an S3-compatible store (omit for AWS) |
| `endpoint`, `pathStyleAccess` | S3-compatible endpoint; path-style addressing |
| `stsUnavailable: true` | No STS: Polaris can serve metadata but cannot vend (`no credentials are available`) |

Server side, the Polaris container gets its own `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
`AWS_REGION` for writing metadata. The object store must trust Polaris for `sts:AssumeRole` on
that role; the examples' SeaweedFS `iam.json` defines `lake-vendor` with a policy on the
`warehouse` bucket, and Polaris narrows each vended session to one table location. A runnable
SeaweedFS S3 + STS setup is in the `polaris-catalog-federation` skill's
`examples/federation/` (`config/iam.json`, `config/s3.json`, `common/polaris.py: create_catalog()`).

Measured role-ARN validation: the placeholder `arn:aws:iam::xxxxxxxxx:role/polaris-storage` is
rejected by 1.1.0-incubating-SNAPSHOT (400 `Invalid role ARN format`) but accepted by 1.8.0 (201);
on 1.8.0 a bad ARN only fails later, when credentials are vended. Validate ARNs yourself.

## FILE Storage: Permissions

Polaris runs as uid 10000. On a shared volume owned by root (mode 755) its first metadata write
fails: `RuntimeIOException: Failed to create file: file:/data/lab/orders/metadata/...`. Quickstart
environments ship a helper that pre-creates `<namespace>/<table>/metadata` and `data` with mode
777 (`examples/quickstart-and-persistence/quickstart_env/icebergdata/table_setup.py`); after it,
writes succeed. Ownership afterwards: `metadata.json` uid 10000 (Polaris), Parquet and Avro uid 0
(the engine). FILE storage stays a learning tool: no isolation, no vending.

## The First Catalog and First User

Catalog body (`POST /api/management/v1/catalogs`):

```json
{"name": "polariscatalog", "type": "INTERNAL",
 "properties": {"default-base-location": "file:///data"},
 "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///data"]}}
```

The setup sequence that makes a fresh deployment usable (all measured 201/200 on the quickstart
image; the new principal then created a table and wrote 2 rows):

| # | Call | Body |
|---|---|---|
| 1 | `POST /api/catalog/v1/oauth/tokens` (form) | `grant_type=client_credentials`, root id/secret, `scope=PRINCIPAL_ROLE:ALL` |
| 2 | `POST /catalogs` | catalog above |
| 3 | `POST /catalogs/polariscatalog/catalog-roles` | `{"catalogRole": {"name": "polariscatalogrole"}}` |
| 4 | `POST /principals` | `{"principal": {"name": "polarisuser"}}` -> `clientId`, `clientSecret` |
| 5 | `POST /principal-roles` | `{"principalRole": {"name": "polarisuserrole"}}` |
| 6 | `PUT /principals/polarisuser/principal-roles` | `{"principalRole": {"name": "polarisuserrole"}}` |
| 7 | `PUT /principal-roles/polarisuserrole/catalog-roles/polariscatalog` | `{"catalogRole": {"name": "polariscatalogrole"}}` |
| 8 | `PUT /catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants` | `{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}` |

Measured quirks: a principal body without the `principal` wrapper and with an undefined
`"type": "user"` field was accepted (201) and `type` ignored. `CATALOG_MANAGE_PRIVILEGES` and
`TABLE_DELETE` do not exist (grant 400); the real names are `CATALOG_MANAGE_ACCESS` and
`TABLE_DROP`. The same sequence via CLI: `polaris-configuration-and-cli` skill.

## Catalog vs Namespace

A catalog is the highest isolation level inside a realm (own namespaces, security and storage
config). Create a new catalog for a hard security boundary, a separate environment
(dev/staging/prod) or a different storage backend / IAM role; use a namespace for grouping
under the same governance and storage. Treat catalogs as static, not as something created and
deleted per job.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| 400 `Unsupported storage type: FILE` | 1.8.0 refuses insecure types | Enable flags (labs only) or use object storage |
| `Failed to create file` on first write | uid 10000 cannot write the shared directory | Pre-create table directories writable for Polaris |
| Catalog created but engines get no data access | `stsUnavailable` or wrong role/endpoint | Correct `roleArn` + `stsEndpoint`, trust policy for Polaris |
| Bad ARN only noticed at query time | 1.8.0 accepts any ARN at create | Validate ARNs in your provisioning |
| 400 on grant | Non-existent privilege name | Use the documented privilege names |

## Related

- [containers-and-persistence.md](containers-and-persistence.md), [observability-and-readiness.md](observability-and-readiness.md).
- `polaris-catalog-federation` skill: SeaweedFS S3/STS example stack, external catalogs.
- `polaris-configuration-and-cli` skill: storage-type flags per realm, CLI catalog creation.
