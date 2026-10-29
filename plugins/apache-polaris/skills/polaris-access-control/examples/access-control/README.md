# Polaris Access-Control Examples

Runnable pytest suites against a real Apache Polaris server with S3-compatible storage and STS.
Every result in the skill's references was measured with these tests (39 tests, about 35 s on a
fresh stack).

| Package | What it proves |
|---|---|
| `catalogs/` | Catalogs are isolated namespace trees with their own roles; grants and vended credentials never cross catalogs; `allowedLocations` is enforced |
| `principals/` | Secret shown once, OAuth2 client credentials (1 h JWT), admin `reset` vs self `rotate` (two-secret rotation), creation/assignment/revocation, a roster-based lifecycle review |
| `catalog_roles/` | All 25 documented privileges measured against 20 operations; documented privilege claims checked one by one; namespace vs table grants and inheritance |
| `principal_roles/` | Many-to-many principals/roles/catalog roles; token scope activates one role; an unheld role in the scope is rejected |
| `roles_as_code/` | Three use cases (multi-tenant, cross-team, sensitive data) as an RBAC model applied idempotently (plan/apply/drift), access matrix verified, 7-rule audit |

## Stack

| Component | Version | Role |
|---|---|---|
| Apache Polaris (`apache/polaris:latest`) | 1.8.0 when measured | Catalog, RBAC, credential vending |
| SeaweedFS (`chrislusf/seaweedfs`) | latest | S3-compatible storage with STS `AssumeRole` |
| Test runner (`python:3.11-slim`) | Python 3.11, PyIceberg 0.9.1, pytest 8.3.4 | Management + Iceberg REST calls, data reads/writes |

No Spark: these tests exercise the control plane. Spark against Polaris is in the `polaris-spark` skill.

## How to Run

```bash
docker compose -p polaris-access-control up -d --build
docker compose -p polaris-access-control exec runner python -m pytest -v
docker compose -p polaris-access-control down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

Root credentials of the local server: `root` / `s3cr3t` (bootstrap, local use only). The tests never
give engines static S3 keys: storage access only comes from credentials Polaris vends per table.

## Layout

```
common/polaris.py        Management API + Iceberg REST client, vended-credential helpers
conftest.py              admin/root fixtures, seed_table(), measurements.json writer
config/                  SeaweedFS identities (s3.json) and STS role (iam.json)
catalogs/ ... roles_as_code/   one package per topic, tests next to the code they exercise
```

## Environment notes

- New catalogs get a `catalog_admin` role without `CATALOG_MANAGE_CONTENT`; the tests grant it so root can create tables with vended credentials.
- Polaris state is in memory: restarting the `polaris` service resets catalogs, principals and roles.
- `DROP TABLE ... PURGE` is enabled per catalog with `polaris.config.drop-with-purge.enabled=true`.
- `apache/polaris:latest` is not pinned; a newer image can change measured privilege semantics, which `test_privilege_matrix_matches_measured_reference` will flag.
