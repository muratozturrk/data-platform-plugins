---
name: polaris-catalog-fundamentals
description: "Apache Polaris fundamentals - what Polaris is and where it sits (Iceberg REST catalog with multi-catalog, RBAC and credential vending), the catalog as metadata pointer and commit check as Polaris implements it (409 'branch main has changed'), the REST handshake (OAuth2 token with PRINCIPAL_ROLE scope, /v1/config?warehouse= prefix, LoadTable with X-Iceberg-Access-Delegation), catalog types and why REST/Polaris over Hadoop/JDBC/Hive, the REST client ecosystem (Trino, PyIceberg, Polars, DuckDB), managed Polaris, and the roadmap with its real status in 1.8.0 (generic tables, policy store, S3_COMPATIBLE, volumes). Use when explaining or choosing Polaris, wiring a first engine to it, debugging invalid_scope / 409 CommitFailedException / missing vended credentials, comparing Polaris with other catalogs, or checking whether a roadmap feature exists."
---

# Polaris Catalog Fundamentals

Apache Polaris is an open-source Iceberg REST catalog. Engines talk to it with the standard
Iceberg REST API; Polaris keeps the pointer to each table's current `metadata.json`, validates
commits, writes metadata on the server, authorizes every request with its own RBAC, and vends
short-lived storage credentials scoped to one table. It hosts many catalogs behind one endpoint
and adds APIs beyond the spec (generic tables, policies, OpenLineage). All facts here were
observed on Polaris 1.8.0 with PyIceberg 0.9.1 unless marked otherwise.

Generic Iceberg (Parquet, metadata tree, snapshots, partitioning) stays out of this skill: see
the `iceberg-table-format` skill of the apache-iceberg plugin. The generic catalog theory and the
full REST spec are in its `iceberg-catalogs` skill.

## When to Use This Skill

- Explaining what Polaris is, where it sits in a lakehouse, and what it adds over a plain REST catalog.
- Connecting a first engine (PyIceberg, Trino) to Polaris without storage keys.
- Debugging `invalid_scope`, 409 `CommitFailedException`, missing `storage-credentials`, 403 on load.
- Choosing between Hadoop / JDBC / Hive / Nessie / Glue / REST reference server / Polaris.
- Checking whether a roadmap feature (generic tables, policies, `S3_COMPATIBLE`, volumes, events) exists in your release.

## Decision Table

| Need | Technique / API | Reference |
|---|---|---|
| Understand what Polaris is and its APIs | Management API + Iceberg REST API + extensions | [polaris_overview.md](references/polaris_overview.md) |
| Create a catalog that vends credentials | `POST /api/management/v1/catalogs` with `roleArn` + `stsEndpoint` | [polaris_overview.md](references/polaris_overview.md) |
| Get a token / pick a catalog | `/v1/oauth/tokens` with `PRINCIPAL_ROLE:ALL`; `/v1/config?warehouse=` | [rest_catalog_and_credential_vending.md](references/rest_catalog_and_credential_vending.md) |
| Engines without bucket keys | `X-Iceberg-Access-Delegation: vended-credentials` | [rest_catalog_and_credential_vending.md](references/rest_catalog_and_credential_vending.md) |
| Handle concurrent writers | Reload + retry on `CommitFailedException` | [rest_catalog_and_credential_vending.md](references/rest_catalog_and_credential_vending.md) |
| Compare catalog types | Pointer location, config, authorization | [catalog_landscape.md](references/catalog_landscape.md) |
| Same code against several REST servers | One `RestCatalog`, server-specific properties | [catalog_landscape.md](references/catalog_landscape.md) |
| Several engines on one table | Trino REST connector, PyIceberg, Polars, DuckDB | [polaris_roadmap.md](references/polaris_roadmap.md) |
| Register Delta/Hudi tables | Generic tables API | [polaris_roadmap.md](references/polaris_roadmap.md) |
| Store maintenance rules next to tables | Policy store + `applicable-policies` | [polaris_roadmap.md](references/polaris_roadmap.md) |

## Topic: Where Polaris Sits

**What it is.** A REST server between engines and object storage. Polaris writes table metadata
with its own storage identity; engines receive only temporary, table-scoped keys.

| Polaris feature | Effect |
|---|---|
| Multi-catalog | Each catalog gets its own URL prefix, selected by `warehouse=<catalog>` |
| RBAC | Principals -> principal roles -> catalog roles -> grants, enforced for every engine |
| Credential vending | STS AssumeRole with a session policy for one table location |
| External catalogs | Other REST catalogs under Polaris RBAC (`polaris-catalog-federation` skill) |

```python
admin.create_catalog("sales", "s3://warehouse/sales")          # INTERNAL, S3 + roleArn + stsEndpoint
admin.grant("sales", "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
```

**Gotchas.** Polaris governs only engines that go through it: do not hand out bucket keys. A new
catalog's `catalog_admin` needs `CATALOG_MANAGE_CONTENT` before root can create content in it.
Drop fails while the catalog still has content or custom catalog roles.

## Topic: Catalog as Metadata Pointer and Commit Check

**What it is.** Every commit writes a new immutable `metadata.json`; Polaris stores which one is
current and rejects commits whose requirements no longer hold.

```python
# raw REST commit with a stale requirement
rest.c("POST", "/namespaces/ops/tables/events", json={
    "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": seen}],
    "updates": [{"action": "set-properties", "updates": {"owner": "late-writer"}}]})
# -> 409 CommitFailedException: Requirement failed: branch main has changed
```

**Gotchas.** PyIceberg 0.9.1 does not retry: catch `CommitFailedException`, `load_table` again,
retry with a bound (6 concurrent writers needed 15 retries in total, no rows lost). Readers see a
commit only on their next load. 4 commits = 4 `metadata.json` files on storage, 3 in `metadata-log`.

## Topic: REST Handshake and Credential Vending

**What it is.** Token -> config -> load -> commit, with Polaris-specific scope and prefix.

```python
RestCatalog("prod", uri="http://polaris:8181/api/catalog", warehouse="sales",
            credential=f"{client_id}:{client_secret}", scope="PRINCIPAL_ROLE:ALL",
            **{"header.X-Iceberg-Access-Delegation": "vended-credentials"})
```

LoadTable then returns `storage-credentials` with `s3.access-key-id` (`ASIA...`),
`s3.secret-access-key`, `s3.session-token`. They read that table's data files, `metadata.json`
and manifests, and are denied on other tables.

**Gotchas.** PyIceberg's default scope `catalog` gets `invalid_scope`. No delegation header (or a
catalog with `stsUnavailable: true`) means no vended keys. S3-compatible stores need
`s3.endpoint` and path-style settings in the client, still without access keys.

## Topic: Catalog Types and Why REST / Polaris

**What it is.** File-system (Hadoop), service (Hive, JDBC, Nessie, Glue) and REST catalogs differ
in where the pointer lives and who writes metadata.

| Measured | JDBC-style `SqlCatalog` | Polaris |
|---|---|---|
| Secrets in engine config | 2 long-lived bucket keys | 1 catalog credential |
| Commit with read-only storage keys | Fails (client writes metadata) | Works with no storage keys (server writes) |
| One key pair's reach | Every table | One principal's grants only (403 / `AccessDenied` elsewhere) |

**Gotchas.** "Same client" is not "same configuration": the Iceberg REST reference server needs
storage keys, has no auth and ignores `warehouse`. Polaris advertised 38 endpoints in `/v1/config`
vs 26 on the reference server.

## Topic: Ecosystem, Managed Polaris and Roadmap

**What it is.** Any REST client works on Polaris tables (Trino 476, PyIceberg, Polars, DuckDB
verified; Spark in the `polaris-spark` skill). Managed options: Dremio Catalog, Snowflake Open
Catalog (not testable locally; vendor extras are not portable). Roadmap status in 1.8.0:

| Item | Status |
|---|---|
| Generic tables | Shipped |
| Policy store | Shipped; types `system.data-compaction`, `system.snapshot-expiry`, `system.metadata-compaction`, `system.orphan-file-removal` |
| Roadmap names `system.compaction`, `system.snapshot_retention`, `system.row_access_policy`, custom types | Rejected (400) |
| Maintenance execution | Not in Polaris (stores and resolves policies only) |
| `S3_COMPATIBLE` storage type | Not shipped (400); use `S3` + `endpoint`/`stsEndpoint`/`pathStyleAccess` |
| Volumes | Not present (404) |
| Event listeners, federated identity | Present in the jars, not verified |
| Catalog UI | Not checked |

```
GET /api/catalog/polaris/v1/{catalog}/applicable-policies?namespace=eco&target-name=orders
-> table policy overrides namespace policy of the same type (inherited=false); others inherited=true
```

**Gotchas.** Check the release you run, not the roadmap. Generic tables are invisible to the
Iceberg API (404) but share the namespace's name space (409 on a clash). With Polars 1.31.0,
`scan_iceberg(...).select(pl.len())` returned 0 after Trino added a file while `collect()` returned
101 rows.

## Run the Examples

Two docker compose stacks, each with a pytest runner. Do not run both at once on a small machine.

```bash
cd examples/catalog-basics           # Polaris + Iceberg REST reference server + SeaweedFS
docker compose -p polaris-catalog-basics up -d --build
docker compose -p polaris-catalog-basics exec runner python -m pytest -v
docker compose -p polaris-catalog-basics down -v

cd examples/rest-ecosystem           # Polaris + Trino + SeaweedFS
docker compose -p polaris-rest-ecosystem up -d --build
docker compose -p polaris-rest-ecosystem exec runner python -m pytest -v
docker compose -p polaris-rest-ecosystem down -v
```

On Git Bash for Windows prefix the `exec` line with `MSYS_NO_PATHCONV=1`. Start from a fresh
stack (`down -v`); the ecosystem suite uses a fixed catalog name and order-dependent policy tests.
Details: [catalog-basics](examples/catalog-basics/README.md), [rest-ecosystem](examples/rest-ecosystem/README.md).

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Giving engines bucket keys "just in case" | Bypasses Polaris RBAC; keys open every table | Vended credentials only |
| Assuming the client library retries commits | PyIceberg 0.9.1 raises `CommitFailedException` | Reload + bounded retry |
| Keeping one table object across other writers' commits | Stale snapshot, conflicts | `load_table` before each write/read |
| Copying PyIceberg config from another REST server | Missing `scope`, unnecessary storage keys | `credential` + `scope=PRINCIPAL_ROLE:...` + delegation header |
| One catalog for every team with ad hoc grants | Hard to audit boundaries | One catalog per governance boundary |
| Designing on roadmap identifiers | Names changed (policy types), items missing (`S3_COMPATIBLE`, volumes) | Probe the API of the release you run |
| Expecting Polaris to compact or expire snapshots | It only stores and resolves policies | External maintenance job reading `applicable-policies` |

## References

- [references/polaris_overview.md](references/polaris_overview.md) - what Polaris is, APIs, catalog creation, managed Polaris.
- [references/rest_catalog_and_credential_vending.md](references/rest_catalog_and_credential_vending.md) - handshake, commits and conflicts, vending.
- [references/catalog_landscape.md](references/catalog_landscape.md) - catalog types, the three challenges, one client on two servers.
- [references/polaris_roadmap.md](references/polaris_roadmap.md) - REST ecosystem, generic tables, policy store, roadmap status in 1.8.0.
- Related skills: `iceberg-table-format`, `iceberg-catalogs`, `iceberg-maintenance` (apache-iceberg plugin); `polaris-catalog-federation`, `polaris-spark`, `polaris-dremio`, `polaris-snowflake`, `polaris-configuration-and-cli`, `polaris-deployment` (this plugin).
