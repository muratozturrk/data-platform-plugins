---
name: iceberg-catalogs
description: "Apache Iceberg catalog layer - the catalog as atomic metadata pointer with optimistic commits, the Iceberg REST Catalog spec (config handshake, requirements/updates commits, 409 conflicts, multi-table transactions, register_table, credential vending), catalog options (REST fixture, Polaris, Nessie, Gravitino, Lakekeeper, Glue, Unity, Dremio, Snowflake Open Catalog, Hive, JDBC, Hadoop) and how to choose between them, Nessie branches, and catalog RBAC (Polaris principals, principal roles, catalog roles, grants). Use when configuring spark.sql.catalog.* or PyIceberg catalogs, debugging CommitFailedException / 'branch main has changed' / 403 / 'no credentials are available', migrating tables between catalogs, scripting REST commits, or designing catalog access control."
---

# Iceberg Catalogs

The catalog is the authoritative index of an Iceberg lakehouse: it maps each table identifier
to its current `metadata.json` and accepts a commit only if the table has not moved underneath
the writer (optimistic concurrency, compare-and-set on the pointer). Every read and every write
starts at the catalog; schema, snapshots and data stay in your object store. This skill covers
the pointer and commit protocol, the Iceberg REST Catalog specification, the catalog ecosystem
and how to choose, Nessie branches, and catalog-based access control with credential vending.

## When to Use This Skill

- Configuring a catalog for Spark (`spark.sql.catalog.<name>.*`), PyIceberg (`RestCatalog`, `SqlCatalog`) or another engine.
- Debugging `CommitFailedException`, `Requirement failed: branch main has changed`, 409/403/400 responses from a REST catalog.
- Writing CI tooling that changes schemas/properties over plain HTTP with commit requirements.
- Moving a table between catalogs (`register_table`) without copying data, or a Hive/Hadoop exit.
- Choosing a catalog (Polaris, Nessie, Gravitino, Lakekeeper, Glue, Unity, Dremio, Snowflake Open Catalog, JDBC, Hive) from requirements.
- Zero-copy environments, stress-test clones and atomic multi-table changes with Nessie branches.
- Designing RBAC (principals, principal roles, catalog roles, grants) and short-lived vended storage credentials.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Understand what a catalog stores and how a commit swaps it | Pointer + compare-and-set, reload-and-retry | [catalog_pointer_and_commits.md](references/catalog_pointer_and_commits.md) |
| Move a table to another catalog without copying files | `register_table` + drop source entry without purge | [catalog_pointer_and_commits.md](references/catalog_pointer_and_commits.md) |
| Talk to any compliant catalog from any language | REST spec: `/v1/config`, LoadTable, commit with requirements | [rest_catalog_spec.md](references/rest_catalog_spec.md) |
| Change several tables all-or-nothing | `POST /v1/{prefix}/transactions/commit` or a Nessie branch merge | [rest_catalog_spec.md](references/rest_catalog_spec.md) |
| Pick a catalog for a team / platform | Requirements scorecard (musts eliminate, preferences rank) | [catalog_options_and_selection.md](references/catalog_options_and_selection.md) |
| Branch production for tests or ETL isolation | Nessie branches, `table@branch`, merge | [catalog_options_and_selection.md](references/catalog_options_and_selection.md) |
| Give engines no long-lived bucket keys; per-team access | Polaris RBAC + vended credentials | [catalog_access_control.md](references/catalog_access_control.md) |

## Topics

### The catalog as atomic metadata pointer
A JDBC-layout catalog entry is 5 columns: `catalog_name, table_namespace, table_name,
metadata_location, previous_metadata_location`. Everything else lives in the file the pointer names.
- **Mechanic**: engine resolves name -> `metadata.json` -> current snapshot -> manifest list ->
  manifests -> data files. A commit writes a new immutable metadata file and swaps the pointer only
  if it still equals the version the writer read (`UPDATE ... WHERE metadata_location = old`).
- **Gotchas**: a stale writer gets `Requirement failed: branch main has changed: expected id ..., found ...`;
  reload the table and rebuild the commit, never patch the old object. After 3 appends there are
  4 metadata files, all kept, listed in `metadata-log`. Registering an older metadata file under a new
  name gives a read-only view of that older state (100 rows vs 200 current).
- Ref: [references/catalog_pointer_and_commits.md](references/catalog_pointer_and_commits.md) · Code: [examples/catalog_layer/catalog_pointer/](examples/catalog_layer/catalog_pointer/)

### Catalog requirements
Turn audit findings into hard constraints (must) and weighted preferences; measure candidates through
the public protocol (LoadTable latency, commits under contention, auth, vending).
- **Mechanic**: a candidate missing a must is eliminated with the reason, never ranked second.
  Correctness (`lost_updates <= 0`) is a must.
- **Facts**: 50 LoadTable calls after warm-up: REST fixture p50 36.1 ms / p95 57.6 ms; Polaris (auth on
  every call) p50 73.1 ms / p95 107.9 ms. 4 writers x 5 schema commits on one table: 20/20 landed on both,
  after 21 and 42 409-retries (5.5 and 4.3 commits/s). A single hot table is limited by optimistic retries,
  not by catalog-wide throughput.
- Ref: [references/catalog_options_and_selection.md](references/catalog_options_and_selection.md) · Code: [examples/catalog_layer/requirements_probe/](examples/catalog_layer/requirements_probe/)

### Iceberg REST Catalog specification
Engine-neutral OpenAPI: commit logic moves from per-language client libraries into the server.
- **Mechanic**: `GET /v1/config?warehouse=` returns `defaults`, `overrides` (`prefix`) and `endpoints`;
  `POST .../tables/{t}` carries `requirements` (`assert-table-uuid`, `assert-current-schema-id`,
  `assert-ref-snapshot-id`, ...) and `updates` (`add-schema`, `set-current-schema`, `add-snapshot`,
  `set-snapshot-ref`, `set-properties`). The server checks, writes `metadata.json`, swaps the pointer, or
  returns 409 `CommitFailedException`.
- **Gotchas**: a commit without requirements cannot conflict and can overwrite a concurrent change.
  The same plain-HTTP client passes 12/12 tests against the reference server (26 endpoints advertised)
  and Polaris (38); a failing requirement in a two-table `transactions/commit` changes neither table.
- Ref: [references/rest_catalog_spec.md](references/rest_catalog_spec.md) · Code: [examples/catalog_layer/rest_spec/](examples/catalog_layer/rest_spec/), [common/iceberg_http.py](examples/catalog_layer/common/iceberg_http.py)

### Catalog options
Built-in (Hadoop, Hive, JDBC), open-source services (Polaris, Nessie, Gravitino, Lakekeeper), managed
(Glue, Dremio catalog, Snowflake Open Catalog, Unity). Hadoop is deprecated; Hive is a migration bridge.
- **Mechanic**: the pointer lives in `version-hint.text` (Hadoop), a SQL row (JDBC), a REST service's
  store (Polaris, Glue, Lakekeeper) or a Nessie commit. Same Spark SQL runs unchanged on all four in the lab.
- **Gotchas**: PyIceberg has no `type="hadoop"` (raises `ValueError`; read with `StaticTable` only).
  `register_table` copies 0 objects (11 before and after), but keeping the source entry creates two owners
  whose histories fork (210 rows in one catalog, 205 in the other). Polaris rejects registered tables outside
  allowed locations unless `polaris.config.allow.unstructured.table.location=true`.
- Ref: [references/catalog_options_and_selection.md](references/catalog_options_and_selection.md) · Code: [examples/catalog_layer/catalog_options/](examples/catalog_layer/catalog_options/)

### Choosing a catalog by scenario
Eight scenario profiles (HMS migration, AWS-native fintech, healthcare residency, SaaS simplicity,
multicloud federation, stress-test clones, legacy query federation, Python-first) each leave exactly one
eligible catalog.
- **Gotchas**: outcomes are temporary; one changed must flips the decision (AWS-native -> cloud-agnostic
  makes a Polaris-based managed service win). The "no service, Python-first" answer is the deprecated Hadoop
  catalog; requiring safe concurrent writers turns it into JDBC (`SqlCatalog`).
- Ref: [references/catalog_options_and_selection.md](references/catalog_options_and_selection.md) · Code: [examples/catalog_layer/catalog_scenarios/](examples/catalog_layer/catalog_scenarios/)

### Nessie refs and branches
A branch is a named pointer to a catalog-wide commit: zero-copy clone of many tables, atomic multi-table merge.
- **Mechanic**: Nessie API v2 `POST /trees?name=&type=BRANCH`, Spark `nessie.ns.\`prices@stress_x\``,
  `POST /trees/main@<hash>/history/merge`. Creating a branch copies 0 objects; a 40% price shock on the
  branch moves exposure 5,950,000 -> 3,570,000 while `main` stays 5,950,000; a two-table ETL branch merges as
  one commit on `main`.
- **Gotchas**: `NessieCatalog` sets `gc.enabled=false`, so `DROP TABLE ... PURGE` fails ("GC is disabled").
- Ref: [references/catalog_options_and_selection.md](references/catalog_options_and_selection.md) · Code: [examples/catalog_layer/catalog_scenarios/nessie_refs.py](examples/catalog_layer/catalog_scenarios/nessie_refs.py)

### Catalog-based access control
The catalog is the policy point: principal -> principal role -> catalog role -> grants on catalog /
namespace / table, plus short-lived storage credentials vended per table and per access mode.
- **Mechanic**: client sends `X-Iceberg-Access-Delegation: vended-credentials`; Polaris checks grants, calls
  STS `AssumeRole` with a session policy for the table prefix (read or read/write), returns `ASIA...` keys (~1 h).
- **Gotchas**: a reader's `append` fails at storage (`ACCESS_DENIED`) and a metadata-only commit fails at the
  catalog (403) - two layers. Revoking a catalog role applies on the next request with the same token. No STS ->
  400 "no credentials are available". Row/column security is not standardized yet.
- Ref: [references/catalog_access_control.md](references/catalog_access_control.md) · Code: [examples/catalog_layer/access_control/](examples/catalog_layer/access_control/)

## Cross-cutting Best Practices

- Plan catalog availability like a database: if it is down, no table can be read or written even with healthy storage.
- Prefer a REST catalog whenever more than one engine or language writes the same tables; commit logic is then implemented once.
- Always send requirements for read-modify-write commits; on 409 reload and rebuild, with bounded retries and backoff.
- Treat a 5xx/timeout on commit as *unknown state*: reload and check before retrying, never delete the files you wrote.
- Migrate catalogs metadata-only: register the current metadata file in the target, then drop the source entry **without purge**; never leave two owners.
- Disable client-side table caching (`cache-enabled=false`) when other clients commit to the same tables and you need fresh reads.
- Keep engines free of bucket keys: catalog-vended, table-scoped, short-lived credentials; grant at namespace level per domain.
- Encode selection requirements as musts vs preferences with a `why`, and re-run the decision as the platform grows.
- New deployments: avoid Hadoop and Hive catalogs; keep a REST-compatible path open.
