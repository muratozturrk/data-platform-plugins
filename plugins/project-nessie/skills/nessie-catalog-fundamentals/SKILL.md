---
name: nessie-catalog-fundamentals
description: What Nessie is and how it models data: repository, references (branches, tags, detached), commits and hashes, content keys, namespaces, content types (Iceberg table/view, namespace), atomic multi-table commits, history and time travel, conflict detection, and how this differs from Polaris, Hive Metastore and Lakekeeper. Use when explaining Nessie concepts or designing a catalog layout.
---

# Nessie Catalog Fundamentals

## When to Use This Skill
- Designing repository/reference/namespace layout
- Explaining why a merge conflict or a stale hash happened
- Deciding Nessie vs other catalogs for a use case

## Core Concepts
- **Repository**: one versioned tree of content; the unit of a Nessie server/version store. Platform: one per environment (dev/stg/prod).
- **References**: `main` (default branch), branches (mutable), tags (immutable pointers), detached hashes. Each reference points at a commit hash.
- **Commit**: atomic set of operations (`Put`, `Delete`, `Unchanged`) on content keys; carries author, message, properties; multi-table changes are one commit → cross-table consistency.
- **Content key**: namespace path + name (`gold_sales.fct_orders`); namespaces are explicit content (`Namespace` type) and may carry properties.
- **Content types**: `ICEBERG_TABLE` (metadata location, snapshot/schema/spec ids), `ICEBERG_VIEW`, `NAMESPACE`, (Delta Lake table in older versions).
- **Operations**: create/assign/delete reference, commit, merge (with conflict detection per key; merge modes normal/force/drop), transplant (cherry-pick commits), diff between references, commit log, get content at any hash → time travel at catalog level (`table@branch#hash` or `table@ref` in Spark/Flink; `FOR VERSION AS OF` at table level in Trino).
- **Conflicts**: optimistic concurrency on expected hash; concurrent commits to the same key on one branch conflict; different keys merge cleanly.
- **Nessie Catalog (Iceberg REST)**: the same tree exposed via Iceberg REST (`/iceberg/{ref}/`), with Nessie managing table metadata JSON and pushing object-store config/security to clients.

## Nessie vs alternatives
| | Nessie | Apache Polaris | Hive Metastore | Lakekeeper |
|---|---|---|---|---|
| Model | Git-like, multi-table transactions, branches/tags | REST catalog with principals/roles/catalog roles | Thrift metastore, no versioning | REST catalog, Rust, single binary |
| Isolation unit | reference / namespace | catalog + namespace | database | warehouse / namespace |
| Object-store security | S3 signing or vended credentials via Nessie Catalog | credential vending | none | credential vending |
| Authorization | CEL metadata rules + external data authz (Ranger) | built-in RBAC | external (Ranger) | built-in |
| Engines | Trino (native + REST), Spark, Flink, Dremio, ClickHouse (REST), PyIceberg | REST engines | most | REST engines |
HKM chose Nessie for branch-based release/audit workflows and multi-table transactions; Polaris remains documented in the `apache-polaris` plugin for reference and migration.

## Anti-patterns
- Treating branches as long-lived environments (dev/stg/prod branches in one prod repo)
- Committing directly to `main` from pipelines
- Personal branches never deleted (GC cannot collect what a reference still points at)
- Assuming Nessie protects data files (it protects metadata; Ranger and object-store policies protect data)

See `references/sources.md`.
