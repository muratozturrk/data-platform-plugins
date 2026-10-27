---
name: iceberg-table-format
description: "Apache Iceberg table format internals and core features: table format vs file format, metadata tree (metadata.json, manifest list, manifests, binary bounds), planning-time pruning and hidden partitioning, ACID commits and optimistic concurrency, schema and partition evolution, time travel and rollback, branches, tags and write-audit-publish, format versions v1/v2/v3, sequence numbers, Puffin statistics and deletion vectors, spec conformance and version upgrades. Use when explaining why plain Parquet lakes fail, debugging why a query scans too many files, reading raw Iceberg metadata, handling CommitFailedException, evolving schemas or partition specs, choosing or upgrading format-version, or checking engine compatibility (Spark, PyIceberg, DuckDB)."
---

# Iceberg Table Format

Apache Iceberg is a table format: a specification for tracking which data files form a table,
its schemas, partition specs and snapshots. Data stays in ordinary Parquet (or ORC/Avro) files on
object storage; Iceberg adds a layered metadata tree and a catalog pointer that is swapped
atomically on every commit. Everything this skill covers (pruning, ACID, evolution, time travel,
branches, version upgrades) is a metadata operation: none of it rewrites data files.

## When to Use This Skill

- Explaining file format vs table format vs catalog, or why a folder of Parquet files is not a table.
- A query reads too many files: checking partition transforms, sort order, bounds, scan metrics.
- Reading or debugging raw metadata: metadata.json, manifest lists, manifests, binary bounds.
- `CommitFailedException`, concurrent writers, stale readers, orphan files after failed commits.
- Adding, renaming, dropping columns; changing partitioning; rolling back a bad load.
- Staging writes on a branch (write-audit-publish), pinning states with tags.
- Choosing v1/v2/v3, upgrading `format-version`, or checking what Spark/PyIceberg support.
- Reading Puffin statistics or deletion vectors; validating a new writer against the spec.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Show why plain lakes fail; pick a layer (file/table/catalog) | Iceberg table + REST catalog | [lakehouse_basics.md](references/lakehouse_basics.md) |
| Prune by time without users knowing the partition column | Hidden partitioning `days(ts)` | [metadata_and_planning.md](references/metadata_and_planning.md) |
| Explain/debug which files a query reads | Walk metadata tree, scan metrics, metadata tables | [metadata_and_planning.md](references/metadata_and_planning.md) |
| Decode manifest bounds / partition summaries | Single-value serialization | [metadata_and_planning.md](references/metadata_and_planning.md) |
| Several writers on one table, stale commits | Optimistic concurrency + refresh/retry | [transactions_and_evolution.md](references/transactions_and_evolution.md) |
| New, renamed, dropped columns | Schema evolution by field id | [transactions_and_evolution.md](references/transactions_and_evolution.md) |
| Partitions too coarse/fine | Partition evolution (`REPLACE PARTITION FIELD`) | [transactions_and_evolution.md](references/transactions_and_evolution.md) |
| Undo a bad load, audit old state | Time travel, `rollback_to_snapshot`, tags | [transactions_and_evolution.md](references/transactions_and_evolution.md) |
| Quality gate before readers see data | Branch + audit + `fast_forward` (WAP) | [transactions_and_evolution.md](references/transactions_and_evolution.md) |
| Choose or upgrade format version | v1/v2/v3 matrix, in-place forward-only upgrade | [format_versions_and_spec.md](references/format_versions_and_spec.md) |
| Row-level deletes representation | v2 position deletes vs v3 deletion vectors | [format_versions_and_spec.md](references/format_versions_and_spec.md) |
| NDV statistics, DV files | Puffin reader | [format_versions_and_spec.md](references/format_versions_and_spec.md) |
| Certify a new engine/connector | Spec conformance checker on raw files | [format_versions_and_spec.md](references/format_versions_and_spec.md) |

## Topics

### File format vs table format
Parquet describes one file (columnar layout, footer stats). A table format describes a dataset:
which files, which schema, which version. Without it, readers see half-written batches, retries
duplicate rows, overwrites delete data first and schema drift breaks readers at query time.
- **Mechanic**: readers only see files referenced by the current snapshot. A failed job never
  swaps the catalog pointer; Spark also deletes the files its successful tasks wrote.
- **Gotchas**: never read a table's `data/` prefix directly (35,000 raw rows vs 15,000 table rows
  after one copy-on-write `DELETE`); a lakehouse still needs curated aggregates for BI.
- Ref: [references/lakehouse_basics.md](references/lakehouse_basics.md) · Code: [examples/lakehouse_basics/](examples/lakehouse_basics/), [examples/iceberg_core_features/files_vs_table/](examples/iceberg_core_features/files_vs_table/)

### Metadata tree and planning-time pruning
Catalog -> `metadata.json` -> manifest list (per snapshot, partition summaries) -> manifests
(per file: partition tuple, column bounds, counts) -> data files.
- **Mechanic**: step 1 skips whole manifests from manifest-list summaries; step 2 skips files
  from partition values and bounds. Scan metrics: `totalDataManifest=5, skippedDataManifests=4,
  resultDataFiles=1, skippedDataFiles=3`. A 3-day filter on a 30-day table opens 2 metadata files
  and 0 data files; the Hive-style layout needs 31 LIST calls and cannot prune.
- **Gotchas**: bounds are little-endian bytes (decimals big-endian), strings truncated to 16
  characters; fastavro returns `date` objects for `days()` partitions; `skippedDataFiles` counts
  only files inside scanned manifests.
- Ref: [references/metadata_and_planning.md](references/metadata_and_planning.md) · Code: [examples/iceberg_core_features/metadata_tree/](examples/iceberg_core_features/metadata_tree/), [examples/lakehouse_basics/hive_vs_iceberg_planning/](examples/lakehouse_basics/hive_vs_iceberg_planning/)

### Hidden partitioning
`PARTITIONED BY (days(order_ts))`: users filter on `order_ts`, the planner projects the filter
through the transform. Same naive timestamp filter: Hive-style 31 of 31 files, Iceberg 1 of 31.
- **Gotchas**: pruning inside a partition needs clustering (`WRITE ORDERED BY`), not more partitions.
- Ref: [references/metadata_and_planning.md](references/metadata_and_planning.md) · Code: [examples/iceberg_core_features/evolution_time_travel_wap/](examples/iceberg_core_features/evolution_time_travel_wap/)

### ACID and optimistic concurrency
Write files, then swap the pointer only if the base is unchanged; otherwise refresh and retry.
- **Mechanic**: Spark retries appends automatically (4 parallel appends -> 4 snapshots, linear
  history). PyIceberg raises `CommitFailedException: Requirement failed: branch main has changed`
  and does not retry. Readers keep the snapshot they loaded.
- **Gotchas**: failed commits leave orphan files; Spark's catalog cache can hide other engines'
  commits (`cache-enabled=false`).
- Ref: [references/transactions_and_evolution.md](references/transactions_and_evolution.md) · Code: [examples/iceberg_core_features/files_vs_table/](examples/iceberg_core_features/files_vs_table/), [examples/spec_conformance/snapshots/](examples/spec_conformance/snapshots/)

### Schema and partition evolution
Columns are tracked by field id: add, rename, reorder, drop keep the identical data file set.
Partition evolution adds a new spec; old files keep theirs and both layouts are pruned.
- **Gotchas**: appends with drifted schemas are rejected (good); unsafe type changes are rejected;
  only widening is allowed.
- Ref: [references/transactions_and_evolution.md](references/transactions_and_evolution.md) · Code: [examples/iceberg_core_features/evolution_time_travel_wap/](examples/iceberg_core_features/evolution_time_travel_wap/)

### Time travel, rollback, branches and tags
`VERSION AS OF` / `TIMESTAMP AS OF` read an older file list; `rollback_to_snapshot` moves `main`
back without copying data; WAP writes to a branch and `fast_forward` publishes.
- **Gotchas**: after `DROP COLUMN`, time travel shows the old values but a filter on that column
  fails (`Cannot find field`); read `main` from `t.refs`, not the newest snapshot; retention limits
  how far back you can go.
- Ref: [references/transactions_and_evolution.md](references/transactions_and_evolution.md) · Code: [examples/iceberg_core_features/evolution_time_travel_wap/](examples/iceberg_core_features/evolution_time_travel_wap/)

### Format versions, snapshots and sequence numbers
v1 = file tracking; v2 = row-level deletes + sequence numbers; v3 = deletion vectors, row lineage,
new types, defaults. Upgrades are explicit, metadata-only (1 new metadata.json) and forward-only.
- **Mechanic**: ADDED manifest entries store null sequence numbers and inherit from the manifest
  list; compaction keeps the data sequence number and sets a new file sequence number.
- **Gotchas**: Iceberg 1.8.1 writes v3 without row lineage and drops column defaults; PyIceberg
  0.9.1 cannot append to v3 or read DVs; Spark never auto-upgrades; downgrade is refused.
- Ref: [references/format_versions_and_spec.md](references/format_versions_and_spec.md) · Code: [examples/spec_conformance/format_versions/](examples/spec_conformance/format_versions/), [examples/spec_conformance/compatibility/](examples/spec_conformance/compatibility/)

### Puffin files and spec conformance
Puffin = blobs + JSON footer (theta sketches from `compute_table_stats`, v3 deletion vectors).
A conformance checker turns the spec's required/optional tables into data and validates raw files.
- **Gotchas**: statistics files describe one snapshot; check each manifest against its own
  `format-version`; verify bounds against the Parquet data, since wrong stats mean wrong pruning.
- Ref: [references/format_versions_and_spec.md](references/format_versions_and_spec.md) · Code: [examples/spec_conformance/puffin/](examples/spec_conformance/puffin/), [examples/spec_conformance/conformance/](examples/spec_conformance/conformance/)

## Best Practices

1. Address tables through the catalog, never through storage paths.
2. Partition with transforms on the columns users filter on; cluster inside partitions with a sort order.
3. Prove pruning with scan metrics or metadata tables, not query time.
4. Wrap non-Spark commits in a refresh-and-retry loop; schedule `remove_orphan_files`.
5. Evolve schemas explicitly; treat a rejected append as a contract check, not an error to bypass.
6. Use WAP branches for pipelines whose output must pass checks; tag published states.
7. Pick the lowest format version every reader in the stack supports; test v3 with real files.
8. Keep catalog, engine and client Iceberg versions aligned and pinned.
9. Keep old metadata.json locations: they are the only rollback path for an upgrade.
10. Expire snapshots deliberately: retention is the time-travel window.
