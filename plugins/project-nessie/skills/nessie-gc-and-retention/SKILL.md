---
name: nessie-gc-and-retention
description: Garbage collection and retention with Nessie: nessie-gc identify/expire/sweep, cut-off policies per reference, interaction with Iceberg expire_snapshots and remove_orphan_files, dry runs, scheduling as a Kubernetes CronJob, cost control, and the KVKK erasure procedure (delete, rewrite, expire, GC across all references, verify, crypto-shred, evidence). Use when planning retention, running GC, or proving data erasure.
---

# Garbage Collection and Retention

## Why GC
Nessie keeps history; deleting rows or dropping a table only moves the branch forward — old snapshots and files remain reachable from older hashes, tags and other branches. The only safe way to remove data is to remove it from the table (`DELETE`/rewrite) and then run GC so it is gone from Nessie history and from the lake.

## nessie-gc
Standalone tool (`nessie-gc` jar / container) with a small repository DB (JDBC or in-memory for one-shot):
1. **identify** — walk all references with cut-off policies (`--default-cutoff`, `--cutoff <ref-regex>=<duration|timestamp|N commits>`), mark live content/snapshots; result is a "live set" per run id.
2. **expire** — compute files of expired snapshots not referenced by live content.
3. **sweep** — delete expired files from object storage (`--defer-deletes` to review first).
`gc` runs identify+expire+sweep in one go; use `--dry-run` for the first production runs and keep the live-set DB for audit.
Cut-off examples: `main=P90D`, `t-*=P30D`, `wap-*=PT1H`, `exp-*=P7D`, `rel-*=NONE` (tags fully retained until policy deletes the tag).

## Interaction with Iceberg maintenance
Engine-side `expire_snapshots`/`remove_orphan_files` operate per table on one reference and do not know about other branches/tags; Nessie GC is the global authority. Order: engine `optimize` (compaction) → engine `expire_snapshots` (optional, speeds GC) → Nessie GC. Never run engine `remove_orphan_files` with a short retention while WAP branches are in flight.

## Scheduling
CronJob nightly per environment (platform namespace), service principal `svc-nessie-gc` with read access to all references and object-store delete rights on lake buckets (not on WORM archive buckets); metrics (files identified/expired/deleted, duration) to Prometheus; alerts on failures and on zero-deletion anomalies.

## KVKK erasure procedure (data subject or retention expiry)
1. Locate subject rows via the catalog/Fides data map; record scope.
2. Delete rows on the institution branch (Trino/Spark `DELETE` or `MERGE`), compact (copy-on-write rewrite so no delete-file remnants).
3. `expire_snapshots` on affected tables; drop or move any experiment/backfill branches that still reference old snapshots; retire tags per policy (tags older than retention are deleted by the OPA retention engine's decision).
4. Nessie GC with cut-offs that exclude the retained window.
5. Verify: scan reachable snapshots across all references for the subject key (job output), confirm files absent from storage listing.
6. WORM/backup copies: crypto-shred the subject's key in OpenBao (data unreadable even though bytes persist); record key id/scope/responsible in immudb with the GC run id.
7. Emit evidence record and close the DSR in Fides.

## Anti-patterns
- Running GC without a dry run after changing cut-offs
- Tags kept forever "for audit" without a retention decision
- Assuming engine `expire_snapshots` on `main` erased data referenced by another branch
- GC service principal with delete rights on Object-Lock archive buckets

Sources: Nessie docs — Garbage Collection (identify/expire/sweep, cut-off policies, repository), Authorization (historical data warning), Iceberg maintenance procedures in Trino/Spark; HKM technology list (lifecycle/retention engine, crypto-shredding, immudb evidence).
