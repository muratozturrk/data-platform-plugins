# Disaster Recovery

Scope: catalog backup and `register_table`, storage protection, cross-region replication with
`rewrite_table_path`, rollback and purge of bad writes, validation against storage, RPO/RTO.

## Problem
An Iceberg table has three loosely coupled layers, and each one fails on its own:

| Failure | Layer | Recovery |
|---|---|---|
| Catalog entry lost or corrupted | Catalog | `register_table` from a known `metadata.json`. Needs catalog exports |
| File deleted or overwritten | Storage | Object versioning, object lock, backups |
| Region outage | Storage + catalog | Cross-region replication with path rewrite, plus replicated catalog state |
| Bad write, dropped partition | Table state | Time travel to diagnose, `rollback_to_snapshot` to undo |
| Unknown recoverability | Process | RPO/RTO per table, automation, drills, validation scans |

Without a catalog entry the table is invisible, even with every file intact. Object storage is durable, but an
unversioned delete cannot be undone. Iceberg 1.x metadata stores **absolute paths**, so a bucket copy is not a usable
backup. A backed-up `metadata.json` is worthless if its data files are gone.

## Catalog: back up the mapping, not the tables

Data and metadata already live in object storage. The piece that gets lost is the catalog's name ->
`metadata-location` mapping. Export it on a schedule (hourly for transactional tables) to a DR bucket:

```python
ids = requests.get(f"{rest}/v1/namespaces/{ns}/tables", timeout=30).json()["identifiers"]
for ident in ids:
    body = requests.get(f"{rest}/v1/namespaces/{ns}/tables/{ident['name']}", timeout=30).json()
    backup[f"{ns}.{ident['name']}"] = {"metadata_location": body["metadata-location"],
                                       "current_snapshot_id": body["metadata"].get("current-snapshot-id")}
```

Restore only what is missing, so the restore is idempotent:

```sql
CREATE NAMESPACE IF NOT EXISTS sales;
CALL lake.system.register_table(table => 'sales.orders',
  metadata_file => 's3://warehouse/sales/orders/metadata/00003-<uuid>.metadata.json');
```

After re-registration, the fingerprints, snapshot lists, current snapshot ids, time travel and tags all match. A
second restore returns `[]`, and the table accepts writes. `register_table` already sets the current snapshot from
the metadata file, so a follow-up `set_current_snapshot` is **not** needed.

**No backup at all**: register the highest-numbered `NNNNN-<uuid>.metadata.json` under `<location>/metadata/`.
This restores property-only commits too (e.g. `00004-...` after create + 3 appends + `SET TBLPROPERTIES`). Use it
only as a last resort: a commit that wrote its metadata file but lost the catalog swap also leaves a high-numbered file.

Versioned catalogs (Nessie) give you catalog-level branches/tags and push-pull between catalogs instead.

## Split-brain

If you register the same `metadata.json` in a second catalog (or under a second name) and **write** there, two owners
share the same files. Measured damage: an overwrite in the secondary, followed by `expire_snapshots(older_than => now,
retain_last => 1)`, deleted every data file of the primary and its current manifest list, and the primary scan failed
with 404. Register exported metadata **read-only** when you only want to inspect a production state.

## Table state: rollback, purge, validate

```sql
CALL lake.system.rollback_to_snapshot('sales.orders', <last_good_id>);
CALL lake.system.set_current_snapshot('sales.orders', <any_snapshot_id>);   -- also allows non-ancestors
CALL lake.system.expire_snapshots(table => 'sales.orders', snapshot_ids => array(<bad1>, <bad2>));
```

| Aspect of `rollback_to_snapshot` | Effect |
|---|---|
| Current pointer | Moves `main`. Affects all later reads and writes |
| Newer snapshots | Stay in history until expired |
| Newer files | Stay on storage until those snapshots are expired |
| Schema | Not reverted: a column added later (`channel`) is still there |
| Expired targets | Cannot be restored. Retention limits how far back you can go |

- **Pick the target by data quality, not by time.** The newest `audited_*` tag written by WAP is a defensible
  restore point, while "yesterday 02:00" is a guess. Walk `parent_id` from `main` back to the target to list the
  abandoned snapshots (and to check that the target really is an ancestor).
- **Purge by id.** `expire_snapshots(older_than => <time before the bad write>)` leaves the abandoned snapshot in
  place, because it is newer than the cutoff. `snapshot_ids => array(...)` removed it together with its 2 data files.
- **For one corrupted partition**, copy that partition forward from an old snapshot (`INSERT OVERWRITE ... SELECT ...
  VERSION AS OF`) instead of a whole-table rollback, so unrelated changes are kept.

Recovery DAG (manual trigger with `{"table": "ns.t"}`): `find_last_good -> rollback -> purge_abandoned ->
validate -> notify`. After two bad commits, all 5 tasks succeed, 2 snapshots are purged, the fingerprint is
restored, and `iceberg_abandoned_snapshots=2` and `iceberg_recovery_success=1` are pushed.

**Validate against storage.** Metadata tables say what *should* exist. Use HEAD requests to prove it does:

```python
ml = spark.sql(f"SELECT manifest_list FROM {t}.snapshots WHERE snapshot_id = {sid}").first()[0]
manifests = [r.path for r in spark.sql(f"SELECT path FROM {t}.manifests VERSION AS OF {sid}").collect()]
files = [r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files VERSION AS OF {sid}").collect()]
missing = [p for p in [ml, *manifests, *files] if not head_ok(p)]
```

`SELECT count(*)` still succeeds with a data file missing, because Iceberg answers it from manifest statistics. Use
HEAD checks or a full scan (`SELECT sum(amount)`). The `manifests` table column is `path`, not `file_path`. After a
deleted file is restored from a versioned or backup copy, validation comes back clean.

## Cross-region replication

A naive bucket copy (what S3 CRR does) produces metadata in which every `file_path` and the table location still
start with `s3://warehouse/`. When the primary region is lost, that replica fails. Rewrite the paths instead:

```sql
CALL lake.system.rewrite_table_path(
  table => 'sales.orders',
  source_prefix => 's3://warehouse/sales/orders',
  target_prefix => 's3://warehouse-dr/sales/orders',
  staging_location => 's3://warehouse-dr/staging/sales_orders/');
-- returns latest_version, file_list_location (CSV of source,target pairs: rewritten metadata + data files)
```

Copy every pair from the file list, then call `register_table` on `<target_prefix>/metadata/<latest_version>` in the DR
catalog. Every path is then under `s3://warehouse-dr/...`, the snapshot ids match the primary's, and validation is
clean. After the primary prefix is deleted, the rewritten replica still returns the primary's fingerprint.

- `rewrite_table_path` writes its copy plan through the **Hadoop FileSystem API**. With only `S3FileIO` it fails with
  `No FileSystem for scheme "s3"`. Add hadoop-aws (S3A) and set `spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`.
- Snapshot ids survive the rewrite, so **replication lag** = primary snapshot ids missing in the replica. Hold
  expiration and orphan cleanup on the primary until the lag is empty, so a replica never loses files it has not copied yet.
- Re-replicate incrementally by running the procedure again (optionally with `start_version`/`end_version`), then
  re-register.

## RPO / RTO and process

| Practice | Detail |
|---|---|
| RPO/RTO per table | A financial reporting table may need an RPO of minutes and an RTO under an hour. These targets drive export frequency and failover design |
| Least privilege | Only the maintenance service may delete. Use object versioning / object lock against ransomware and overwrites |
| Guard destructive procedures | Validate bounds (snapshot age, file counts, data volume) before running expire/orphan cleanup |
| Version-matched restores | Test restores with the exact client versions you would use in an incident |
| Drills | Quarterly, both partial (missing manifest) and full (region loss). Log duration and issues |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Table gone, files intact | Catalog entry lost | `register_table` from the latest export |
| Restored table reads the old bucket | Naive copy, absolute paths | `rewrite_table_path`, copy the plan, register |
| `No FileSystem for scheme "s3"` | Hadoop S3A missing | hadoop-aws + `fs.s3.impl` |
| Storage did not shrink after rollback | Abandoned snapshots still referenced | `expire_snapshots(snapshot_ids => ...)` |
| Primary returns 404 after a test in another catalog | Split-brain expiration | Read-only registrations only. Restore files from versioning |
| `count(*)` works but scans fail | Missing data file | HEAD-validate, restore the object version |
