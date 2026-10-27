# Format Versions, Snapshots, Puffin and Spec Conformance

## Problem
Iceberg is a written, versioned contract: metadata.json, manifest lists, manifests, delete files
and Puffin files have fixed fields and field ids, so any engine can read what another wrote.
Features that change the file layout need a new `format-version`; a reader must refuse a version
it does not know. "Supports v3" is not binary: libraries implement parts of a version.

## Versions at a glance

| Version | Status | Adds |
|---|---|---|
| v1 | Adopted | Snapshots, manifests, schema evolution, partition values in metadata; Parquet/Avro/ORC |
| v2 | Adopted | Row-level deletes (position and equality delete files), sequence numbers, stricter writer rules |
| v3 | Adopted | Deletion vectors (Puffin), row lineage, `timestamp_ns`/`timestamptz_ns`, `variant`, `geometry`, `geography`, `unknown`, column default values, multi-argument transforms, table encryption keys |
| v4 | Proposal | Single-file commits, Parquet-based metadata, typed projectable stats, relative paths |

### What a version bump changes on disk
| Element | v1 | v2 | v3 |
|---|---|---|---|
| metadata.json | `schema`, `partition-spec` (single) | + `last-sequence-number` | spec: + `next-row-id` |
| Snapshot | no `sequence-number` | `sequence-number` 1, 2, 3 ... | spec: + `first-row-id`, `added-rows` |
| Manifest list fields | 500-514 | + `sequence_number` 515, `min_sequence_number` 516, `content` 517 | spec: + `first_row_id` 520 |
| Manifest entry | `status`, `snapshot_id`, `data_file` | + `sequence_number` 3, `file_sequence_number` 4 | same |
| `data_file` | `block_size_in_bytes` 105 | + `content` 134, `equality_ids` 135, `referenced_data_file` 143; - 105 | + `content_offset` 144, `content_size_in_bytes` 145 |
| Row-level delete (Spark MOR) | Refused: `Deletes are supported in V2 and above`; copy-on-write works | Parquet position delete files (`file_path` id 2147483546, `pos` id 2147483545) | Deletion vector blob `deletion-vector-v1`, one per data file, in a Puffin file |

The same delete of 13 rows writes 11 Parquet position delete files on v2, and 11 DVs packed into
1 Puffin file on v3.

### Implementation gaps (Spark 3.5.5 + Iceberg 1.8.1, PyIceberg 0.9.1)
- Iceberg 1.8.1 writes v3 tables without row lineage: no `next-row-id`, `first-row-id`,
  `added-rows`, no `first_row_id` (520). The v3 metadata keys are identical to v2.
- `ALTER TABLE ... ADD COLUMN priority INT DEFAULT 5` on v3 succeeds but drops the default: no
  `initial-default`/`write-default`, existing rows read NULL, an INSERT omitting the column fails
  with `Cannot find data for the output column`.
- Spark 3.5 cannot create v3-only types: `TIMESTAMP_NTZ` -> `timestamp` (micros),
  `TIMESTAMP` -> `timestamptz`; no `timestamp_ns`.
- PyIceberg 0.9.1 reads v1, v2 (applies position deletes) and v3 without deletes; fails on v3
  with DVs (`'PUFFIN' is not a valid FileFormat`); cannot append to any v3 table
  (`Cannot write manifest for table version: 3`), although it can create one.
- `apache/iceberg-rest-fixture` newer than the client rejects v3 commits from 1.8.1 clients: keep
  catalog and client versions aligned.

| Condition | Version |
|---|---|
| Append-only, oldest readers in the stack | v1 |
| Updates, deletes, CDC, GDPR erasure | v2 (broad engine support) |
| Heavy merge-on-read, need DVs / new types / lineage | v3, after testing every reader and writer with real v3 files |
| High commit rates, portable tables | v4 (not finalized) |

## Snapshots and sequence numbers
- Every commit writes a new, immutable metadata.json. Six files for create + 5 commits, all
  byte-identical after later commits; `metadata-log` has 5 entries; size grows 1,066 -> 7,291 bytes.
  Snapshot expiration bounds that growth.
- Sequence numbers (v2+): each snapshot gets the next number; files inherit it.

| Rule | Effect |
|---|---|
| ADDED entries store null `sequence_number`/`snapshot_id`; readers inherit from the manifest list | A retried commit rewrites only the manifest list, not the manifests (all 32 entries of a 5-commit table are null on disk) |
| Only ADDED entries may inherit | EXISTING/DELETED entries carry explicit values |
| Data vs file sequence number | Compaction keeps the old data sequence number, records the commit in the file sequence number (`replace` snapshot seq 4: new file data seq 3, file seq 4) |
| Position delete applies to data seq <= delete seq; equality delete to data seq < delete seq | Engines know whether a delete is newer than the data |
| v1 manifests/snapshots have none | Read as sequence number 0 |

## Puffin files
```
Magic "PFA1" | Blob 1 | ... | Blob n | Footer
Footer = Magic | FooterPayload (UTF-8 JSON) | PayloadSize (int32 LE) | Flags (4 bytes) | Magic
```
Read the last 12 bytes (payload size, flags, magic), then the JSON footer (blob type, fields,
offset, length, codec), then only the needed blob by byte range. Flag bit 0 = LZ4 footer.

| Blob type | Writer | Content |
|---|---|---|
| `apache-datasketches-theta-v1` | `CALL lake.system.compute_table_stats(table => 'db.t')` | Theta sketch per column (NDV), zstd; `ndv` property |
| `deletion-vector-v1` | v3 row-level deletes | `length (int32 BE) | magic D1D33964 | 64-bit roaring bitmap | CRC-32 (BE)` |

- `compute_table_stats` writes one blob per column (7 columns: 62,820-byte file, 1,472-byte
  footer, both recorded in `metadata.json` `statistics[]`); sketch NDV equals exact
  `count(DISTINCT)` on 2,000 rows. Statistics files are tied to a snapshot id and go stale.
- A DV for 143 deleted positions out of 1,000 rows is a 326-byte blob. The manifest's
  `content_offset`/`content_size_in_bytes` equal the footer's offset/length, so readers skip the
  footer. DV blob `snapshot-id` and `sequence-number` are `-1` (inherited from the manifest entry);
  `fields` is `[2147483645]` (`_pos`).
- Validate length, magic and CRC: a flipped byte must fail loudly.

## REST catalog spec (client view)
- `GET /v1/config` returns `defaults`, `overrides`, `endpoints`; read `endpoints` before using
  optional routes. The 1.8.1 fixture advertises 26 endpoints (transactions, register, metrics,
  views) and no scan planning; `POST .../tables/{t}/plan` returns HTTP 400 `No route for request`.
- `loadTable` returns `metadata-location` plus a `metadata` object equal to the file on storage.
- Commit = `POST .../tables/{t}` with requirements + updates; 409 on a stale base.
  `POST /v1/{prefix}/transactions/commit` commits several tables atomically.

## Conformance checking
Turn the spec tables into data and check raw files, not engine behavior.
```python
TABLE_METADATA = {"format-version": "rrr", "table-uuid": "orr", "last-sequence-number": "-rr",
                  "schema": "r--", "schemas": "orr", "next-row-id": "--r"}      # r/o/- per v1 v2 v3
MANIFEST_LIST = {"manifest_path": (500, "rrr"), "sequence_number": (515, "-rr"),
                 "content": (517, "-rr"), "first_row_id": (520, "--o")}
```
- Check each manifest list and manifest against the `format-version` in its own Avro key-value
  metadata: files written before an upgrade keep their old layout.
- Compare bounds and counts with the Parquet file they describe; wrong stats cause wrong pruning.
- Results: Spark-written v1 and v2 tables pass (922 and 1,128 checks, 263 bound checks); v3 fails
  exactly `next-row-id`, `first-row-id`, `added-rows`.
- Rules worth checking: id references (schema, spec, sort order), partition field ids >= 1000,
  `main` ref = `current-snapshot-id`, seq <= `last-sequence-number`, unique and increasing seqs,
  known `operation` (`append`, `replace`, `overwrite`, `delete`), manifest counts vs entries,
  partition summaries cover entry values, DV entries have `referenced_data_file` + offset + size.

## Compatibility and migration
| Direction | Rule |
|---|---|
| Read older | Newer engines read older versions |
| Read newer | Older readers must refuse (`format-version` gate) |
| Write | No v3 features into a v1 table: refuse or upgrade |
| Upgrade | Explicit, metadata-only, forward-only |

```sql
ALTER TABLE db.t SET TBLPROPERTIES ('format-version'='2');   -- adds exactly 1 object: new metadata.json
```
- v1 -> v2 leaves old metadata byte-identical; `last-sequence-number` is 0; the current manifest
  list keeps `format-version` 1; after the first v2 commit (sequence 1) v1-era files read as seq 0.
- v2 -> v3 then a DELETE writes Puffin DVs against v1-era data files; mixed-version tables pass
  conformance except the 1.8.1 row-lineage gaps.
- Spark does not auto-upgrade: a merge-on-read DELETE on v1 fails and the table stays v1.
- Downgrade is refused: `Cannot downgrade v3 table to v2`. Recovery means registering an older
  metadata.json, so keep old metadata locations.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Deletes are supported in V2 and above` | MOR delete on v1 | Upgrade to v2 or set `write.delete.mode=copy-on-write` |
| `'PUFFIN' is not a valid FileFormat` in Python | v3 DVs, PyIceberg 0.9.1 | Compact deletes away, keep table on v2, or upgrade client |
| `Cannot write manifest for table version: 3` | PyIceberg cannot write v3 | Write via Spark/Java or stay on v2 |
| v3 commit rejected by catalog | Catalog newer/older than client | Align versions |
| Column DEFAULT ignored | 1.8.1 does not write v3 defaults | Populate in the writer |
| Stats-based plan uses old NDVs | Statistics file for an older snapshot | Re-run `compute_table_stats` |
