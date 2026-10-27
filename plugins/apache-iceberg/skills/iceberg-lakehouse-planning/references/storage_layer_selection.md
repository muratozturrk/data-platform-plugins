# Selecting the Storage Layer for Iceberg

Scope: requirements, block vs object, Parquet + S3 API, measured governance, selection scorecard.
Latencies come from a shared laptop stack: compare them with each other, not with benchmarks.

## 1. Requirements first

| Category | Questions | Evidence |
|---|---|---|
| Performance | Read/write speed under concurrency; your access patterns | Throughput, p95/p99, concurrency tests |
| Security | Who reads/writes what; encryption; audit; location | IAM/bucket/prefix policies, SSE + KMS/BYOK, access logs, residency |
| Integrity | Survive hardware failure and human error | Durability nines, replication/EC, versioning, checksums, HA |
| Cost & operations | TCO and effort | Storage, requests, transfer, replication, archival; staff, tuning, scaling |

Keep requirements as data (`max_p99_small_get_ms`, `min_concurrency_speedup`, `checksum_validation`,
`strong_consistency`, `deny_anonymous`, `encryption_at_rest`) and evaluate every candidate against the
same object; the output is a list of gaps per store.

- Report percentiles: 200 GETs of 4 KiB gave mean 10.66 / p50 9.81 / p95 15.52 / p99 19.23 /
  max 22.55 ms on SeaweedFS 4.48.
- Concurrency 1 -> 16: throughput x1.41 (SeaweedFS) and x1.91 (RustFS 1.0.3), p99 x9.8 and x6.7.
  A test that reports only requests/second hides the tail.
- Both stores reject a bad `Content-MD5`, return MD5 ETags for single-part uploads, and show new and
  overwritten objects immediately in GET and LIST.
- Encryption at rest is configuration as much as product: RustFS accepts SSE-S3 only with
  `RUSTFS_SSE_S3_MASTER_KEY` or a KMS; SeaweedFS without a key source fails that requirement.
- Storage snapshots are not table backups: data files, manifests, manifest lists, `metadata.json` and
  the catalog pointer must be restored together.

## 2. Block vs object storage

| Aspect | Block (HDFS, local FS) | Object (S3 API) |
|---|---|---|
| Namespace | Directories, atomic rename | Flat keys; "directories" = synthesized `CommonPrefixes` |
| Scaling | Coupled to compute | Independent, elastic |
| Latency | Low, consistent IOPS | Per-request cost (100-200 ms under load on S3 is possible) |
| Ops | RAID, block size tuning, manual re-replication | Managed replication, versioning, lifecycle, IAM |
| Fit | Legacy Hadoop, latency-critical private clusters | Default for new lakehouses |

Commit protocols:

| Catalog + storage | How a commit becomes visible | Measured cost of one append |
|---|---|---|
| Hadoop catalog on local disk | Write `vN.metadata.json` (rename) + `version-hint.text` | `v1..v4.metadata.json`, hint file, `.crc` files |
| Hadoop catalog on `s3a://` | Rename emulated as COPY + DELETE (not atomic) | 65 S3 requests: 2 COPY, 11 DELETE, 18 LIST, 11 HEAD 404 |
| REST catalog + S3FileIO | Unique `0000N-<uuid>.metadata.json`, pointer swap | 13 requests: 7 PUT, 6 GET; no COPY/DELETE/LIST |

- Renaming a 50-object prefix = 50 COPY + 50 DELETE + 1 LIST (LIST pages hold <= 1,000 keys); `os.rename` of a 50-file dir is one
  metadata operation (same inode). A crash after 20 objects leaves 30 old + 20 new: both prefixes show
  an incomplete dataset. Iceberg never renames data files for this reason.
- 16 MiB as one object: 0.054 s sequential, 4 requests. As 256 x 64 KiB: 1.70 s sequential,
  0.25 s with 16 parallel readers, 1,024 requests. Parallelism hides most of the per-request cost;
  compaction removes it.

## 3. Standards: Parquet + S3 API

Reader access pattern on object storage: range GET of the footer, then range GETs of the needed
column chunks in matching row groups. Iceberg prunes whole files from manifest stats first.

| Operation on a 18,987,761 B Parquet file (10 row groups x 10 columns) | Transferred |
|---|---|
| Open | 1 GET of 65,536 B at offset 18,922,225 (the tail) |
| Read 1 of 10 columns | 2,042,076 B (10.8%) in 11 range GETs |
| Filter `id` in [45000, 55000) (2 row groups) | 1,963,062 B (10.3%), 10,000 rows |

- Iceberg tracks columns by field ID: after `RENAME COLUMN name TO full_name` the old file still says
  `name`, the new one `full_name`, both field ID 2; the query returns both rows with no rewrite.
- With 8 daily partitions, `SUM(amount)` over the table caused 27 ranged GETs; one day caused 4.
  Manifest pruning removes storage requests, not only CPU.
- Keep engine file/row-group defaults unless metadata volume demands otherwise; Parquet for analytics.

S3 compatibility suite (run on every candidate, through the real SDK):

| Check | Needed by | Required |
|---|---|---|
| PUT/GET roundtrip, HEAD size | All files, planning | yes |
| Suffix range `bytes=-N`, offset range (206) | Footer and column chunk reads | yes |
| Multipart upload (5 MiB + tail) | S3FileIO writes > `s3.multipart.size` (32 MB) | yes |
| Batch delete | `expire_snapshots`, purge | yes |
| List pagination | `remove_orphan_files`, prefix listing | yes |
| Overwrite visible at once | Strong read-after-write | yes |
| `If-None-Match: *`, `If-Match` | Conditional writes | no |
| No directory objects, empty prefix vanishes | AWS-like flat namespace | no |

SeaweedFS and RustFS pass all 8 required checks and both conditional writes. SeaweedFS answers
`HEAD dir/` with an `application/x-directory` object and keeps empty "directories" in delimiter
listings, so a Hadoop-catalog namespace stayed non-empty after `DROP TABLE ... PURGE`; RustFS behaves
like AWS S3. The same INSERT + DELETE + `rewrite_data_files` workload gave identical results on both.

FileIO choice: `S3FileIO` made 13 requests for an append (no LIST/DELETE); `HadoopFileIO` over
`s3a` made 30 through the same REST catalog (6 LIST, 6 DELETE for directory probes and fake
directory-marker cleanup). Use HadoopFileIO only for engines that speak nothing else.

## 4. Governance features, measured through Iceberg

Statuses: enforced / accepted (stored, effect not observable) / not supported / breaks writes.

| Feature | SeaweedFS 4.48 | RustFS 1.0.3 |
|---|---|---|
| Bucket policy, versioning, compliance Object Lock, SSE-C, tagging | enforced | enforced |
| Lifecycle | accepted | accepted |
| SSE-S3 | not supported | enforced (master key) |
| Default bucket encryption | breaks writes (every later PUT HTTP 500) | enforced |

S3FileIO governance properties (per catalog):

`s3.write.tags.<k>=<v>` (tag on write), `s3.delete-enabled=false` + `s3.delete.tags.<k>=<v>` (tag
instead of delete, lifecycle expires), `s3.sse.type=custom` + `s3.sse.key` + `s3.sse.md5` (SSE-C),
`s3.sse.type=s3` or `kms` + `s3.sse.key=<arn>`.

- A read-only key queries the table but `INSERT` fails with `S3Exception: Access Denied (403)`, nothing
  committed; a bucket-scoped key gets `AccessDenied` on other teams' files. Storage is the trust boundary.
- Versioned bucket: deleting one data file breaks the query (`NoSuchKeyException`, 404); removing the
  delete marker restores identical results. Versioning protects files, not metadata consistency.
- Compliance Object Lock: `DROP TABLE ... PURGE` succeeds but only adds delete markers (0 visible
  objects, 8 of 8 versions retained, version delete `AccessDenied`). Reconcile retention with GDPR
  erasure before choosing lock settings; governance mode allows authorized override.
- Soft delete: `expire_snapshots` reported 4 deleted data files, all still on storage tagged
  `{team: finance, iceberg-deleted: true}`. Without `older_than`, `expire_snapshots` uses a 5-day default.
- SSE-C: Parquet and Avro files written by Spark are unreadable without the key, but `metadata.json`
  is written by the REST catalog server with its own FileIO and stays unencrypted: configure the
  catalog server too.
- RustFS returns a non-MD5 ETag for SSE-S3 objects; Iceberg S3FileIO with `s3.sse.type=s3` then fails
  every write with "Data read has a different checksum than expected" (AWS SDK v2), while boto3 works.
  Test features with the client that will use them.

## 5. Vendor matrix and selection

| Vendor | Deployment | S3 API | Shines | Watch out |
|---|---|---|---|---|
| HDFS | On-prem | No | Existing Hadoop, data locality | Not elastic, weak multitenancy; transitional |
| Amazon S3 | AWS | Native | Object Lock, lifecycle, CRR, IAM, CloudTrail, S3 Tables | Request/egress/inter-region cost |
| GCS | GCP | Native | Strong consistency, storage classes, CMEK/HSM | Best inside Google ecosystem |
| Azure Blob/ADLS | Azure | Native | ADLS hierarchical namespace, atomic rename, POSIX ACLs | Latency on metadata-heavy ops |
| MinIO | On-prem/hybrid/edge | Full | Lightweight, EC, Object Lock, LDAP/OIDC | No global managed services |
| Ceph | On-prem | Full (RGW) | Unified object/block/file, self-healing | Highest ops complexity, tuning |
| NetApp StorageGRID | On-prem/hybrid | Full | Policy placement, WORM, geo-replication | Batch/large-object oriented |
| Everpure (formerly Pure Storage) FlashBlade | On-prem appliance | Full | Deterministic low latency, small objects | Premium price |
| Dell ECS | On-prem/hybrid | Full | Geo-replication, multitenancy, WORM | Network/pool tuning for peak |
| Wasabi | Vendor cloud | Full | Flat per-TB, no egress/API fees | Coarse ACL, storage only |

Scorecard: must-haves (legal, deployment) filter; weighted capabilities (1 = nice, 3 = central) score;
listed weaknesses subtract; ties keep matrix order; shortlist = reference picks + 2.

- Dashboards: shortlist `[Everpure, GCS, S3, MinIO]`, Ceph ranks below (needs tuning); HIPAA
  (must: encryption at rest + audit logging): `[ADLS, StorageGRID, Dell ECS, S3]`; startup:
  `[Wasabi, MinIO, S3, GCS]`, with on-prem required MinIO first and Wasabi filtered out.
- Rows built only from evidence: SeaweedFS misses `encryption_at_rest` + `audit_logging`, RustFS
  misses `audit_logging` (never probed), so neither qualifies for HIPAA; both qualify for long-term
  retention because Object Lock was enforced. Unprobed means absent.
- Raw capacity for 100 TB usable: 300 TB with 3x replication, 150 TB with EC 8+4.
- Converge on one platform; confirm it with a proof of concept under the most demanding scenario.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Every PUT HTTP 500 after enabling bucket encryption | Store has no SSE-S3 key source | Configure KMS/master key or remove default encryption |
| "Data read has a different checksum than expected" | Non-MD5 ETag for SSE objects | Different SSE mode/store, or verify SDK checksum settings |
| PURGE "succeeds" but storage does not shrink | Object Lock / versioning keeps versions | Lifecycle on noncurrent versions; align retention |
