# The REST Catalog Handshake, Commits and Credential Vending in Polaris

## Concept

Every Iceberg commit writes a new, immutable `metadata.json`. The catalog is the only place that
says which one is current and the only place that can reject a commit built on an outdated state.
Polaris implements this through the Iceberg REST spec: the client sends *requirements* and
*updates*, the server validates them, writes the new `metadata.json` and swaps its pointer. With
vending, the same server also hands out the storage credentials. Spec-level details (all
requirement types, multi-table transactions) are in the `iceberg-catalogs` skill of the
apache-iceberg plugin.

## The handshake as Polaris implements it

```
1. POST /api/catalog/v1/oauth/tokens
     grant_type=client_credentials, client_id, client_secret, scope=PRINCIPAL_ROLE:ALL
   -> {"access_token": ...}
2. GET  /api/catalog/v1/config?warehouse=<catalog>     (Authorization: Bearer ...)
   -> {"overrides": {"prefix": "<catalog>"}, "defaults": {...}, "endpoints": [... 38 entries ...]}
3. GET  /api/catalog/v1/<prefix>/namespaces/<ns>/tables/<t>
        X-Iceberg-Access-Delegation: vended-credentials
   -> {"metadata-location": "s3://.../00004-....metadata.json", "metadata": {...},
       "config": {...}, "storage-credentials": [{"prefix": ..., "config": {
           "s3.access-key-id": "ASIA...", "s3.secret-access-key": ..., "s3.session-token": ...}}]}
4. POST /api/catalog/v1/<prefix>/namespaces/<ns>/tables/<t>
   {"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": S1}],
    "updates": [{"action": "set-properties", "updates": {"owner": "late-writer"}}]}
   -> 200 with new metadata-location, or 409 CommitFailedException
```

| Step | Polaris-specific detail |
|---|---|
| Token | Scope must be `PRINCIPAL_ROLE:ALL` or `PRINCIPAL_ROLE:<name>`; anything else is `invalid_scope` |
| Config | `warehouse` selects one of many catalogs; the response `prefix` goes into every later path |
| Multi-level namespaces | Joined with `0x1F` (URL-encoded) in REST paths |
| LoadTable | Vended keys only when the delegation header is sent and the catalog has an STS role |
| Commit | Server writes the metadata file: the client needs no storage write access at all |

## Commit conflicts (optimistic concurrency)

Observed on Polaris 1.8.0:

- A raw REST commit with `assert-ref-snapshot-id` on an outdated snapshot returns HTTP 409
  `{"error": {"message": "Requirement failed: branch main has changed: expected id ... != ...",
  "type": "CommitFailedException", "code": 409}}`.
- PyIceberg 0.9.1 does not retry: a writer holding an old table object raises
  `CommitFailedException`; after `load_table` the append succeeds.
- 6 concurrent PyIceberg writers with an application retry loop needed 15 retries in total
  (1 to 6 attempts each) and all 60 rows arrived.

```python
def append_with_retry(engine, data, attempts=10):
    for attempt in range(1, attempts + 1):
        try:
            engine.load_table("ops.events").append(data)    # reload = fresh base snapshot
            return attempt
        except CommitFailedException:
            continue
    raise RuntimeError("gave up")
```

Spark has built-in commit retries (tune them per engine, see the `polaris-spark` skill).

## Credential vending

| Piece | Where it is configured | Example value |
|---|---|---|
| Role Polaris assumes | Catalog `storageConfigInfo.roleArn` | `arn:aws:iam::000000000000:role/lake-vendor` |
| STS endpoint (S3-compatible stores) | `storageConfigInfo.stsEndpoint` | `http://seaweedfs:8333` |
| Allowed locations | `storageConfigInfo.allowedLocations` | `["s3://warehouse/<catalog>"]` |
| Client opt-in | Header `X-Iceberg-Access-Delegation: vended-credentials` | PyIceberg: `header.X-Iceberg-Access-Delegation` |
| Trino opt-in | `iceberg.rest-catalog.vended-credentials-enabled=true` | in `config/trino/lake.properties` |

Polaris adds a session policy scoped to the table location and to read or read+write, so the
temporary keys only fit that one table. Verified: a principal with `TABLE_READ_DATA` on namespace
`finance` could read `finance.ledger`, got 403 loading `hr.ledger`, and its vended keys got
`AccessDenied` reading an `hr` manifest list directly from storage. The same keys read the
table's `metadata.json` and Avro manifests, not only data files.

## Worked example: reading the metadata tree with vended keys only

```python
load = CatalogClient(catalog, client_id, client_secret, vended=True).load_table("sales", "orders")
cfg = dict(load.get("config", {}))
for cred in load.get("storage-credentials", []):
    cfg |= cred["config"]
s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1",
                  aws_access_key_id=cfg["s3.access-key-id"],
                  aws_secret_access_key=cfg["s3.secret-access-key"],
                  aws_session_token=cfg.get("s3.session-token"))
meta = json.loads(s3.get_object(Bucket=b, Key=k)["Body"].read())   # b, k from load["metadata-location"]
```

Measured: 4 commits (create, add partition field, append, append) left 4 `metadata.json` files on
storage and 3 entries in `metadata-log`; Polaris pointed to the last one. The manifest list's
partition summaries (2025-01-06..12, 2025-02-03..09) let a reader skip one of two manifests for a
2025-01-08 filter.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| 409 `branch main has changed` | Another writer committed first | Reload the table and retry; bound the loop |
| Reader does not see a new commit | Long-lived table object | `load_table` again before reading |
| No `storage-credentials` in LoadTable | Header missing, or catalog created with `stsUnavailable` | Send the delegation header; configure `roleArn` + STS |
| Vended keys denied on another table | Working as designed: keys are table-scoped | Load each table to get its own keys |
| Same client code, different config per server | Auth and storage access are server-specific | Polaris: credential + scope; reference server: storage keys |

## Related

- [polaris_overview.md](polaris_overview.md), [catalog_landscape.md](catalog_landscape.md).
- Example tests: `catalog_mechanics/`, `metadata_pointer/`, `rest_spec/` in
  [../examples/catalog-basics/README.md](../examples/catalog-basics/README.md).
