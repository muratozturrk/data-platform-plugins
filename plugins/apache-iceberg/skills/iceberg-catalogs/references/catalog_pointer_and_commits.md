# The Catalog as Atomic Metadata Pointer

Scope: what a catalog stores, how an engine resolves a table, optimistic commits, catalog migration.

## Problem
Two engines append to `sales.orders` at the same time. Without one arbiter of "which metadata
version is current", both can believe their commit succeeded and one snapshot silently disappears.
Iceberg replaces locks with atomic replacement of table metadata, and that needs a component that
performs the replacement: the catalog. Storage, compute and metadata coordination stay separate,
so many engines can share tables across tools and clouds.

## Responsibilities

| Responsibility | What the catalog does |
|---|---|
| Registration and discovery | Resolves a table name in a namespace to a metadata location |
| Metadata location management | Owns the identifier -> metadata association; updates it transactionally |
| Namespace organization | Groups tables by domain (`analytics.marketing.sales_data`); unit of discovery and grants |
| Transaction coordination | Accepts a commit only if it is based on the current state |

## What is actually stored
A JDBC-layout catalog (Iceberg `JdbcCatalog`, PyIceberg `SqlCatalog`) has one table, `iceberg_tables`,
with exactly 5 columns:

```sql
SELECT catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location
FROM iceberg_tables WHERE table_namespace = 'sales' AND table_name = 'orders';
-- s3://warehouse/jdbc/sales/orders/metadata/00002-<uuid>.metadata.json | ...00001-<uuid>.metadata.json
```

Schema, partition specs, sort orders, snapshots and properties are only in the `metadata.json` the
pointer names. The catalog database stays small, and moving a table between catalogs is cheap.

## Resolution chain (every read)

```
name --catalog--> metadata.json --current-snapshot-id--> snapshot --> manifest list (.avro)
     --> manifests (file paths + column stats) --> data files to scan after pruning
```

```python
table = catalog.load_table("sales.orders")         # 1. name -> metadata.json (catalog's job)
snap = table.current_snapshot()                    # 2. metadata.json -> current snapshot
manifests = snap.manifests(table.io)               # 3. manifest list -> manifests
files = [e.data_file.file_path for m in manifests  # 4. manifests -> data files
         for e in m.fetch_manifest_entry(table.io)]
```

Three PyIceberg appends (fast-append, one new manifest per commit) resolve to 1 pointer, 1 snapshot,
1 manifest list, 3 manifests and 3 Parquet files.

## Optimistic commit (compare-and-set)

```
Writer A: load v1          Writer B: load v1
Writer A: commit v2 expecting v1  -> OK, pointer v1 -> v2
Writer B: commit v2' expecting v1 -> rejected (current is v2)
Writer B: reload v2, rebuild, commit v3 expecting v2 -> OK
```

- JDBC: `UPDATE iceberg_tables SET metadata_location = :new, previous_metadata_location = :old
  WHERE ... AND metadata_location = :old` - 0 rows updated means conflict.
- REST: requirements in the commit body; server answers 409 `CommitFailedException`.
- After each append `previous_metadata_location` equals the pointer from before the commit: a swap,
  not a blind overwrite. 3 appends produce 4 distinct metadata files, all still on storage, and
  `metadata-log` of the newest lists the 3 previous ones in order.

Retry the way Spark and Flink do internally - reload, never patch:

```python
from pyiceberg.exceptions import CommitFailedException

def append_with_retry(catalog, identifier, rows, max_attempts=5):
    for attempt in range(1, max_attempts + 1):
        table = catalog.load_table(identifier)      # fresh metadata every attempt
        try:
            table.append(rows)
            return attempt
        except CommitFailedException:
            time.sleep(0.05 * attempt)              # backoff
    raise RuntimeError("gave up")
```

A stale writer's first attempt fails with `Requirement failed: branch main has changed: expected id
5120849895417538088, found ...`; the second attempt succeeds and no row is lost (250 rows, 3 snapshots).

Spark/Java writers retry automatically; tune with table properties:

```sql
ALTER TABLE cat.sales.orders SET TBLPROPERTIES (
  'commit.retry.num-retries' = '10',
  'commit.retry.min-wait-ms' = '100',
  'commit.retry.max-wait-ms' = '60000');
```

## The pointer defines the table
Every metadata version is immutable; rollback and time travel are a matter of which version is
referenced. Registering an older metadata file under a new name gives that older state:

```python
old = catalog.load_table("sales.orders").metadata_location   # taken after the 1st append
# ... more appends ...
as_of = catalog.register_table("sales.orders_as_of_v1", old)
as_of.scan().to_arrow().num_rows      # 100   (current table: 200)
catalog.drop_table("sales.orders_as_of_v1")   # entry only - files belong to sales.orders
```

Do not write through such an alias: it creates a second owner (see below).

## Catalog migration with `register_table`

```python
def migrate(source, target, identifier):
    loc = source.load_table(identifier).metadata_location
    moved = target.register_table(identifier, loc)   # 1. target points at the same file
    source.drop_table(identifier)                    # 2. catalog entry only, NO purge
    return moved
```

- A REST-catalog table registered into a JDBC catalog and into Polaris: both read 200 rows at the
  original location; 11 objects under the table location before and after - zero bytes copied.
- **Split brain**: register without dropping the source and write through both catalogs - one sees
  210 rows, the other 205, and both histories fork from the same parent metadata file. Neither
  catalog knows about the other's snapshots; expiration in one can delete files the other needs.
- Cutover = stop writers, register, drop source without purge, repoint engines.
- Spark: `CALL target.system.register_table(table => 'db.t', metadata_file => 's3://.../vN.metadata.json')`.

## Gotchas

- The catalog is on the critical path of every query and commit: plan HA like a database.
- `DROP TABLE ... PURGE` (or PyIceberg `purge_table`) deletes data; plain `drop_table` removes only
  the entry in most catalogs - check the catalog's default before migrating.
- Engines that cache tables (Spark `cache-enabled=true` default) may serve a stale version when
  another client commits; set `cache-enabled=false` or refresh.
- A commit that fails with an unknown outcome (timeout, 5xx `CommitStateUnknownException`) may have
  succeeded: reload and check before retrying, and never clean up the written files.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `CommitFailedException: branch main has changed` | Another writer committed after your load | Reload table, rebuild commit, retry with backoff |
| Many retries on one table | Hot table, many concurrent writers | Batch commits, fewer writers, raise `commit.retry.*` |
| Two catalogs show different row counts for "one" table | Source entry kept after `register_table` | Pick an owner, drop the other entry without purge |
| Table reads an old state | Pointer names an older metadata file (alias, cache) | Check `metadata_location`; refresh / disable cache |
| Data gone after migration | Source dropped with purge | Drop entry only; restore from backups |
