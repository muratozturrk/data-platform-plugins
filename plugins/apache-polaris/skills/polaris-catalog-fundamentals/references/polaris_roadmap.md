# Polaris Ecosystem and Roadmap (Status in 1.8.0)

## Concept

Because Polaris implements the Iceberg REST spec, any REST client can use it without
Polaris-specific code. Beyond that, a mid-2025 roadmap announced generic tables, a policy store,
a table maintenance framework, pluggable persistence, S3-compatible storage, a catalog UI,
catalog federation, federated roles, event listeners and volumes. The roadmap itself warned that
none of it was released at the time. This reference records what Polaris 1.8.0 actually does.
**Check the release notes of the version you run, not the roadmap, before designing around a
feature**; several names changed on the way to a release.

## Status table

| Item | Status in 1.8.0 | Evidence |
|---|---|---|
| Generic tables | Shipped | Tested: create / load / list / drop |
| Policy store (CRUD, attach, inheritance, override) | Shipped | Tested |
| Policy versioning | Partly: version counter + optimistic concurrency; no rollback endpoint observed | Tested |
| Roadmap policy type names, custom types | Not shipped as named (400) | Tested |
| Table maintenance execution | Not in Polaris: it stores and resolves policies, nothing runs compaction or expiry | Tested (no such endpoint/keys) |
| SQL / NoSQL persistence | `relational-jdbc` shipped; `nosql` (beta); `in-memory`, `in-memory-atomic` | Runtime defaults, not run here |
| `S3_COMPATIBLE` storage type | Not shipped (400); use type `S3` with `endpoint`, `stsEndpoint`, `pathStyleAccess`; flag `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` exists | Tested |
| Catalog federation | EXTERNAL catalogs with an Iceberg REST connection | See `polaris-catalog-federation` |
| Federated identity / roles | Authentication types `internal`, `external`, `mixed` exist | Jars only, **not verified** |
| Event listeners | 149 event types (e.g. `BEFORE/AFTER_COMMIT_TRANSACTION`, `BEFORE/AFTER_REFRESH_TABLE`); listener types `persistence-in-memory-buffer`, `aws-cloudwatch` | Jars and runtime defaults, **not verified** |
| Volumes for unstructured data | Not present (404) | Tested |
| Catalog UI | **Not checked** | - |

## Generic tables

Non-Iceberg tables (Delta, Hudi, plain files) registered in Polaris namespaces through separate
endpoints. Polaris stores name, format, base location, doc and properties; it does not read or
validate Delta metadata, and there are no commits, snapshots or vended credentials for them.
Spark reaches them through the Polaris Spark plugin (`polaris-spark-runtime-3.5_2.12`, see the
`polaris-spark` skill).

```
POST   /api/catalog/polaris/v1/{catalog}/namespaces/{ns}/generic-tables
       {"name": "delta_events", "format": "delta",
        "base-location": "s3://warehouse/lake/eco/delta_events",
        "doc": "Delta table registered in Polaris", "properties": {"owner": "analytics"}}
GET    .../generic-tables              -> {"identifiers": [...]}
GET    .../generic-tables/{name}       -> {"table": {"format": "delta", ...}}
DELETE .../generic-tables/{name}       -> 204
```

Measured: create 200, load returns format `delta`, list shows it, drop 204, later load 404. It is
not in the Iceberg `listTables` result and Iceberg `loadTable` returns 404. A generic table named
like an existing Iceberg table returned 409 `Table already exists`: names are unique across entity
types in a namespace.

## Policy store

```
POST /api/catalog/polaris/v1/{catalog}/namespaces/{ns}/policies
     {"name": "ns_retention", "type": "system.snapshot-expiry", "content": "{\"enable\": true}"}
PUT  .../policies/{name}/mappings   {"target": {"type": "namespace", "path": ["eco"]}}
PUT  .../policies/{name}/mappings   {"target": {"type": "table-like", "path": ["eco", "orders"]}}
PUT  .../policies/{name}            {"current-policy-version": 0, "content": "..."}
GET  /api/catalog/polaris/v1/{catalog}/applicable-policies?namespace=eco&target-name=orders
```

| Policy type accepted in 1.8.0 | Roadmap name (rejected, 400) |
|---|---|
| `system.data-compaction` | `system.compaction` |
| `system.snapshot-expiry` | `system.snapshot_retention` |
| `system.metadata-compaction` | - |
| `system.orphan-file-removal` | - |
| - | `system.row_access_policy`, `custom.*` |

The 400 message lists the valid values. Inheritance, measured on `eco.orders` with namespace
policies `ns_compaction` (data-compaction) and `ns_retention` (snapshot-expiry) plus the table's
own `orders_compaction` (data-compaction):

```python
effective == {"system.data-compaction": ("orders_compaction", False),   # table overrides namespace
              "system.snapshot-expiry": ("ns_retention", True)}         # inherited from namespace
```

Other observations: attach returns 204; a second policy of the same type on the same table is 409;
`content` that is not valid JSON is 400 `Invalid policy`; an update with `current-policy-version: 0`
produced version 1 and a second update with the stale version 0 got 409. Policy privileges such as
`POLICY_CREATE`, `POLICY_ATTACH`, `POLICY_DETACH` let SRE teams manage rules without data write
access. Maintenance property keys shown in the roadmap (`polaris.maintenance.data-rewrite.policy`
and similar) do not occur in 1.8.0; the policy types replace them. An external job (Spark, Flink)
must read `applicable-policies` and run the work (see the `iceberg-maintenance` skill of the
apache-iceberg plugin).

## REST ecosystem (one table, many clients)

Measured with Polaris 1.8.0, Trino 476, PyIceberg 0.9.1, Polars 1.31.0, DuckDB 1.3.2:

| Client | Connects through | Result |
|---|---|---|
| Trino | Iceberg REST connector with vended credentials, no S3 keys | Read 100 rows / sum 5625.0; CTAS of 50 rows loadable by PyIceberg; INSERT visible to others |
| PyIceberg | Native REST client | Writer of `eco.orders`; sees 101 rows and 2 snapshots after Trino's INSERT |
| Polars | `pl.scan_iceberg(table)` on a PyIceberg table | Same 100 rows / 5625.0 |
| DuckDB | SQL on PyIceberg's Arrow scan (its own Iceberg extension not used) | Same 100 rows / 5625.0 |

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=lake
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:s3cr3t
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.vended-credentials-enabled=true
fs.native-s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.path-style-access=true
```

Listed as REST-compatible but not run here: Spark (see `polaris-spark`), Flink, Presto, StarRocks,
Kafka Connect, Confluent, Redpanda, Estuary, DataHub.

## Pitfalls

| Symptom | Cause | Fix |
|---|---|---|
| `Unknown policy type ...` (400) | Roadmap names (`system.compaction`, `system.snapshot_retention`) | Use `system.data-compaction`, `system.snapshot-expiry` |
| Policies attached but nothing compacts | Polaris only stores and resolves | Schedule a maintenance job that reads `applicable-policies` |
| Iceberg client cannot load a Delta table | Generic tables live behind `/generic-tables` | Use the Polaris Spark plugin or the generic-table API |
| `S3_COMPATIBLE` catalog rejected (400) | Type does not exist in 1.8.0 | `storageType: S3` + `endpoint`, `stsEndpoint`, `pathStyleAccess` |
| Polars `scan_iceberg(...).select(pl.len())` returns 0 after another engine wrote | Observed with Polars 1.31.0 (`collect()` returned all 101 rows) | Check counts from a real scan when mixing writers |

## Related

- [polaris_overview.md](polaris_overview.md) for managed Polaris (Dremio Catalog, Snowflake Open Catalog).
- Example stack: [../examples/rest-ecosystem/README.md](../examples/rest-ecosystem/README.md).
