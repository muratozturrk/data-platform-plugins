# Iceberg REST Catalog API on Polaris

## Concept

Under `/api/catalog`, Polaris implements the Iceberg REST catalog specification: the calls Spark,
Trino, Flink, Dremio, PyIceberg and Snowflake make. One server hosts many catalogs, so every
client first asks `/v1/config` for a catalog's **prefix** and then uses `/v1/{prefix}/...` paths.
On Polaris the prefix equals the catalog name.

## Config Handshake

```
GET /api/catalog/v1/config?warehouse=dev
-> {"defaults": {...}, "overrides": {"prefix": "dev"}, "endpoints": [... 38 entries on 1.8.0 ...]}
```

Without `?warehouse=` the call is 400 `Please specify a warehouse`. Engines pass the catalog name
as `warehouse` in their catalog config for exactly this reason.

## Namespaces

| Operation | Call | Measured |
|---|---|---|
| Create | `POST /v1/{p}/namespaces` `{"namespace": ["analytics"], "properties": {...}}` | 200; Polaris adds a `location` property |
| Load | `GET /v1/{p}/namespaces/{ns}` | 200 |
| Exists | `HEAD /v1/{p}/namespaces/{ns}` | 204 |
| Set / remove properties | `POST /v1/{p}/namespaces/{ns}/properties` `{"updates": {...}}` or `{"removals": [...]}` | 200 when sent separately |
| Drop | `DELETE /v1/{p}/namespaces/{ns}` | 204 empty; 409 `not empty` otherwise |

Sending `updates` and `removals` in one request returned 409 `Concurrent modification`, with the
updates applied and the removals not. Multi-level namespaces are joined with `0x1F` in paths.

## Tables

| Operation | Call | Measured |
|---|---|---|
| Create | `POST /v1/{p}/namespaces/{ns}/tables` | 200, LoadTableResult |
| List | `GET .../tables` | 200 |
| Load | `GET .../tables/{t}` | 200; schema, specs, snapshots are inside `metadata` |
| Exists | `HEAD .../tables/{t}` | 204 |
| Commit | `POST .../tables/{t}` requirements + updates | 200, new `metadata-location` |
| Rename | `POST /v1/{p}/tables/rename` `{"source", "destination"}` | 204 (`/v1/{p}/rename` is 404) |
| Register | `POST /v1/{p}/namespaces/{ns}/register` `{"name", "metadata-location"}` | 200, full LoadTableResult |
| Metrics | `POST .../tables/{t}/metrics` CommitReport / ScanReport | 204; free-form body 400 |
| Drop | `DELETE .../tables/{t}` | 204, files stay |
| Drop with purge | `DELETE .../tables/{t}?purgeRequested=true` | 403 unless `polaris.config.drop-with-purge.enabled=true` on the catalog, then 204 |

Create body: the field is `partition-spec`, with field ids.

```json
{"name": "customer_profiles",
 "schema": {"type": "struct", "schema-id": 0, "fields": [
   {"id": 1, "name": "customer_id", "required": true, "type": "long"},
   {"id": 3, "name": "created_at", "required": true, "type": "timestamp"}]},
 "partition-spec": {"spec-id": 0, "fields": [
   {"source-id": 3, "field-id": 1000, "transform": "day", "name": "day_created"}]},
 "stage-create": false}
```

A body with `"spec"` instead returned 200 and produced `partition-specs: [{"spec-id": 0, "fields": []}]`:
an unpartitioned table and no error. Always check the specs after creating.

Commit body: requirements guard against concurrent changes; the server writes the new metadata.json.

```json
{"requirements": [{"type": "assert-table-uuid", "uuid": "<table-uuid>"}],
 "updates": [{"action": "set-properties", "updates": {"owner": "crm"}}]}
```

Sending `{"updates": {"metadata-location": ..., "base": ...}}` was 400.

## Multi-Table Transactions

```json
{"table-changes": [
  {"identifier": {"namespace": ["analytics"], "name": "orders_tx"},
   "requirements": [{"type": "assert-table-uuid", "uuid": "<uuid-1>"}],
   "updates": [{"action": "set-properties", "updates": {"release": "2025.06"}}]},
  {"identifier": {"namespace": ["analytics"], "name": "customers_tx"},
   "requirements": [{"type": "assert-table-uuid", "uuid": "<uuid-2>"}],
   "updates": [{"action": "set-properties", "updates": {"release": "2025.06"}}]}]}
```

`POST /v1/{p}/transactions/commit` returned 204 for two tables. With one stale requirement it
returned 409 and **nothing** was applied: all or nothing across tables. A body with `updates`
instead of `table-changes` was 400.

## Notifications and External Catalogs

Notifications register tables managed by an engine outside Polaris in an EXTERNAL catalog
("static facade"):

```json
{"notification-type": "CREATE",
 "payload": {"table-name": "events", "timestamp": 1718900000000,
             "table-uuid": "<uuid>", "metadata-location": "s3://.../metadata/00001-....metadata.json"}}
```

Measured: INTERNAL catalog 400 `Cannot update internal catalog via notifications`; EXTERNAL catalog
204 and the table loads with that metadata location; a commit on it afterwards 400 `Cannot update
table on static-facade external catalogs`. Federation with live remote catalogs is a different
feature (`polaris-catalog-federation` skill).

## Views

| Operation | Call | Measured |
|---|---|---|
| List | `GET .../namespaces/{ns}/views` | 200 |
| Create | `POST .../views` with `name`, `schema`, `view-version` | 200; existing name 409 |
| Load | `GET .../views/{v}` | 200; SQL in `metadata.versions[].representations` |
| Replace | `POST .../views/{v}` commit | 200; previous versions kept |
| Exists | `HEAD .../views/{v}` | 204 |
| Rename | `POST /v1/{p}/views/rename` `{"source", "destination"}` | 204; `from`/`to` 400 |
| Drop | `DELETE .../views/{v}` | 204, then HEAD 404 |

```json
{"requirements": [],
 "updates": [{"action": "add-view-version", "view-version": {"version-id": 2, "schema-id": 0,
     "timestamp-ms": 1718900000000, "summary": {"engine-name": "etl"}, "default-namespace": ["analytics"],
     "representations": [{"type": "sql", "sql": "SELECT ...", "dialect": "spark"}]}},
   {"action": "set-current-view-version", "view-version-id": -1}]}
```

A view body with a string `version-id` and a top-level `sql` was rejected with `Cannot parse
missing string: name`.

## Pitfalls and Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| 400 `Please specify a warehouse` | No `warehouse` in config call | `?warehouse=<catalog>` / engine `warehouse` property |
| Table not partitioned | `spec` key | `partition-spec` |
| 404 on rename | Path `/v1/{p}/rename` | `/v1/{p}/tables/rename` |
| 409 on namespace properties | Updates and removals together | Two requests |
| 403 on purge | Purge disabled per catalog | `polaris.config.drop-with-purge.enabled=true` |
| 409 on transaction | A requirement no longer holds | Reload, rebuild requirements, retry |
| 409 creating a view | View exists | Commit a new version |

## Related

- [management_api.md](management_api.md), [tokens_and_scopes.md](tokens_and_scopes.md)
- `iceberg-catalogs` skill (apache-iceberg plugin): the REST commit protocol and conflicts in general.
- `polaris-spark`, `polaris-dremio`, `polaris-snowflake` skills: the same calls made by engines.
