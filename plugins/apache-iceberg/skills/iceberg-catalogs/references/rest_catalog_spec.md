# Iceberg REST Catalog Specification

Scope: the HTTP protocol every REST catalog implements (Polaris, Gravitino, Lakekeeper, Nessie,
Glue, Dremio catalog, Snowflake Open Catalog, Unity), how commits work, how to script it.

## Problem
Before the spec, each catalog shipped client libraries in Java, Python, Go and Rust with uneven
feature coverage, and commit coordination and conflict detection lived in every client. Small
mismatches in library versions or classpaths could cause wrong conflict detection, lost updates or
inconsistent state - and the worst failures were silent (the write "succeeded"). Hive Metastore
gave broad interoperability, but the correctness problem stayed in the clients. The REST spec
(OpenAPI, `open-api/rest-catalog-open-api.yaml` in the Iceberg repo) moves commit validation and
metadata writing into the catalog service: implemented once, any engine, any language.

## Endpoints

| Operation | Endpoint |
|---|---|
| Handshake | `GET /v1/config?warehouse=<wh>` -> `defaults`, `overrides` (e.g. `prefix`), `endpoints` |
| OAuth2 token (if the server hosts it) | `POST /v1/oauth/tokens` (`grant_type=client_credentials`) |
| List / create namespaces | `GET` / `POST /v1/{prefix}/namespaces` (`?parent=a%1Fb`) |
| Load / drop namespace | `GET` / `DELETE /v1/{prefix}/namespaces/{ns}` |
| Create / list tables | `POST` / `GET /v1/{prefix}/namespaces/{ns}/tables` |
| Load / exists / drop table | `GET` / `HEAD` / `DELETE .../tables/{t}` (`?purgeRequested=true`) |
| Commit (update table) | `POST .../tables/{t}` with `requirements` + `updates` |
| Multi-table commit | `POST /v1/{prefix}/transactions/commit` |
| Register existing metadata | `POST /v1/{prefix}/namespaces/{ns}/register` |
| Vended credentials refresh | `GET .../tables/{t}/credentials` |
| Views | `.../namespaces/{ns}/views[/{view}]` |

Multi-level namespaces are joined with the unit separator `0x1F`, URL-encoded as `%1F`
(`analytics%1Fmarketing`). Read `endpoints` from `/v1/config` to know what the server supports;
the reference server 1.8.1 advertises 26 endpoints, Polaris 38. The `prefix` is server-defined:
none on the reference server, the catalog name on Polaris (`?warehouse=sales` -> `/v1/sales/...`).

## Commit flow

```
Engine -> GET  /v1/config?warehouse=sales            <- prefix, defaults, endpoints
Engine -> GET  .../tables/orders                     <- metadata-location, metadata, config
Engine -> writes data files + manifests to storage   (vended or own credentials)
Engine -> POST .../tables/orders {requirements, updates}
Server:   checks requirements against current metadata, writes new metadata.json, swaps pointer
       <- 200 {metadata-location, metadata}  |  409 CommitFailedException
```

The client sends a diff, not a file; after one commit the server - not the client - wrote the
second `metadata.json`, and the previous file is in `metadata-log`.

| Requirement type | Guards against |
|---|---|
| `assert-create` | Table already exists (create transaction) |
| `assert-table-uuid` | Table dropped and recreated under the same name |
| `assert-ref-snapshot-id` (`ref`, `snapshot-id`, `null` = ref must not exist) | Concurrent append/overwrite on that branch |
| `assert-current-schema-id`, `assert-last-assigned-field-id` | Concurrent schema change |
| `assert-default-spec-id`, `assert-last-assigned-partition-id` | Concurrent partition evolution |
| `assert-default-sort-order-id` | Concurrent sort-order change |

Common updates: `add-schema`, `set-current-schema` (`-1` = the schema added in this commit),
`add-spec`, `set-default-spec`, `add-sort-order`, `add-snapshot`, `set-snapshot-ref`,
`remove-snapshots`, `set-properties`, `remove-properties`, `set-location`, `upgrade-format-version`.

## Guarded schema change over plain HTTP

```python
def add_column(meta, name, type_):
    cur = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    new_id = meta["last-column-id"] + 1
    schema = {"type": "struct", "schema-id": cur["schema-id"] + 1,
              "fields": cur["fields"] + [{"id": new_id, "name": name, "type": type_, "required": False}]}
    requirements = [{"type": "assert-table-uuid", "uuid": meta["table-uuid"]},
                    {"type": "assert-current-schema-id", "current-schema-id": meta["current-schema-id"]}]
    updates = [{"action": "add-schema", "schema": schema, "last-column-id": new_id},
               {"action": "set-current-schema", "schema-id": -1}]
    return requirements, updates

meta = requests.get(f"{base}/v1/{prefix}/namespaces/sales/tables/orders", headers=h).json()["metadata"]
req, upd = add_column(meta, "channel", "string")
r = requests.post(f"{base}/v1/{prefix}/namespaces/sales/tables/orders", headers=h,
                  json={"requirements": req, "updates": upd})
if r.status_code == 409:   # someone changed the schema: reload and rebuild
    ...
```

Two clients that loaded schema 0: the first commit wins; the second gets 409
`CommitFailedException` "Requirement failed: current schema changed: expected id 0 != 1" - the same
message from the reference server and Polaris (both run Iceberg core's own check). The loser's
column is absent; a retry on fresh metadata succeeds.

Guard a maintenance decision made on a stale view:

```json
{"requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": null}],
 "updates": [{"action": "set-properties", "updates": {"history.expire.max-snapshot-age-ms": "1"}}]}
```

After a concurrent append, this returns 409 and the property is not applied.

## Multi-table transaction

```json
POST /v1/{prefix}/transactions/commit
{"table-changes": [
  {"identifier": {"namespace": ["sales"], "name": "orders"},      "requirements": [...], "updates": [...]},
  {"identifier": {"namespace": ["sales"], "name": "order_items"}, "requirements": [...], "updates": [...]}]}
```

One failing requirement (e.g. `assert-current-schema-id: 42`) returns 409 and **both** tables keep
schema 0; a valid transaction moves both to schema 1. Check that `endpoints` advertises it.

## Errors

| Status / type | Meaning | Client action |
|---|---|---|
| 400 `BadRequestException` | Malformed request; on Polaris also "no credentials are available" | Fix request / catalog storage config |
| 401 / 419 | Missing or expired token | Refresh OAuth token |
| 403 `ForbiddenException` | Authenticated but not granted | Grant privilege (see access control) |
| 404 `NoSuchTableException` / `NoSuchNamespaceException` | Not found | - |
| 409 `AlreadyExistsException` | Create of an existing table/namespace | - |
| 409 `CommitFailedException` | A requirement failed | Reload, rebuild, retry |
| 500/502/504 `CommitStateUnknownException` | Outcome unknown | Reload and verify; do not delete written files |

## Engine configuration

```properties
# Spark
spark.sql.catalog.cat=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.cat.type=rest
spark.sql.catalog.cat.uri=http://polaris:8181/api/catalog
spark.sql.catalog.cat.warehouse=sales                       # becomes the prefix on Polaris
spark.sql.catalog.cat.credential=<client_id>:<client_secret>
spark.sql.catalog.cat.scope=PRINCIPAL_ROLE:ALL
spark.sql.catalog.cat.oauth2-server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens
spark.sql.catalog.cat.header.X-Iceberg-Access-Delegation=vended-credentials
```

```python
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog("cat", uri="http://rest:8181", **{"s3.endpoint": "http://s3:8333"})
```

## Gotchas

- Always send requirements for read-modify-write commits; an empty requirements list cannot conflict.
- Do not hardcode URL paths: honour `prefix` from `/v1/config` and the `endpoints` list.
- `/v1/config` `defaults` are overridable by the client, `overrides` are not.
- Next steps of the protocol: server-side scan planning (clients no longer parse manifests) and
  centralized policy enforcement - check `endpoints` before relying on them.
