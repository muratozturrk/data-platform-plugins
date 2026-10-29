---
name: polaris-rest-api
description: "The two HTTP APIs of Apache Polaris as measured on 1.8.0 - the Management API (/api/management/v1: catalogs with storageConfigInfo, principals with generated client ids, reset vs rotate, catalog roles, principal roles, grants added with PUT and revoked with POST, assignments, entityVersion optimistic locking) and the Iceberg REST catalog API (/api/catalog/v1: config?warehouse= prefix, OAuth2 client-credentials tokens with PRINCIPAL_ROLE scopes, namespaces, tables with partition-spec, requirements+updates commits, tables/rename, register, metrics reports, notifications for EXTERNAL static-facade catalogs, multi-table transactions with table-changes, purge, views and view versions). Use when scripting Polaris with curl/requests, automating environment setup, debugging engine-catalog calls, or when a Polaris request returns 400/404/405/409/501 or silently ignores a field."
---

# Polaris REST API

Polaris exposes two APIs on one port (8181):

| API | Base path | Purpose |
|---|---|---|
| Management API | `/api/management/v1` | Catalogs, principals, principal roles, catalog roles, grants |
| Iceberg REST catalog API | `/api/catalog/v1` | Config, OAuth2 tokens, namespaces, tables, views (what every Iceberg engine calls) |

54 commonly written requests were sent to Polaris 1.8.0 as written: 26 worked, 7 worked with a
different response than expected, 19 failed and 2 succeeded but did something else. The
corrected bodies are in `examples/rest-api/common/spec_bodies.py`, each verified by a test. The
meaning of roles and privileges is in the `polaris-access-control` skill; the REST commit protocol
in general is in the `iceberg-catalogs` skill of the apache-iceberg plugin.

## When to Use This Skill

- Writing curl or Python automation for catalogs, principals, roles and grants.
- Building CI tooling that creates namespaces/tables or changes table properties over HTTP.
- Debugging what an engine sends to Polaris (config handshake, token, commits).
- A Polaris call returns 400, 404, 405, 409 or 501, or a field seems to be ignored.
- Recovering a table (`register`), mirroring engine-owned tables (notifications) or releasing changes to several tables at once.

## Decision Table

| Need | Call | Reference |
|---|---|---|
| Get a token | `POST /api/catalog/v1/oauth/tokens` + `scope` | [tokens_and_scopes.md](references/tokens_and_scopes.md) |
| Create a catalog | `POST /catalogs` with `storageConfigInfo` | [management_api.md](references/management_api.md) |
| Update any entity | Read it, `PUT` with `currentEntityVersion` | [management_api.md](references/management_api.md) |
| Add / remove a privilege | `PUT` / `POST` on `.../catalog-roles/{r}/grants` | [management_api.md](references/management_api.md) |
| Find a catalog's REST prefix | `GET /v1/config?warehouse=<catalog>` | [catalog_api.md](references/catalog_api.md) |
| Partitioned table | `partition-spec` (not `spec`) | [catalog_api.md](references/catalog_api.md) |
| Change table metadata | `requirements` + `updates` commit | [catalog_api.md](references/catalog_api.md) |
| Change several tables atomically | `POST /v1/{prefix}/transactions/commit` with `table-changes` | [catalog_api.md](references/catalog_api.md) |
| Replace a view | Commit on `/views/{view}` (`add-view-version`) | [catalog_api.md](references/catalog_api.md) |

## Management API in One Screen

```bash
TOKEN=$(curl -s -X POST "$P/api/catalog/v1/oauth/tokens" -d grant_type=client_credentials \
  -d client_id=root -d client_secret="$ROOT_SECRET" -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
curl -s "${H[@]}" -X POST "$P/api/management/v1/catalogs" -d '{"catalog": {"type": "INTERNAL",
  "name": "finance", "properties": {"default-base-location": "s3://lake/finance"},
  "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://lake/finance"],
                        "roleArn": "arn:aws:iam::<account>:role/<vendor-role>"}}}'
curl -s "${H[@]}" -X PUT  "$P/api/management/v1/catalogs/finance/catalog-roles/viewer/grants" \
  -d '{"grant": {"type": "catalog", "privilege": "TABLE_READ_DATA"}}'      # add
curl -s "${H[@]}" -X POST "$P/api/management/v1/catalogs/finance/catalog-roles/viewer/grants" \
  -d '{"grant": {"type": "catalog", "privilege": "TABLE_READ_DATA"}}'      # revoke
```

Gotchas (measured): `storageConfigInfo` is required (400 `must not be null`); a `clientId` sent
when creating a principal is ignored; admins `reset`, only the principal itself can `rotate`;
grant revoke with `DELETE` is 405 and `?cascade=true` is 501; assigning a role that does not exist
is 404; an update with a stale `currentEntityVersion` is 409.

## Iceberg REST API in One Screen

```
POST /v1/oauth/tokens              grant_type=client_credentials, client_id, client_secret, scope
GET  /v1/config?warehouse=dev      -> overrides.prefix = "dev", defaults, endpoints (38 on 1.8.0)
POST /v1/dev/namespaces            {"namespace": ["analytics"], "properties": {...}}
POST /v1/dev/namespaces/analytics/tables          name, schema, partition-spec
POST /v1/dev/namespaces/analytics/tables/{t}      requirements + updates
POST /v1/dev/tables/rename                        {"source": ..., "destination": ...}
POST /v1/dev/transactions/commit                  {"table-changes": [...]}
```

Gotchas (measured): `/v1/config` without `?warehouse=` is 400 `Please specify a warehouse`; a token
request without `scope` is 400 `invalid_scope`; a create-table body with `"spec"` returns 200 but
the table is **not partitioned**; namespace `updates` and `removals` in one request give 409 with
the updates applied and the removals not; drop with `purgeRequested=true` is 403 unless the
catalog has `polaris.config.drop-with-purge.enabled=true`.

## Views

Create once (`POST .../views` with `name`, `schema` and a full `view-version`); a second create
with the same name is 409, it never replaces. Replace by committing `add-view-version` +
`set-current-view-version` on `/views/{view}`; history is kept. `HEAD` on a view is 204; rename
uses `source` / `destination`.

## Run the Examples

```bash
cd skills/polaris-rest-api/examples/rest-api
docker compose -p polaris-rest-api up -d --build
docker compose -p polaris-rest-api exec runner python -m pytest -v
docker compose -p polaris-rest-api down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`. The status table
of all 54 requests is written to `measurements.json` (`request_check`).

## Anti-Patterns

| Anti-pattern | Why it hurts | Instead |
|---|---|---|
| Hard-coding `entityVersion` | 409 as soon as anything else changed the entity | GET, then PUT with the returned version |
| Trusting a 200 on table create | `spec` is silently ignored | Check `metadata.partition-specs` after create |
| Writing `metadata-location` yourself in a commit | Not the protocol; 400 | Send requirements + updates, the server writes metadata.json |
| One request for namespace updates and removals | 409, half applied | Two requests |
| Re-POSTing a view to change it | 409 | Commit a new view version |
| Free-form metrics bodies | 400 | Iceberg `CommitReport` / `ScanReport` |
| Notifications to an INTERNAL catalog | 400 | EXTERNAL catalog with `notification-type` + `payload` |
| Long-lived root tokens in scripts | Over-privileged automation | A dedicated principal with a scoped role |

## References

- [references/management_api.md](references/management_api.md): catalogs, principals, roles, grants, assignments, entity versions, status codes.
- [references/catalog_api.md](references/catalog_api.md): config, namespaces, tables, commits, rename, register, metrics, notifications, transactions, purge, views.
- [references/tokens_and_scopes.md](references/tokens_and_scopes.md): token endpoint, scopes, JWT contents, vended-credential header, external IdP.
- Examples: `examples/rest-api/` (3 packages, 10 tests); `common/spec_bodies.py` is a cheat sheet of correct bodies.
- Specs: Polaris management API (`spec/polaris-management-service.yml` in apache/polaris), Iceberg REST catalog OpenAPI (`open-api/rest-catalog-open-api.yaml` in apache/iceberg).
- Related skills: `polaris-access-control`, `polaris-configuration-and-cli` (the same operations through the `polaris` CLI), `polaris-catalog-federation` (external catalogs).
