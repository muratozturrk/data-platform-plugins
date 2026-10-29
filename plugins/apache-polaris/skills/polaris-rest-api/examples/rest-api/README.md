# Polaris REST API Examples

Runnable pytest suites for the Polaris Management API and the Iceberg REST catalog API as Polaris
serves them (10 tests, about 9 s on a fresh stack).

54 requests in the form they are commonly written (from tutorials, older docs and guesses at the
spec) are sent to a real Polaris server; only host, token and names are substituted. Each HTTP
status is asserted, and where the naive form fails or silently does something else, the corrected
request is sent and verified too. The rows are written to `measurements.json` under
`request_check`.

| Package | Covers | Requests |
|---|---|---|
| `management_api/` | catalogs, principals, catalog roles, principal roles, grants, assignments | 28 |
| `table_api/` | config, OAuth2 tokens, namespaces, tables, register, metrics, notifications, multi-table commits | 19 |
| `view_api/` | views: create, load, replace, exists, rename, drop | 7 |

`common/spec_bodies.py` holds the corrected, spec-conformant request bodies (catalog create,
grant revoke, partitioned table create, table commit, multi-table transaction, metrics report,
view create/replace, rename). It doubles as a cheat sheet for the Polaris 1.8.0 API.
`common/requestcheck.py` sends a request, asserts its status and records the row.

## Stack

| Component | Version | Role |
|---|---|---|
| Apache Polaris (`apache/polaris:latest`) | 1.8.0 when measured | Management API + Iceberg REST API |
| SeaweedFS (`chrislusf/seaweedfs`) | latest | S3 + STS |
| Test runner (`python:3.11-slim`) | requests 2.32.3, PyIceberg 0.9.1, pytest 8.3.4 | HTTP client, real snapshots |

## How to Run

```bash
docker compose -p polaris-rest-api up -d --build
docker compose -p polaris-rest-api exec runner python -m pytest -v
docker compose -p polaris-rest-api down -v
```

On Git Bash for Windows prefix `docker compose exec` with `MSYS_NO_PATHCONV=1`.

`test_deletes_last` in `management_api/` removes the shared catalog and principal, so run the
package as a whole (pytest's default file order), not single tests out of order.
