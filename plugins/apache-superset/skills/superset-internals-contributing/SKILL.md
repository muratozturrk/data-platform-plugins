---
name: superset-internals-contributing
description: "Apache Superset internals for developers: app factory (create_app, config.py, superset_config.py), DAO and Command layering (BaseDAO, BaseCommand run/validate, exception hierarchy, @transaction), where code lives, the superset-core package and its extension points, Extensions CLI lifecycle (init, validate, build, bundle, update, dev; .supx), WebSocket async-event server (JWT cookie, Redis streams routing, backpressure, connection limits), coordination service, Superset CLI commands, requirements pinning with uv, CI and test stack (pytest, Jest, Playwright, Cypress, Storybook, ESLint), and role/permission matrix pointers. Use when extending, debugging or contributing to Superset itself, as opposed to configuring or using it."
---

# Superset Internals and Contributing

Extension/MCP usage is in `superset-extensions-mcp`; this is the code map. Facts come from source research on Superset 6.x (commit c83fb2bb1dcf baseline); re-verify paths on your version.

## Layering: where code lives
| Layer | Path | Rule |
|---|---|---|
| App factory | `superset/app.py` (`create_app`, `SupersetAppInitializer`); config `superset/config.py`, overrides `superset_config.py`/env | Never edit config.py in deployments |
| REST API | per-entity packages (`superset/charts`, `dashboards`, `datasets`, `databases`...) | Thin: parse, authorise, call a Command |
| Commands | `superset/commands/<entity>/`; base `superset/commands/base.py` `BaseCommand` with `run()` and `validate()` | All business logic; `@transaction` (`superset.utils.decorators`) for atomicity; uses `security_manager` |
| Exceptions | `superset/commands/exceptions.py`: `CommandException`, `ObjectNotFoundError`, `CommandInvalidError` (aggregates validation errors), `Create/Update/DeleteFailedError`, `ForbiddenError` | Raise these, not raw errors |
| DAOs | `superset/daos/`; `BaseDAO` in `daos/base.py`; Chart, Dashboard, Dataset, Database, Query, ReportSchedule, RLS, Tag, Task, Log, KeyValue, semantic-layer DAOs | All persistence; no business rules |
| Query path | `superset/common` (`QueryObject`, `QueryContext`) | Charts compose a QueryContext; caching and timing live here |
| Connectors | `superset/connectors/sqla` (tables, columns, metrics, Jinja) | |
| Cache invalidation | `superset/cachekeys` | |
| Coordination | `superset/coordination` | Pub/sub, KV, event streams, locks over Redis/Valkey (standalone or Sentinel) |

Flow: API -> Command.validate() -> Command.run() -> DAO. A new feature is a Command + DAO method + thin endpoint + tests, never SQLAlchemy in the endpoint.

## superset-core and extension points
`superset-core` is a separate package of abstract interfaces: models, DAOs, queries, REST APIs, extensions framework, MCP tool registration, tasks framework. Concrete Superset implementations are bound through dependency injection in `superset/core`. Extensions should import only `superset-core` APIs, not Superset internals.

## Extensions CLI (`apache-superset-extensions-cli`, dir `superset-extensions-cli`)
| Command | Does |
|---|---|
| `init` | Scaffold; frontend, backend, both or neither; writes `extension.json`, `package.json`, webpack/tsconfig, `pyproject.toml`, entry points |
| `validate` | Checks `extension.json`, backend `pyproject.toml`, entry points, metadata consistency |
| `build` | Compiles frontend, copies backend, writes `manifest.json` into `dist/` |
| `bundle` | Packs `dist/` into a `.supx` archive |
| `update` | Syncs version/license from `extension.json` into `package.json` and `pyproject.toml` |
| `dev` | Initial build, then watcher with rebuild and manifest update |

Lifecycle: init -> dev -> validate -> build -> bundle -> deploy the `.supx`. CLI tests are pytest per command (`test_cli_<cmd>.py`) using an isolated temp-filesystem fixture.

## WebSocket async-event server (Node.js)
- Pushes backend events to the web UI: broadcast `entity.changed` nudges and targeted task-status events.
- Auth: Flask mints a JWT only after checking `can_read` on the Realtime resource; the server reads it from the cookie named by `WEBSOCKET_JWT_COOKIE_NAME`, verifies claims (channel, sub, principal type) and builds a socket identity bound to a principal channel. Origin is validated at upgrade.
- Routing: envelopes arrive through Redis (streams-backed) and `routeRedisMessage` fans out globally or to a channel/principal.
- Protection: limits on total and per-channel connections; a socket whose outbound buffered amount exceeds `maxSocketBufferBytes` is terminated (backpressure); ping/pong health checks and a periodic sweep drop expired-JWT or dead sockets.
- Debug order: cookie name/domain, JWT expiry, Redis reachability, origin allow-list, connection limits.

## Superset CLI (`superset ...`)
`init` (permission and role sync), `import-export`, `load-examples`, `export-example`, `thumbnails`, `guest-token` (bumps the revocation version, invalidating all embedded guest tokens), `deletion-retention` (soft-delete purge windows, force purge by UUID), `sync-subjects`, `viz-migrations`, `update` (database URIs, tags, docs, re-encrypt secrets), `mcp` (run the MCP service), `reset` (factory reset; needs admin and a flag). Run `superset init` after upgrades or permission changes.

## Dependencies, CI, tests
- Python deps are compiled and pinned by `scripts/uv-pip-compile.sh`: edit inputs, regenerate pins, commit both.
- CI: GitHub Actions in `.github/workflows` with shared helpers in `bashlib.sh`; Docker via `Makefile`, `docker/docker-bootstrap.sh` starts web, Celery worker/beat, MCP.
- Backend: pytest with coverage, unit and integration; `docker-pytest-entrypoint.sh` prepares the test DB.
- Frontend (`superset-frontend`): Jest unit tests, Playwright e2e, Cypress (existing e2e), Storybook for components (docs derived via Storybook-to-MDX), ESLint/TypeScript; a `js-to-ts` command guides JS-to-TS migration.
- Docs are split into admin, developer and component docs, each versioned.

## Permissions pointers
Standard roles and the permission matrix are in the admin docs; change them through `superset init` and role-as-code, not by hand; see `superset-security-model`.

## Rules
- Business logic in Commands, persistence in DAOs, nothing else.
- Extension code uses `superset-core` only.
- Add tests at the layer you change; run linters before a PR.
- Pin dependencies; do not float them.

Sources: `references/sources.md`.
