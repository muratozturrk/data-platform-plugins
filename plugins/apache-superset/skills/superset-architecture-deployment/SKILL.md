---
name: superset-architecture-deployment
description: Apache Superset 6.x components and production deployment on Kubernetes (Stackable Superset operator or community Helm chart): web, Celery workers/beat, PostgreSQL metadata DB, Redis, custom image with drivers, config via superset_config.py, feature flags, scaling, upgrades and backups. Use when deploying, sizing or upgrading Superset.
---

# Superset Architecture and Deployment

## When to Use This Skill
- Installing Superset on the platform cluster; choosing operator vs Helm
- Sizing web/worker replicas; enabling async, alerts/reports, thumbnails
- Building the platform's Superset image; upgrading between versions

## Core Concepts

### Components
| Component | Role | Notes |
|---|---|---|
| Web (Gunicorn + Flask/FAB) | UI, REST API, chart data, SQL Lab | gevent workers; `SUPERSET_WEBSERVER_TIMEOUT` ≤ ingress timeout |
| Celery worker(s) | async queries, alerts & reports, thumbnails, cache warm-up, Global Task Framework (6.1, flagged) | separate deployments/queues per task type |
| Celery beat | scheduler | exactly one replica |
| Metadata DB | PostgreSQL (managed/HA) | backups before every upgrade; never SQLite in prod |
| Redis | data cache, filter-state cache, Celery broker/results, async query results | separate DB indices or instances per purpose |
| Object storage | thumbnails/report screenshots (optional) | |
| MCP service (6.1) | optional separate process, JWT auth | only if the platform's AI features need it |

### Deployment options
- **Stackable Superset operator** (`SupersetCluster` CR): fits the platform's operator-first substrate; config via `configOverrides`, Keycloak/OPA demos available; check the SDP release matrix for the Superset version it ships.
- **Community Helm chart** (`apache/superset`): most common; `configOverrides` block, `init` job runs `superset db upgrade` + roles; needs your own image for drivers.
- Upstream SIP-149 operator is in progress; re-evaluate yearly.

### Custom image
Extend `apache/superset:<exact-version>` (never `latest`, never `-dev` in prod) with: `trino` (sqlalchemy-trino), `clickhouse-connect`, `psycopg2-binary`, `redis`, `gevent`, `authlib`, `Pillow` (thumbnails), platform security manager module, extensions bundle. Non-root user; pinned requirements; SBOM.

### Configuration essentials (`superset_config.py`)
`SECRET_KEY` (from Vault; rotating it invalidates encrypted DB passwords), `SQLALCHEMY_DATABASE_URI`, `CACHE_CONFIG`/`DATA_CACHE_CONFIG`/`FILTER_STATE_CACHE_CONFIG`/`EXPLORE_FORM_DATA_CACHE_CONFIG` (Redis), `CELERY_CONFIG`, `RESULTS_BACKEND`, `GLOBAL_ASYNC_QUERIES_*`, `FEATURE_FLAGS` (`EMBEDDED_SUPERSET`, `ALERT_REPORTS`, `THUMBNAILS`, `ENABLE_TEMPLATE_PROCESSING` only if needed, `TAGGING_SYSTEM`, `DASHBOARD_RBAC`, `DRILL_TO_DETAIL`, `GLOBAL_ASYNC_QUERIES`), `TALISMAN_CONFIG`, `AUTH_*`, `ROW_LIMIT`/`SQL_MAX_ROW`, `SQLLAB_TIMEOUT`, `SUPERSET_WEBSERVER_TIMEOUT`, `WEBDRIVER_*` for reports, `STATS_LOGGER`/`PROMETHEUS`, `LOG_FORMAT` JSON.

### Scaling
Web: HPA on CPU/RPS, 2+ replicas, Gunicorn `-w` ≈ 2×vCPU with gevent; workers: separate deployments for `sql_lab`, `reports`, `thumbnails` queues; Redis memory sized for cache TTL × result size; PostgreSQL connection pool sized for web+workers.

### Upgrades
Read `UPDATING.md` for breaking changes and removed feature flags → backup metadata DB → upgrade staging copy (`superset db upgrade`, `superset init`) → smoke test dashboards, embedding, SSO, assets import → roll out web then workers → pin embedded SDK and extensions to the matching version.

## Anti-patterns
- `latest`/`-dev` images; SQLite metadata; Redis shared with other apps without index separation
- Running beat with multiple replicas (duplicate reports)
- Changing `SECRET_KEY` without re-encrypting
- Timeouts mismatched between ingress, Gunicorn and SQL Lab

See `references/sources.md`.
