---
name: superset-performance-caching
description: Making Superset dashboards fast and the warehouse safe: Redis cache layers and TTL strategy, cache warm-up, async queries (Celery and Global Async Queries), timeouts and row limits, query cost labelling, dashboard load budgets, Celery queue isolation and capacity. Use when dashboards are slow, warehouses are overloaded, or sizing Redis/Celery.
---

# Performance and Caching

## Core Concepts
### Cache layers (`superset_config.py`)
| Config | What it caches | Guidance |
|---|---|---|
| `DATA_CACHE_CONFIG` | chart data (query results) | Redis; default TTL by dataset `cache_timeout`, aligned to pipeline freshness; key prefix per instance |
| `CACHE_CONFIG` | metadata/other | Redis, short TTL |
| `FILTER_STATE_CACHE_CONFIG`, `EXPLORE_FORM_DATA_CACHE_CONFIG` | native filter state, explore form data | Redis; must be shared across web replicas |
| `THUMBNAIL_CACHE_CONFIG` | thumbnails | Redis/S3 |
Dashboard-level `cache_timeout` overrides dataset; chart-level overrides both. Cache warm-up via Celery beat (`cache-warmup` with `TopNDashboardsStrategy` / `DashboardTagsStrategy`) after pipelines finish — trigger from the platform's Airflow DAG completion rather than fixed cron.

### Async execution
- SQL Lab: `RESULTS_BACKEND` + Celery `sql_lab` queue; `SQLLAB_ASYNC_TIME_LIMIT_SEC`.
- Charts/dashboards: `GLOBAL_ASYNC_QUERIES` feature flag (WebSocket/polling) so long queries do not block web workers; `GLOBAL_ASYNC_QUERIES_JWT_*`, Redis stream config.
- 6.1 Global Task Framework (flagged) with distributed coordination backend for Redis.

### Limits and timeouts
`SUPERSET_WEBSERVER_TIMEOUT` < ingress timeout; `SQLLAB_TIMEOUT`, `SQLLAB_ASYNC_TIME_LIMIT_SEC`; `ROW_LIMIT`, `SQL_MAX_ROW`, `DISPLAY_MAX_ROW`; per-database `query cost estimation` (Trino/Presto supports `estimate` before run); `DASHBOARD_AUTO_REFRESH_INTERVALS` bounded.

### Warehouse protection
- Engine-side: Trino resource groups per tenant/tier, ClickHouse quotas and settings profiles, OPA deny on unbounded scans where possible.
- Superset-side: certified aggregated datasets for dashboards, default time ranges, native filter defaults, limit on cross-filters, disable `Explore` for viewers who only need dashboards.
- Tag every query with tenant and dashboard/chart id (session properties / `log_comment`) for cost attribution in the platform's usage ledger.

### Capacity
Redis: cache memory ≈ Σ(dashboards × charts × result size × hit ratio); eviction `allkeys-lru`; separate instances for broker vs cache if > 70% memory. Celery: `sql_lab`, `reports`, `thumbnails`, `cache_warmup` queues with independent autoscaling; `prefork` with `-c` ≈ vCPU; reports need Chromium/Firefox + memory.

### Budgets and observability
Dashboard p95 load < 5 s (cached) / < 15 s (cold); chart query p95 < 3 s; cache hit ratio > 70% for production dashboards. Metrics via `STATS_LOGGER` (StatsD → Prometheus) and query logs joined with engine query ids; alert on slow charts per tenant.

## Anti-patterns
- Same Redis DB for broker and cache without memory separation
- "No filter" default time ranges on event tables
- Cache warm-up on a cron unrelated to data arrival
- Sync-mode SQL Lab on a warehouse with multi-minute queries

Sources: Superset docs (Caching, Async queries via Celery, Configuring Superset, Alerts & Reports); Preset 6.1 release notes (Global Task Framework); Galaxy K8s guide (scaling web vs workers); Trino resource groups; ClickHouse quotas.
