---
name: superset-dataset-semantic-layer
description: Designing Superset datasets as the semantic layer over Trino, ClickHouse and Iceberg tables: physical vs virtual datasets, metrics and calculated columns, time columns, certification, dbt sync, Jinja macros, engine connection settings and query cost control. Use when creating or reviewing datasets, metrics, or database connections.
---

# Datasets and the Semantic Layer

## Core Concepts
- **Dataset = contract**: a physical table/view (preferred) or a virtual dataset (SQL) with typed columns, `is_dttm` time columns, **metrics** (SQL aggregates with D3 formats and descriptions), calculated columns, default time range/grain, `cache_timeout`, owners, certification, description, tags.
- Metrics live on the dataset, not in charts; charts choose from them. Ad-hoc metrics are for exploration in dev.
- Gold-layer marts (`fct_`/`dim_` or `mart_`) are the natural dataset sources; silver tables are not exposed to Gamma roles.
- dbt is the upstream semantic definition: model `meta.superset` (metrics, descriptions) synced by `superset-cli sync dbt-core`; OpenMetadata ingests both so lineage dashboard → dataset → table is visible.
- 6.1 ships semantic layers as extensions; evaluate when the platform standardises a semantic-layer engine.

## Engine specifics
| Engine | Connection notes |
|---|---|
| Trino | `trino://user@host:8443/{catalog}` with `impersonate_user=True`, session properties for tenant labelling, `allows_virtual_table_explore`; cost control via resource groups; prefer pre-aggregated marts for dashboards |
| ClickHouse | `clickhousedb://` (clickhouse-connect); set `log_comment`/`query_id` tags with tenant; row policies at engine; use materialized views for KPIs; beware `FINAL` |
| Iceberg via Trino | partition filters in time columns; `cache_timeout` aligned with pipeline freshness SLA |
| PostgreSQL (control-plane metrics) | read-only replica only |

## Rules
1. Every production dataset: owner, description, certification, tags (`tenant`, `domain`, `layer`), `cache_timeout` ≤ freshness SLA, main time column set, metrics documented.
2. Virtual datasets only for shaping that cannot live in the warehouse yet; move to a dbt model within one sprint (ratchet).
3. Column names follow `data-asset-naming` (`_at`, `is_`, `_id`); verbose names for UI, raw names preserved.
4. No Jinja in production datasets unless `ENABLE_TEMPLATE_PROCESSING` is deliberately on and the macro set is allow-listed; prefer native filters.
5. Row limits: `ROW_LIMIT`, `SAMPLES_ROW_LIMIT`, chart-level limits; time range defaults bounded (last 30 days, not "No filter").
6. Sync, don't click: datasets created by the assets pipeline (from dbt or YAML).

## Anti-patterns
- Charts with their own SQL metrics duplicating dataset metrics
- Virtual datasets with 500-line SQL and joins across catalogs
- Datasets on silver/bronze tables exposed to viewers
- Missing time column → charts default to no time filtering

Sources: Superset docs (datasets, metrics, Jinja templating, databases/connecting); preset-cli `sync dbt-core`; dbt exposures; Trino and ClickHouse SQLAlchemy dialect docs; Preset blog on semantic layer extensions (2026).
