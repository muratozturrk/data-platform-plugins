---
name: data-asset-naming
description: Naming conventions for data assets on the platform: catalogs, schemas, layers (bronze/silver/gold or staging/intermediate/marts), tables, views, columns, keys, dbt models/sources/tests, Iceberg namespaces and storage paths. Use when creating or reviewing any dataset, dbt model, DDL, Iceberg table or lake path.
---

# Data Asset Naming

## When to Use This Skill
- Creating a table, view, dbt model, Iceberg namespace or lake folder
- Reviewing DDL or dbt PRs
- Mapping Azure/Fabric lakehouse naming to the OSS platform

## Core Concepts

### Hierarchy
`{catalog}.{schema}.{table}` in SQL / Iceberg REST maps to `{tenant_catalog}.{layer}_{domain}.{entity}` on the platform. Tenant lives in the catalog (catalog-per-tenant) or the namespace prefix (pool), never inside table names.

### Layers (pick one vocabulary and keep it)
| Platform layer | dbt equivalent | Prefix | Materialisation |
|---|---|---|---|
| bronze / landing | source | none (`src` in dbt yml) | files / raw tables |
| silver staging | staging | `stg_{source}__{entity}` | view/ephemeral |
| silver intermediate | intermediate | `int_{entity}__{verb_past}` (`int_orders__deduplicated`) | ephemeral/table |
| gold | marts | `fct_{process}` (events, verbs) / `dim_{entity}` (nouns) | table/incremental |
| serving / semantic | exposures | `mart_{domain}_{subject}` or BI-facing views `v_` | view |
dbt style: objects plural (`stg_stripe__invoices`), source and entity separated by double underscore, intermediate ends with a past-tense verb, facts are immutable verbs and dims are mutable nouns.

### Columns
| Rule | Example |
|---|---|
| snake_case, lowercase, no reserved words, ≤ 63 chars | `order_total_amount` |
| Primary key `{entity}_id`, never bare `id` | `customer_id` |
| Foreign keys use the referenced entity's key name verbatim | `customer_id` in `fct_orders` |
| Timestamps `_at` (UTC, timestamp type), dates `_date`, durations `_ms/_seconds`, amounts with currency `_amount_try` or a separate `_currency` column | `created_at`, `order_date`, `latency_ms` |
| Booleans `is_/has_` | `is_cancelled` |
| Counts `_count`, rates `_rate` or `_pct` | `item_count`, `fill_rate` |
| Same concept → same column name across all tables and layers | no `cust_id` / `customerid` variants |
| Technical columns prefixed `_` | `_ingested_at`, `_source_file`, `_tenant_id`, `_dbt_updated_at` |
| No type/encoding in names, no `flag`, no abbreviations outside the list | |

### Tables and views
- Singular or plural: choose one; dbt ecosystem uses plural, Kimball uses singular. Platform default: **plural for tables** (`orders`), singular inside names of entities (`customer_id`).
- No environment, date, version or author in names. Snapshots: `{entity}_snapshot`; history: `{entity}_history`; partitions by columns, not by table name.
- Views prefixed `v_` only in serving layer; materialised views `mv_`.
- Constraints: `pk_{table}`, `fk_{table}__{ref_table}`, `uq_{table}__{cols}`, `ix_{table}__{cols}`.

### Storage paths (lake)
`{root}/{environment}/tenants/{tenant}/{layer}/{domain}/{entity}/` (pool) or `{tenant_root}/{environment}/{layer}/{domain}/{entity}/` (silo); partition folders Hive-style `event_date=2026-01-01/`. Environment is the first segment because it is a deployment dimension.

### dbt specifics
- Files mirror model names; one model per file; folders by layer then source/domain.
- Sources `src_{source}.yml`; tests named by dbt (`unique_{model}_{column}`) — do not rename.
- Use `alias` only during a rename transition.

## Anti-patterns
- `tbl_`, `dim`, `fact` as suffixes mixed with prefixes
- CamelCase tables (quoted identifiers forever)
- `final_v2_new` tables; environment in table names
- `id` as the primary key name (ambiguous in joins)
- Dates in table names instead of partitions

See `references/sources.md`.
