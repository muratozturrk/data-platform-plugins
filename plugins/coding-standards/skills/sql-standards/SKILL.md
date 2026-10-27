---
name: sql-standards
description: SQL and dbt coding standard: formatting (sqlfluff), CTE structure, explicit joins and columns, dbt model layout, tests, Jinja usage, performance habits for Trino/Spark SQL/ClickHouse. Use when writing or reviewing SQL, dbt models or sqlfluff config.
---

# SQL and dbt Standards

Baseline: dbt Labs "How we style our SQL" + GitLab SQL style guide + Simon Holywell's guide; enforced with sqlfluff (dbt templater).

## Core Rules

| # | Rule | Enforced by |
|---|---|---|
| 1 | Lowercase keywords, functions and identifiers; 4-space indent; one column per line in SELECT; leading commas **no**, trailing commas yes | sqlfluff CP*, LT* |
| 2 | Never `SELECT *` outside of staging `source()` passthroughs; list columns in a stable order: ids, FKs, dimensions, measures, timestamps, technical `_` columns | sqlfluff AM04, review |
| 3 | Explicit `JOIN ... ON`; no implicit comma joins; `INNER`/`LEFT` written out; join keys named identically on both sides | sqlfluff AM05, naming |
| 4 | CTEs over subqueries; import CTEs first (`with orders as (select ... from {{ ref('stg_orders') }})`), logical CTEs next, `final` CTE last, `select * from final` | review |
| 5 | Every CTE and column alias explicit (`as`); table aliases meaningful (`orders` not `o`) | sqlfluff AL* |
| 6 | Window functions: always `order by` deterministic keys; `qualify` where supported | review |
| 7 | Null handling explicit (`coalesce`, `is distinct from`); no `= null` | sqlfluff CV05 |
| 8 | Dates/timestamps in UTC; `date_trunc` over string ops; no implicit casts | review |
| 9 | dbt: `ref()`/`source()` only; one model per file; config in `dbt_project.yml` by folder, in-model only when different; every model has a `.yml` with description and tests on PK (`unique`, `not_null`) and FKs (`relationships`) | dbt-checkpoint hooks |
| 10 | dbt Jinja: no business logic in macros that hides SQL; macros documented; `{{ }}` spacing per style guide | sqlfluff JJ01 |
| 11 | Engine-specific: Trino — avoid cross joins and `approx_*` without comment; Spark SQL — partition filters first; ClickHouse — `PREWHERE`, `ORDER BY` key awareness, no `FINAL` in production queries | review |
| 12 | Comments: `--` single line, explain intent/edge cases; header comment with grain ("one row per order per day") for marts | review |

## Patterns
- State the grain of every model in its description.
- Materialisation per layer (view/ephemeral staging, table/incremental marts) set at folder level.
- Incremental models: idempotent merge on key + `_updated_at` watermark; never `insert_overwrite` without partition predicate.

## Anti-patterns
- Deeply nested subqueries; `distinct` to hide join fan-out
- Hard-coded `database.schema.table`
- `order by 1, 2`
- Jinja loops generating hundreds of columns without a macro and docs

See `references/sources.md`.
