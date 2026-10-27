# Workload Isolation and Governance at the Federation Layer

Scope: semantic layer as governed views, file-based access control (row filters, column masks,
catalog/function rules), resource groups, Trino Gateway, and where engine-level governance ends.

## Problem
Federation widens access, and wider access breaks two things: meaning (each tool computes "net
revenue" its own way) and isolation (an ad hoc query against an OLTP source blocks dashboards).
A self-managed engine also leaves authorization and workload isolation to the platform team.

## Semantic layer as SECURITY DEFINER views
Definitions are SQL views deployed once, versioned in their comment, and stored in the Iceberg REST
catalog as Iceberg views (PyIceberg `list_views()` sees them, so they are not locked into one engine).

```sql
CREATE OR REPLACE VIEW lake.semantic.net_revenue_daily
COMMENT 'Net revenue in USD per order day: currency converted, returns excluded [definition v1]'
SECURITY DEFINER AS
SELECT o.order_date, count(*) AS orders, sum(o.amount * f.rate_to_usd) AS net_revenue_usd
FROM lake.sales.orders o
JOIN pg.public.fx_rates f ON f.currency = o.currency
LEFT JOIN pg.public.order_returns r ON r.order_id = o.order_id
WHERE r.order_id IS NULL
GROUP BY o.order_date;

-- discoverability for BI tools and agents
SELECT table_name, comment FROM system.metadata.table_comments
WHERE catalog_name = 'lake' AND schema_name = 'semantic';
```

- `SECURITY DEFINER` (Trino's default): consumers need rights on the view, not on its sources.
- BI (DB-API, `source=superset`), a notebook and a raw REST app all returned 2,351,142,352.02 USD,
  while the raw sum of amounts was 2,127,396,830.02. Redeploying v2 (pending orders excluded)
  changed every consumer to 2,281,175,346.50, exactly v1 minus 69,967,005.52.
- Central changes only propagate if tools query the view; copying its SQL into a dashboard forks it.
- A full semantic layer adds docs, YAML metric definitions, lineage and tags; views are the
  executable part. Trino has no built-in one; Dremio does.

## File-based access control (Trino)

```properties
# access-control.properties
access-control.name=file
security.config-file=/etc/trino/rules.json
```

```json
{
  "catalogs": [
    {"user": "admin|engineer|etl", "catalog": ".*", "allow": "all"},
    {"user": "analyst_.*|bi_dashboard", "catalog": "lake", "allow": "read-only"},
    {"catalog": "system", "allow": "read-only"},
    {"catalog": ".*", "allow": "none"}
  ],
  "tables": [
    {"user": "analyst_eu", "catalog": "lake", "schema": "semantic", "table": "customer_360",
     "privileges": ["SELECT"], "filter": "region = 'EUROPE'",
     "columns": [{"name": "email", "mask": "regexp_replace(email, '^[^@]+', '***')"}]},
    {"user": "analyst_.*|bi_dashboard", "catalog": "lake", "schema": "semantic|accel", "privileges": ["SELECT"]}
  ],
  "functions": [
    {"user": "admin|engineer", "catalog": "pg", "schema": "system", "function": "query", "privileges": ["EXECUTE"]}
  ]
}
```

Rules are evaluated top-down; the first matching rule wins, so put specific rules (filters, masks)
before broad ones. Effects measured: `analyst_eu` got 300 rows, all EUROPE, emails `***@example.com`;
`bi_dashboard` got 1,500 unmasked rows; `analyst_eu` got `PERMISSION_DENIED` for the `pg` catalog,
`lake.sales.orders`, `pg.system.query` and `CREATE VIEW`, but could read `net_revenue_daily`.

Multi-tenant variant: `{"user": "m[0-9]+", "schema": ".*_semantic", "privileges": ["SELECT"],
"filter": "merchant_id = current_user"}` plus a final `{"privileges": []}` deny rule.

Authentication is separate: without an authenticator Trino trusts the `X-Trino-User` header.
Production needs OAuth2, LDAP, Kerberos or certificates in front of these rules.

## Where engine governance ends
- **Rules attach to objects, not data.** `analyst_eu` saw 300 rows through `customer_360` but all
  1,500 in an acceleration table built from it. Every copy needs its own rules.
- **Rules do not follow the data to other engines.** Spark read the same Iceberg view through the
  REST catalog and saw all 5 merchants while Trino user `m01` saw 1. Keep storage credentials away
  from consumers, give them only governed endpoints, and prefer controls inherited from the catalog
  layer (catalog RBAC, credential vending) over engine-only rules.
- `TABLE(pg.system.query(...))` bypasses table rules; restrict the function.

## Workload isolation with resource groups

```properties
# resource-groups.properties
resource-groups.configuration-manager=file
resource-groups.config-file=/etc/trino/resource-groups.json
```

```json
{
  "rootGroups": [{
    "name": "global", "softMemoryLimit": "100%", "hardConcurrencyLimit": 50, "maxQueued": 500,
    "schedulingPolicy": "weighted",
    "subGroups": [
      {"name": "etl",   "softMemoryLimit": "50%",  "hardConcurrencyLimit": 4,  "maxQueued": 50,  "schedulingWeight": 1},
      {"name": "bi",    "softMemoryLimit": "40%",  "hardConcurrencyLimit": 10, "maxQueued": 100, "schedulingWeight": 3},
      {"name": "adhoc", "softMemoryLimit": "20%",  "hardConcurrencyLimit": 1,  "maxQueued": 1,   "schedulingWeight": 1},
      {"name": "other", "softMemoryLimit": "100%", "hardConcurrencyLimit": 20, "maxQueued": 200, "schedulingWeight": 1}
    ]
  }],
  "selectors": [
    {"user": "etl", "group": "global.etl"},
    {"source": "(superset|tableau|powerbi).*", "group": "global.bi"},
    {"clientTags": ["adhoc"], "group": "global.adhoc"},
    {"group": "global.other"}
  ]
}
```

- Selectors match in order, first match wins; route on what tools really send: user, `source`
  (`X-Trino-Source`, e.g. `superset-prod`) and client tags (`X-Trino-Client-Tags`). No query parsing.
- Verify placement: `SELECT state, resource_group_id FROM system.runtime.queries WHERE source = '...'`.
- With `adhoc` at 1 running / 1 queued: the second ad hoc query queued 5,953 ms, the third failed
  with `QUERY_QUEUE_FULL`, and a dashboard query in `global.bi` queued 1 ms and finished in 0.9 s
  while the first ad hoc query still ran.
- Resource groups protect Trino's slots and memory, **not the source**: the ad hoc load was a
  6-second `pg_sleep` in PostgreSQL. Protect sources with limits there or a read replica.
- Across clusters: Trino Gateway gives one URL, rule-based routing, blue/green and canary upgrades,
  health-checked failover. Dremio isolates with separate engines sized and paused independently.

| Workload | Isolation mechanism |
|---|---|
| ETL vs BI vs ad hoc in one cluster | Resource groups + selectors |
| Different SLAs, upgrades, failover | Separate clusters behind Trino Gateway / Dremio engines |
| Source protection | Source-side limits, read replicas, acceleration tables |
| Per-user data visibility | Row filters, column masks on semantic views |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Analyst sees unmasked PII | Broad rule matched before the masking rule | Order rules specific-first |
| Consumers can read raw tables | Catalog/schema rules too broad | Default-deny last rule; grant only `semantic` schemas |
| View works for admin, denied for analyst | `SECURITY INVOKER` view or missing view grant | Use `SECURITY DEFINER` and grant SELECT on the view |
| Dashboards slow during exploration | All queries in one group | Separate groups by `source`/client tag with concurrency caps |
| `QUERY_QUEUE_FULL` errors | `maxQueued` reached | Expected backpressure; raise limits or move the workload |
| Spark/other engine bypasses row filters | Engine-local rules | Catalog-level governance; no raw storage credentials for consumers |
