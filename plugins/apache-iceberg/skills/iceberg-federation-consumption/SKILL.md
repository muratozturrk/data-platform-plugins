---
name: iceberg-federation-consumption
description: "Design the federation and consumption layers on top of an Apache Iceberg lakehouse: Trino and Dremio as federation engines (connectors, pushdown, dynamic filtering, native query passthrough), semantic layers as SECURITY DEFINER views, file-based access control with row filters and column masks, workload isolation with Trino resource groups and Trino Gateway, self-managed vs managed deployments (Athena, Starburst, Dremio Cloud), platform decision scenarios, acceleration alternatives (CTAS tables, reflections, OneLake shortcuts, Spice.ai), and consumption interfaces (JDBC/ODBC, Arrow Flight/ADBC, MCP) for BI, notebooks, ML and AI agents. Use when joining Iceberg with PostgreSQL or other live sources, choosing Trino vs Dremio, debugging pushdown or source load, writing rules.json or resource-groups.json, building governed dashboards or MCP tools, or picking consumption tools per scenario."
---

# Iceberg Federation and Consumption Layers

The federation layer queries Iceberg tables and external systems (OLTP databases, SaaS, legacy
stores) through one engine, without copying the data first. The consumption layer turns the same
curated tables into dashboards, notebooks, ML training sets and agent answers through open
interfaces. Iceberg stays the hub that serves most queries. Federation fills gaps, a semantic layer
gives every metric one meaning, and the engine is where policies are enforced. Every number below
was measured on Trino 476/483 with an Iceberg REST catalog 1.8.1, PostgreSQL 16, PyIceberg 0.9.1
and Spark 3.5.5.

## When to Use This Skill

- Joining Iceberg tables with live PostgreSQL/MySQL/SaaS data, or deciding federate vs ingest.
- Checking what Trino pushes into a JDBC source (filters, TopN, aggregations, joins) and why the source is still loaded.
- Choosing Trino, Dremio or a managed distribution (Athena, EMR, Starburst, Cloudera, Dremio Cloud).
- Defining shared business metrics as versioned views and governing them with row filters and masks.
- Separating ETL, dashboards and ad hoc exploration with resource groups or multiple clusters.
- Replacing repeated federated queries with acceleration tables, reflections or shortcuts.
- Exposing lakehouse data to BI tools (JDBC/ODBC, SQLAlchemy), notebooks (Arrow Flight, PyIceberg) and AI agents (MCP).
- Shortlisting consumption tools for a specific organization (startup, bank, SaaS, healthcare, ...).

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Query OLTP/SaaS data in place, join with Iceberg | Federation engine catalogs (Trino `postgresql` + `iceberg` connectors) | [federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) |
| Know whether a predicate/aggregate reaches the source | `EXPLAIN (TYPE DISTRIBUTED)` + `processedRows` pushdown check | [federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) |
| Pick Trino vs Dremio and a deployment model | Hard constraints, then explained weighted scoring | [federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) |
| Cut source load of repeated dashboard queries | CTAS acceleration table / reflections / shortcuts | [federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) |
| One definition of a metric for all tools | `SECURITY DEFINER` semantic views, versioned comments | [workload_isolation_and_governance.md](references/workload_isolation_and_governance.md) |
| Per-user rows and masked PII | File-based access control `rules.json` (filter, mask, deny) | [workload_isolation_and_governance.md](references/workload_isolation_and_governance.md) |
| Ad hoc queries must not block dashboards | Resource groups + selectors (user, source, client tags), Trino Gateway | [workload_isolation_and_governance.md](references/workload_isolation_and_governance.md) |
| Same KPI in Spark, Trino, pandas, DuckDB, BI | Store once, pin snapshot ids, semantic layer | [consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) |
| Fast bulk reads into Python/ML | Arrow Flight / ADBC, catalog-direct PyIceberg | [consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) |
| Let AI agents query governed data | MCP server exposing metric tools, run as the caller | [consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) |
| Choose BI/notebook/ML tools for an organization | Scenario-driven tool selection | [consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) |

## Federation Engines and Pushdown

Each source is a Trino catalog backed by a connector. The planner pushes what the source can run
and executes the rest itself.
- **Mechanic**: plain-column filters, projections and TopN are pushed. Aggregations are pushed on
  numeric keys and joins are pushed when the cost model favors it (`join-pushdown.enabled=true`).
  Dynamic filtering prunes the Iceberg side of a join with keys from the small side.
- **Facts**: `region = 'EUROPE'` sent 300 of 1,500 rows. `lower(region) = 'europe'` sent all 1,500.
  `GROUP BY` on a text column was not pushed because collations can differ between engines.
  Dynamic filtering cut an Iceberg scan from 15,000 to 576 rows and from 116 to 39 splits.
- **Gotchas**: pushdown reduces transfer, not source work. 5 federated queries cost PostgreSQL 5
  scans and 7,500 tuple reads because the filter column had no index. `TABLE(pg.system.query(...))`
  runs SQL in the source's own dialect, and table rules cannot see that SQL text.
- Ref: [references/federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) · Code: [examples/federation_layer/](examples/federation_layer/)

## Trino vs Dremio and Deployment

Dremio is Iceberg-first. It ships an integrated catalog, autonomous maintenance, reflections, a
semantic layer and a UI, and it does not support Hudi. Trino is format-neutral and has the widest
connector set. It ships no semantic layer and leaves you to schedule maintenance yourself. With a
REST catalog, Trino runs CTAS, MERGE and `optimize` on Iceberg (5 data files compacted to 1).
- **Deployment**: Dremio Community (no FGAC), Enterprise (needs Kubernetes) or Cloud (AWS/Azure).
  Trino as open source, Athena (pay per query), EMR, Cloudera, or Starburst Enterprise/Galaxy.
- **Decide**: apply hard constraints first (formats, location, Kubernetes, FGAC), then score the
  remaining options and give a reason for every point. The reference scenarios: fragmented estate
  leads to Trino, Iceberg-committed to Dremio, occasional Hudi queries on S3 to Athena, a Cloudera
  estate to Cloudera Trino.
- Ref: [references/federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) · Code: [examples/federation_layer/platform_decision/](examples/federation_layer/platform_decision/)

## Acceleration and Alternatives

A live federated view scans the source on every refresh. A materialized copy removes that load but
goes stale between refreshes.
- **Mechanic**: `CREATE OR REPLACE TABLE lake.accel.x COMMENT 'refreshed_at=...' AS <view>` (each
  refresh is a snapshot). Count staleness with a symmetric `EXCEPT`. Trino does not support
  `CREATE MATERIALIZED VIEW` on an Iceberg REST catalog.
- **Facts**: 5 dashboard runs caused 5 PostgreSQL scans on the live path and 0 on the accelerated
  copy, with a median of 1,101 ms vs 586 ms.
- **Gotchas**: a copy does not inherit row filters (300 rows through the view, 1,500 through the
  copy). OneLake-style shortcuts push only filters and projections. Spice.ai-style views target app
  backends.
- Ref: [references/federation_engines_trino_dremio.md](references/federation_engines_trino_dremio.md) · Code: [examples/federation_layer/federation_alternatives/](examples/federation_layer/federation_alternatives/)

## Semantic Layer and Access Control

- **Mechanic**: deploy each definition as a `SECURITY DEFINER` view whose comment carries a
  version, stored as an Iceberg view. Attach `filter` and `columns[].mask` rules to the view in
  `rules.json` and end with a default deny.
- **Facts**: BI, notebook and REST clients returned the same 2,351,142,352.02 USD. A v2 deploy
  changed all of them at once. `analyst_eu` saw 300 EUROPE rows with masked emails and got
  `PERMISSION_DENIED` on the raw catalog.
- **Gotchas**: rules attach to objects and stay inside the engine. Spark read the same Iceberg view
  and saw all 5 merchants while Trino user `m01` saw 1. Keep storage credentials away from
  consumers. Without an authenticator, Trino trusts the `X-Trino-User` header.
- Ref: [references/workload_isolation_and_governance.md](references/workload_isolation_and_governance.md) · Code: [examples/federation_layer/federation_requirements/](examples/federation_layer/federation_requirements/), [examples/consumption_layer/bi_and_ml_tools/](examples/consumption_layer/bi_and_ml_tools/)

## Workload Isolation

- **Mechanic**: `resource-groups.json` holds groups with `hardConcurrencyLimit`, `maxQueued`,
  `softMemoryLimit` and `schedulingWeight`. Selectors are evaluated in order and the first match
  wins: `user`, then `source` regex, then `clientTags`, then a default.
- **Facts**: with `adhoc` limited to 1 running and 1 queued query, the second ad hoc query queued
  for 5,953 ms and the third failed with `QUERY_QUEUE_FULL`. A dashboard query meanwhile queued for
  only 1 ms.
- **Gotchas**: resource groups protect the engine, not the source system. Protect the source with
  source-side limits or replicas. To isolate across clusters, use Trino Gateway or Dremio engines.
- Ref: [references/workload_isolation_and_governance.md](references/workload_isolation_and_governance.md) · Code: [examples/federation_layer/workload_isolation/](examples/federation_layer/workload_isolation/)

## Consumption: Store Once, Measure Requirements

- **Mechanic**: all tools read the same Iceberg table, and an engine-neutral snapshot id pins each
  read (`VERSION AS OF`, `FOR VERSION AS OF`, `snapshot_id=`). Requirements become measurements:
  `cursor.stats` bytes and rows, `plan_files()`, commit age, p95 latency.
- **Facts**: six engines computed identical KPIs. Two team "revenue" definitions on that same table
  differed by 25.3%. A one-day filter read 110,993 of 3,340,096 bytes. A bare `count(*)` read
  0 bytes because Trino answered it from manifests. Files under 3MB are read whole
  (`parquet_small_file_threshold`), so projection saved nothing until the threshold was lowered.
- Ref: [references/consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) · Code: [examples/consumption_layer/store_once_read_everywhere/](examples/consumption_layer/store_once_read_everywhere/), [examples/consumption_layer/consumption_requirements/](examples/consumption_layer/consumption_requirements/)

## Open Interfaces: JDBC/ODBC, Arrow Flight, MCP

- **JDBC/ODBC** stays the universal BI path. Underneath it is Trino's REST protocol
  (`POST /v1/statement`, then follow `nextUri`).
- **Arrow Flight** moved 300,000 rows in 0.60 s, against 7.59 s through DB-API (12.6x), and kept
  `int32` and tz-aware timestamps. Authenticate with bearer-token middleware and enforce tenant
  filters on the server.
- **MCP** exposes `list_datasets`, `describe_metrics` and `query_metrics`, with no raw SQL. Calls
  run as the caller, so engine rules still apply, and an unknown dimension returns `isError=True`.
- Ref: [references/consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) · Code: [examples/consumption_layer/open_interfaces/](examples/consumption_layer/open_interfaces/)

## BI, ML and Tool Selection

- Dashboard tiles name metrics and contain no SQL. Superset connects with
  `trino://user@host:8080/iceberg`. Changing the physical layout (partition evolution, a sort
  rewrite) left the dashboards unchanged.
- ML: pin the snapshot and make the computation deterministic. DuckDB multi-threaded float sums
  changed the result hash until `SET threads TO 1`. Log the snapshot id together with a content
  hash.
- Tool choice follows context: notebooks only for a Python startup, a semantic layer behind
  JDBC/ODBC for a multi-BI bank, embedded BI with tenant filters for SaaS, region-bound tools for
  data residency, MCP for agents.
- Ref: [references/consumption_interfaces_and_tools.md](references/consumption_interfaces_and_tools.md) · Code: [examples/consumption_layer/bi_and_ml_tools/](examples/consumption_layer/bi_and_ml_tools/), [examples/consumption_layer/tool_selection/](examples/consumption_layer/tool_selection/)

## Cross-Cutting Best Practices

1. Keep Iceberg the hub. Federate for freshness and exploration, and ingest or accelerate data
   that is scanned every day.
2. Measure source load where the source owner sees it (`pg_stat_user_tables`), not only in the
   engine.
3. Verify pushdown from the plan and from `processedRows`. Rewrite function predicates as plain
   column predicates.
4. Define every metric once as a versioned view or semantic model and never copy its SQL into
   tools.
5. Order access rules from specific to general and end with a default deny. Restrict
   `system.query`.
6. Treat every copy (extract, acceleration table) as a new object that needs its own rules, a
   refresh owner and a freshness SLA.
7. Send identity on every path (JDBC user, Flight token, MCP principal) and give no consumer a
   privileged back door or raw storage credentials.
8. Route workloads by user, `source` and client tags. Protect the source system separately.
9. Pin snapshot ids for reproducible ML and research results, and log them with a content hash.
10. Keep secrets out of Trino catalog files by using `${ENV:VAR}` substitution.
