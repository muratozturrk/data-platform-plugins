# Federation Engines: Trino and Dremio over Iceberg

Scope: querying Iceberg plus external systems in place, pushdown, engine internals, deployment
models, platform decisions and alternatives to live federation.

## Problem
Not all data belongs in Iceberg. Live OLTP state, SaaS/CRM data, datasets of unknown value,
hybrid/multicloud estates and ephemeral high-volume data are cheaper to query in place than to
ingest. A federation layer queries them through one engine and joins them with lakehouse tables.
The lakehouse stays the hub (often more than 90% of queries); federation fills gaps.

A federation layer has three parts: a distributed engine with connectors and pushdown, a semantic
layer of shared definitions, and standard interfaces (JDBC/ODBC, REST, Arrow Flight/ADBC, MCP).

| Condition | Choice | Reason |
|---|---|---|
| Real-time data owned by an OLTP system | Federate | Freshness without a CDC pipeline |
| Value of a dataset still unknown | Federate first, ingest later | Cheap experimentation |
| Same remote data queried heavily every day | Ingest into Iceberg or accelerate | Repeated scans load the source |
| Data must be governed and optimized long term | Ingest into Iceberg | Maintenance and governance are strongest there |

Challenges: varying source performance, sources not built for central governance, inconsistent
schemas/freshness, and **load on source systems** (a dashboard on a federated view turns every
refresh into an OLTP query).

## Pushdown mechanics (Trino JDBC connectors)
The planner rewrites part of the plan into the source's own SQL. Read it from
`EXPLAIN (TYPE DISTRIBUTED)`: a pushed operation appears inside the table handle
(`constraint on [...]`, `limit=`, `sortOrder=`, a generated `Query[SELECT ...]`); a non-pushed one
appears as a Trino operator (`filterPredicate = ...`, `Aggregate[type = ...]`, `InnerJoin[`).
Confirm with `cursor.stats["processedRows"]`, the rows that actually left the source.

| Query on a 1,500-row PostgreSQL table | Pushed? | Rows to Trino |
|---|---|---|
| `WHERE region = 'EUROPE'` (plain column) | filter + projection | 300 |
| `WHERE lower(region) = 'europe'` | no, evaluated in Trino | 1,500 |
| `ORDER BY credit_limit DESC LIMIT 5` | TopN | 5 |
| `GROUP BY loyalty_tier` (smallint) | aggregation | 4 |
| `GROUP BY segment` (text) | no (collation can differ) | 1,500 |
| Join of 2 PostgreSQL tables with selective filter | join (`join-pushdown.enabled=true`) | 3 |
| Same join without filter | cost-based: join in Trino | 2,640 |

Pushdown reduces transfer, not necessarily source work: 5 runs of a federated revenue query cost
PostgreSQL 5 scans and 7,500 tuple reads on `customers`, because `status` had no index. Measure
source load where the source team sees it:

```sql
SELECT coalesce(seq_scan,0) + coalesce(idx_scan,0)          AS scans,
       coalesce(seq_tup_read,0) + coalesce(idx_tup_fetch,0) AS tuples
FROM pg_stat_user_tables WHERE relname = 'customers';   -- call pg_stat_clear_snapshot() first
```

## Dremio vs Trino

| Aspect | Dremio | Trino |
|---|---|---|
| Positioning | Lakehouse platform, Iceberg-first | Format-neutral federated SQL engine (Presto fork, renamed from PrestoSQL in late 2020) |
| Architecture | Coordinators behind a load balancer; engines of executors; Arrow columnar | One coordinator + workers; stages, tasks, splits; cost-based optimizer |
| Iceberg | Integrated catalog (Polaris/REST), automatic compaction and vacuum | Iceberg connector over external catalogs (REST, Glue, Hive, Nessie); also Delta, Hudi, Hive |
| Sources | Catalogs, object stores, RDBMS/NoSQL, warehouses; depth over breadth | Widest connector set incl. Kafka, Pinot, Druid, TPC-H, Faker, JMX; SPI for custom connectors |
| Acceleration | Reflections (raw/aggregation), columnar cloud cache, runtime filtering | Dynamic filtering, broadcast/partitioned joins, external caches (Alluxio, Rubix) |
| Semantic layer, UI | Built in | None; pair with Cube, AtScale, dbt metrics or a vendor |
| Interfaces | JDBC, ODBC, REST, Arrow Flight, MCP | Client REST protocol, JDBC, ODBC, community MCP servers |
| Workload isolation | Multiple engines, routing rules | Resource groups; Trino Gateway across clusters |
| Hudi | Not supported | Supported |

Trino on Iceberg is not read-only: with a REST catalog it runs CTAS, INSERT, MERGE and
`ALTER TABLE ... EXECUTE optimize` (4 INSERTs + 1 MERGE produced 5 data files; after `optimize`:
1 data file, 0 delete files, 7 snapshots; PyIceberg read all 15,000 rows). What Trino lacks is
autonomy: you schedule maintenance yourself.

## Trino execution facts
- Each source is a catalog backed by a connector: `SELECT catalog_name, connector_name FROM system.metadata.catalogs`.
- A federated join over 80 Iceberg data files ran as 5 stages, 116 splits, 2 SOURCE fragments.
- The CBO broadcasts a small dimension (`REPLICATED`); `SET SESSION join_distribution_type = 'PARTITIONED'` changes the plan, not the result.
- **Dynamic filtering**: join keys from the small side prune the large side. Joining
  `lake.sales.orders` (partitioned by `month(order_date)`) with 3 promotion days from PostgreSQL read
  576 instead of 15,000 Iceberg rows, 39 instead of 116 splits, 9,437 instead of 250,325 bytes.
  Toggle with `enable_dynamic_filtering`; partitioning on the join key maximizes the effect.
- **Native passthrough** runs source-dialect SQL (Dremio's "external query" counterpart):

```sql
SELECT * FROM TABLE(pg.system.query(query =>
  'SELECT region, percentile_cont(0.5) WITHIN GROUP (ORDER BY credit_limit) FROM customers GROUP BY region'));
```
The SQL text is opaque to table rules: grant `EXECUTE` on `pg.system.query` only to engineers.

## Deployment models

| Engine | Option | Model / key points |
|---|---|---|
| Dremio | Community Edition | Self-managed, free, core features; no fine-grained access control |
| Dremio | Enterprise Edition | Self-managed, **requires Kubernetes**; RBAC/FGAC, integrated catalog, autonomous maintenance and reflections |
| Dremio | Dremio Cloud | SaaS on AWS or Azure; vendor control plane, customer data and compute |
| Trino | Open source | Self-managed anywhere; you deploy, tune and govern |
| Trino | Amazon Athena / EMR | Serverless per-query pricing (Glue, QuickSight) / managed clusters |
| Trino | Cloudera | Managed component; alternative to Impala |
| Trino | Starburst Enterprise / Galaxy | Enterprise distribution / SaaS: observability, security, autoscaling |

## Decision scenarios

| Situation | Choice | Deciding factor |
|---|---|---|
| Fragmented estate: many clouds, RDBMS, NoSQL, files | Trino | Connector breadth; bridge to a future lakehouse |
| Committed Iceberg lakehouse, managed maintenance wanted | Dremio (Cloud) | Integrated catalog, auto optimization, reflections |
| Business users with inconsistent BI-specific semantics | Dremio | Native semantic layer, UI, access control |
| Occasional queries on Hudi in S3, low ops capacity | Trino via Athena | Pay per query; Dremio has no Hudi |
| On-prem Cloudera with Impala limits | Cloudera managed Trino | Upgrade path inside the platform |
| Hybrid on-prem Hadoop to ADLS, Iceberg strategy | Dremio Enterprise | Hybrid deployment, Iceberg support |
| No Kubernetes on-prem, FGAC required | Not Dremio Enterprise/Community | Hard constraints |

Encode decisions as code: **hard constraints first** (unsupported format, location, Kubernetes,
missing FGAC exclude an option with a reason), then weighted, explained scores. Each point carries
a reason string so reviewers argue with a weight, not a black box. Close calls are real: open
source Trino beat Starburst Enterprise by 1 point (a cost preference); Dremio Cloud and Enterprise
tie at 5 when only the engine is decided. See `examples/federation_layer/platform_decision/`.

## Alternatives to live federation

| Approach | Strength | Limitation |
|---|---|---|
| Federation engine | Expressive cross-system SQL | Source load, variable latency |
| Shortcuts (OneLake-style) | Simple, predictable; external S3/ADLS/GCS looks native | Only filters/projections reach storage; joins and aggregations run locally |
| AI-native virtualization (Spice.ai-style) | Small fast materialized views for apps/agents (DataFusion, Arrow Flight) | Application backends, not broad analytics |
| Materialized acceleration (reflections, CTAS) | No source load | Stale between refreshes, needs its own access rules |

Trino cannot `CREATE MATERIALIZED VIEW` on an Iceberg REST catalog
(`createMaterializedView is not supported for Iceberg REST catalog`); build acceleration with
`CREATE OR REPLACE TABLE lake.accel.x COMMENT 'refreshed_at=...' AS <federated sql>` (each refresh
is a snapshot). Measured: 5 dashboard runs caused 5 PostgreSQL scans live and 0 accelerated;
31,470 vs 1,500 processed rows; median 1,101 ms vs 586 ms. Count staleness with a symmetric
`EXCEPT` (one OLTP update gave 2 differing rows; 0 after refresh). Storage-level access delivered
1,500 rows to Trino for `count(*), sum(credit_limit) ... WHERE region = 'EUROPE'`; the PostgreSQL
connector pushed the aggregate and returned 1 row.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| OLTP team reports load from analytics | Every federated query scans the source | Index filter columns, read replica, or accelerate/ingest |
| Federated filter pulls the whole table | Function on the column (`lower()`, regex) | Rewrite to a plain predicate or add a computed source column |
| `GROUP BY` on text not pushed | Collation semantics differ | Group by numeric keys or pre-aggregate in the source |
| Join on two JDBC tables runs in Trino | Join pushdown disabled or not cost-effective | `join-pushdown.enabled=true`, add selective filters |
| Large Iceberg scan in a federated join | Dynamic filtering off or partitioning unrelated to the join key | Enable it; partition/sort on the join key |
| `NOT_SUPPORTED` on `CREATE MATERIALIZED VIEW` | REST catalog | CTAS acceleration table with scheduled refresh |
