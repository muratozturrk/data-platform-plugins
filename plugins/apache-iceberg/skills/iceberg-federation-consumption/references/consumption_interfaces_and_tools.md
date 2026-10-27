# Consumption Layer: Interfaces, BI/ML Tools and Tool Selection

Scope: "store once, read everywhere" on Iceberg, measurable consumption requirements, open
interfaces (JDBC/ODBC, Arrow Flight/ADBC, MCP), BI and ML tools, and choosing tools by context.

## Store once, read everywhere
Data stays in curated Iceberg tables (bronze, silver, gold); a semantic layer defines metrics once;
a federation engine exposes them through standard interfaces; tools come to the data.

- Six consumers (Spark SQL, Trino, PyIceberg/Arrow, pandas, Polars, DuckDB) computed identical
  per-region KPIs on a 300,000-row table (revenue within 1e-6 relative, counts exact). Pinned to one
  snapshot id, all six still returned the old answer after a 10,000-row append.
- A departmental Parquet extract matched at export time and was stale after the next append.
- One copy is not one meaning: marketing revenue (list price, not cancelled) = 89,054,595.00 and
  finance revenue (after discount, completed) = 71,079,541.50 on the same table, 25.3% apart.

Snapshot id is the shared contract across engines:

```sql
SELECT ... FROM db.sales VERSION AS OF 123456789;           -- Spark
SELECT ... FROM iceberg.db.sales FOR VERSION AS OF 123456789; -- Trino
```
```python
tbl.scan(row_filter="status == 'completed'", selected_fields=("region", "quantity"),
         snapshot_id=123456789).to_arrow()                  # PyIceberg
```

## Consumption requirements as measurements

| Requirement | Mechanism | Measure with |
|---|---|---|
| Latency, cost | Partition pruning, projection, rollups | Trino `cursor.stats`: `processedRows`, `physicalInputBytes`, `elapsedTimeMillis`; PyIceberg `plan_files()` |
| Freshness | Commit age | `now - current_snapshot().timestamp_ms / 1000` |
| Concurrency | Engine scaling, caching | N parallel sessions, p95 latency, identical answers |
| Reproducibility | Snapshot pinning | Snapshot id + content hash of the result |
| Governance | Row filters, masks, lineage from the catalog | Per-user query results |

Measured on 300,000 rows in 30 daily (`days(order_ts)`) partitions, 111 KB files:
- One-day filter: PyIceberg plans 1 of 30 files; Trino reads 110,993 of 3,340,096 bytes and 10,000
  of 300,000 rows. A bare `count(*)` on whole partitions reads 0 bytes (manifest row counts).
- Projection: column chunks are 8,640 bytes (2 columns) vs 3,249,976 (all), but Trino reads files
  below `parquet_small_file_threshold` (default 3MB) in one request: 3,340,096 bytes either way.
  With `SET SESSION iceberg.parquet_small_file_threshold = '1kB'`: 1,490,100 vs 4,724,536 bytes.
  Projection pays off only on large files; compact small files.
- A 3,600-row daily rollup answered a 7-day revenue tile reading 3,600 rows / 25,207 bytes instead
  of 70,000 rows / 777,331 bytes, same answer to the cent.
- 8 parallel dashboard sessions returned identical results, p95 2.54 s.
- Thresholds come from your audit: e.g. latency 30 s, 1 MB per query, freshness 86,400 s turn into
  PASS/FAIL lines (a full-history query of 3.36 MB failed the 1 MB budget).

## Open interfaces

| Interface | Data shape | Use for | Limitation |
|---|---|---|---|
| JDBC / ODBC (Trino client REST protocol underneath) | Rows, synchronous | Tableau, Power BI, Looker, Excel, reporting | Weak for bulk transfer; driver management |
| Arrow Flight / Flight SQL / ADBC | Arrow record batches over gRPC | Notebooks, ML feature pulls, custom apps, bulk APIs | Adoption still growing; complements JDBC |
| MCP | Tool calls with structured results | LLM agents and assistants | Tools must be registered, policy-aware, monitored |
| REST API on the federation layer | JSON | Partner/customer programmatic access | Build auth, rate limits, auditing |

Trino REST protocol in a few lines (what every driver speaks): `POST /v1/statement` with
`X-Trino-User`/`X-Trino-Source` headers, then follow `nextUri` until absent; a small query took 8
HTTP requests and returned the same rows as DB-API.

Arrow Flight service over PyIceberg (bearer token -> user, tenant filter added server side):

```python
class IcebergFlightServer(flight.FlightServerBase):
    def get_flight_info(self, context, descriptor):
        scan = self._scan(context, descriptor.command)        # JSON: table, columns, filter, snapshot_id
        schema = schema_to_pyarrow(scan.projection(), include_field_ids=False)
        return flight.FlightInfo(schema, descriptor, [flight.FlightEndpoint(descriptor.command, [])], -1, -1)
    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self._scan(context, ticket.ticket).to_arrow_batch_reader())
```
- 300,000 rows x 5 columns: DB-API 7.59 s (39,504 rows/s) vs Flight 0.60 s (496,328 rows/s),
  12.6x. The Flight path skipped Trino's planner, so this mostly shows the cost of row-wise JSON
  decoding in a Python client, not an engine benchmark.
- Schema arrives in `GetFlightInfo` before data; types survive (`int32`, `timestamp[us, tz]`).
- Stream with `to_arrow_batch_reader()`; do not materialize the whole result on the server.

MCP server: expose intent, not SQL. Tools `list_datasets`, `describe_metrics`, `query_metrics`
compile governed metrics to SQL and run on the federation engine **as the caller**:

```python
@mcp.tool()
def query_metrics(metrics: list[str], dimensions: list[str] | None = None, limit: int = 100) -> dict:
    """Compute governed metrics, optionally grouped by dimensions. No free-form SQL is accepted."""
    sql = compile_query(metrics, dimensions or [], source) + f" LIMIT {min(int(limit), 1000)}"
    with trino_connect(user) as conn: ...
```
An unknown dimension (`customer_email`) returns `isError=True`; tenant `m02` sees only its rows and
one dataset. Bind identity to the MCP session principal (OAuth) in production.

## BI and ML tools

| Category | Examples | Evaluate |
|---|---|---|
| Open source BI | Superset, Metabase, Lightdash, Redash | JDBC/SQLAlchemy to Trino/Dremio, honors row/column rules, caching, extensibility |
| Commercial BI | Tableau, Power BI, Looker (LookML), Qlik, ThoughtSpot, Sigma | Native connectors, central semantics, licensing, concurrency |
| Notebooks / ML | JupyterLab, PyIceberg + DuckDB, MLflow, Feast/Tecton, SageMaker/Vertex | Arrow reads, snapshot and schema control, observability |
| Vector stores / agents | FAISS, Weaviate, Pinecone; LangChain, Haystack, MCP | Structured filtering plus semantic search, governed tools |

- Tiles name metrics, never SQL; Superset connects with `trino://user@host:8080/iceberg` (SQLAlchemy).
  SQLAlchemy and DB-API clients rendered the same 3 tiles; 5 merchants' net revenues summed exactly
  to the admin total (45,144,020.00); the raw table returned Access Denied for merchants.
- Adding `bucket(4, merchant_id)` and a sorted `rewrite_data_files` (more than 30 files) left both
  dashboards unchanged: tools are decoupled from physical layout.
- Reproducible training: pin the snapshot and make the computation deterministic. DuckDB
  multi-threaded float sums gave different SHA-256 hashes on one run; `SET threads TO 1` fixed it.
  Log snapshot id + content hash (e.g. in MLflow). A 20,000-row append left the pinned set identical.

## Tool selection by context

| Signal | Lean toward | Avoid |
|---|---|---|
| Small team, Python only (AI startup) | JupyterLab + DuckDB + PyIceberg, MLflow, snapshot pinning | Premature dashboards and metric layers |
| Many teams and BI tools (global bank, media group) | Central semantic layer (dbt, AtScale, LookML) behind JDBC/ODBC; RLS, masking, audit | Forcing one BI tool |
| Merchants/tenants consume data (e-commerce, SaaS) | Embedded BI (Metabase, Sigma, Superset) + engine tenant filters + caching; FastAPI + Flight for bulk | Per-tenant copies or per-client ETL |
| Public plus restricted data (government) | Separate domains, preapproved views, REST API, time travel | Public dashboards on the internal endpoint |
| Residency/HIPAA (healthcare) | Region-bound BI and compute, masking, audit trail | One global deployment |
| Live ops plus history (logistics, manufacturing) | Live dashboards (Sigma, ThoughtSpot, Grafana alerts) + semantic BI on the same tables | Separate pipelines per time horizon |
| Partners and research (nonprofit) | JupyterHub, read-only Flight + DuckDB, snapshots per team, MLflow | Shared mutable extracts |

The federation engine stays the policy enforcement point in every scenario; tools are chosen for
user experience. A rule-based helper covering ten scenarios recommended every reference component
(recall 100%); 54 of 80 recommendations (68%) matched the reference picks, the rest are reviewable
additions, so show the reason for each recommendation.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Same KPI differs between tools | Metric logic in each BI tool | Semantic views/metrics compiled centrally |
| Notebook pulls are slow | Row-oriented driver for bulk data | Arrow Flight/ADBC or catalog-direct PyIceberg reads |
| Projection saves no bytes | Files under `parquet_small_file_threshold` | Compact to larger files |
| Training set hash changes at same snapshot | Non-deterministic parallel float sums | Single-threaded or ordered aggregation |
| Agent produces wrong or unsafe queries | Raw SQL tool exposed | MCP tools over governed metrics, caller identity |
| Spark view query fails `TABLE_OR_VIEW_NOT_FOUND` | Trino-created view references Trino catalog name | Register a Spark catalog with that name, or create views per engine dialect |
