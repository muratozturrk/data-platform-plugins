# Lineage Methods, Blind Spots and Impact Analysis

Lineage is not a feature you switch on; it is the union of several extraction methods, each of
which sees part of the truth. This file explains the methods, what each one misses, how column
lineage and impact analysis should work, and where OpenLineage fits. Product behaviour is cited
from source code of OpenMetadata `1.13.3-release`, DataHub `v1.7.0` and Apache Atlas
`release-2.5.0` (paths relative to each repo root).

## The four extraction methods

| Method | How it works | Typical sources | Strength | Cost |
|---|---|---|---|---|
| **Runtime / event-based** | A listener or hook inside the running job emits inputs/outputs when it runs (OpenLineage events, product-specific hooks) | Spark, Airflow, Flink, dbt runs, Hive | Sees what *actually* ran, including non-SQL code; per-run facts (time, status) | Must be installed in every engine; versions drift; emits only when jobs run |
| **Query-log / SQL parsing** | A parser reads executed queries from the warehouse/engine query history and derives table and column edges | Warehouses and SQL engines with query history | No agent in the job; covers ad-hoc and scheduled SQL alike | Only sees SQL; parser coverage per dialect; needs access to query history |
| **Definition parsing** | Parse static definitions: view DDL, dbt manifests, BI datasets/semantic models, pipeline config | Views, dbt projects, BI tools, ETL tools with declarative config | Available before anything runs; stable | Describes intent, not execution; misses runtime parameters |
| **Manual** | People draw edges in the UI or post them via API | Spreadsheets, file drops, external partners, legacy jobs | Covers anything | Decays; no one updates it; needs ownership |

A fifth, research-grade method — **static analysis of data-science scripts** (deriving
provenance from Python/pandas code) — is described by Namaki et al., *Vamsa* (KDD 2020). None of
the three measured products ships it; treat notebook and script lineage as manual or runtime.

### How the measured products combine them

| Product | Runtime | Query/definition parsing | Edge provenance recorded |
|---|---|---|---|
| OpenMetadata 1.13.3 | OpenLineage (HTTP + Kafka/Kinesis), Airflow, dbt, Spline and other pipeline connectors | SQL parsing in **25 dialects** via sqlfluff/sqlglot/sqlparse (`ingestion/src/metadata/ingestion/lineage/models.py:106-136`); view, dashboard, cross-database lineage | Yes — each edge has a `source` enum: Manual, ViewLineage, QueryLineage, PipelineLineage, DashboardLineage, DbtLineage, SparkLineage, OpenLineage, ExternalTableLineage, CrossDatabaseLineage, ChildAssets (`openmetadata-spec/.../type/entityLineage.json:70`). No confidence field (K) |
| DataHub 1.7.0 | Spark agent, Airflow plugin, Dagster/Prefect integrations; OpenLineage endpoint | sqlglot-based parsing, 20+ dialects, merged in one aggregator (`metadata-ingestion/src/datahub/sql_parsing/sql_parsing_aggregator.py:973-1360`) | Yes — `confidenceScore` per fine-grained edge (`metadata-models/.../dataset/FineGrainedLineage.pdl:49`), filled from parser confidence on some paths and hard-coded `1.0` on others (`sql_parsing_aggregator.py:784,844` vs `:973,1058`) (K) |
| Apache Atlas 2.5.0 | Hooks for 7 systems (Hive, HBase, Sqoop, Storm, Falcon, Impala, Couchbase), all derived from `AtlasHook` (`notification/.../hook/AtlasHook.java:58-126`) | **None general-purpose** — no SQL parser (Calcite/JSqlParser) in the repo; Hive column lineage comes from Hive's own runtime lineage info | No confidence field (K) |

Two implementation details matter when you plan runtime capture: DataHub's Spark agent is built
**on the OpenLineage Spark listener** and converts its events into DataHub's model
(`metadata-integration/java/acryl-spark-lineage/.../DatahubEventEmitter.java:36-38` imports
`io.openlineage.spark.agent.EventEmitter`), and its Airflow 3 plugin also uses OpenLineage
internally (`metadata-ingestion-modules/airflow-plugin/src/datahub_airflow_plugin/airflow3/`).
Standardising pipelines on OpenLineage therefore does not lock you out of DataHub's richer agents.

## Blind spots by method

| Blind spot | Runtime | Query/SQL parsing | Definition parsing | Manual |
|---|---|---|---|---|
| Non-SQL transforms (Python, pandas, Scala UDF bodies) | Partly (dataset-level I/O seen; column mapping often not) | Missed | Missed | If someone draws it |
| Dynamic SQL, string-built queries, stored procedures | Seen if the engine reports | Often unparsable or partial | Missed | — |
| Temporary / staging tables | Seen but clutter the graph | Chains break at temp tables unless collapsed | — | — |
| `SELECT *`, unresolved columns | Depends on engine | Needs schema to expand; otherwise table-only | Same | — |
| File-based hand-offs (object store drops, SFTP, exports) | Only if the writer/reader emits | Missed | Missed | Common gap |
| BI semantic layers, calculated fields | — | Missed | Only if the BI connector parses them | — |
| Jobs that did not run in the window | Missed (no event) | Missed (no query) | Seen | Seen |
| Cross-system identity (same table, different names) | Namespace/naming mismatch splits nodes | Same | Same | — |
| Retired jobs | Old edges linger unless expired | Same | Removed on re-parse | Never removed |

Mitigations that work in practice:

- **Use two methods for every important path** — runtime for jobs, parsing for SQL and views —
  and keep the edge provenance so you can tell them apart (OpenMetadata `source`, DataHub
  `confidenceScore`).
- **Normalise dataset identity once.** OpenLineage dataset namespaces and names must resolve to
  catalog entities; OpenMetadata has a dedicated resolver/normaliser
  (`openmetadata-service/.../openlineage/OpenLineageEntityResolver.java`,
  `OpenLineageDatasetNameNormalizer.java`). Test it with your actual connection URIs.
- **Collapse temp tables.** OpenMetadata records the path through intermediate tables on the edge
  (`tempLineageTables`, `entityLineage.json:94`).
- **Measure coverage.** For each source: share of production tables with ≥1 upstream edge, share
  of orchestrator tasks with emitted lineage, age of newest edge. No measured product detects
  broken chains automatically (D3.1 level 4 not met by any of the three) — you must.
- **Reconcile with the orchestrator.** Every DAG task that writes a dataset should appear as a
  lineage edge; diff the two lists on a schedule.
- **Give manual edges an owner and a review date**, or they become fiction.

## Column-level lineage

What a column edge should carry: source columns, target column, the **transform** (copy,
rename, expression, aggregation), the query or job that created it, and provenance.

| Product | Model | Transform text | Automatic depth | Score |
|---|---|---|---|---|
| OpenMetadata | `columnLineage {fromColumns[], toColumn, function}` (`entityLineage.json:24-42`) | Schema has `function`, but built-in extractors create edges with only `fromColumns`/`toColumn` (e.g. `ingestion/.../lineage/sql_lineage.py:644`, `source/dashboard/dashboard_service.py:764`) — the expression is not shown | Joins, aggregations, CASE parsed from SQL | 3 (K) |
| DataHub | `FineGrainedLineage` with `transformOperation` and `confidenceScore` (`FineGrainedLineage.pdl:44,49`) | Stored (e.g. `SQL: 2 * bar.b AS b`) and shown in the lineage side panel | Plus chart/dashboard `inputFields` chain a source column to a BI field | **4** (K) |
| Apache Atlas | `hive_column_lineage` process entities with `dependencyType` and `expression` (`addons/hive-bridge/.../hook/events/CreateHiveProcess.java:232-240`) | Hive only; Impala has no expression field; the Trino extractor defines `trino_column_lineage` types (`addons/models/6000-Trino/6000-trino_model.json:58`) but its code produces none | Hive runtime | 3 (K) |

Practical rule: if you need column lineage for **PII propagation** or **schema-change impact**,
an edge without the transform is enough. If you need it for **metric root cause** or
**regulatory explanation of a derived value**, require the transform text (D3.2 level 4).

## Impact analysis

The question is "if I change or break X, who and what is affected, and who must I tell?"

| Step | What to require | OpenMetadata | DataHub | Atlas |
|---|---|---|---|---|
| Multi-hop downstream | Depth parameter | `GET /v1/lineage/getLineage` with `upstreamDepth`/`downstreamDepth` (`LineageResource.java:190-214`) | `scrollAcrossLineage`; UI degree filter (`ImpactAnalysis.tsx`, default degree 1) | `GET` lineage with `direction` + `depth` (`webapp/.../rest/LineageREST.java:70-110`) |
| Filter downstream | Only production, only tagged critical, only dashboards | `query_filter` on lineage queries (`LineageResource.java:214,322`) | Filter by degree, entity type, domain, saved view | Not available (no attribute/tag filter) |
| Export affected list | CSV/API with owners | `/v1/lineage/export` and `/exportAsync` (`LineageResource.java:402-486`) | CSV download from the impact view | JSON from API only |
| Notify affected owners | Automatic | Downstream-owner recipients for alerts (`openmetadata-service/.../notifications/recipients/downstream/DownstreamHandler.java`) | Not in open source | No |
| What-if simulation | Simulate a schema change before it happens | No | No | No |
| **Score D3.4** | | 3 (K) | 3 (K) | 2 (K) |

Run impact analysis as a **process step**, not a UI click: the schema-change pull request calls
the lineage API, exports affected assets and owners, and posts the list on the change for
sign-off. The catalog supplies the graph; the approval loop lives in your CI or change process.

## OpenLineage

OpenLineage (LF AI & Data) defines **Run**, **Job** and **Dataset** entities and extensible
**facets** (schema, data quality, column lineage, job/run metadata). A catalog can be a
*consumer* (receives events and maps them to its model) or a *producer* (publishes its own
lineage as OpenLineage events).

| Product | Consumer | Producer | Score D3.5 |
|---|---|---|---|
| OpenMetadata | HTTP `POST /v1/openlineage/lineage` and `/lineage/batch` (`openmetadata-service/.../resources/lineage/OpenLineageResource.java:54,104-105,181-182`); also a pipeline connector that reads OpenLineage events from Kafka or Kinesis (`.../connections/pipeline/openLineageConnection.json:17-19,110-111`); column-lineage facet mapped. The Java SDK's `OpenLineage` helper only *posts into* the server | No | 2 (K) |
| DataHub | `POST /openapi/openlineage/api/v1/lineage` (`metadata-service/openapi-servlet/.../openlineage/controller/LineageApiImpl.java:31,60-61`), mapped incl. column-lineage facet; `openlineage-converter` library | No emitter in the repo | 2 (K) |
| Apache Atlas | None — zero repo matches for OpenLineage/Marquez; ingest request ATLAS-4550 still open | No | 0 (K + İ) |

Design consequences:

- **Emit from pipelines, not from the catalog.** Instrument Spark, Airflow, dbt and Flink with
  OpenLineage once; route events through a fan-out (HTTP proxy or a topic) so a second consumer
  (observability tool, another catalog) can subscribe. This also keeps you portable if you
  change catalogs.
- **Do not expect round-trips.** No measured catalog republishes its lineage as OpenLineage, so
  catalog-only edges (manual, view, query-parsed) cannot be pulled back out in that format; use
  the catalog's own export API (D1.7).
- **Test facet mapping on your data.** Check that column-lineage and schema facets land as
  column edges and schemas, and that dataset names resolve to existing entities instead of
  creating duplicates.
