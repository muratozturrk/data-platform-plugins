# Lineage in OpenMetadata 1.13.3 — Methods per Engine

Paths are relative to the OpenMetadata source at `1.13.3-release`. Spec schemas are under
`openmetadata-spec/src/main/resources/json/schema/` (abbreviated `spec/`). Evidence is **K**
(source code) unless marked.

## 1. Model

| Element | Where | Notes |
|---|---|---|
| Edge `fromEntity` → `toEntity` + `lineageDetails` | `spec/type/entityLineage.json:104-184` | Tables, containers, topics, pipelines, dashboards, ML models can be nodes |
| Column lineage `fromColumns[]` → `toColumn` + `function` | `entityLineage.json:24-40` | `function` is in the schema but **no built-in extractor fills it** (D3.2 note) |
| `lineageDetails`: `sqlQuery`, `columnsLineage`, `pipeline`, `description`, `source`, created/updated by/at | `entityLineage.json:41-102` | The `pipeline` reference lets a job sit *on* the edge instead of being a node |
| `source` enum | `entityLineage.json:70` | `Manual`, `ViewLineage`, `QueryLineage`, `PipelineLineage`, `DashboardLineage`, `DbtLineage`, `SparkLineage`, `OpenLineage`, `ExternalTableLineage`, `CrossDatabaseLineage`, `ChildAssets` |
| Storage | `openmetadata-service/.../jdbi3/LineageRepository.java:135-182,433-452` | Relational store + search index; crosses service boundaries (D3.1 = 3) |

Scores (rubric 0–4): table lineage D3.1 = 3, column lineage D3.2 = 3, hybrid collection D3.3 = 3,
impact analysis D3.4 = 3, OpenLineage D3.5 = 2.

## 2. Choosing a method

| Source of truth | Method | Edge `source` | Column level |
|---|---|---|---|
| SQL run by an engine with query history (Trino, ClickHouse, PostgreSQL) | `<engine>-lineage` workflow | `QueryLineage` | Yes, parsed |
| Views | Same workflow, `processViewLineage: true` (default) | `ViewLineage` | Yes |
| Same table seen through Trino and through the backing engine | Trino cross-database lineage | `CrossDatabaseLineage` | Yes (name match) |
| SQL you have as text (Spark SQL, Flink SQL, ad-hoc ETL) | CSV query log, or `metadata lineage` for one statement | `QueryLineage` | Yes, if tables resolve |
| dbt models | `dbt` workflow | `DbtLineage` | Yes |
| Airflow tasks | `inlets` / `outlets` | `PipelineLineage` | No |
| Spark jobs | OpenLineage (preferred) or Spline | `OpenLineage` / `PipelineLineage` | OL: via columnLineage facet |
| Flink, NiFi processors, custom code | OpenLineage events or explicit API edges | `OpenLineage` / `Manual` | If you send it |
| Kafka Connect / Debezium | `kafkaconnect` workflow | pipeline lineage | No |

## 3. Parse: query-log lineage

Workflow source type `<engine>-lineage`, `sourceConfig.config.type: DatabaseLineage`
(verified example: `ingestion/src/metadata/examples/workflows/trino_lineage.yaml`). Keys from
`spec/metadataIngestion/databaseServiceQueryLineagePipeline.json`:

| Key | Default | Purpose |
|---|---|---|
| `queryLogDuration` | 1 (days) | Look-back window |
| `queryLogFilePath` | — | Read a CSV file or a directory of `.csv` instead of the engine |
| `resultLimit` | 1000 | Max queries; a warning is logged when hit (`lineage_source.py:365-370`) |
| `parsingTimeoutLimit` | 300 s | Per-query parse timeout |
| `queryParserConfig.type` | `Auto` | `Auto` (sqlglot → sqlfluff → sqlparse), `SqlGlot`, `SqlFluff` (`parserconfig/queryParserConfig.json:12-16`) |
| `filterCondition` | — | Extra SQL filter on the history query |
| `processViewLineage` / `processQueryLineage` / `processStoredProcedureLineage` | true | Toggle each family |
| `threads` | 1 | Parallel parsing |
| `processCrossDatabaseLineage` + `crossDatabaseServiceNames` | false / — | See §4 |
| `enableTempTableLineage` | false | Snowflake temp/transient tables |
| `incrementalLineageProcessing` | true | Skip already-processed queries |

Dialect comes from the service type (`ingestion/src/metadata/ingestion/lineage/models.py:106-183`:
Trino → `trino`, ClickHouse → `clickhouse`, PostgreSQL → `postgres`, Hive → `hive`, DeltaLake →
`sparksql`, unknown → `ansi`); about 25 dialects (D3.3).

Engine notes:
- **Trino** reads `queryHistoryTable` (default `"system"."runtime"."queries"`), only `FINISHED`
  queries, skipping ones tagged `/* {"app": "OpenMetadata"…} */` or dbt (`trino/queries.py:17-34`).
  The default table reflects what the coordinator still holds in memory (Trino behaviour, not OM):
  schedule the lineage run often, or point `queryHistoryTable` at a persisted query log.
- **ClickHouse** reads `system.query_log` (`clickhouse/queries.py:30,77`).
- **PostgreSQL** reads `pg_stat_statements` (`postgres/query_parser.py:78`); install the extension.

CSV query log (`lineage_source.py:299-327`, `query_parser_source.py:89-94`): columns
`query_text` (required), `database_name`, `schema_name` (optional). Usage has a shipped example
(`examples/workflows/query_log_usage.yaml`, source `query-log-usage`); the lineage counterpart is
`QueryLogLineageSource` (`database/query/service_spec.py`, service type `QueryLog`).

One statement, ad hoc — `metadata lineage -c file.yaml` (`ingestion/src/metadata/cli/lineage.py:39-46`):

```yaml
serviceName: lake_trino            # an existing database service; tables resolve inside it
query: "INSERT INTO iceberg.silver.orders SELECT * FROM iceberg.bronze.orders_raw"
# filePath: ./job.sql              # alternative to query
parseTimeout: 300
workflowConfig:
  openMetadataServerConfig:
    hostPort: http://openmetadata:8585/api
    authProvider: openmetadata
    securityConfig: {jwtToken: "${OM_BOT_JWT}"}
```

It calls `add_lineage_by_query` (`ometa/mixins/lineage_mixin.py:371`). Good for CI: parse the SQL
of a Spark/Flink job at deploy time and register its edges.

## 4. Cross-database lineage (Trino)

`trino-lineage` with `processCrossDatabaseLineage: true` and `crossDatabaseServiceNames: [ch_svc, pg_svc]`
links each Trino table to a table in the listed services when the FQN with the database part
swapped resolves **and** the table name and the set of column names are equal
(`database/trino/lineage.py:101-175`). Edge direction: backing table → Trino table, with column
lineage. Every Trino table is compared with every listed database, including system schemas
(code comment) — filter the services you list.

## 5. Runtime: Airflow, dbt, Spline, Kafka Connect

**Airflow** — the `airflow` connector reads `inlets`/`outlets` of each task and groups them by
`key` (`airflow/lineage_parser.py:85-140,402`). Three accepted forms
(`ingestion/examples/airflow/dags/airflow_lineage_example.py`):

```python
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

@task(inlets={"tables": ["lake_trino.iceberg.bronze.orders_raw"]},
      outlets={"tables": ["lake_trino.iceberg.silver.orders"]})
def simple(): ...

@task(inlets=[OMEntity(entity=Table, fqn="lake_trino.iceberg.bronze.orders_raw", key="orders")],
      outlets=[{"entity": "table", "fqn": "lake_trino.iceberg.silver.orders", "key": "orders"}])
def typed(): ...
```

FQNs must match entities already in OM. Push-style alternatives: `OpenMetadataLineageBackend`
(enable with `OPENMETADATA_LINEAGE_ENABLED=true`, per the example DAG docstring),
`OpenMetadataLineageOperator(server_config=…, service_name=…, only_keep_dag_lineage=…)`
(`airflow_provider_openmetadata/lineage/operator.py:35-60`).

**dbt** — lineage from the manifest (`DbtLineage`); requires the warehouse service ingested first.
For dbt on Spark, the tables dbt writes must be catalogued in the OM service named in
`serviceName`, otherwise edges are dropped with "Table … not found under service".

**Spline** — `spline` connector pulls execution events from a Spline server (`hostPort`, `uiHostPort`,
`lineageInformation.dbServiceNames`; `examples/workflows/spline.yaml`). It resolves datasources
only of the form `jdbc:…` or `dbfs:/…` (`pipeline/spline/metadata.py:135-160`); Spark writes to
`s3a://` paths on MinIO do **not** resolve to tables.

**Kafka Connect** — topic↔table edges from connector configs (Debezium, JDBC, Iceberg sink);
see [connector-matrix.md](connector-matrix.md).

**Flink / NiFi** — no dataset lineage from their connectors (`flink/metadata.py:125-128`;
`nifi/metadata.py:237-245`, issue #6950).

## 6. OpenLineage (consumer side)

Two entry points (D3.5 = 2):

| Entry | Detail |
|---|---|
| HTTP | `POST /api/v1/openlineage/lineage` (one RunEvent), `POST /api/v1/openlineage/lineage/batch` (`openmetadata-service/.../resources/lineage/OpenLineageResource.java:54,104-184`). Authenticated like any API call (bot JWT) |
| Broker | Pipeline connector `openlineage` (extra `openlineage`, `setup.py:367`): `brokerConfig` Kafka (`brokersUrl`, `topicName`, `consumerGroupName`, `consumerOffsets`, `securityProtocol`, `sslConfig`, `saslConfig`) or Kinesis (`streamName`, `awsConfig`), plus `namespaceToServiceMapping`, `pipelineFilterPattern` (`connections/pipeline/openLineageConnection.json:17-194`) |

Server settings (`spec/configuration/openLineageSettings.json:9-36`): `enabled` (default true),
`autoCreateEntities` (default true), `eventTypeFilter`, `defaultPipelineService` (default
`openlineage`), `namespaceToServiceMapping`. Only `COMPLETE` events are processed by default
(`openlineage/OpenLineageMapper.java:68`).

Resolution rules (`openlineage/OpenLineageDatasetNameNormalizer.java`, `OpenLineageEntityResolver.java`):
1. Candidates: every `symlinks` facet identifier first, then the dataset name
   (`Normalizer.java:75-93`).
2. A dataset whose namespace starts with `s3://`, `s3a://`, `gs://`, `abfss://`, `abfs://`,
   `wasbs://`, `adl://` is a storage path (`Normalizer.java:52-53`): its name is not treated as a
   `db/table`, and storage datasets resolve to **containers** (`Resolver.java:137-146`).
3. Namespace → OM service via `namespaceToServiceMapping` (`Resolver.java:278`).
4. Auto-create makes a Table only when a matching **schema** already exists in OM; otherwise it logs
   "schema not found" and drops the dataset (`Resolver.java:407-430`).
5. The `columnLineage` facet becomes column lineage on each input→output edge
   (`OpenLineageMapper.java:111-134`).

Consequence for Spark/Flink writing Iceberg on MinIO: emit events whose datasets carry the catalog
table identifier (symlink), ingest the Trino schemas first, and map the namespace to the Trino
service — then edges land on the Trino-catalogued Iceberg tables. Which identifiers a given
producer emits is producer-specific: verify with a captured event before rollout.

## 7. Manual and programmatic edges

`PUT /api/v1/lineage` with `{"edge": {"fromEntity": {"id": "…", "type": "table"}, "toEntity": {…},
"lineageDetails": {"source": "Manual", "sqlQuery": "…", "columnsLineage": [...], "pipeline": {…}}}}`
(`LineageResource.java:722`; `spec/api/lineage/addLineage.json`). Python SDK equivalent:
`OpenMetadata.add_lineage(AddLineageRequest(...))` (`ometa/mixins/lineage_mixin.py:109`). Edit with
`PATCH /api/v1/lineage/{fromEntity}/{fromId}/{toEntity}/{toId}` (`:781-782`).

## 8. Impact analysis

| Need | Call | Evidence |
|---|---|---|
| Graph around an asset | `GET /api/v1/lineage/getLineage?fqn=…&type=table&upstreamDepth=2&downstreamDepth=3` (+ `query_filter`, `column_filter`, `includeDeleted`, paging `from`/`size`) | `LineageResource.java:190-259` |
| One direction | `GET /api/v1/lineage/getLineage/{direction}` | `:295-314` |
| Export CSV | `GET /api/v1/lineage/export` or `/exportAsync` | `:402-486` |
| Quality along lineage | `GET /api/v1/lineage/getDataQualityLineage` | `:369-370` |
| Notify downstream owners | Alert with `notifyDownstream` / `downstreamDepth` | `spec/events/eventSubscription.json:160-172` |

## 9. Known limits (1.13.3)

| Limit | Evidence |
|---|---|
| No detection of broken lineage chains | D3.1 note |
| `function` on column lineage never filled by built-in extractors | D3.2 note |
| No per-edge confidence field | D3.3 note |
| No "what-if" change simulation | D3.4 note |
| OM consumes OpenLineage but does not produce it | D3.5 note |
| Usage data does not feed impact analysis automatically | D10.5 note |
