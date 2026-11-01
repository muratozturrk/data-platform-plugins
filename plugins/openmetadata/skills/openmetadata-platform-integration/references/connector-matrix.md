# Connector Matrix — Typical On-Prem Stack × OpenMetadata 1.13.3

All paths are relative to the OpenMetadata source tree at `1.13.3-release`
(`255f6694913b`). Connector code lives in `ingestion/src/metadata/ingestion/source/<category>/<name>/`,
connection schemas in `openmetadata-spec/src/main/resources/json/schema/entity/services/connections/`
(abbreviated `connections/` below), example workflows in `ingestion/src/metadata/examples/workflows/`.
Evidence type is **K** (source code) unless marked.

## How to read a connector's capabilities

Every connection schema carries flags from `connections/connectionBasicType.json:23-92`
(`supportsMetadataExtraction`, `supportsUsageExtraction`, `supportsLineageExtraction`,
`supportsViewLineageExtraction`, `supportsProfiler`, `supportsDBTExtraction`, `supportsDataDiff`,
`supportsQueryComment`, …). Grep the flags in the connector's `*Connection.json` before promising a
feature — a connector without `supportsLineageExtraction` has no `-lineage` source.

Overall breadth: ~94 connectors in 11 categories (database 47, dashboard 17, pipeline 13,
messaging 4, metadata 3, …) (D11.1 = 3, `ServiceType.constant.ts:178-208`). Beta/GA status is
only listed in the UI constant `BETA_SERVICES`.

## Coverage classes (T4 legend)

| Class | Meaning |
|---|---|
| built-in | Connector code in the main repo, registered in the ingestion framework |
| separate module | Official but outside the main ingestion framework (own install/lifecycle) |
| community | Third-party or other project's repository |
| type model only | Entity/connection type defined, no code that produces the data |
| none | No match in the repository |

## Per-component detail

### Kafka + Schema Registry — built-in

| Item | Value |
|---|---|
| Source type / extra | `kafka` / `openmetadata-ingestion[kafka]` (`ingestion/setup.py:336`) |
| Key config | `bootstrapServers`, `schemaRegistryURL`, `saslUsername`/`saslPassword`, `securityProtocol`, `saslMechanism`, `consumerConfig`, `schemaRegistryConfig`, `schemaRegistryTopicSuffixName`, `schemaRegistrySSL`, `topicFilterPattern` (`connections/messaging/kafkaConnection.json:25-101`) |
| Extracts | Topics, partitions/config, schema text from the registry incl. referenced schemas (`messaging/common_broker_source.py:218-269`), schema types Avro / Protobuf / JSON (`type/schema.json:13-16`), sample messages when `generateSampleData: true` |
| Schema Registry | Not a separate service: a Confluent `SchemaRegistryClient` inside the Kafka connector (`messaging/kafka/connection.py:22`). Subject looked up as `<topic>` + suffix (default `-value`) |
| Lineage | None from this connector. Topic edges come from Kafka Connect, OpenLineage, Airflow outlets or the API |
| Capabilities flag | Only `supportsMetadataExtraction` (`kafkaConnection.json:106`) |

Kafka Connect is a separate pipeline connector (`pipeline/kafkaconnect/`, extra `kafkaconnect`,
`setup.py:337`) that builds topic↔table lineage from connector configs: Debezium `topic.prefix` /
`database.server.name`, `table.include.list`, JDBC `table.name.format`, and Iceberg sink
`iceberg.tables` (`pipeline/kafkaconnect/constants.py:18-60`, `metadata.py:1753-1800`). It needs
`messagingServiceName` to resolve topics (`kafkaConnectConnection.json:60`). The target tables
must already exist in some OM service.

### Flink — built-in, metadata and status only

| Item | Value |
|---|---|
| Source type | `flink`; config `hostPort` (JobManager REST), `verifySSL`, `sslConfig`, `pipelineFilterPattern` (`connections/pipeline/flinkConnection.json:24-44`) |
| Extracts | Jobs as `Pipeline` entities, job vertices as tasks (`pipeline/flink/metadata.py:78-89`), run status (`:130`) |
| Lineage | **None** — `yield_pipeline_lineage_details` has only a docstring (`pipeline/flink/metadata.py:125-128`) |

### NiFi — built-in, pipeline-level lineage only

| Item | Value |
|---|---|
| Source type / extra | `nifi` (`setup.py:366`); `nifiConfig` = basic auth or client certificate (`nifiConnection.json:15-80`) |
| Extracts | Process groups as pipelines, processors as tasks, run status (`pipeline/nifi/metadata.py:192`, `:287`) |
| Lineage | `yield_pipeline_lineage_details` is empty and references issue #6950 (`nifi/metadata.py:237-245`). A bulk step adds **pipeline→pipeline** edges for parent/child groups and output→input port connections (`nifi/metadata.py:359-430`). No processor→dataset edges |

### Airflow — built-in connector + lineage backend

| Item | Value |
|---|---|
| Source type / extra | `airflow` (`setup.py:199`) |
| Connection | `hostPort`, `numberOfStatus`, `connection` = Airflow REST API, `Backend`, MySQL, PostgreSQL or SQLite metadata DB (`airflowConnection.json:24-37`) |
| Extracts | DAGs, tasks, owners, recent run statuses |
| Lineage | From DAG/task `inlets`/`outlets` (`pipeline/airflow/metadata.py:830-871`, parser `airflow/lineage_parser.py:85-402`), tagged `PipelineLineage` |
| Push variants | `OpenMetadataLineageBackend` (`ingestion/src/airflow_provider_openmetadata/lineage/backend.py:34`), `OpenMetadataLineageOperator` (`lineage/operator.py:35`), success/failure callbacks (`lineage/callback.py`) |
| Managed mode | `openmetadata-airflow-apis/` lets the OM server deploy ingestion DAGs into Airflow |

### dbt — built-in

| Item | Value |
|---|---|
| Source type | `dbt`, `sourceConfig.config.type: DBT` |
| Artefact sources | `dbtConfigSource.dbtConfigType`: `cloud`, `local`, `http`, `s3`, `gcs`, `azure` (`examples/workflows/dbt.yaml`) |
| Extracts | Models linked to existing tables, descriptions (`dbtUpdateDescriptions`), owners (`dbtUpdateOwners`), tags into classification `dbtClassificationName`, domain, test definitions + run results (`database/dbt/metadata.py:367-1770`; `dbtPipeline.json:48-105`) |
| Lineage | `DbtLineage` from manifest dependencies and compiled SQL (`dbt/metadata.py:1241-1393`) |
| Precondition | Tables must exist under `serviceName`, else "Table … not found under service" (`dbt/metadata.py:790`) |

### Spark — type model only

`connections/pipeline/sparkConnection.json` has no connection fields; its only capability is
`supportsMetadataExtraction` (`:28`). Also `metadataIngestion/engine/sparkEngineConfig.json` and
`automations/testSparkEngineConnection.json`. No `source/pipeline/spark/` directory; `SparkConnection`
never appears in ingestion code. The only PySpark code is a Delta Lake client
(`database/deltalake/clients/pyspark.py`, extra `deltalake-spark`, `setup.py:292`). OpenMetadata
ships a Spark lineage agent product-wide, but its producer code is not in this repository, so it
cannot be verified here. Routes for Spark lineage are in [lineage.md](lineage.md).

### MinIO / S3 — built-in (two connectors)

| Connector | What it does | Key config |
|---|---|---|
| `s3` storage service | Buckets and prefixes as `Container` entities; structure from an `openmetadata.json` manifest in the bucket or a global manifest (`storage/storage_service.py:84,198-301`); manifest entries: `dataPath` (glob), `structureFormat`, `isPartitioned`, `partitionColumns`, `unstructuredData` (`metadataIngestion/storage/containerMetadataConfig.json:14-104`) | `awsConfig`, `bucketNames`, `containerFilterPattern`, `consoleEndpointURL` (`s3Connection.json:23-61`) |
| `datalake` database service | Reads files under a bucket/prefix as tables: CSV, TSV, Avro, Parquet variants, JSON/JSONL (`readers/dataframe/reader_factory.py:38-54`); profiler supported | `configSource.securityConfig` (AWS credentials), `bucketName`, `prefix` (`examples/workflows/datalake_s3.yaml`) |

MinIO: no MinIO-specific connector in any of the three open-source products. In OM the S3 API
endpoint is `awsConfig.endPointURL` (`security/credentials/awsCredentials.json:36`), and the console
link is `consoleEndpointURL`, with MinIO named in code (`storage/s3/metadata.py:911,941`).

### Iceberg — none

No `*iceberg*` file or directory in `ingestion/src` or `openmetadata-spec/src`; `Iceberg` is not a
value of `databaseService.json`'s service-type enum. What exists:

| Fragment | Evidence | Practical value |
|---|---|---|
| `TableType.Iceberg` enum value | `entity/data/table.json:34` | Tables can be marked Iceberg |
| Trino sets `TableType.Iceberg` when the catalog's connector is `iceberg` | `database/trino/metadata.py:283-302`, query `trino/queries.py:65-69` | Iceberg tables visible as Trino tables, typed correctly |
| Athena ingests Iceberg table properties as custom properties (`includeCustomProperties`) | `databaseServiceMetadataPipeline.json:93-98`, `athena/metadata.py:390` | Athena only — not Trino |
| Datalake JSON reader recognises Iceberg/Delta metadata-file shape and extracts columns | `utils/datalake/datalake_utils.py:748-772` | Schema of one metadata file; no snapshots |
| Kafka Connect parses Iceberg sink `iceberg.tables` | `kafkaconnect/constants.py:31` | Topic→table lineage if the table exists in OM |

Snapshots, partition specs, delete files and table history are not modelled. Workarounds:
[missing-connectors-and-workarounds.md](missing-connectors-and-workarounds.md).

### Trino, ClickHouse, PostgreSQL — built-in, full SQL-engine set

| Engine | Extra | Flags (connection schema) | Query history for usage/lineage |
|---|---|---|---|
| Trino | `trino` (`setup.py:412`) | metadata, usage, lineage, dbt, profiler, database, query comment, data diff (`trinoConnection.json:109-140`) | `queryHistoryTable`, default `"system"."runtime"."queries"` (`trinoConnection.json:144-148`), only `FINISHED`, excludes OM/dbt-tagged queries (`trino/queries.py:17-34`) |
| ClickHouse | `clickhouse` (`setup.py:233`) | metadata, usage, lineage, dbt, profiler, query comment (`clickhouseConnection.json:104-121`) | `system.query_log` (`clickhouse/queries.py:30,77`) |
| PostgreSQL | `postgres` (`setup.py:371`) | metadata, usage, lineage, dbt, profiler, database, query comment, data diff (`postgresConnection.json:131-160`) | `pg_stat_statements` (`postgres/query_parser.py:78`) — extension must be enabled |

Trino adds cross-database lineage to the same tables in other services (see [lineage.md](lineage.md)).

### Great Expectations — built-in (checkpoint action)

| Item | Value |
|---|---|
| Extras | `great-expectations` (GE ~=0.18) and `great-expectations-1xx` (GE ~=1.0) (`setup.py:33-34,309-310`) |
| GE 0.18 | `metadata.great_expectations.action.OpenMetadataValidationAction` (`great_expectations/action.py:91-120`) |
| GE 1.x | `OpenMetadataValidationAction1xx`, `type: open_metadata_validation_action` (`great_expectations/action1xx.py:69-95`) |
| Parameters | `config_file_path` (OM connection YAML), `database_service_name`, `database_name`, `schema_name` (default `default`), `table_name`, `expectation_suite_table_config_map` |
| Result | Expectations become OM test cases with results; they enter OM's quality report and incident flow (D5.7 = 3). One-way: OM rules cannot be exported back to GE |

Note: `great_expectations/README.md` still shows `ometa_service_name`; the code parameter is
`database_service_name`.

### Ranger — scaffolding only; Vault — not supported

See [missing-connectors-and-workarounds.md](missing-connectors-and-workarounds.md). Short form:
`securityService` (enum value `Ranger`, `entity/services/securityService.json:20-21`),
`connections/security/rangerConnection.json` (`hostPort`, `authType`) and a base class whose
topology has `children=[]` (`ingestion/source/security/security_service.py:68`); no `ranger` entry
in `setup.py`, no `service_spec.py`. Secrets-manager values are `db`, `managed-aws`, `aws`,
`managed-aws-ssm`, `aws-ssm`, `managed-azure-kv`, `azure-kv`, `in-memory`, `gcp`, `kubernetes`
(`conf/openmetadata.yaml:624`; `security/secrets/secretsManagerProvider.json:8`) — no HashiCorp Vault.

## Comparison (T4, 13 platform components)

| Product | built-in | separate module | type model only | none | Working coverage |
|---|---|---|---|---|---|
| OpenMetadata 1.13.3 | 11 | 0 | 1 (Spark) | 1 (Iceberg) | **11 / 13** |
| DataHub v1.7.0 | 11 | 2 (Spark Java agent, Airflow plugin) | 0 | 0 | **13 / 13** |
| Apache Atlas 2.5.0 | 0 | 4 (Kafka, Trino, Iceberg partial, Schema Registry) | 4 | 5 | **4 / 13** |

Where the others differ on the stack's weak points:

| Component | DataHub v1.7.0 | Atlas 2.5.0 |
|---|---|---|
| Iceberg | built-in `IcebergSource` + profiler (`metadata-ingestion/src/datahub/ingestion/source/iceberg/iceberg.py:149`) | Only via the Trino extractor (`addons/trino-extractor/.../IcebergEntityConnector.java`); Iceberg typedefs missing from `addons/models/` |
| Spark | Push-based Java agent (`metadata-integration/java/acryl-spark-lineage/`) | Type model only; producer in an external repo |
| Flink | built-in, lineage module, status INCUBATING | Type model only |
| Ranger | Same level as OM (D6.7 = 1) | Native Ranger authorizer (D6.7 = 3) |

Atlas has no central ingestion framework: every integration is a separate `addons/` Maven module,
so "separate module" there is architecture, not a defect — but costlier to operate.
