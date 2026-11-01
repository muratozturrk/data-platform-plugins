# Ingestion Workflows — Writing, Running, Scheduling (OpenMetadata 1.13.3)

Every YAML key below exists in a 1.13.3 example under `ingestion/src/metadata/examples/workflows/`
or in the connection/pipeline JSON Schema cited next to it. Host names, service names and
credentials are placeholders. Evidence is **K** (source code) unless marked.

## 1. Anatomy

```yaml
source:
  type: <connector>[-lineage|-usage]   # e.g. trino, trino-lineage, trino-usage
  serviceName: <OM service name>       # created on first run if serviceConnection is given
  serviceConnection: {config: {type: <ConnectionType>, ...}}
  sourceConfig: {config: {type: <PipelineType>, ...filters...}}
processor: {type: query-parser | orm-profiler | orm-test-runner, config: {}}   # some pipelines only
stage:     {type: table-usage, config: {filename: /tmp/x}}                    # usage only
sink:      {type: metadata-rest, config: {}}
bulkSink:  {type: metadata-usage, config: {filename: /tmp/x}}                 # usage only
workflowConfig:
  loggerLevel: INFO                     # DEBUG, INFO, WARN, ERROR
  openMetadataServerConfig:
    hostPort: http://openmetadata:8585/api
    authProvider: openmetadata
    securityConfig: {jwtToken: "${OM_BOT_JWT}"}
    storeServiceConnection: true        # false = do not persist the connection in OM
    # secretsManagerProvider / secretsManagerLoader, verifySSL, sslConfig (see nifi.yaml)
```

The framework is a chain of `Source → Processor → Stage → Sink/BulkSink` steps
(`ingestion/src/metadata/ingestion/api/steps.py:34-97`). The source type string resolves the module:
`clickhouse-lineage` → `source/database/clickhouse/lineage.py` (`ingestion/src/metadata/utils/importer.py:80-120`).

`sourceConfig.config.type` per pipeline: `DatabaseMetadata`, `DatabaseLineage`, `DatabaseUsage`,
`Profiler`, `DBT`, `TestSuite`, `MessagingMetadata`, `PipelineMetadata`, `StorageMetadata`
(`spec/metadataIngestion/*Pipeline.json`).

## 2. CLI

| Command | Workflow class | Use for |
|---|---|---|
| `metadata ingest -c f.yaml` | `MetadataWorkflow` (`cli/ingest.py:42`) | Metadata, `-lineage`, dbt, messaging, pipeline, storage |
| `metadata usage -c f.yaml` | `UsageWorkflow` (`cli/usage.py:42`) | `-usage` sources |
| `metadata profile -c f.yaml` | `ProfilerWorkflow` (`cli/profile.py:44`) | Profiler |
| `metadata test -c f.yaml` | `TestSuiteWorkflow` (`cli/dataquality.py:46`) | Test suites |
| `metadata classify -c f.yaml` | `AutoClassificationWorkflow` | PII auto-classification |
| `metadata ingest-dbt`, `metadata lineage`, `metadata app`, `metadata scaffold-connector` | — | dbt from a project dir, one SQL statement, apps, connector skeleton |

Commands from `ingestion/src/metadata/cmd.py:46-57`. Install the matching extra, e.g.
`pip install "openmetadata-ingestion[trino,kafka,clickhouse,postgres,airflow,dbt,datalake-s3]"` —
extras are keys of `plugins` in `ingestion/setup.py` (`kafka` :336, `clickhouse` :233, `trino` :412,
`postgres` :371, `airflow` :199, `dbt` :245, `datalake-s3` :279, `s3` :289, `nifi` :366,
`openlineage` :367, `kafkaconnect` :337). Flink has no extra.

## 3. Verified examples per component

**Trino metadata / lineage / usage / profiler** — `trino.yaml`, `trino_lineage.yaml`,
`trino_usage.yaml`, `trino_profiler.yaml`:

```yaml
# metadata
source:
  type: trino
  serviceName: lake_trino
  serviceConnection:
    config:
      type: Trino
      hostPort: trino:8080
      username: om_ingest
      authType: {password: "${TRINO_PASSWORD}"}   # or {jwt: ...}
      catalog: iceberg
  sourceConfig:
    config: {type: DatabaseMetadata, schemaFilterPattern: {excludes: ["information_schema"]}}
sink: {type: metadata-rest, config: {}}
---
# lineage (connection reused from the existing service)
source:
  type: trino-lineage
  serviceName: lake_trino
  sourceConfig: {config: {type: DatabaseLineage, queryLogDuration: 1, resultLimit: 10000}}
sink: {type: metadata-rest, config: {}}
---
# profiler
source:
  type: trino
  serviceName: lake_trino
  sourceConfig: {config: {type: Profiler}}
processor: {type: orm-profiler, config: {}}
sink: {type: metadata-rest, config: {}}
```

**ClickHouse usage** (`clickhouse_usage.yaml`) — note the extra steps:

```yaml
source:
  type: clickhouse-usage
  serviceName: dwh_clickhouse
  serviceConnection:
    config: {type: Clickhouse, username: om_ingest, password: "${CH_PASSWORD}", hostPort: clickhouse:8123}
  sourceConfig: {config: {type: DatabaseUsage, queryLogDuration: '1'}}
processor: {type: query-parser, config: {}}
stage: {type: table-usage, config: {filename: /tmp/clickhouse_usage}}
bulkSink: {type: metadata-usage, config: {filename: /tmp/clickhouse_usage}}
```

**PostgreSQL** (`postgres.yaml`): `type: Postgres`, `hostPort`, `username`, `authType.password`,
`database`, `ingestAllDatabases`, `sslMode`, `databaseFilterPattern`, `schemaFilterPattern`.

**Kafka + Schema Registry** (`kafka.yaml`):

```yaml
source:
  type: kafka
  serviceName: events_kafka
  serviceConnection:
    config:
      type: Kafka
      bootstrapServers: kafka-1:9092,kafka-2:9092
      schemaRegistryURL: http://schema-registry:8081
      # securityProtocol / saslMechanism / saslUsername / saslPassword for SASL clusters
  sourceConfig:
    config: {type: MessagingMetadata, topicFilterPattern: {excludes: ["_.*"]}, generateSampleData: false}
sink: {type: metadata-rest, config: {}}
```

Keep `generateSampleData: false` when topics carry personal data — samples are stored in OM.

**Flink** (`flink.yaml`): `type: Flink`, `hostPort: http://flink-jobmanager:8081`, `verifySSL`,
`sslConfig`; `sourceConfig.config.type: PipelineMetadata`.

**NiFi** (`nifi.yaml`): `type: Nifi`, `hostPort: https://nifi:8443`, `nifiConfig: {username, password, verifySSL}`
or client-certificate fields; `PipelineMetadata` with optional `lineageInformation.dbServiceNames` /
`storageServiceNames`, `includeLineage`, `markDeletedPipelines`.

**Airflow** (`airflow.yaml`): `type: Airflow`, `hostPort`, `numberOfStatus`, `connection` = the Airflow
metadata DB (e.g. `type: Postgres`, `hostPort`, `username`, `authType.password`, `database`), or
`type: Backend` when the workflow runs inside that Airflow.

**dbt from files** (`dbt.yaml`, local variant):

```yaml
source:
  type: dbt
  serviceName: lake_trino            # the warehouse service the models live in
  sourceConfig:
    config:
      type: DBT
      dbtConfigSource:
        dbtConfigType: local         # also http, s3 (MinIO via dbtSecurityConfig), gcs, azure, cloud
        dbtCatalogFilePath: /dbt/target/catalog.json
        dbtManifestFilePath: /dbt/target/manifest.json
        dbtRunResultsFilePath: /dbt/target/run_results.json
      dbtUpdateDescriptions: true
      includeTags: true
sink: {type: metadata-rest, config: {}}
```

**MinIO via the datalake connector** (`datalake_s3.yaml` + `awsCredentials.json:15-36`):

```yaml
source:
  type: datalake
  serviceName: minio_files
  serviceConnection:
    config:
      type: Datalake
      configSource:
        securityConfig:
          awsAccessKeyId: "${MINIO_ACCESS_KEY}"
          awsSecretAccessKey: "${MINIO_SECRET_KEY}"
          awsRegion: us-east-1
          endPointURL: http://minio:9000
      bucketName: landing
      prefix: exports
  sourceConfig: {config: {type: DatabaseMetadata}}
sink: {type: metadata-rest, config: {}}
```

**MinIO via the S3 storage connector** (no example YAML in the repo; keys from `s3Connection.json:17-61`
and `storageServiceMetadataPipeline.json:11-74`): `type: S3`, `awsConfig` (same keys as above),
`bucketNames`, `consoleEndpointURL`; `sourceConfig.config.type: StorageMetadata`. Without
`storageMetadataConfigSource` the connector looks for `s3://<bucket>/openmetadata.json`
(`storage/s3/metadata.py:951-996`):

```json
{"entries": [{"dataPath": "transactions", "structureFormat": "parquet",
              "isPartitioned": true,
              "partitionColumns": [{"name": "event_date", "dataType": "DATE"}]}]}
```

**Test suite** (`test_suite.yaml`): `source.type: TestSuite`, `entityFullyQualifiedName`,
`processor.type: orm-test-runner` with `testCases[]` (`name`, `testDefinitionName`, `entityLink`,
`parameterValues`).

**Great Expectations 1.x checkpoint action** (`great_expectations/action1xx.py:69-95`):

```python
from metadata.great_expectations.action1xx import OpenMetadataValidationAction1xx
action = OpenMetadataValidationAction1xx(
    config_file_path="/etc/om/",          # directory holding config.yml (OM connection)
    database_service_name="lake_trino",
    database_name="iceberg", schema_name="silver", table_name="orders")
```

`config.yml` is rendered as a Jinja template, so secrets can come from `{{ env('OM_BOT_JWT') }}`
(`great_expectations/utils/ometa_config_handler.py:30-87`). For GE 0.18 use
`metadata.great_expectations.action.OpenMetadataValidationAction` with the same parameters.

## 4. Run order

1. Metadata for every database, messaging, storage and pipeline service.
2. dbt (needs tables), Kafka Connect (needs topics and tables).
3. Lineage and usage (query logs), then OpenLineage producers switched on.
4. Profiler, test suites, auto-classification.

Lineage and dbt sources skip entities that are not yet catalogued — only a log line tells you.

## 5. Where ingestion runs

| Option | How | Evidence |
|---|---|---|
| Managed Airflow (default) | OM server deploys DAGs through `AirflowRESTClient`; Airflow has `openmetadata-managed-apis` installed | `conf/openmetadata.yaml:543-547`; `openmetadata-airflow-apis/` |
| Kubernetes jobs | `K8sPipelineClient` (K8s jobs; with the operator, `OMJob`/`CronOMJob` custom resources) | `openmetadata-service/.../clients/pipeline/k8s/README.md:5-74` |
| No scheduler from OM | `className: …noop.NoopClient`, run the CLI from your own scheduler | `conf/openmetadata.yaml:545` |
| Your own Airflow DAG | `MetadataWorkflow.create(cfg).execute()` in a `PythonOperator` | `ingestion/examples/airflow/dags/airflow_metadata_extraction.py` |
| Container per run | Docker operator image | `ingestion/operators/docker/` |

Pipeline service client settings: `apiEndpoint`, `metadataApiEndpoint`, `verifySSL`,
`logStorageConfiguration` (S3-compatible log bucket incl. `endPointURL` for MinIO),
`secretsManagerLoader` (`noop`, `airflow`, `env`) (`conf/openmetadata.yaml:543-577`).

Air-gapped builds: the ingestion image installs `openmetadata-managed-apis` and
`openmetadata-ingestion[...]` from PyPI and pulls Airflow constraints from GitHub at build time
(`ingestion/Dockerfile:104-108`) — build against an internal PyPI mirror and a vendored
constraints file. The bundled Airflow pin is 3.2.2 (`ingestion/setup.py:23`).

## 6. Secrets

| Mechanism | Detail | Evidence |
|---|---|---|
| `${VAR}` in workflow files | The CLI loader runs `os.path.expandvars` on the file | `ingestion/src/metadata/config/common.py:90-109` |
| Server-side secrets manager | `secretsManager`: `db` (default, Fernet-encrypted in the OM DB), `aws`, `aws-ssm`, `managed-*`, `azure-kv`, `gcp`, `kubernetes`, `in-memory` | `conf/openmetadata.yaml:623-624`; `secrets/DBSecretsManager.java:50` |
| Ingestion side | `secretsManagerProvider` + `secretsManagerLoader` in `openMetadataServerConfig` | `examples/workflows/nifi.yaml`; `openMetadataConnection.json:75-80` |
| Bot tokens | Use an ingestion bot JWT, never the admin token baked into examples | — |

HashiCorp Vault is not a provider (searched `vault` in `utils/secrets/` and
`openmetadata-service/.../secrets/`: only Azure Key Vault). Options in
[missing-connectors-and-workarounds.md](missing-connectors-and-workarounds.md).

## 7. Custom connectors

- Skeleton: `metadata scaffold-connector` (`ingestion/src/metadata/cli/scaffold.py:11-60`).
- No-fork route: a connection of type `CustomDatabase` (also custom pipeline/storage/messaging
  connections) with `sourcePythonClass: my_pkg.my_module.MySource`; the source `type` must start
  with `custom` (e.g. `customdatabase`) (`spec/.../database/customDatabaseConnection.json:13-29`;
  `workflow/ingestion.py:258-266`; `utils/constants.py:177`). Runs under the same scheduler/monitoring.
- License: `ingestion/` is under the Collate Community License v1.0, not Apache-2.0. Internal use is
  free; publishing a derived connector carries the licence's terms — see `openmetadata-architecture`.

## 8. Workflows as code

YAML workflows can be applied by CLI or API, but there is no built-in CI validation, no drift
detection between Git and the server, and no Terraform provider; the Kubernetes operator only offers
job CRDs (D11.6 = 2). Keep YAML in Git and apply from CI yourself.
