# D5 / D10 — How Three Open-Source Catalogs Measured

Products and exact source trees inspected (2026-08-11):

| Product | Version | Commit | Claim (B) share |
|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b` | 0 % |
| DataHub (open-source edition) | `v1.7.0` | `7f81ccbfe27b` | 0 % |
| Apache Atlas | `release-2.5.0` | `234608315f29` | 0 % |

All cells below are evidence type **K** (read in source code). Paths are relative to each
product's repository root. The products are examples measured with the same rubric. None of them
is assumed to be your platform.

## Matrix

| ID | Capability | OM | DH | Atlas |
|---|---|---|---|---|
| D5.1 | Rule authoring | 3 | 1 | 0 |
| D5.2 | Profiling | 3 | 3 | 1 |
| D5.3 | Test execution and scheduling | 2 | 0 | 0 |
| D5.4 | Quality incidents and SLA | 2 | 2 | 0 |
| D5.5 | Quality score and reporting | 3 | 1 | 0 |
| D5.6 | Anomaly / drift detection | 1 | 0 | 0 |
| D5.7 | External quality tool integration | 3 | 3 | 1 |
| **D5** | **mean** | **2.4** | **1.4** | **0.3** |
| D10.1 | Freshness monitoring | 1 | 2 | 1 |
| D10.2 | Volume / row count | 3 | 2 | 1 |
| D10.3 | Schema drift | 3 | 3 | 2 |
| D10.4 | Pipeline run status | 3 | 3 | 1 |
| D10.5 | Usage analytics | 3 | 3 | 0 |
| D10.6 | Cost analytics | 3 | 1 | 1 |
| **D10** | **mean** | **2.7** | **2.3** | **1.0** |

Pattern: **recording is strong, enforcement is weak.** Across all three, no capability that needs
a timer, a gate or a learned model reaches 3: D5.3 circuit breaker, D5.4 SLA/escalation, D5.6
seasonality, D10.1 alerting in OM.

## D5 detail

### D5.1 Rule authoring
- **OM 3.** 25 built-in test definitions are used with parameters in the UI. There is a free-SQL
  test (`tableCustomSQLQuery`) and `tableDiff` for cross-table checks. No natural-language rules
  and no rule suggestions. `openmetadata-service/src/main/resources/json/data/tests/` (25 files),
  `.../tests/tableDiff.json:1-77`.
- **DH 1.** `NATIVE` and `INFERRED` assertion sources are marked "DataHub Cloud only". OSS has
  `upsertCustomAssertion`; rules come from dbt/GX or SDK code.
  `metadata-models/src/main/pegasus/com/linkedin/assertion/AssertionSource.pdl:17-32`.
- **Atlas 0.** No rule mechanism in code or docs. `DATA_QUALITY` appears only as a classification
  example. `intg/src/main/java/org/apache/atlas/model/instance/AtlasEntity.java:77-95`.

### D5.2 Profiling
- **OM 3.** Histograms, quartiles, null/unique ratios, regex/LIKE pattern metrics and sample data,
  scheduled and stored as a time series. No correlation or candidate-key detection.
  `openmetadata-spec/src/main/resources/json/schema/entity/data/table.json:497-660`,
  `ingestion/src/metadata/profiler/metrics/`.
- **DH 3.** Histogram, quantiles, distinct-value frequencies, stdev, median, samples, on the
  ingestion schedule. No format/pattern detection. `.../dataset/DatasetFieldProfile.pdl`.
- **Atlas 1.** Copies Hive metastore `numRows/totalSize/numFiles` into `hive_table.parameters`.
  No time series. `addons/models/1000-Hadoop/1030-hive_model.json:66`.

### D5.3 Test execution and scheduling
- **OM 2.** Tests run as a `TestSuite` ingestion pipeline on cron, with run history. No
  load-triggered run and no circuit breaker.
  `openmetadata-spec/.../entity/services/ingestionPipelines/ingestionPipeline.json:14,101-106`,
  `ingestion/src/metadata/data_quality/processor/test_case_runner.py`.
- **DH 0.** OSS does not execute rules. It only stores external results as `AssertionRunEvent`.
  Checked in code (no monitor entity in the registry) and in the official Cloud/OSS matrix.
  `AssertionSource.pdl:16-30`.
- **Atlas 0.** The task framework knows only classification propagation, audit reduction and
  search download. `repository/src/main/java/org/apache/atlas/tasks/TaskFactoryRegistry.java:1-60`.

### D5.4 Quality incidents and SLA
- **OM 2.** Each failure becomes an incident with status `New/Ack/Assigned/Resolved`, assigned as
  a task, severity `Severity1–5`, and a resolution reason
  (`FalsePositive/MissingData/Duplicates/OutOfBounds/Other`). No due date, SLA or escalation; no
  external ITSM link. `openmetadata-spec/.../tests/testCaseResolutionStatus.json:10-98` (status
  enum at :14, severity at :26), `.../tests/resolved.json:13`.
  The severity classifier works only on asset features (tier, has-owner, followers, votes). Its
  coefficients come from synthetic data, and it returns nothing when the asset has no tier.
  `openmetadata-service/.../util/incidentSeverityClassifier/LogisticRegressionIncidentSeverityClassifier.java:21-27,98-105`.
- **DH 2.** First-class incident: state `ACTIVE/RESOLVED`; stage `TRIAGE / INVESTIGATION /
  WORK_IN_PROGRESS / FIXED / NO_ACTION_REQUIRED`; priority 0–3; assignees. Raising an incident is
  manual in OSS; there is no SLA or escalation field.
  `metadata-models/.../incident/IncidentInfo.pdl:17-116`, `.../incident/IncidentStatus.pdl:19-57`.
- **Atlas 0.** No incident, SLA or assignment model. `intg/.../model/tasks/AtlasTask.java:37-51`.

### D5.5 Quality score and reporting
- **OM 3.** Tests carry a dimension (Completeness, Accuracy, Consistency, Validity, Uniqueness,
  Integrity, SQL, plus `NoDimension`). Results are aggregated and exported through
  `/dataQualityReport` and tracked over time. Weights cannot be configured.
  `openmetadata-spec/.../tests/testDefinition.json:108-120`,
  `openmetadata-service/.../resources/dqtests/TestSuiteResource.java:506`.
- **DH 1.** A categorical health indicator (PASS/FAIL/WARN) per asset, derived from assertions
  and incidents. No numeric multi-dimensional score in OSS.
  `datahub-graphql-core/.../resolvers/health/EntityHealthResolver.java:61-120`.
- **Atlas 0.** `MetricsService` counts types/entities/tags only.
  `repository/src/main/java/org/apache/atlas/services/MetricsService.java:89-104`.

### D5.6 Anomaly / drift detection
- **OM 1.** Only a `useDynamicAssertion` flag on the test case. No seasonality model, no
  prophet/statsmodels dependency, no anomaly schema in the repo.
  `openmetadata-spec/.../tests/testCase.json:150-153`, `ingestion/setup.py`.
  **Caveat (from the research's own cross-check):** the OSS docs publish an anomaly-detection
  how-to page *without* a commercial-only badge, so this score may be **too low**. The research
  froze it at 1 and flagged it for review.
- **DH 0 (OSS).** Anomaly detection exists only in DataHub Cloud. The docs mark it `saasOnly`, and
  the Cloud description ("trains on the historical pattern… trends, seasonality") matches level 3.
  It is "not in OSS", not "absent".
- **Atlas 0.** No statistical deviation code or docs. `MetricsService.java:202-222`.

### D5.7 External quality tool integration
- **OM 3.** dbt tests are imported as definition + case + result
  (`ingestion/src/metadata/ingestion/source/database/dbt/metadata.py:1601-1770`). Great
  Expectations sends results through a validation action for GX 0.18 (`great_expectations/action.py`)
  and 1.x (`great_expectations/action1xx.py`, action `type: open_metadata_validation_action`).
  Action parameters: `config_file_path`, `database_service_name`, `database_name`, `schema_name`
  (default `"default"`), `table_name`, `expectation_suite_table_config_map`
  (`action.py:107-118`, `action1xx.py:85-92`). External results feed the same quality report and
  incident flow. One-way only.
- **DH 3.** Great Expectations, dbt tests, Snowflake DMFs and ODCS results all map onto the same
  `Assertion`/`AssertionRunEvent` model and the health indicator. GX severities map
  `critical→HIGH`, `warning→MEDIUM`, `info→LOW`
  (`metadata-ingestion-modules/gx-plugin/src/datahub_gx_plugin/common.py:190-206`). The GX action
  needs `server_url` and optionally `token`, `platform_alias`, `platform_instance_map`
  (`.../datahub_gx_plugin/action.py:89-104`). One-way only.
- **Atlas 1.** External results can only be attached by uploading a Business Metadata CSV.
  `webapp/src/main/java/org/apache/atlas/web/rest/EntityREST.java:1176-1197`.

## D10 detail

| ID | OM | DH | Atlas |
|---|---|---|---|
| D10.1 Freshness | **1** — `lifeCycle.updated` filled from query logs. No freshness test among the 25 definitions (`tableDataToBeFresh` only in `bootstrap/sql/migrations/native/1.12.0/postgres/postDataMigrationSQLScript.sql:13-28`). Contract `refreshFrequency`/`maxLatency` never evaluated | **2** — `Operation` aspect last-update time series, yearly heat map, dbt `warn_after/error_after` as assertions. Native freshness monitor Cloud-only. `common/Operation.pdl:56-60` | **1** — static `createTime/lastAccessTime`. `1030-hive_model.json:61-62` |
| D10.2 Volume | **3** — row count and size profiled as a time series. Threshold tests (range, equal, inserted-rows-per-period) raise alerts. No %-change test. `table.json:892-937`, `tests/tableRowCountToBeBetween.json` | **2** — `rowCount`/`sizeInBytes` time series and chart. Threshold volume assertion Cloud-only. `dataset/DatasetProfile.pdl:8-43` | **1** — last-sync `parameters` value only. `BaseHiveEvent.java:87,412` |
| D10.3 Schema drift | **3** — `ENTITY_UPDATED` with `matchAnyFieldChange({'columns','dataModel','joins'})` triggers an alert, with `notifyDownstream`/`downstreamDepth` along lineage. Drop and add are not given different severities. `EntityObservabilityFilterDescriptor.json:54-61`, `events/eventSubscription.json:164-174` | **3** — schema diff on every ingest: removal and type change = `MAJOR` ("backwards incompatible"), addition = `MINOR`. `SchemaMetadataChangeEventGenerator.java:378-493` (MAJOR at :391, :455; MINOR at :422) | **2** — hook writes `ENTITY_UPDATE` to audit. No alert or severity. `EntityAuditEventV2.java:260-266` |
| D10.4 Pipeline runs | **3** — task-level status/time/error history; `matchPipelineState` alerts. `entity/data/pipeline.json:12-27,44-102,168-233`, `AlertsRuleEvaluator.java:377-440` | **3** — `DataProcessInstanceRunEvent` SUCCESS/FAILURE/SKIPPED/UP_FOR_RETRY plus duration. `dataprocess/DataProcessInstanceRunEvent.pdl:11-43` | **1** — `Process` has no status; failed Hive queries are never recorded. `0010-base_model.json:74-83` |
| D10.5 Usage | **3** — query-log usage for 17 sources with percentiles; web analytics per user/team. `type/usageDetails.json` | **3** — query counts, users, column counts, TopUsers. `dataset/DatasetUsageStatistics.pdl:16-49` | **0** — none in code or docs |
| D10.6 Cost | **3** — Snowflake/BigQuery cost per query, top-10 costly query groups, used/unused storage report. `entity/data/queryCostRecord.json:28-51`, `QueryCostResource.java:113-131` | **1** — raw `sizeInBytes` only. `DatasetProfile.pdl:39-43` | **1** — raw `fileSize`. `1020-fs_model.json:44` |

## Reading the scores for an SLA programme

| Need | What the measured products give | What you add |
|---|---|---|
| Rules without code | OM UI templates (3). DH and Atlas: code only | GX/dbt in the repo, results pushed in |
| Stop bad data | Nobody (D5.3 ≤ 2) | Orchestrator gate |
| Freshness SLA | DH shows dbt freshness (2). OM shows a timestamp (1) | Freshness sensor + alert |
| Incident with deadline | Status + assignee (2) in OM and DH. No timer anywhere | Due dates + escalation job |
| Quality score | OM per dimension (3). DH pass/fail health (1) | Criticality weighting in reporting |
| Anomaly detection | OM flag only (1, may be higher). DH Cloud only | Same-weekday baselines; a statistical job |
| Breaking schema change | DH classifies MAJOR/MINOR. OM alerts on column changes | Contract check in CI (see `data-contracts`) |

## Items checked beyond the research

- OM: exactly 25 files in `json/data/tests/`. None is a freshness test.
- OM incident severity classifier: features, synthetic-data note and no-tier → no rating
  (above). The research said only "severity is classified automatically".
- OM's data contract has an `slaValidation.json` result schema (`refreshFrequencyMet`,
  `latencyMet`, `availabilityMet`). No service code fills it, and `validateContract` checks only
  schema, semantics and quality tests (`DataContractRepository.java:1031-1075`). This confirms
  the research's D10.1 finding.
- OM 1.13.3 also ships dimensional test results with an impact score
  (`ingestion/src/metadata/data_quality/validations/impact_score.py`:
  `failure_rate² × volume_factor × sample_weight / 1.5`). The research did not score this.
- DH incident `priority` is an int documented as `0 CRITICAL … 3 LOW` (`IncidentInfo.pdl:70-78`).
