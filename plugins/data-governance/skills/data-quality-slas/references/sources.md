# Sources

## Research files used (paths inside the research repository)

| File | Used for |
|---|---|
| `research/rubric/capabilities.yaml` (built from `research/rubric/parts/D1-D5.yaml`, `research/rubric/parts/D6-D10.yaml`) | D5.1–D5.7 and D10.1–D10.6 definitions and 0–4 level texts |
| `research/rubric/README.md` | Scoring guide, monotonic levels, "absent is a weak claim" rule, evidence types |
| `research/rubric/sources.yaml` | Rubric source records (below) |
| `research/evidence/openmetadata.yaml` | OM scores, evidence paths, notes for D5, D10 |
| `research/evidence/datahub.yaml` | DH scores, evidence paths, notes for D5, D10 |
| `research/evidence/atlas.yaml` | Atlas scores, evidence paths, notes for D5, D10 |
| `research/evidence/versions.yaml` | Product versions and commits |
| `research/analysis/tables/T2-tam-matris.md` | Full capability matrix |
| `research/analysis/gaps.md` (§4 class 3 "present but weak", §7.2–7.3 issue/roadmap check, §8 OSS gate) | D5.3/D5.4/D10.1 gap wording, DataHub Cloud gating, D5.6 caveat |

Organisation-specific requirements, sector examples and closed-source vendor assessments in the
research were left out.

## Products inspected

| Product | Tag | Commit | Inspected |
|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` | 2026-08-11 |
| DataHub | `v1.7.0` | `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80` | 2026-08-11 |
| Apache Atlas | `release-2.5.0` | `234608315f29b643c2dd7632cad0e8ef2a15c696` | 2026-08-11 |

## Source files re-verified for this skill (product repo-relative)

OpenMetadata:
- `openmetadata-service/src/main/resources/json/data/tests/` (25 test definitions, no freshness test)
- `bootstrap/sql/migrations/native/1.12.0/postgres/postDataMigrationSQLScript.sql:13-28` (`tableDataToBeFresh`)
- `openmetadata-spec/src/main/resources/json/schema/tests/testCaseResolutionStatus.json:14,26`
- `openmetadata-spec/src/main/resources/json/schema/tests/resolved.json:13`
- `openmetadata-spec/src/main/resources/json/schema/tests/testDefinition.json:108-120`
- `openmetadata-spec/src/main/resources/json/schema/tests/testCase.json:150-153`
- `openmetadata-service/src/main/java/org/openmetadata/service/util/incidentSeverityClassifier/LogisticRegressionIncidentSeverityClassifier.java:21-27,98-105`
- `openmetadata-service/src/main/java/org/openmetadata/service/resources/dqtests/TestSuiteResource.java:506`
- `ingestion/src/metadata/great_expectations/action.py:107-118`, `ingestion/src/metadata/great_expectations/action1xx.py:85-92`
- `ingestion/src/metadata/ingestion/source/database/dbt/metadata.py:1601-1770`
- `openmetadata-spec/src/main/resources/json/schema/entity/services/ingestionPipelines/ingestionPipeline.json:14`
- `openmetadata-service/src/main/resources/json/data/EntityObservabilityFilterDescriptor.json:54-61`
- `openmetadata-spec/src/main/resources/json/schema/events/eventSubscription.json:164-174`
- `openmetadata-spec/src/main/resources/json/schema/entity/datacontract/slaValidation.json`
- `openmetadata-service/src/main/java/org/openmetadata/service/jdbi3/DataContractRepository.java:1031-1075`
- `ingestion/src/metadata/data_quality/validations/impact_score.py`

DataHub:
- `metadata-models/src/main/pegasus/com/linkedin/assertion/AssertionSource.pdl:17-32`
- `metadata-models/src/main/pegasus/com/linkedin/incident/IncidentInfo.pdl:70-93`, `IncidentStatus.pdl:19-57`
- `metadata-io/src/main/java/com/linkedin/metadata/timeline/eventgenerator/SchemaMetadataChangeEventGenerator.java:391,422,455`
- `metadata-ingestion-modules/gx-plugin/src/datahub_gx_plugin/common.py:190-206`, `action.py:89-104`
- `metadata-ingestion/src/datahub/ingestion/source/dbt/dbt_core.py:354`

Apache Atlas:
- `addons/models/1000-Hadoop/1065-avro_model.json:29-46`, `intg/src/main/java/org/apache/atlas/model/audit/EntityAuditEventV2.java:261`

Other paths in [d5-d10-comparison.md](d5-d10-comparison.md) are carried over from the research
evidence files unchanged.

## Frameworks and literature (rubric sources)

| Ref | Source |
|---|---|
| dmbok2-ka11 | DAMA-DMBOK2, Data Quality knowledge area (Figure 91 context diagram summary, DAMA Rocky Mountain Chapter), https://damarmc.org/news/13443040 |
| dmbok2-ka4 | DAMA-DMBOK2, Data Storage and Operations knowledge area (context diagram summary) |
| iso-8000 | ISO 8000-61:2016 Data quality — Part 61: Data quality management: Process reference model, https://www.iso.org/standard/63086.html |
| dcam | EDM Council, DCAM — Data Management Capability Assessment Model, https://edmcouncil.org/frameworks/dcam/ |
| akad-veri-kalitesi-boyutlari | R.Y. Wang, D.M. Strong, "Beyond Accuracy: What Data Quality Means to Data Consumers", *Journal of Management Information Systems* 12(4):5–33, 1996 |
| finops-framework | FinOps Foundation framework (D10.6 source) |

## General practice (no evidence tag)

SLI/SLO/SLA terms, error budgets and error-budget policy follow the site reliability engineering
literature (B. Beyer, C. Jones, J. Petoff, N.R. Murphy (eds.), *Site Reliability Engineering*,
O'Reilly, 2016, chapter "Service Level Objectives"). Check tiers, write-audit-publish, the severity
matrix and the response targets are common industry practice. They are examples, not research
findings.
