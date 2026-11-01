# Sources

## Research

Internal evidence-based capability research comparing open-source data catalogs against a frozen
rubric (14 domains, 90 capabilities, scores 0-4). Research text is Turkish; this skill
translates and generalises it. Organisation-specific context, sector examples and closed-source
vendor assessments are omitted. Files used (paths relative to the research repository):

| File | Used for |
|---|---|
| `research/rubric/parts/D6-D10.yaml`, `research/rubric/capabilities.yaml` | D6.1-D6.8 names, definitions, rationale, 0-4 level definitions; D9.4 levels |
| `research/rubric/README.md`, `research/rubric/sources.yaml` | Scoring rules (`null` ≠ 0), rubric sources (DMBOK2 KA5, ISO/IEC 38505-1, ISO/IEC 20889) |
| `research/evidence/openmetadata.yaml`, `datahub.yaml`, `atlas.yaml` (D6 entries; D9.4) | Scores, evidence paths, notes |
| `research/evidence/versions.yaml` | Product versions and commits |
| `research/analysis/tables/T2-tam-matris.md` | Full score matrix (D6, D7.3, D9.4, D11.4) |
| `research/analysis/tables/T3-mimari.md` | Atlas optional Ranger/external authorisation dependency |
| `research/analysis/gaps.md` §2 (class 1: D6.3, D6.4, D6.8), §3 (class 2: D6.7, D9.4), §4 (D6.5), §4.3 (catalog-Ranger policy link), §7.3(a) (Policy Agent) | Cross-product findings, "no catalog in the data path", bridge gap |
| `research/analysis/novelty.md` (candidate C01) | Forward vs reverse direction of the catalog-enforcer bridge; Atlas tagsync, RANGER-4978, commercial one-way sink |
| `report/OpenMetadata-Bosluk-Rehberi.md` (D6.3, D6.4, class 4.3) | Three-direction bridge and difficulty ordering, wrong vs right expectations of a catalog |

## Products and commits

| Product | Version / tag | Commit |
|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` |
| DataHub | `v1.7.0` | `7f81ccbfe27b` |
| Apache Atlas | `release-2.5.0` | `234608315f29` |

## Code locations cited (repo-relative)

**OpenMetadata**
- `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`; `ingestion/src/metadata/pii/algorithms/tags.py:19-111`
- `openmetadata-service/src/main/resources/json/data/tags/piiTagsWithRecognizers.json:6-10,23,112`
- `openmetadata-spec/src/main/resources/json/schema/type/recognizerFeedback.json:21-105`
- `openmetadata-service/src/main/java/org/openmetadata/service/security/mask/PIIMasker.java:45-48,54-120`
- `openmetadata-service/src/main/java/org/openmetadata/service/security/DefaultAuthorizer.java:190-193`
- `openmetadata-service/src/main/java/org/openmetadata/service/security/policyevaluator/RuleEvaluator.java:54-405`, `PolicyEvaluator.java:47-67`
- `openmetadata-service/src/main/java/org/openmetadata/service/resources/permissions/PermissionsResource.java:296-345`
- `openmetadata-spec/src/main/resources/json/schema/entity/policies/accessControl/resourceDescriptor.json:13-71`
- `openmetadata-spec/src/main/resources/json/schema/entity/data/dataContract.json:94`, `entity/data/table.json:1210-1213`
- `openmetadata-spec/src/main/resources/json/schema/entity/services/securityService.json:20-25`,
  `entity/services/connections/security/rangerConnection.json`,
  `metadataIngestion/securityServiceMetadataPipeline.json`
- `ingestion/src/metadata/ingestion/source/security/security_service.py:68`
- `openmetadata-spec/src/main/resources/json/schema/entity/applications/configuration/external/automator/lineagePropagationAction.json:1-114`
- `conf/openmetadata.yaml:330-342`; `openmetadata-service/src/main/java/org/openmetadata/service/security/Authorizer.java:24-56`
- `bootstrap/sql/migrations/native/1.12.0/mysql/schemaChanges.sql:9-32`; `openmetadata-service/.../resources/audit/AuditLogResource.java:51,172-192`

**DataHub**
- `metadata-ingestion/src/datahub/ingestion/glossary/classifier_registry.py:1-21`
- `metadata-ingestion/src/datahub/ingestion/source/ge_profiling_config.py:99`
- `metadata-auth/auth-api/src/main/java/com/datahub/authorization/EntityFieldType.java:4-38`
- `metadata-service/factories/src/main/java/com/linkedin/gms/factory/auth/AuthorizerChainFactory.java:78-99`;
  `metadata-service/auth-impl/src/main/java/com/datahub/authorization/AuthorizerChain.java:88-91`
- `datahub-actions/src/datahub_actions/plugin/action/snowflake/snowflake_util.py:80-116`
- `datahub-actions/src/datahub_actions/plugin/action/tag/tag_propagation_action.py:139-150`, `.../tag/README.md`
- `metadata-models/src/main/pegasus/com/linkedin/structured/StructuredPropertyDefinition.pdl:36-64`
- `metadata-service/openapi-servlet/src/main/java/io/datahubproject/openapi/v1/event/ExternalEventsController.java:204-253`

**Apache Atlas**
- `authorization/src/main/java/org/apache/atlas/authorize/AtlasAuthorizerFactory.java:32-86`
- `authorization/src/main/java/org/apache/atlas/authorize/simple/AtlasSimpleAuthzPolicy.java:204-297`, `AtlasSimpleAuthorizer.java:80,256-283`
- `authorization/src/main/java/org/apache/atlas/authorize/AtlasAuthorizer.java:77-99`
- `intg/src/main/java/org/apache/atlas/model/instance/AtlasClassification.java:56-58`;
  `intg/src/main/java/org/apache/atlas/model/typedef/AtlasRelationshipDef.java:373-375`
- `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/tasks/ClassificationPropagateTaskFactory.java:37-80`
- `repository/src/main/java/org/apache/atlas/repository/audit/HBaseBasedAuditRepository.java:555-631`
- `intg/src/main/java/org/apache/atlas/model/typedef/AtlasBusinessMetadataDef.java:41-46`

## Issues, PRs and documentation (İ / D)

- OpenMetadata PR #27396 "Policy agent feature implementation" (merged to `main`, migration targets 2.0.0); PR #28595 (masked access); issue #8271 "Ability to push Open Metadata tags to Apache Ranger" (closed 2022).
- apache/ranger PR #544 and JIRA RANGER-4978 (OpenMetadata tag source for Ranger tagsync; PR closed March 2026, JIRA open, no fix version).
- Apache Ranger wiki, "Tag Based Policies": https://cwiki.apache.org/confluence/display/RANGER/Tag+Based+Policies
- DataHub, "Configuring Authorization with Apache Ranger": https://docs.datahub.com/docs/how/configuring-authorization-with-apache-ranger
- DataHub, authorisation policies: https://docs.datahub.com/docs/authorization/policies
- OpenMetadata data governance guide: https://docs.open-metadata.org/latest/how-to-guides/data-governance
- Commercial OpenMetadata distribution, Ranger connector page (one-way, tags-only sync): https://docs.getcollate.io/connectors/security/ranger

## Standards and legal texts

- DAMA-DMBOK2, Knowledge Area 5 "Data Security" (rubric anchor for D6).
- ISO/IEC 38505-1 (governance of data) — rubric source for D6.2.
- ISO/IEC 20889 (privacy-enhancing de-identification) — rubric source for D6.8.
- NIST SP 800-162, *Guide to Attribute Based Access Control (ABAC) Definition and Considerations*;
  OASIS XACML 3.0 (PAP/PDP/PEP/PIP vocabulary). Added for this skill, not from the research.
- Regulation (EU) 2016/679 (GDPR) Art. 4(5) pseudonymisation, Art. 32 security of processing.
  Added for this skill.
- Turkish Law No. 6698 (KVKK) Art. 12/1 (data security measures); KVKK Board Decision 2018/10
  (adequate measures for special-category data: scope and duration of access, periodic
  authorisation checks).

## Items checked for this skill beyond the research

- OpenMetadata `PIITag` enum has **38** entity types (`tags.py:25-86`) and the shipped `PII`
  classification carries **43** default recognizers (39 under `Sensitive`, 4 under
  `NonSensitive`); the research summary says "41 PII types".
- OpenMetadata maps `DATE_TIME`, `NRP`, `LOCATION`, `PHONE_NUMBER`, `URL` to `NonSensitive` by
  default (`tags.py:95-111`), so its sample masker (keyed to `PII.Sensitive`) does not mask them.
- OpenMetadata's unmasked sample view is limited to admins, bots and owners (`DefaultAuthorizer.java:190-193`), not policy-driven.
- OpenMetadata ships a `LineagePropagationAction` schema under `external/automator/`; no executor
  in the OSS repo (treated as not verified).
- DataHub `tag_propagation` action propagates to downstream **datasets** only, with prefix filter
  and removal handling.
- Atlas classification propagation: per-classification `propagate`, relationship direction enum,
  `removePropagationsOnEntityDelete`, `validityPeriods`.
- The research report's competitor table lists DataHub D6.7 = 1; the evidence file and full
  matrix say 2 (short-circuit on first ALLOW). This skill uses 2.
- Ranger policy JSON and engine masking syntax in `policy-enforcement.md` are illustrative and were
  not verified against Ranger or warehouse source code.
