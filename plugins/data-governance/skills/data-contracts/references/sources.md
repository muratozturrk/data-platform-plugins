# Sources

## Research files used (paths inside the research repository)

| File | Used for |
|---|---|
| `research/rubric/capabilities.yaml` (built from `research/rubric/parts/D1-D5.yaml`, `research/rubric/parts/D6-D10.yaml`) | Level texts for D1.3, D1.4, D1.6, D7.1, D7.2, D9.1–D9.6 |
| `research/rubric/README.md` | Scoring guide, evidence types, "absent is a weak claim" rule |
| `research/rubric/sources.yaml` | Rubric source records (below) |
| `research/evidence/openmetadata.yaml` | OM scores and evidence for D1.3/1.4/1.5/1.6, D7.1/7.2, D9.x, D11.6 |
| `research/evidence/datahub.yaml` | DH scores and evidence for the same capabilities |
| `research/evidence/atlas.yaml` | Atlas scores and evidence for the same capabilities |
| `research/evidence/versions.yaml` | Product versions and commits |
| `research/analysis/tables/T2-tam-matris.md` | Full capability matrix |
| `research/analysis/tables/T7-openmetadata-bosluklar.md` | `dataContract` free-text consumer groups, `rowFilters` documentation-only, `termsOfUse` |
| `research/analysis/gaps.md` (§2 class 1, §7 issue/roadmap check) | D9.4/D9.6 roadmap status, DataHub RFC #17242 |

The research has no dedicated data-contract study. This skill combines the rubric's D9.3
measurement with the capabilities a contract programme depends on. Everything without a tag
(compatibility modes, semver policy, deprecation windows, CI outline, YAML template) is general
practice. Organisation-specific material and closed-source vendor assessments were left out.

## Products inspected

| Product | Tag | Commit | Inspected |
|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` | 2026-08-11 |
| DataHub | `v1.7.0` | `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80` | 2026-08-11 |
| Apache Atlas | `release-2.5.0` | `234608315f29b643c2dd7632cad0e8ef2a15c696` | 2026-08-11 |

## Source files verified for this skill (product repo-relative)

OpenMetadata:
- `openmetadata-spec/src/main/resources/json/schema/entity/data/dataContract.json:11-27,57-127,128-221,222-234,285-422`
- `openmetadata-spec/src/main/resources/json/schema/type/status.json:8-16`, `type/contractExecutionStatus.json`
- `openmetadata-spec/src/main/resources/json/schema/entity/datacontract/odcs/odcsDataContract.json:5-14`
- `openmetadata-spec/src/main/resources/json/schema/entity/datacontract/slaValidation.json`
- `openmetadata-service/src/main/java/org/openmetadata/service/jdbi3/DataContractRepository.java:186-189,415-463,1031-1075,1228-1237,1602-1612`
- `openmetadata-service/src/main/java/org/openmetadata/service/resources/data/DataContractResource.java:940,985,1357,1384`
- `openmetadata-service/src/main/java/org/openmetadata/service/apps/bundles/dataContracts/DataContractValidationApp.java:36-130`
- `openmetadata-service/src/main/java/org/openmetadata/service/rules/RuleEngine.java` (JsonLogic)
- `openmetadata-service/src/main/java/org/openmetadata/service/jdbi3/EntityRepository.java:8963-8970,9724,9767-9806`
- `openmetadata-service/src/main/java/org/openmetadata/service/util/EntityUtil.java:138-142,787-793`
- `openmetadata-spec/src/main/resources/json/schema/entity/domains/dataProduct.json:11-133,201-228`
- `openmetadata-spec/src/main/resources/json/schema/api/domains/dataProductPortsView.json:36-45`
- `openmetadata-service/src/main/java/org/openmetadata/service/resources/domains/DataProductResource.java:452-734,1144-1300`
- `openmetadata-service/src/main/resources/json/data/governance/workflows/` (two default workflows)
- `openmetadata-spec/src/main/resources/json/schema/events/eventSubscription.json:164-174`

DataHub:
- `metadata-models/src/main/pegasus/com/linkedin/datacontract/DataContractProperties.pdl:11-59`, `DataContractStatus.pdl:21,25`
- `metadata-ingestion/src/datahub/cli/specific/datacontract_cli.py:21-28`
- `metadata-io/src/main/java/com/linkedin/metadata/timeline/eventgenerator/SchemaMetadataChangeEventGenerator.java:41-44,391,422,455,471-485,539,564`
- `metadata-models/src/main/pegasus/com/linkedin/dataproduct/DataProductProperties.pdl:11-45`, `DataProductAssociation.pdl:13`

Apache Atlas:
- `addons/models/1000-Hadoop/1065-avro_model.json:29-46`
- `intg/src/main/java/org/apache/atlas/model/audit/EntityAuditEventV2.java:261`
- `intg/src/main/java/org/apache/atlas/model/notification/EntityNotification.java:210-211`

Other paths in the references are carried over from the research evidence files unchanged.

## Frameworks (rubric sources)

| Ref | Source |
|---|---|
| datamesh-governance | Z. Dehghani, "Data Mesh Principles and Logical Architecture", martinfowler.com, 2020, https://martinfowler.com/articles/data-mesh-principles.html |
| dmbok2-ka1 / ka6 / ka10 | DAMA-DMBOK2 knowledge areas Data Governance, Data Integration and Interoperability, Metadata (context diagram summaries) |
| dcam | EDM Council, DCAM, https://edmcouncil.org/frameworks/dcam/ |
| akad-katalog-metaveri | J.M. Hellerstein, V. Sreekanti, J.E. Gonzalez et al., "Ground: A Data Context Service", CIDR 2017 |
| openlineage-spec | OpenLineage specification (D1.6 source) |
| delta-sharing-spec, iso-38505-1 | D9.6 sources |

## Standards and general practice (no evidence tag)

- **Open Data Contract Standard (ODCS)**, Bitol project (Linux Foundation AI & Data), v3.x. This
  is the layout of the illustrative contract. OpenMetadata 1.13.3 supports import/export of
  v3.1.0 down to v2.2.0 (K, above).
- **Open Data Product Standard (ODPS)** v4.1. OpenMetadata's `dataProductType` taxonomy is
  aligned to it (K).
- **Semantic Versioning 2.0.0**, https://semver.org.
- Compatibility modes `BACKWARD`, `FORWARD`, `FULL`, `*_TRANSITIVE`, `NONE` follow the
  terminology of schema registries for event streams (for example Confluent Schema Registry
  documentation).
- Deprecation windows, the change-classification table, consumer pinning and the CI gate are
  common practice and examples, not research findings.
