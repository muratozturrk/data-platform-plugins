# Sources

## Research

- Internal evidence-based capability research on OpenMetadata `1.13.3-release` (commit
  `255f6694913b84797064a42859cda3f2a3425dc6`), inspected 2026-08-11: D14 rubric (11 KVKK
  capabilities, 0–4 level definitions), legislation → capability mapping (49 obligations), KVKK
  compliance matrix with weakest-link derivation, D6 privacy/security and D14 evidence entries,
  gap catalog (class 5 "KVKK and regulatory gaps") and the gap-closing guide's KVKK section.
  Organisation-specific context, sector examples and closed-source vendor assessments omitted.
- Comparison products in the same research: DataHub `v1.7.0` (`7f81ccbfe27b`), Apache Atlas
  `release-2.5.0` (`234608315f29`).
- Evidence types: **K** = verified in source code, **D** = official documentation, **İ** =
  issue/RFC/roadmap, **B** = vendor claim. All D14 scores in this skill are type K.

## Legal texts (each article number and duration verified against the official text)

| Ref | Instrument | Official source |
|---|---|---|
| kvkk-6698 | Law No. 6698 on the Protection of Personal Data, consolidated (incl. Law No. 7499, 2/3/2024: Art. 6/2 repealed, 6/3 amended, Art. 9 replaced) | mevzuat.gov.tr consolidated PDF |
| kvkk-verbis-yon | Regulation on the Data Controllers' Registry (VERBİS) | Official Gazette 30/12/2017-30286 (+ amendment 28/4/2019-30758) |
| kvkk-imha-yon | Regulation on Erasure, Destruction or Anonymisation of Personal Data | Official Gazette 28/10/2017-30224 (+ mevzuat.gov.tr consolidated text) |
| kvkk-aydinlatma-teblig | Communiqué on Procedures and Principles for Fulfilling the Obligation to Inform | Official Gazette 10/3/2018-30356 |
| kvkk-basvuru-teblig | Communiqué on Procedures and Principles for Applications to the Data Controller | Official Gazette 10/3/2018-30356 |
| kvkk-ihlal-karar | Board Decision 24.01.2019 / 2019/10 (data breach notification, 72 hours) | kvkk.gov.tr |
| kvkk-ihlal-icerik-karar | Board Decision 18.09.2019 / 2019/271 (minimum content of notice to data subjects) | kvkk.gov.tr |
| kvkk-ozel-nitelikli-karar | Board Decision 31.01.2018 / 2018/10 (adequate measures for special-category data) | kvkk.gov.tr |
| kvkk-yurtdisi-yon | Regulation on Procedures and Principles for Transfer of Personal Data Abroad | Official Gazette 10/7/2024-32598 |
| btig-genelge | Presidential Circular No. 2019/12 on Information and Communication Security Measures | Official Gazette 6/7/2019-30823 (mevzuat.gov.tr) |
| kvkk-kamu-rehber | KVKK Compliance Guide for Public Institutions and Public Professional Organisations (KVKK Publications No. 114, March 2026; §2.3, §3.3.1, ch. 6 on Art. 28) | https://www.kvkk.gov.tr/Icerik/8833/kamu-kurumlari-icin-kisisel-verilerin-korunmasi-kanununa-uyum-rehberi |
| kvkk-kamu-yayim-karar | Board Principle Decision 01/07/2026, 2026/1301, on public-law data controllers sharing personal data on the internet (published 27 July 2026) | https://www.kvkk.gov.tr/Icerik/8835/kamu-tuzel-kisiligini-haiz-veri-sorumlulari-tarafindan-kisisel-verilerin-internet-ortaminda-paylasilmasi-hakkinda-kisisel-verileri-koruma-kurulunun-01-07-2026-tarihli-ve-2026-1301-sayili-ilke-kararina-iliskin-kamuoyu-duyurusu |

Not used: the Information and Communication Security Guide itself (the research could not reach its
official host); only the Circular that mandates it was verified.

## OpenMetadata source files cited (repo-relative, tag `1.13.3-release`)

- `openmetadata-spec/src/main/resources/json/schema/type/customProperty.json:1-71`,
  `type/customProperties/enumConfig.json:9-19`, `type/customProperties/complexTypes.json:136-157`,
  `type/basic.json:27-157,242-305`, `type/entityHistory.json:9-104`
- `openmetadata-spec/src/main/resources/json/schema/entity/data/table.json:374,1210-1213,1266`,
  `entity/data/dataContract.json:57-127,222-234`, `entity/domains/dataProduct.json:36-41`,
  `entity/feed/thread.json:12-21,49-101`, `entity/docStore/document.json`,
  `entity/policies/policy.json:18-119`, `entity/policies/accessControl/resourceDescriptor.json:13-71`,
  `entity/teams/team.json`, `entity/services/databaseService.json` (no `extension` field)
- `openmetadata-spec/src/main/resources/json/schema/configuration/entityRulesSettings.json`,
  `governance/workflows/elements/nodes/automatedTask/checkEntityAttributesTask.json`,
  `security/client/oidcClientConfig.json:92`, `events/eventSubscription.json:98-200`
- `openmetadata-spec/src/main/resources/elasticsearch/en/column_index_mapping.json`
- `openmetadata-service/src/main/resources/json/data/tags/personalDataTags.json`,
  `json/data/tags/piiTagsWithRecognizers.json:1-12`,
  `json/data/entity/entityCsvDocumentation.json:5-183`,
  `json/data/governance/workflows/GlossaryApprovalWorkflow.json:58-206`
- `openmetadata-service/src/main/java/org/openmetadata/service/`: `security/mask/PIIMasker.java:45-48,54-120`,
  `security/policyevaluator/PolicyEvaluator.java:44-79`, `security/policyevaluator/RuleEvaluator.java:55-398`,
  `apps/bundles/dataRetention/DataRetention.java:139-206`, `jdbi3/TypeRepository.java:164-192`,
  `jdbi3/EntityRepository.java:2408-2507`, `resources/types/TypeResource.java:78-85,503-528`,
  `resources/lineage/LineageResource.java:190-259,402-486`,
  `resources/databases/DatabaseSchemaResource.java:589`, `util/EntityUtil.java:1236-1287`
- `ingestion/src/metadata/pii/scanners/ner_scanner.py:57-79`, `ingestion/src/metadata/pii/algorithms/tags.py:25-111`
- `conf/openmetadata.yaml:26` (API root `/api/*`)

## Items checked for this skill beyond the research

- `dataProduct.visibility` has five values (`PRIVATE`, `INVITATION`, `ORGANISATION`, `DATASPACE`,
  `PUBLIC`, default `PRIVATE`); the research mentions only `PUBLIC`/`DATASPACE`.
- `team.json`, `user.json`, `databaseService.json`, `storageService.json`, `policy.json`,
  `classification.json` and `docStore/document.json` have no `extension` field, so they cannot carry
  custom properties; `database`, `databaseSchema`, `container`, `glossaryTerm`, `dataProduct`,
  `dataContract` and table columns can.
- `PersonalData` classification is `mutuallyExclusive: true`; the `PII` classification ships with an
  `autoClassificationConfig` (`minimumConfidence` 0.6).
- Thread task types include a `Generic` type; it still has no due-date field.
- The research's "schedulers run ingestion only / no scheduled rule engine" is too strong: a
  cron-scheduled governance workflow trigger exists (`periodicBatchEntityTrigger.json:12-75`,
  `PeriodicBatchEntityTrigger.java:114-124`). What stays true is that tasks and approvals have no
  per-record due date, timeout or escalation.
