# Sources

## Research files used (paths inside the research repository)

- `research/rubric/README.md` — rubric purpose, freeze rule, 0–4 scoring guide, level-writing
  rules, evidence types, "absence is a weak claim" rule.
- `research/rubric/capabilities.yaml`, `research/rubric/parts/D1-D5.yaml`,
  `research/rubric/parts/D11-D13.yaml` — level definitions for D1, D2, D3, D11, D12 (frozen rubric:
  14 domains, 90 capabilities).
- `research/rubric/sources.yaml` — framework, standard and paper references below.
- `research/evidence/openmetadata.yaml`, `research/evidence/datahub.yaml`,
  `research/evidence/atlas.yaml` — per-capability score, evidence type, `path:line`, note.
- `research/evidence/versions.yaml` — releases and commits inspected.
- `research/analysis/tables/T2-tam-matris.md` — full capability matrix.
- `research/analysis/tables/T3-mimari.md` — architecture, dependencies, deployment, licence,
  governance, open-source vs cloud split, Neo4j finding.
- `research/analysis/tables/T4-connector.md` — 13-component connector classification.
- `research/analysis/tables/T5-topluluk.md` — community and sustainability indicators
  (GitHub REST API and Apache JIRA, pulled 2026-08-11).
- `research/analysis/crosscheck.md` — manual verification of the D11.4 spread and Atlas zeros.

A closed-source product assessed in the same research is omitted entirely.

## Products inspected

| Product | Repository | Tag | Commit |
|---|---|---|---|
| OpenMetadata | https://github.com/open-metadata/OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` |
| DataHub | https://github.com/datahub-project/datahub | `v1.7.0` | `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80` |
| Apache Atlas | https://github.com/apache/atlas (ASF mirror) | `release-2.5.0` | `234608315f29b643c2dd7632cad0e8ef2a15c696` |

Evidence types: **K** = source code (`path:line`), **D** = official documentation, **İ** =
issue/RFC/roadmap, **B** = vendor claim.

## Frameworks, standards and papers

| Ref | Source | Used for |
|---|---|---|
| dmbok2-ka10 | DAMA-DMBOK2, KA10 Metadata (context diagram summary, DAMA RMC, https://damarmc.org/news/13430245) | D1, D2 |
| dmbok2-ka6 | DAMA-DMBOK2, KA6 Data Integration and Interoperability (https://damarmc.org/news/13381350) | D3, D11 |
| dmbok2-ka2 / ka4 | DAMA-DMBOK2, Data Architecture; Data Storage and Operations | D12 |
| dcam | EDM Council, DCAM — Data Management Capability Assessment Model (https://edmcouncil.org/frameworks/dcam/) | D1.2, D1.5 |
| iso-11179 | ISO/IEC 11179-3:2023, Metadata registries — Part 3 (https://www.iso.org/standard/78915.html) | D1.7 |
| openlineage-spec | OpenLineage Object Model (Core Specification), LF AI & Data (https://openlineage.io/docs/spec/object-model) | D1.6, D3.5 |
| akad-soyagaci-cikarim | M.H. Namaki, A. Floratou, F. Psallidas et al., "Vamsa: Automated Provenance Tracking in Data Science Scripts", KDD 2020 (https://arxiv.org/abs/2001.01861) | D3.1–D3.3 |
| akad-katalog-metaveri | J.M. Hellerstein, V. Sreekanti, J.E. Gonzalez et al., "Ground: A Data Context Service", CIDR 2017 | D1.1, D1.3, D2, D3.4 |
| akad-llm-metaveri-zenginlestirme | J. Giner-Miguelez, A. Gómez, J. Cabot, "Using Large Language Models to Enrich the Documentation of Datasets for Machine Learning", arXiv 2404.15320 (2024) | D2.3, D2.4 |
| akad-veri-kesif-kisisellestirme | Bianchini, De Antonellis, Garda, "A semantics-enabled approach for personalised Data Lake exploration", Knowledge and Information Systems 66 (2023) | D2.5 |
| ATLAS-4550 | Apache JIRA, OpenLineage ingest request (https://issues.apache.org/jira/browse/ATLAS-4550) | Atlas D3.5 = 0 |

## Claims re-checked against the cloned repositories for this skill

All matched the research unless noted.

- OM lineage edge `source` enum (11 values) — `openmetadata-spec/src/main/resources/json/schema/type/entityLineage.json:70`.
- OM `columnLineage.function` exists (`entityLineage.json:38-41`) but built-in extractors build
  `ColumnLineage(fromColumns=…, toColumn=…)` without it (`ingestion/src/metadata/ingestion/lineage/sql_lineage.py:644`,
  `ingestion/src/metadata/ingestion/source/dashboard/dashboard_service.py:764`).
- OM 25 SQL dialects — `ingestion/src/metadata/ingestion/lineage/models.py:106-135`.
- OM OpenLineage consumer endpoints — `OpenLineageResource.java:54,104-105,181-182`; Kafka/Kinesis
  connector `openLineageConnection.json:17,110`; SDK helper only posts into OM
  (`openmetadata-sdk/src/main/java/org/openmetadata/sdk/fluent/OpenLineage.java`); no producer found.
- OM lineage query/export endpoints — `LineageResource.java:190-214,402-438`.
- OM subscription types Webhook/Slack/MsTeams/GChat/Email and statuses incl. `retryLimitReached`
  — `eventSubscription.json:101,182`; failed-event handling `AbstractEventConsumer.java:144-167`.
- DH `transformOperation` and `confidenceScore` — `FineGrainedLineage.pdl:44,49`; aggregator
  writes parser confidence at `sql_parsing_aggregator.py:973,1058,1265` and `1.0` at `:784,844`.
- DH OpenLineage endpoint — `LineageApiImpl.java:31,60-61`; no OpenLineage emitter in the repo.
- DH Spark agent built on OpenLineage's Spark `EventEmitter` — `DatahubEventEmitter.java:36-38`
  (detail added here, not in the research).
- DH graph default Elasticsearch — `application.yaml:377,1172`; Helm prerequisites Neo4j —
  `docs/deploy/kubernetes.md:46-55`.
- DH 116 source plugins — `metadata-ingestion/setup.py:1128-1245`.
- Atlas: zero OpenLineage/Marquez matches across `*.java`, `*.json`, `*.xml`, `*.md`.
- Atlas Hive column lineage expression — `CreateHiveProcess.java:238-239`; Trino extractor emits
  no column lineage although `trino_column_lineage` is typed (`6000-trino_model.json:58`).
- Atlas Kafka topics `ATLAS_HOOK`, `ATLAS_ENTITIES` — `distro/src/conf/atlas-application.properties:109`.
- Atlas lineage REST parameters `direction`, `depth` — `LineageREST.java:70,100`.

Score review (2026-11-04): Atlas D3.3 was lowered from 3 to 1 because Atlas has no general SQL
parser, which the rubric's level 2 requires (monotonicity rule); the comparison footnote explains
the cell.
