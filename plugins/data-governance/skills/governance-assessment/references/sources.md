# Sources

## Research files used (paths inside the research repository)

| File | Used for |
|---|---|
| `research/rubric/capabilities.yaml` | Frozen rubric v1.0 (`frozen: true`, `frozen_at: 2026-08-11`): 14 domains, 90 capabilities, levels, `sources` per capability |
| `research/rubric/parts/D1-D5.yaml`, `parts/D6-D10.yaml`, `parts/D11-D13.yaml`, `parts/D14.yaml` | Domain sources the merged rubric is generated from |
| `research/rubric/README.md` | Scale, level-writing rules, evidence types, "absence is a weak claim", D14 double-counting rule and Art. 28 note, known limitations |
| `research/rubric/CHANGELOG.md` | Freeze record, pre-freeze expert and legal reviews |
| `research/rubric/sources.yaml` | 48-source registry (`dmbok2-ka1` ... `dmbok2-ka11` and the rest) |
| `research/rubric/kvkk-mevzuat.md` | D14 legislation → capability mapping (referenced, not reproduced) |
| `research/evidence/openmetadata.yaml`, `datahub.yaml`, `atlas.yaml`, `versions.yaml` | Scores, evidence and version pins; all aggregates in this skill recomputed from these |
| `research/analysis/crosscheck.md` | Cross-check flags and manual verification decisions |
| `research/analysis/tables/T1-yonetici-ozeti.md` | Domain summary (recomputed here for three products) |
| `research/analysis/tables/T2-tam-matris.md` | Full matrix |
| `research/analysis/tables/T5-topluluk.md` | Community sustainability metrics and API endpoints |
| `research/analysis/tables/T6-dmbok-kapsama.md` | DMBOK2 coverage method, KA7 gap |
| `research/tools/validate.py`, `research/tools/crosscheck.py` | What validation and cross-check actually test |
| `docs/superpowers/specs/2026-08-11-openmetadata-yonetisim-arastirma-design.md` | Phase design: freeze gate, parallel assessors, consistency guard |

Excluded on purpose: the closed-source vendor assessment and its evidence file, organisation- and
sector-specific context, and product-strategy material.

## Products and pins (cloned 2026-08-11, each clone equal to its release tag)

| Product | Repository | Tag | Commit |
|---|---|---|---|
| OpenMetadata | https://github.com/open-metadata/OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` |
| DataHub | https://github.com/datahub-project/datahub | `v1.7.0` | `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80` |
| Apache Atlas | https://github.com/apache/atlas | `release-2.5.0` | `234608315f29b643c2dd7632cad0e8ef2a15c696` |

## Code claims re-verified for this skill (against the pinned clones)

| Claim | Location | Result |
|---|---|---|
| OM notification targets Webhook, Slack, MsTeams, GChat, Email | `openmetadata-spec/.../events/eventSubscription.json:98-110` | Confirmed |
| OM retries (default 3) and delivery status incl. `retryLimitReached` | `eventSubscription.json:180-185,334` | Confirmed |
| OM governance workflows on Flowable | `openmetadata-service/pom.xml:918-928` (flowable-engine 7.2.0, json-logic-java) | Confirmed |
| OM policy has only an `enabled` flag, no lifecycle status | `openmetadata-spec/.../policies/policy.json:48` | Confirmed |
| OM `ingestion/` module under Collate Community License, core Apache-2.0 | `ingestion/LICENSE:1-2`, `LICENSE:1-2` | Confirmed |
| OM SECURITY.md still lists 1.11.x/1.10.x as supported | `SECURITY.md:10-11` | Confirmed |
| OM "no new entity types" (D1.1) | `TypeResource.java:78-85` | Text says no new custom **data types** for properties; the entity-type limit is plausible but not stated there |
| DH approval workflows and assertion monitors Cloud-only | `docs/managed-datahub/managed-datahub-overview.md:30-33,48` | Confirmed |
| DH no proposals/action requests in OSS | GraphQL schema files, `entity-registry.yml` | Confirmed: zero matches |
| DH Actions retry settings | `datahub-actions/src/datahub_actions/pipeline/pipeline.py:89-90` | Confirmed (research cites `teams.py:44-47`, which holds the webhook config) |
| DH Postgres quickstart without Kafka (pgQueue) | `docker/profiles/README.md:78-83`, `docker-compose.actions.yml:48-55` | Confirmed |
| DH history statement | `README.md:187` | Confirmed |
| Atlas `owner` is a single free-text string | `addons/models/0000-Area0/0010-base_model.json:37-47` | Confirmed |
| Atlas has no webhook/SMTP/chat notification code | repo-wide search | Confirmed (only hit: bundled Swagger UI JS) |
| No golden-record/survivorship concept in any of the three | repo-wide search | Confirmed |
| `validate.py` scans adjectives/product names | `research/tools/validate.py` | **Not present**; the README claim is wrong |
| `crosscheck.py` all-zero needs every product known | `research/tools/crosscheck.py` (`len(known) == len(scores)`) | Confirmed |

## Frameworks and standards cited by the rubric

| Ref | Source |
|---|---|
| dmbok2-ka1 ... ka11 | DAMA-DMBOK2 knowledge-area context diagrams (Figures 14, 21, 28, 54, 63, 66, 71, 75, 79, 84, 91), DAMA Rocky Mountain Chapter summaries, https://damarmc.org/news/13242927 (KA1) ... https://damarmc.org/news/13443040 (KA11) |
| dcam | EDM Council, Data Management Capability Assessment Model, https://edmcouncil.org/frameworks/dcam/ |
| dgi | DGI Data Governance Framework, https://datagovernance.com/the-dgi-data-governance-framework/ |
| iso-38505-1 | ISO/IEC 38505-1:2017 Governance of data, https://www.iso.org/standard/56639.html |
| iso-8000 | ISO 8000-61:2016 Data quality management process reference model, https://www.iso.org/standard/63086.html |
| iso-11179 | ISO/IEC 11179-3:2023 Metadata registries, https://www.iso.org/standard/78915.html |
| iso-20889 | ISO/IEC 20889:2018 De-identification terminology, https://www.iso.org/standard/69373.html |
| iso-22301 | ISO 22301:2019 Business continuity, https://www.iso.org/standard/75106.html |
| datamesh-governance | Data Mesh Principles and Logical Architecture (2020), https://martinfowler.com/articles/data-mesh-principles.html |
| openlineage-spec | OpenLineage object model, https://openlineage.io/docs/spec/object-model |
| delta-sharing-spec | Delta Sharing protocol, https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md |
| eu-ai-act | Regulation (EU) 2024/1689, Art. 10, https://artificialintelligenceact.eu/article/10/ |
| nist-ai-rmf, nist-genai-profile | NIST AI RMF 1.0 (2023) and Generative AI Profile (2024); the first is registered but unused |
| finops-framework | FinOps Framework, Reporting & Analytics, https://www.finops.org/framework/capabilities/reporting-analytics/ |
| unicode-cldr | Unicode CLDR, https://cldr.unicode.org |
| forrester-catalog | The Forrester Wave methodology (method reference only) |

## Academic papers cited by the rubric

- Hellerstein et al., "Ground: A Data Context Service", CIDR 2017.
- Namaki et al., "Vamsa: Automated Provenance Tracking in Data Science Scripts", KDD 2020 (arXiv:2001.01861).
- Wang and Strong, "Beyond Accuracy: What Data Quality Means to Data Consumers", JMIS 1996.
- Sweeney, "k-anonymity: A model for protecting privacy", 2002.
- Dwork, "Differential Privacy", ICALP 2006.
- "Improving Data Governance in Large Organizations through Ontology and Linked Data" (IEEE, 2016).
- "Using Large Language Models to Enrich the Documentation of Datasets for Machine Learning" (arXiv:2404.15320, 2024).
- "RAG Security and Privacy: Formalizing the Threat Model and Attack Surface" (arXiv:2509.20324, 2025).
- "A semantics-enabled approach for personalised Data Lake exploration" (Knowledge and Information Systems, 2023).

## Legal texts behind D14 (Turkey)

Law No. 6698 on the Protection of Personal Data (consolidated, incl. Law No. 7499 of 2024); VERBİS
Regulation; Regulation on Erasure, Destruction or Anonymisation of Personal Data; Communiqués on
the Obligation to Inform and on Applications; Regulation on Transfers Abroad (2024); Board
Decisions 2018/10, 2019/10, 2019/271 and Principle Decision 2026/1301; KVKK Compliance Guide for
Public Institutions (2026); Presidential Circular 2019/12. Article-level detail and URLs:
`research/rubric/kvkk-mevzuat.md` and the sibling `kvkk-compliance` skill.

## Community data endpoints (fetched 2026-08-11)

`api.github.com/repos/{repo}`, `/contributors`, `/commits?since=`, `/stats/commit_activity`,
`/releases`, `/search/issues`; `issues.apache.org/jira/rest/api/2/search` (project ATLAS);
`downloads.apache.org/atlas/`.
