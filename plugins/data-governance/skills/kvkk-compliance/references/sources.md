# Sources

## Research files used (paths inside the research repository)

| File | Used for |
|---|---|
| `research/rubric/kvkk-mevzuat.md` | 49 obligations, verified durations, Art. 28 scope, Law No. 7499 regime |
| `research/rubric/parts/D14.yaml` | D14.1–D14.11 definitions, rationales, level 0–4 texts |
| `research/rubric/README.md` | Scale definitions, D14 note (double-counting rule, Art. 28 scope), 14 domains / 90 capabilities |
| `research/rubric/sources.yaml` | Legal source register: Official Gazette numbers, URLs, articles verified per text |
| `research/analysis/tables/T9-kvkk-uyum.md` | Three-product obligation matrix, weakest-link rule, totals |
| `research/evidence/openmetadata.yaml`, `datahub.yaml`, `atlas.yaml` | D14 and supporting scores with `path:line` evidence (all K) |
| `research/analysis/gaps.md` (§6, KVKK class) | Structural roots: no mandatory flag, no per-record deadlines, documents not modelled |
| `research/analysis/anonimlestirme-mimarisi.md` (§5.2, §10) | Anonymisation as a destruction method, evidence limits, table-format destruction latency |
| `research/analysis/1pb-sistem-muhendisligi.md` (Iceberg defaults table) | Snapshot/orphan-file defaults, Trino minimum retention |

Organisation-specific context, sector examples, closed-source vendor assessments and product
strategy from the research are deliberately omitted.

## Products and versions (inspected 2026-08-11)

| Product | Tag | Commit |
|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b84797064a42859cda3f2a3425dc6` |
| DataHub | `v1.7.0` | `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80` |
| Apache Atlas | `release-2.5.0` | `234608315f29b643c2dd7632cad0e8ef2a15c696` |

Evidence types: **K** = source code, **D** = official docs, **İ** = issue/roadmap, **B** = vendor
claim. All D14 scores are K.

## Legal texts (articles and durations verified against official text)

| Ref | Instrument | Official source |
|---|---|---|
| kvkk-6698 | Law No. 6698 on the Protection of Personal Data, consolidated; OG 7/4/2016-29677; amended by Law No. 7499 (2/3/2024): Art. 6/2 repealed, 6/3 amended, Art. 9 replaced | https://www.mevzuat.gov.tr/mevzuatmetin/1.5.6698.pdf |
| kvkk-verbis-yon | Regulation on the Data Controllers' Registry (VERBİS); OG 30/12/2017-30286, amended OG 28/4/2019-30758 (added "legal basis" to the inventory definition) | https://www.resmigazete.gov.tr/eskiler/2017/12/20171230-7.htm |
| kvkk-imha-yon | Regulation on Erasure, Destruction or Anonymisation of Personal Data; OG 28/10/2017-30224, in force 1/1/2018 | https://www.resmigazete.gov.tr/eskiler/2017/10/20171028-10.htm |
| kvkk-aydinlatma-teblig | Communiqué on Procedures and Principles for Fulfilling the Obligation to Inform; OG 10/3/2018-30356 | https://www.resmigazete.gov.tr/eskiler/2018/03/20180310-5.htm |
| kvkk-basvuru-teblig | Communiqué on Procedures and Principles for Applications to the Data Controller; OG 10/3/2018-30356 | https://www.resmigazete.gov.tr/eskiler/2018/03/20180310-6.htm |
| kvkk-ihlal-karar | Board Decision 24/01/2019, No. 2019/10 (breach notification; 72 h from learning) | https://www.kvkk.gov.tr/Icerik/5362/Veri-Ihlali-Bildirimi |
| kvkk-ihlal-icerik-karar | Board Decision 18/09/2019, No. 2019/271 (minimum content of the notice to data subjects) | https://www.kvkk.gov.tr/Icerik/5547/2019-271 |
| kvkk-ozel-nitelikli-karar | Board Decision 31/01/2018, No. 2018/10 (adequate measures for special categories; also OG 7/3/2018) | https://www.kvkk.gov.tr/Icerik/4110/2018-10 |
| kvkk-yurtdisi-yon | Regulation on Procedures and Principles for Transfer of Personal Data Abroad; OG 10/7/2024-32598 | https://www.resmigazete.gov.tr/eskiler/2024/07/20240710-2.htm |
| btig-genelge | Presidential Circular No. 2019/12 on Information and Communication Security Measures; OG 6/7/2019-30823 | https://www.mevzuat.gov.tr/MevzuatMetin/CumhurbaskanligiGenelgeleri/20190706-12.pdf |
| kvkk-kamu-rehber | KVKK Compliance Guide for Public Institutions and Public Professional Organisations (KVKK Publications No. 114, March 2026; §2.3, §3.3.1, ch. 6) | https://www.kvkk.gov.tr/Icerik/8833/kamu-kurumlari-icin-kisisel-verilerin-korunmasi-kanununa-uyum-rehberi |
| kvkk-kamu-yayim-karar | Board Principle Decision 01/07/2026, No. 2026/1301, on public-law controllers sharing personal data on the internet (announced 27 July 2026) | kvkk.gov.tr announcement page (Icerik/8835) |

Not used: the Information and Communication Security Guide itself. The research could not reach
its official host, so only the Circular that mandates it was verified.

Other: Article 29 Data Protection Working Party, *Opinion 05/2014 on Anonymisation Techniques*
(WP216), cited for "k-anonymity alone is not sufficient".

## Checked for this skill beyond the research

- **Law text** (consolidated PDF, re-read 2026-11-01): Art. 5/2 (a)–(f), Art. 6/1, Art. 6/3 (a)–(g)
  after Law No. 7499, Art. 11/1 (a)–(ğ), Art. 28/1 (a)–(d) and 28/2 (a)–(ç). These are used for the
  enum values in [metadata-model.md](metadata-model.md).
- **Destruction Reg.** (OG text, re-read 2026-11-01): Art. 7/3 (all operations recorded, ≥ 3 years),
  7/4 (methods explained in the policy and procedures), 7/5 (controller selects the method;
  explains the reason on the data subject's request), 11/2 (≤ 6 months), 11/3 (3 months).
- **OpenMetadata** (`openmetadata-spec/src/main/resources/json/schema/`):
  `type/customProperty.json:50-71` has five keys and no required flag. `extension` is present in
  `entity/data/glossaryTerm.json:200`, `glossary.json:120`, `database.json:116`,
  `databaseSchema.json:112`, `container.json:203`, `table.json:374,1266` and
  `entity/domains/dataProduct.json:245`. It is absent in `entity/teams/team.json`, `user.json`,
  `entity/services/databaseService.json` and `storageService.json`. `entity/feed/thread.json` and
  `governance/workflows/elements/nodes/userTask/userApprovalTask.json` have no `dueDate`/`timeout`.
  `governance/workflows/elements/triggers/periodicBatchEntityTrigger.json:12-30` has a schedule and
  entity types. `dataProduct.json:36-41` has five `visibility` values, default `PRIVATE`.
  `openmetadata-service/src/main/resources/json/data/tags/personalDataTags.json:6,14` has a
  mutually exclusive classification and a single `SpecialCategory` tag.
  `openmetadata-service/src/main/java/org/openmetadata/service/security/mask/PIIMasker.java:45`
  has `SENSITIVE_PII_TAG = "PII.Sensitive"`.
- **DataHub** (`metadata-models/src/main/pegasus/com/linkedin/`):
  `structured/StructuredPropertyDefinition.pdl:52-82` (`allowedValues`, `cardinality`, `entityTypes`),
  `metadata/query/filter/Condition.pdl:31,36` (`IS_NULL`, `EXISTS`),
  `form/FormPrompt.pdl:59-61` (`required` prompt, default false),
  `knowledge/DocumentInfo.pdl:81` (`relatedAssets`),
  `incident/IncidentInfo.pdl:104,115` (`startedAt`, `created`; no learned-at);
  `metadata-service/auth-impl/src/main/java/com/datahub/authorization/DataHubAuthorizer.java:179-196`
  (first granting policy wins, otherwise DENY).
- **Atlas**: `authorization/src/main/java/org/apache/atlas/authorize/simple/AtlasSimpleAuthorizer.java:145-160`
  (permission needed for every classification on the entity);
  `intg/src/main/java/org/apache/atlas/type/AtlasStructType.java:222-240` (non-optional attributes
  validated on write);
  `repository/src/main/java/org/apache/atlas/repository/store/graph/v2/AtlasStructDefStoreV2.java:162-164`
  (adding a mandatory attribute to an existing type needs the `ADD_MANDATORY_ATTRIBUTE` typedef patch);
  `addons/models/3000-Cloud/3030-aws_s3_v2_typedefs.json:22` (`region` as an optional technical attribute).
