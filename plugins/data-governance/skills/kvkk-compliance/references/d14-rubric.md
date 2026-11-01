# D14 Rubric: KVKK Capabilities of a Catalog

D14 is the KVKK domain of a frozen governance rubric of **14 domains and 90 capabilities**. D14 has
11 capabilities. Unlike D1–D13, it is not derived from DMBOK2, DCAM or ISO. It comes from Turkish
law (see [obligations-map.md](obligations-map.md)). Use it to score any catalog or data platform.

## Scale (0–4)

| Score | Label | Meaning |
|---|---|---|
| 0 | Absent | The concept is not in the product model; done outside or by hand |
| 1 | Experimental | Indirect only: free text, manual work, one narrow scenario |
| 2 | Basic | Structured, but fed by hand; no automatic enforcement or feedback |
| 3 | Mature | Works automatically, fine-grained, reported or tied to a workflow |
| 4 | Differentiator | Rare: auditability, immutability, cross-system inference, automatic non-conformity detection, closed loop |

Each level includes the one below it. `null` means **not assessed** and is never 0.

**Double-counting rule.** D14 never re-scores a mechanism scored in D6 (security and privacy) or
D9 (data products and sharing). It measures only the KVKK *delta*: whether the generic mechanism
can be tied to the legal obligation. Examples: D6.5 measures the retention field, while D14.3
measures the destruction *method*, the 6-month bound and the minutes. D6.1 measures PII
detection, while D14.8 measures separation of the special categories and policy precedence. D6.8
measures anonymisation quality, while D14 measures only that anonymisation is *recorded* as a
destruction method.

## Capabilities and level thresholds

Each row gives the definition, then what distinguishes levels 2, 3 and 4.

**D14.1 Inventory derived from the catalog** (VERBİS Reg. Art. 4/1(h), 5/1(ç), 13; Law Art. 16)
- 2: data category, subject group and recipient group as structured fields; exportable as a table.
- 3: plus max retention, transfers abroad and measures in the same record; auto-filled or
  suggested from classification and ownership; one-asset notification output.
- 4: continuously updated from source scans; new or vanished personal fields produce a dated diff
  sent to the owner; inventory history makes the **7-day** change rule traceable.
- Scope: may narrow under Art. 28/2 (Art. 16). Purpose and legal basis are measured in D14.2.

**D14.2 Purpose and legal basis per asset** (Law Art. 5, 6; Information Comm. Art. 5/1(h); Art. 28/1)
- 2: purpose and legal basis are closed-list fields per asset; the list is customisable to the
  Art. 5/6 conditions.
- 3: differs per column; "out of scope under Art. 28/1" is a distinct value; personal-data fields
  with an empty purpose or basis go to a non-conformity list with an owner.
- 4: several purpose–basis pairs per asset, each with its own retention; in-scope and
  out-of-scope reported separately with the exemption clause and reasoning; changes audited with
  date, reason and user.

**D14.3 Destruction method and periodic destruction** (Destruction Reg. Art. 4/1(c), 6/1(g),(ğ), 7/3, 7/4, 11/2)
- 2: method is a closed list (erasure / destruction / anonymisation); retention + method export as
  the retention-destruction table.
- 3: a recurring destruction job whose interval can be bounded to **≤ 6 months**; every run leaves
  a record with date, scope and method.
- 4: records immutable for **≥ 3 years** and exportable; third-party recipients of destroyed data
  derived from lineage; alert when the job misses its window or expired data remains.

**D14.4 Consent state and withdrawal downstream** (Law Art. 3/1(a), 7/1; Information Comm. Art. 5/1(f))
- 2: consent state (given / not given / withdrawn), consent text version and date as fields.
- 3: read from an external consent store; withdrawal lists downstream assets via lineage.
- 4: withdrawal opens a destruction task; closure tracked per affected asset; another valid basis
  is recorded with its reasoning and removes the asset from destruction scope.

**D14.5 Data subject request** (Law Art. 11, 13/2; Application Comm. Art. 5/1, 6/5)
- 2: a person identifier (national ID, customer no.) can be searched across assets and columns;
  result exportable.
- 3: request record with **all four**: a type covering Art. 11/1 (a)–(ğ), application date, owner,
  remaining time against **30 days**. Missing any one fails the level.
- 4: affected set auto-expanded up- and downstream via lineage, incl. copies and recipients;
  reasoning, reply and action audited; alerts before expiry.
- Scope: may narrow under Art. 28/2 (Art. 11, except damages). A generic ticket is not enough.

**D14.6 Breach scope** (Law Art. 12/5; BD 2019/10, 2019/271)
- 2: for a selected asset set, one exportable report of data categories and subject groups.
- 3: downstream copies added via lineage automatically; report split by category **and**
  personal vs special-category.
- 4: breach record with scope, subject categories, consequences and measures, assigned to an owner;
  a **learned-at** field separate from occurred-at and created-at drives the **72 h** countdown;
  record and attachments immutable and exportable.

**D14.7 Transfer abroad and its mechanism** (Law Art. 9; Cross-border Reg. Art. 4/1(e), 6, 10, 14; Circular 2019/12)
- 2: recipient country and domestic/abroad flag per transfer; all transfers abroad in one list.
- 3: legal mechanism as a structured field distinguishing adequacy, the four safeguards and the
  incidental exception; document date and validity recorded.
- 4: transfers without a valid mechanism auto-reported; source region compared with the record;
  onward transfers via lineage; standard-contract signature date tracked for **5 business days**.

**D14.8 Special categories and extra measures** (Law Art. 6/1, 6/4; BD 2018/10)
- 2: the 11 categories as separate tags, assignable to columns.
- 3: a separate policy per category; the category policy **overrides** the generic sensitivity
  policy, and that precedence is fixed by the product, not by user configuration.
- 4: extra-measure status (scoped/timed access, periodic review, remote 2FA, encryption) recorded
  from source or owner declaration, gaps reported; new columns get the category policy
  automatically.

**D14.9 Processor relationship** (Law Art. 12/2; Destruction Reg. Art. 12/1(b); Cross-border Reg. Art. 4/1(ğ),(h); Public Guide §2.3, §3.3.1)
- 2: counterparty identity and role (controller / processor / joint controller) as fields;
  processors and their assets in one list.
- 3: contract reference, date and validity; relationships with no or expired contract listed as
  non-conformities with an owner.
- 4: processor's asset set derived via lineage incl. derivatives; on termination, the set needing
  destruction or return is reported; same list serves Art. 12/1(b) notifications.

**D14.10 Versioned privacy notice** (Law Art. 10; Information Comm. Art. 5/1(b),(ç),(e))
- 2: notice is an object with version and effective date, linkable to assets.
- 3: personal-data assets without a linked notice reported; old versions kept; "which version
  applied to this asset on date X" is answerable.
- 4: drift between asset purpose/basis/recipients and the notice reported; assets whose purpose
  changed stay non-conformant until the notice is updated.
- Scope: may narrow under Art. 28/2 (Art. 10).

**D14.11 Publicly published personal data** (PD 2026/1301; Law Art. 4/2(ç),(d))
- 2: published flag, URL and date; published assets in one list.
- 3: masking/anonymisation method and publication period recorded; expired publications listed
  for removal with an owner.
- 4: periodic **retrospective** scan of live publications; personal-data publications with no
  masking record or empty legal basis reported; removal tracked to closure.

## Measured scores (frozen evidence, all K)

| ID | OM 1.13.3 | DH v1.7.0 | Atlas 2.5.0 |
|---|---|---|---|
| D14.1 | 2 | 2 | 2 |
| D14.2 | 2 | **3** | 2 |
| D14.3 | 2 | 2 | 2 |
| D14.4 | 2 | 2 | 2 |
| D14.5 | 2 | 2 | 2 |
| D14.6 | 2 | 2 | 2 |
| D14.7 | 1 | 2 | 1 |
| D14.8 | 2 | 2 | **3** |
| D14.9 | 1 | 2 | 1 |
| D14.10 | 1 | 2 | 1 |
| D14.11 | 2 | 2 | 2 |

Supporting scores that decide weakest links: D1.2 custom fields OM 2 / DH 4 / AT 4; D6.1 PII
detection 3 / 1 / 0; D6.3 masking 1 / 1 / 0; D6.8 anonymisation 0 / 0 / 0; D7.2 approval workflow
3 / 1 / 0; D11.4 alerting 4 / 2 / 0.

## Why each product lands where it does (code evidence)

Paths are repo-relative at the inspected commits (see [sources.md](sources.md)).

**Custom fields: the route to every `partial`.**
- OpenMetadata: typed custom properties (enum, date, duration, hyperlink, entity refs) on tables
  and columns. The property definition has exactly five keys (`name`, `displayName`,
  `description`, `propertyType`, `customPropertyConfig`) and **no required flag**
  (`openmetadata-spec/.../type/customProperty.json:50-71`, `additionalProperties: false`). They attach only to entities whose
  schema has an `extension` field. `glossaryTerm`, `glossary`, `database`, `databaseSchema`,
  `container`, `dataProduct` and `table` do. `team`, `user`, `databaseService` and
  `storageService` do not. Spot-verified in `entity/**/*.json` (e.g. `glossaryTerm.json:200`,
  `database.json:116`, `container.json:203`, `table.json:374,1266`).
- DataHub: structured properties with `allowedValues` and `cardinality` on any entity incl.
  `schemaField` (`metadata-models/.../structured/StructuredPropertyDefinition.pdl:52-82`).
  `IS_NULL`/`EXISTS` filters (`.../query/filter/Condition.pdl:31,36`) plus Forms find empties.
  Forms prompts can be `required` (`.../form/FormPrompt.pdl:59-61`). This tracks completion; it
  does not block a write. That combination is why DataHub's D14.2 = 3.
- Atlas: Business Metadata and `AtlasEnumDef` give typed, closed-list fields. Entity-type
  attributes with `isOptional=false` are **validated on write** (`intg/.../type/AtlasStructType.java:222-240`).
  Adding a mandatory attribute to an existing type is rejected unless it is applied as a typedef
  patch `ADD_MANDATORY_ATTRIBUTE` (`repository/.../graph/v2/AtlasStructDefStoreV2.java:162-164`).
  This is the only true "mandatory field" among the three, and it is costly to introduce.

**Special categories (D14.8).**
- OpenMetadata ships `PersonalData.SpecialCategory` as **one** tag in a `mutuallyExclusive`
  classification (`openmetadata-service/.../json/data/tags/personalDataTags.json:6,14`). Policies
  can match tags, but conflict resolution is fixed deny-first, so a category policy cannot override
  a generic one. Built-in masking is hard-wired to `PII.Sensitive`
  (`.../security/mask/PIIMasker.java:45`) and applies to catalog sample data only.
- DataHub policies are allow-only: the first policy that grants wins, otherwise DENY
  (`metadata-service/auth-impl/.../DataHubAuthorizer.java:179-196`). A category policy cannot
  override another.
- Atlas requires permission for **every** classification on the entity before it allows access
  (`authorization/.../simple/AtlasSimpleAuthorizer.java:145-160`). The precedence is fixed by the
  product, so D14.8 = 3.

**Deadlines.** None of the three has a due date with escalation on a request, task or incident.
OpenMetadata `thread.json` and `userApprovalTask.json` have no `dueDate`/`timeout`. OpenMetadata
does have a cron-scheduled governance workflow trigger
(`.../governance/workflows/elements/triggers/periodicBatchEntityTrigger.json:12-75`) that can
sweep entities on a schedule. DataHub's research note says its scheduler runs ingestion only
(`ingestion-scheduler/.../IngestionScheduler.java:49-160`). Other scheduled routes in DataHub were
**not verified**.

**Missing objects.** No product has a transfer, breach, DSR or consent object. DataHub's
`incident` is a data-quality concept with `startedAt` and `created` but no learned-at
(`.../incident/IncidentInfo.pdl:104,115`), and structured properties cannot be attached to it.
DataHub v1.7.0 `document` links to assets via `relatedAssets` (`.../knowledge/DocumentInfo.pdl:81`),
which makes it the only privacy-notice stand-in (D14.10 = 2). OpenMetadata's data product has a
five-value `visibility` (`PRIVATE`…`PUBLIC`, `entity/domains/dataProduct.json:36-41`), a ready
flag for D14.11. Region appears only as a technical connection parameter (e.g. Atlas
`addons/models/3000-Cloud/3030-aws_s3_v2_typedefs.json:22`), not as a transfer record.
