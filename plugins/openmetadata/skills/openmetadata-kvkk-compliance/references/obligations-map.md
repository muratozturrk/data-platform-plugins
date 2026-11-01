# KVKK Obligations → Catalog Capabilities → OpenMetadata 1.13.3

> **Not legal advice.** This map shows which technical catalog capability each verified legal
> obligation corresponds to, and how far OpenMetadata 1.13.3 supports it. Compliance depends on the
> organisation's processes as much as on the product. Have counsel own the legal reading.

Every article number and every duration below was read directly from the official text listed in
[sources.md](sources.md); nothing unverified is included. Product statuses come from source-code
inspection of OpenMetadata `1.13.3-release` (commit `255f6694913b`) unless marked otherwise.

## Abbreviations

| Short form | Instrument |
|---|---|
| **Law** | Law No. 6698 on the Protection of Personal Data (KVKK), consolidated text in force (incl. Law No. 7499 amendments of 2/3/2024) |
| **VERBİS Reg.** | *Veri Sorumluları Sicili Hakkında Yönetmelik* — Regulation on the Data Controllers' Registry (VERBİS); OG 30/12/2017-30286, amended OG 28/4/2019-30758 |
| **Destruction Reg.** | *Kişisel Verilerin Silinmesi, Yok Edilmesi veya Anonim Hale Getirilmesi Hakkında Yönetmelik* — Regulation on Erasure, Destruction or Anonymisation of Personal Data; OG 28/10/2017-30224 |
| **Information Comm.** | *Aydınlatma Yükümlülüğünün Yerine Getirilmesinde Uyulacak Usul ve Esaslar Hakkında Tebliğ* — Communiqué on the Obligation to Inform; OG 10/3/2018-30356 |
| **Application Comm.** | *Veri Sorumlusuna Başvuru Usul ve Esasları Hakkında Tebliğ* — Communiqué on Applications to the Data Controller; OG 10/3/2018-30356 |
| **Cross-border Reg.** | *Kişisel Verilerin Yurt Dışına Aktarılmasına İlişkin Usul ve Esaslar Hakkında Yönetmelik* — Regulation on Transfer of Personal Data Abroad; OG 10/7/2024-32598 |
| **BD 2019/10, 2019/271, 2018/10** | Decisions of the Personal Data Protection Board (*Kurul*): breach notification; minimum content of notice to data subjects; adequate measures for special-category data |
| **PD 2026/1301** | Board Principle Decision (*İlke Kararı*) of 01/07/2026 on public-law data controllers sharing personal data on the internet |
| **Public Guide** | KVKK Compliance Guide for Public Institutions (KVKK Publications No. 114, 2026) |
| **Circular 2019/12** | Presidential Circular No. 2019/12 on Information and Communication Security Measures; OG 6/7/2019-30823 |

Notation: "Art. 9/5" = Article 9, paragraph 5; sub-paragraph letters keep the Turkish alphabet
(a, b, c, ç, d, e, f, g, ğ, h).

## Verified durations

| Duration | Where it applies | Source |
|---|---|---|
| **30 days** | Latest date to conclude a data subject application (free of charge) | Law Art. 13/2; Application Comm. Art. 6/5; Destruction Reg. Art. 12/1(a),(c) |
| **72 hours** | Breach notification to the Board, counted **from the date the controller learned of the breach** | BD 2019/10 — **not in the Law text**; Law Art. 12/5 only says "as soon as possible" (*en kısa sürede*) |
| **6 months** | Maximum periodic-destruction interval ("in any case cannot exceed six months") | Destruction Reg. Art. 11/2 |
| **3 months** | Destruction period for a controller not obliged to prepare a retention-destruction policy | Destruction Reg. Art. 11/3 |
| **3 years** | Minimum retention of destruction-operation records | Destruction Reg. Art. 7/3 |
| **5 business days** | Notifying the Authority of a standard contract, from completion of signatures | Law Art. 9/5; Cross-border Reg. Art. 14/5 |
| **4 years** | Maximum interval for re-evaluating an adequacy decision | Law Art. 9/2; Cross-border Reg. Art. 9/1 |
| **7 days** | Notifying changes to registered information via VERBİS | VERBİS Reg. Art. 13 |
| **30 days** | Registration deadline for a controller that later becomes obliged to register | VERBİS Reg. Art. 8/2 |
| 30 / 60 days | Complaint to the Board (from learning of the reply / from application) | Law Art. 14/1 |
| 15 days | Sending information/documents requested by the Board | Law Art. 15/3 |
| 30 days | Implementing a Board decision | Law Art. 15/5 |

The last three rows are deliberately **not mapped to any product capability**: complaints and
Board decisions are handled at organisational-process level.

## How the OpenMetadata status is derived

- Cell values: `meets` (contributing capability scores 3–4), `partial` (2: the field or mechanism
  can be built, but the organisation must build, fill and police it), `weak` (1: free text or an
  indirect route only), `none` (0).
- Each obligation is fed by a primary D14 capability plus non-D14 supporting capabilities that are
  really needed (e.g. a deletion request needs D14.5 + D3 lineage + D6.1 PII classification + D7.2
  workflow). The cell is the **minimum** score in the chain — a chain is as strong as its weakest
  link. This is deliberately conservative.
- OpenMetadata 1.13.3 result over 49 obligations: **0 meets · 30 partial · 18 weak · 1 none.**
  (DataHub v1.7.0: 4 · 37 · 7 · 1; Apache Atlas 2.5.0: 0 · 21 · 13 · 15.)

Supporting-capability scores used below (OpenMetadata 1.13.3): D1.2 custom properties = 2,
D1.4 versioning = 3, D1.7 export = 2, D3.2 column lineage = 3, D3.4 impact analysis = 3,
D6.1 PII detection = 3, D6.2 policy engine = 4, D6.3 masking = 1, D6.5 retention = 2,
D6.6 audit trail = 3, D6.8 anonymisation = 0, D7.2 approval workflow = 3, D11.4 alerting = 4,
D12.4 authentication/SSO = 3.

## The 49-obligation map

| # | Provision | Obligation (short) | Catalog capability it needs | D14 | OM | Weakest link |
|---|---|---|---|---|---|---|
| 1 | VERBİS Reg. Art. 5/1(ç), 9/2 | Registered controllers keep a Personal Data Processing Inventory; registry disclosures are based on it | Asset/column inventory carrying inventory fields, exportable | 14.1 | partial | D1.2 |
| 2 | VERBİS Reg. Art. 4/1(h) | Inventory holds purposes **and legal basis**, data category, recipient group, data-subject group, max retention, foreign transfers, measures | All fields producible together for one asset | 14.1, 14.2 | partial | D1.2 |
| 3 | Law Art. 16/3 (a-f) | Registry notification content (purpose, subjects, categories, recipients, foreign transfers, measures, max period) | Catalog output covering the VERBİS field set | 14.1 | partial | D1.2 |
| 4 | VERBİS Reg. Art. 9/4 | Max retention notified **matched to data categories** | Retention as structured field tied to category | 14.1, 14.3 | partial | D1.2 |
| 5 | VERBİS Reg. Art. 13 | Changes notified via VERBİS within **7 days** | Dated diff of inventory changes | 14.1 | partial | D14.1 |
| 6 | Law Art. 4/2(c), 5, 6/3 | Specific, explicit, legitimate purpose and a statutory processing condition | Purpose + legal basis as closed-list fields per asset/column | 14.2 | partial | D1.2 |
| 7 | Information Comm. Art. 5/1(h) | "Legal basis" = which Art. 5/6 condition; must be stated explicitly | Legal-basis field bound to the statutory list | 14.2 | partial | D1.2 |
| 8 | Information Comm. Art. 5/1(ç) | Information given to data subjects consistent with registry disclosures | Match notice version to catalog purpose/basis | 14.2 | partial | D14.2 |
| 9 | Information Comm. Art. 5/1(b) | Purpose change → inform again before processing | Dated, reasoned audit of purpose/basis changes | 14.2 | partial | D14.2 |
| 10 | Law Art. 4/2(d); VERBİS Reg. Art. 5/1(ğ) | Keep only as long as law/purpose requires; notified max period drives Art. 7 destruction | Retention linked to a destruction trigger | 14.3 | partial | D14.3 |
| 11 | Destruction Reg. Art. 6/1(g),(ğ) | Policy contains retention/destruction table and periodic destruction periods | Table generated from the product | 14.3 | partial | D1.7 |
| 12 | Destruction Reg. Art. 11/2 | Periodic destruction interval **≤ 6 months** | Recurring job, bounded interval, alert on miss | 14.3 | partial | D14.3 |
| 13 | Destruction Reg. Art. 4/1(c), 8, 9, 10 | Destruction = erasure, destruction **or anonymisation** | Method recorded as separate field | 14.3 | partial | D14.3 * |
| 14 | Destruction Reg. Art. 7/3 | All destruction operations recorded; records kept **≥ 3 years** | Immutable, exportable destruction minutes | 14.3 | partial | D1.7 |
| 15 | Destruction Reg. Art. 12/1(b) | If destroyed data was transferred, notify third parties | Recipient list derived from lineage | 14.3, 14.5 | partial | D14.3 |
| 16 | Law Art. 3/1(a), 5/1 | Explicit consent definition; consent as default basis | Consent status + consent text version per asset | 14.4 | partial | D1.2 |
| 17 | Information Comm. Art. 5/1(f) | Informing and obtaining consent done **separately** | Consent record separate from notice record | 14.4 | partial | D1.2 |
| 18 | Law Art. 7/1; Destruction Reg. Art. 7/1 | When **all** processing conditions cease → erase/destroy/anonymise ex officio | Consent withdrawal feeds destruction workflow | 14.4 | partial | D14.4 |
| 19 | Law Art. 11 (a-ğ) | Data subject rights (know, request info, purpose, recipients, rectify, erase, notify third parties, object to automated analysis, damages) | Request workflow typed by the right list | 14.5 | partial | D14.5 |
| 20 | Law Art. 13/2; Application Comm. Art. 6/5 | Conclude within **30 days**, free | Application date + remaining-time indicator + alert | 14.5 | partial | D14.5 |
| 21 | Application Comm. Art. 5/1 | Application may be filed via "software or an application developed for the purpose of application" | Direct legal basis for an in-product request flow | 14.5 | partial | D14.5 |
| 22 | Law Art. 11/1(ç) | Right to know domestic/foreign recipients | Lineage-based recipient listing per person | 14.5, 14.7 | weak | D14.7 |
| 23 | Law Art. 12/5 | Unlawful acquisition → notify subject and Board as soon as possible | Breach record + scope extraction | 14.6 | partial | D14.6 |
| 24 | BD 2019/10 | "As soon as possible" = **72 h** from learning; explain delays | Countdown from a separate *learned-at* field | 14.6 | partial | D14.6 |
| 25 | BD 2019/10 | Breach facts, effects and measures recorded, kept for Board review | Immutable, exportable breach record | 14.6 | partial | D1.7 |
| 26 | BD 2019/271 | Notice to subjects by **data category, separating personal vs special-category** | Category-based, special-category-split scope report | 14.6 | partial | D14.6 |
| 27 | Cross-border Reg. Art. 4/1(e) | Transfer abroad = transmitting **or otherwise making accessible** | Transfer record also covering read grants to foreign parties | 14.7 | weak | D14.7 |
| 28 | Law Art. 9/1-4-6; Cross-border Reg. Art. 6 | Three tiers: adequacy → appropriate safeguard → incidental exception | Mechanism as structured field per transfer | 14.7 | weak | D14.7 |
| 29 | Law Art. 9/4; Cross-border Reg. Art. 10 | Four safeguards: agreement (+Board permission), BCRs (+Board approval), standard contract, undertaking (+Board permission) | Mechanism distinguishes the four + permit date/validity | 14.7 | weak | D14.7 |
| 30 | Law Art. 9/5; Cross-border Reg. Art. 14/5, 14/8 | Standard contract notified within **5 business days**; also on change/termination | Track signature date and notification status | 14.7 | weak | D14.7 |
| 31 | Law Art. 9/2; Cross-border Reg. Art. 9/1 | Adequacy decision re-evaluated at least every **4 years** | Report transfers whose basis expired | 14.7 | weak | D14.7 |
| 32 | Law Art. 9/8; Cross-border Reg. Art. 5/2 | Same safeguards for **onward transfers** | Chain transfers via lineage | 14.7 | weak | D14.7 |
| 33 | Circular 2019/12 Art. 1, 3 | Critical data stored in-country; public data not in cloud except domestic providers under institutional control | Compare physical region with location/transfer record | 14.7 | weak | D14.7 |
| 34 | Law Art. 6/1 | Closed list of special categories | Each category taggable separately from generic "sensitive" | 14.8 | partial | D14.8 |
| 35 | Law Art. 6/4 | Adequate measures set by the Board required | Category-specific policy set distinct from generic one | 14.8 | partial | D14.8 |
| 36 | BD 2018/10 | Extra measures: separate policy, scoped/timed access, periodic access review, cryptographic storage, logging of all actions, 2FA for remote access | Product checks and reports unmet measures | 14.8 | weak | D6.3 |
| 37 | Law Art. 6/3(a); Cross-border Reg. Art. 10/1(c) | Extra measures in standard contract for special categories | Flag transfers containing special-category columns | 14.7, 14.8 | weak | D14.7 |
| 38 | Law Art. 12/2 | Joint liability for measures when a processor processes on the controller's behalf | Counterparty legal role as structured field | 14.9 | weak | D14.9 |
| 39 | Cross-border Reg. Art. 4/1(ğ),(h) | "Exporter"/"recipient" defined by controller/processor role | Party identity + role in transfer record | 14.9 | weak | D14.9 |
| 40 | Public Guide §2.3 | In public bodies the institution itself is the controller; units not determining purposes/means are not | Role assigned by purpose/means test, not org chart | 14.9 | weak | D14.9 |
| 41 | Public Guide §3.3.1; Law Art. 8 | Domestic transfers (public↔public, public↔private) also need an explicit legal basis beforehand | Basis recorded for domestic transfers too | 14.9 | weak | D14.9 |
| 42 | Destruction Reg. Art. 12/1(b) | Notify third parties that received destroyed data | Processor's asset set from lineage → notification list | 14.9 | weak | D14.9 |
| 43 | Law Art. 10; Information Comm. Art. 5/1(e) | Duty to inform; **burden of proof on the controller** | Versioned notice object with effective date, linked to assets | 14.10 | weak | D14.10 |
| 44 | Information Comm. Art. 5/1(ç) | Notice consistent with registry disclosures | Drift report between asset fields and notice | 14.10 | weak | D14.10 |
| 45 | Information Comm. Art. 5/1(b) | Re-inform **before** processing when purpose changes | Open non-conformity until linked notice is updated | 14.10 | weak | D14.10 |
| 46 | PD 2026/1301 | Publishing personal data online is a processing activity; needs Art. 5 (Art. 6) condition | Flag asset/output as publicly published | 14.11 | partial | D1.2 |
| 47 | PD 2026/1301; Law Art. 4/2(ç) | Minimum data; masking, anonymisation | Record masking/anonymisation method applied | 14.11 | **none** | D6.8 |
| 48 | PD 2026/1301; Law Art. 4/2(d) | Set publication periods; remove/erase/destroy/anonymise at expiry | Publication-expiry field + expiry list | 14.11, 14.3 | partial | D14.11 |
| 49 | PD 2026/1301 | Review **already-published** content promptly; remove or mask unlawful items | Retrospective periodic scan (a forward rule engine is not enough) | 14.11 | partial | D14.11 |

\* Row 13 measures only *recording* the method. Applying anonymisation in the product (D6.8) is 0
in OpenMetadata, DataHub and Atlas; had it been in the chain, the cell would be `none` everywhere.

## Article 28 exemptions and scope

Law Art. 28 sets out exemptions by an exhaustive list, and they apply **per processing activity,
not per organisation** (Public Guide ch. 6):

- **Art. 28/1 — full exemption** (the Law does not apply at all). Most relevant for public bodies:
  (b) processing for research, planning and statistics **by anonymising** within official
  statistics; (ç) preventive, protective and intelligence activities by bodies legally tasked with
  national defence, security, public order or economic security.
- **Art. 28/2 — partial exemption**: only **Art. 10** (informing), **Art. 11** (data subject
  rights, *except* the right to claim damages) and **Art. 16** (VERBİS registration) do not apply.

Consequence for the map: rows **1–5** (VERBİS), **19–22** (Art. 11) and **43–45** (Art. 10) may
narrow in scope but never disappear; every other row (Arts. 4, 5, 6, 7, 8, 9, 12, 13) always
applies. Being out of scope under Art. 28/1 is modelled as **a value of the legal-basis field**
(D14.2), not as a separate capability — see [modelling-in-openmetadata.md](modelling-in-openmetadata.md).

## Law No. 7499 (2/3/2024): the regime you must model

- **Art. 6/2 repealed, Art. 6/3 amended**: one common, extended list of processing conditions
  (a–g) for all special categories (the old health/sex-life vs other split is gone). The Art. 6/4
  "adequate measures determined by the Board" requirement remains.
- **Art. 9 replaced entirely** — three tiers that do not substitute for each other:
  1. **Adequacy decision** (Art. 9/1-3) for a country, sector within a country, or international
     organisation; published in the Official Gazette; re-evaluated **at least every four years**.
  2. **Appropriate safeguards** (Art. 9/4), absent adequacy: (a) non-treaty agreement + Board
     permission, (b) Board-approved binding corporate rules, (c) Board-announced standard contract
     (notified within **five business days** of signing, Art. 9/5), (ç) written undertaking + Board
     permission.
  3. **Incidental exceptional cases** (Art. 9/6 a-f), only if incidental.
- **Explicit consent is no longer a general basis for transfers abroad.** It is now only one of the
  incidental exceptions (Art. 9/6-a), and under Art. 9/7 it **does not apply to activities of
  public institutions governed by public law** (repeated in Cross-border Reg. Art. 16/4).
- The pre-2024 regime (consent or a "safe countries" list / undertaking + Board permission) is not
  in force; do not build enum values on it.

## Reading the result

- **No OpenMetadata obligation is `meets`.** The `partial` route is always the same: custom
  properties defined, filled and policed by the organisation. Custom properties have no
  "required" flag (D1.2 = 2), so an empty legal-basis field produces no non-conformity by itself.
- **Weakest clusters** (in all three open-source products): cross-border transfer (27–33), versioned
  privacy notice (43–45), processor relationship (38–42), and anonymisation/masking (13, 47).
- **No verified duration** (30 days, 72 h, 6 months, 5 business days, 4 years, 7 days) can be
  modelled as a countdown in any product: task/request records have no due-date field
  (`thread.json`, `userApprovalTask` have no `dueDate`/`timeout`). A cron-scheduled workflow
  trigger exists (`periodicBatchEntityTrigger.json:12-75`), but it sweeps entities on a timetable;
  it does not give one request or record its own deadline and escalation.
- **Where OpenMetadata is best of the three:** data subject requests (rows 19–21: OM `partial`,
  DataHub `weak`, Atlas `none`) thanks to column search + PII tags; special-category tagging
  (row 34, `partial` vs DataHub `weak`). Atlas leads only on D14.8 (= 3): per-classification
  permission is product-fixed. DataHub leads on D14.2 (= 3) and is the only `meets` source.
