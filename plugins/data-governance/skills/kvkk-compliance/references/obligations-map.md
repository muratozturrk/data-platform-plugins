# KVKK Obligations → Catalog Capabilities

> **Not legal advice.** This map shows which technical catalog capability each verified legal
> obligation needs, and how far three open-source catalogs support it. Compliance depends on the
> organisation's processes as much as on any product. Counsel owns the legal reading.

Every article number and duration below was read directly from the official text listed in
[sources.md](sources.md). Nothing unverified is included. Product cells come from the frozen,
source-code-verified (K) evidence for OpenMetadata `1.13.3-release`, DataHub `v1.7.0` and Apache
Atlas `release-2.5.0`.

## Abbreviations

| Short form | Instrument |
|---|---|
| **Law** | Law No. 6698 on the Protection of Personal Data, consolidated text in force (incl. Law No. 7499 of 2/3/2024) |
| **VERBİS Reg.** | Regulation on the Data Controllers' Registry; OG 30/12/2017-30286, amended OG 28/4/2019-30758 |
| **Destruction Reg.** | Regulation on Erasure, Destruction or Anonymisation of Personal Data; OG 28/10/2017-30224 |
| **Information Comm.** | Communiqué on the Obligation to Inform; OG 10/3/2018-30356 |
| **Application Comm.** | Communiqué on Applications to the Data Controller; OG 10/3/2018-30356 |
| **Cross-border Reg.** | Regulation on Transfer of Personal Data Abroad; OG 10/7/2024-32598 |
| **BD 2019/10, 2019/271, 2018/10** | Board decisions: breach notification; minimum content of the notice to data subjects; adequate measures for special categories |
| **PD 2026/1301** | Board Principle Decision of 01/07/2026 on public-law controllers sharing personal data on the internet |
| **Public Guide** | KVKK Compliance Guide for Public Institutions (KVKK Publications No. 114, 2026) |
| **Circular 2019/12** | Presidential Circular No. 2019/12 on Information and Communication Security Measures; OG 6/7/2019-30823 |

"Art. 9/5" = Article 9, paragraph 5. Sub-paragraph letters follow the Turkish alphabet (a, b, c,
ç, d, e, f, g, ğ, h).

## Verified durations

| Duration | Where it applies | Source |
|---|---|---|
| **30 days** | Latest date to conclude a data subject application (free) | Law Art. 13/2; Application Comm. Art. 6/5; Destruction Reg. Art. 12/1(a),(c) |
| **72 hours** | Breach notice to the Board, **from the date the controller learned of it**; delays must be explained | BD 2019/10 — **not in the Law**; Law Art. 12/5 says only "as soon as possible" |
| **6 months** | Maximum periodic-destruction interval ("in any case cannot exceed six months") | Destruction Reg. Art. 11/2 |
| **3 months** | Destruction deadline (from when the duty arose) for a controller not obliged to keep a retention-destruction policy | Destruction Reg. Art. 11/3 |
| **3 years** | Minimum retention of records of all destruction operations | Destruction Reg. Art. 7/3 |
| **5 business days** | Notify the Authority of a standard contract, from completion of signatures | Law Art. 9/5; Cross-border Reg. Art. 14/5 |
| **4 years** | Maximum interval for re-evaluating an adequacy decision | Law Art. 9/2; Cross-border Reg. Art. 9/1 |
| **7 days** | Notify changes to registered information via VERBİS | VERBİS Reg. Art. 13 |
| **30 days** | Register after later becoming obliged to register | VERBİS Reg. Art. 8/2 |
| 30 / 60 days | Complaint to the Board (from learning of the reply / from the application) | Law Art. 14/1 |
| 15 days | Send information/documents the Board requests | Law Art. 15/3 |
| 30 days | Implement a Board decision | Law Art. 15/5 |

The last three rows are deliberately **not mapped to a product capability**. Complaints and
Board decisions are handled as organisational processes.

## How the product cells are derived

- `meets` = every capability in the chain scores 3–4; `partial` = weakest is 2 (the field or
  mechanism can be built, but the organisation must build, fill and police it); `weak` = weakest
  is 1 (free text or an indirect route); `none` = weakest is 0.
- Each obligation is fed by its primary D14 capability **plus** the non-D14 capabilities really
  needed to fulfil it. For example, an erasure request needs D14.5, D3 lineage, D6.1 PII
  classification and D7.2 workflow. The cell is the **minimum** score in the chain. This rule is
  deliberately conservative: an obligation cannot be fulfilled if any link is broken.
- Supporting capabilities: D1.2 custom fields, D1.4 versioning, D1.7 export, D3.1/3.2 lineage,
  D3.4 impact analysis, D6.1 PII detection, D6.2 policy engine, D6.3 masking, D6.5 retention,
  D6.6 audit trail, D6.8 anonymisation, D7.2 approval workflow, D11.4 alerting, D12.4 SSO/2FA.

Columns: **OM** = OpenMetadata 1.13.3, **DH** = DataHub v1.7.0, **AT** = Atlas 2.5.0.
Values: M = meets, P = partial, W = weak, **N** = none.

## The 49-obligation map

| # | Provision | Obligation (short) | Catalog capability it needs | Chain | OM | DH | AT |
|---|---|---|---|---|---|---|---|
| 1 | VERBİS Reg. Art. 5/1(ç), 9/2 | Registered controllers keep a Processing Inventory; registry disclosures are based on it | Asset/column inventory with inventory fields, exportable | 14.1 + D1.2 + D1.7 | P | P | P |
| 2 | VERBİS Reg. Art. 4/1(h) | Inventory = purposes **and legal basis**, data category, recipient group, subject group, max retention, transfers abroad, measures | All fields producible together for one asset | 14.1 + 14.2 + D1.2 | P | P | P |
| 3 | Law Art. 16/3 (a-f) | Registry notification content | Catalog output covering the VERBİS field set | 14.1 + D1.2 + D1.7 | P | P | P |
| 4 | VERBİS Reg. Art. 9/4 | Max retention notified **matched to data categories** | Retention as a structured field tied to category | 14.1 + 14.3 + D1.2 + D6.5 | P | P | P |
| 5 | VERBİS Reg. Art. 13 | Changes notified within **7 days** | Dated diff of inventory changes | 14.1 + D1.4 | P | P | P |
| 6 | Law Art. 4/2(c), 5, 6/3 | Specific, explicit, legitimate purpose and a statutory condition | Purpose + legal basis as closed-list fields | 14.2 + D1.2 | P | M | P |
| 7 | Information Comm. Art. 5/1(h) | "Legal basis" = which Art. 5/6 condition, stated explicitly | Legal-basis field bound to the statutory list | 14.2 + D1.2 | P | M | P |
| 8 | Information Comm. Art. 5/1(ç) | Notice consistent with registry disclosures | Match notice version to purpose/basis | 14.2 + D1.4 | P | M | P |
| 9 | Information Comm. Art. 5/1(b) | Purpose change → inform again before processing | Dated, reasoned audit of purpose/basis changes | 14.2 + D6.6 | P | M | P |
| 10 | Law Art. 4/2(d); VERBİS Reg. Art. 5/1(ğ) | Keep only as long as needed; notified max period drives Art. 7 destruction | Retention linked to a destruction trigger | 14.3 + D6.5 | P | P | P |
| 11 | Destruction Reg. Art. 6/1(g),(ğ) | Policy holds retention/destruction table and periodic destruction periods | Table generated from the catalog | 14.3 + D6.5 + D1.7 | P | P | P |
| 12 | Destruction Reg. Art. 11/2 | Periodic destruction **≤ 6 months** | Recurring job, bounded interval, alert on miss | 14.3 + D11.4 | P | P | **N** |
| 13 | Destruction Reg. Art. 4/1(c), 8, 9, 10 | Destruction = erasure, destruction **or anonymisation** | Method recorded as a separate field | 14.3 | P | P | P |
| 14 | Destruction Reg. Art. 7/3 | All destruction operations recorded, kept **≥ 3 years** | Immutable, exportable destruction minutes | 14.3 + D6.6 + D1.7 | P | P | P |
| 15 | Destruction Reg. Art. 12/1(b) | Destroyed data that was transferred → notify third parties | Recipient list from lineage | 14.3 + 14.5 + D3.1 + D3.4 | P | P | P |
| 16 | Law Art. 3/1(a), 5/1 | Explicit consent; consent as default basis | Consent status + consent text version | 14.4 + D1.2 | P | P | P |
| 17 | Information Comm. Art. 5/1(f) | Informing and obtaining consent done **separately** | Consent record separate from notice record | 14.4 + D1.2 | P | P | P |
| 18 | Law Art. 7/1; Destruction Reg. Art. 7/1 | When **all** conditions cease → erase/destroy/anonymise ex officio | Consent withdrawal feeds destruction workflow | 14.4 + D3.4 | P | P | P |
| 19 | Law Art. 11 (a-ğ) | Data subject rights list | Request workflow typed by the rights list | 14.5 + D7.2 | P | W | **N** |
| 20 | Law Art. 13/2; Application Comm. Art. 6/5 | Conclude within **30 days**, free | Application date + remaining time + alert | 14.5 + D7.2 + D11.4 | P | W | **N** |
| 21 | Application Comm. Art. 5/1 | Application may come via "software or an application developed for the purpose" | Direct basis for an in-product request flow | 14.5 + D7.2 | P | W | **N** |
| 22 | Law Art. 11/1(ç) | Right to know domestic/foreign recipients | Lineage-based recipient list per person | 14.5 + 14.7 + D3.2 + D3.4 | W | P | W |
| 23 | Law Art. 12/5 | Unlawful acquisition → notify subject and Board as soon as possible | Breach record + scope extraction | 14.6 | P | P | P |
| 24 | BD 2019/10 | "As soon as possible" = **72 h** from learning; explain delays | Countdown from a separate *learned-at* field | 14.6 + D11.4 | P | P | **N** |
| 25 | BD 2019/10 | Breach facts, effects, measures recorded, kept for Board review | Immutable, exportable breach record | 14.6 + D6.6 + D1.7 | P | P | P |
| 26 | BD 2019/271 | Notice to subjects **by data category, personal vs special-category** | Category-based, special-split scope report | 14.6 + D6.1 | P | W | **N** |
| 27 | Cross-border Reg. Art. 4/1(e) | Transfer abroad = transmitting **or otherwise making accessible** | Transfer record covering read grants to foreign parties | 14.7 + D6.2 | W | P | W |
| 28 | Law Art. 9/1-4-6; Cross-border Reg. Art. 6 | Three tiers: adequacy → appropriate safeguard → incidental exception | Mechanism as a structured field per transfer | 14.7 + D1.2 | W | P | W |
| 29 | Law Art. 9/4; Cross-border Reg. Art. 10 | Four safeguards (agreement + permission, BCRs + approval, standard contract, undertaking + permission) | Mechanism distinguishes four + permit date/validity | 14.7 + D1.2 | W | P | W |
| 30 | Law Art. 9/5; Cross-border Reg. Art. 14/5, 14/8 | Standard contract notified in **5 business days**; also on change/termination | Signature date + notification status | 14.7 + D11.4 | W | P | **N** |
| 31 | Law Art. 9/2; Cross-border Reg. Art. 9/1 | Adequacy re-evaluated at least every **4 years** | Report transfers whose basis lapsed | 14.7 + D11.4 | W | P | **N** |
| 32 | Law Art. 9/8; Cross-border Reg. Art. 5/2 | Same safeguards for **onward transfers** | Chain transfers via lineage | 14.7 + D3.4 | W | P | W |
| 33 | Circular 2019/12 Art. 1, 3 | Critical data stored in-country; public data not in cloud except domestic providers under institutional control | Compare physical region with location/transfer record | 14.7 | W | P | W |
| 34 | Law Art. 6/1 | Closed list of special categories | Each category taggable separately | 14.8 + D6.1 | P | W | **N** |
| 35 | Law Art. 6/4 | Adequate measures set by the Board | Category-specific policy set | 14.8 + D6.2 | P | P | P |
| 36 | BD 2018/10 | Separate policy, scoped/timed access, periodic access review, cryptographic storage, logging of all actions, 2FA for remote access | Check and report unmet measures | 14.8 + D6.3 + D6.6 + D12.4 | W | W | **N** |
| 37 | Law Art. 6/3(a); Cross-border Reg. Art. 10/1(c) | Extra measures in standard contract for special categories | Flag transfers containing special-category columns | 14.7 + 14.8 + D6.1 | W | W | **N** |
| 38 | Law Art. 12/2 | Joint liability for measures when a processor processes on the controller's behalf | Counterparty legal role as a structured field | 14.9 + D1.2 | W | P | W |
| 39 | Cross-border Reg. Art. 4/1(ğ),(h) | "Exporter"/"recipient" defined by controller/processor role | Party identity + role in transfer record | 14.9 + D1.2 | W | P | W |
| 40 | Public Guide §2.3 | In public bodies the institution is the controller; units not deciding purposes/means are not | Role by purpose/means test, not org chart | 14.9 + D1.2 | W | P | W |
| 41 | Public Guide §3.3.1; Law Art. 8 | Domestic transfers also need an explicit legal basis beforehand | Basis recorded for domestic transfers too | 14.9 + D1.2 | W | P | W |
| 42 | Destruction Reg. Art. 12/1(b) | Notify third parties that received destroyed data | Processor's asset set from lineage → notification list | 14.9 + D3.4 | W | P | W |
| 43 | Law Art. 10; Information Comm. Art. 5/1(e) | Duty to inform; **burden of proof on the controller** | Versioned notice object with effective date, linked to assets | 14.10 + D1.4 | W | P | W |
| 44 | Information Comm. Art. 5/1(ç) | Notice consistent with registry disclosures | Drift report between asset fields and notice | 14.10 + D1.4 | W | P | W |
| 45 | Information Comm. Art. 5/1(b) | Re-inform **before** processing when purpose changes | Open non-conformity until linked notice is updated | 14.10 + D11.4 | W | P | **N** |
| 46 | PD 2026/1301 | Publishing personal data online is processing; needs an Art. 5 (Art. 6) condition | Flag asset/output as publicly published | 14.11 + D1.2 | P | P | P |
| 47 | PD 2026/1301; Law Art. 4/2(ç) | Minimum data; masking, anonymisation | Record the masking/anonymisation method applied | 14.11 + D6.3 + D6.8 | **N** | **N** | **N** |
| 48 | PD 2026/1301; Law Art. 4/2(d) | Set publication periods; remove/erase/destroy/anonymise at expiry | Publication-expiry field + expiry list | 14.11 + 14.3 + D6.5 + D11.4 | P | P | **N** |
| 49 | PD 2026/1301 | Review **already-published** content promptly | Retrospective periodic scan (a forward rule engine is not enough) | 14.11 + D11.4 | P | P | **N** |

Row 13 measures only *recording* the method. Applying anonymisation in the product (D6.8) is 0 in
all three; had it been in the chain, the row would be `none` everywhere. Row 36: only three of the
six BD 2018/10 measures are measurable in a catalog (separate policy, action logging, access
scope). Cryptographic storage and 2FA belong to the source systems.

**Totals (M / P / W / N):** OpenMetadata 0 / 30 / 18 / 1 · DataHub 4 / 37 / 7 / 1 · Atlas
0 / 21 / 13 / 15. DataHub's four `meets` all rest on one capability (D14.2: closed-list legal
basis with empty-field detection). Atlas's 15 `none` come almost entirely from four zero links:
PII detection, approval workflow, alerting and masking.

## Article 28 exemptions and scope

Law Art. 28 lists exemptions exhaustively. They apply **per processing activity, not per
organisation** (Public Guide ch. 6):

- **Art. 28/1 — full exemption** (the Law does not apply). For public bodies, the most relevant
  are (b) processing for research, planning and statistics **by anonymising**, within official
  statistics, and (ç) preventive, protective and intelligence activities by bodies legally tasked
  with national defence, security, public order or economic security.
- **Art. 28/2 — partial exemption**: only **Art. 10** (informing), **Art. 11** (data subject
  rights, *except* the right to claim damages) and **Art. 16** (VERBİS registration) do not apply.
  The exemption must be proportionate and consistent with the Law's purpose and principles.

**Consequence.** Rows 1–5 (VERBİS), 19–22 (Art. 11) and 43–45 (Art. 10) may narrow in scope but
never disappear. Every other row (Arts. 4, 5, 6, 7, 8, 9, 12, 13) always applies. Model the full
exemption as **a value of the legal-basis field** (D14.2), together with the exemption clause and
the reasoning. Do not model it as a separate capability or an organisation-level switch.
Art. 28/1(b) depends on anonymisation, so a measured anonymisation result is its technical
precondition (see `data-anonymization`).

## Law No. 7499 (2/3/2024): the regime to model

- **Art. 6/2 repealed, Art. 6/3 amended.** There is one common, extended list of conditions (a–g)
  for all special categories; the old health/sex-life split is gone. Art. 6/4 ("adequate measures
  determined by the Board") remains.
- **Art. 9 replaced.** There are three tiers, and one cannot stand in for another:
  1. **Adequacy decision** (Art. 9/1-3) for a country, a sector within a country or an
     international organisation. It is published in the Official Gazette and re-evaluated **at
     least every four years**.
  2. **Appropriate safeguards** (Art. 9/4), absent adequacy: (a) non-treaty agreement + Board
     permission, (b) Board-approved binding corporate rules, (c) Board-announced standard contract
     (notified in **five business days**, Art. 9/5), (ç) written undertaking + Board permission.
  3. **Incidental exceptional cases** (Art. 9/6 a-f), only if incidental.
- **Explicit consent is no longer a general basis for transfers abroad.** It is now only one of
  the incidental exceptions (Art. 9/6-a). Under Art. 9/7 it **does not apply to public-law
  activities of public institutions** (repeated in Cross-border Reg. Art. 16/4).
- "Transfer" includes making data **accessible** abroad (Cross-border Reg. Art. 4/1(e)). A read
  grant to a foreign party or a foreign-hosted service is a transfer record, not just a file
  export.
- The pre-2024 regime (consent, or a "safe countries" list / undertaking + permission) is not in
  force. Do not build enum values on it.
