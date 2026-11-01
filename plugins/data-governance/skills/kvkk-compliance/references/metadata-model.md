# A Tool-Neutral KVKK Metadata Model

This is the field set a catalog must carry for the obligations in
[obligations-map.md](obligations-map.md). It is written without assuming any product. The last
section maps it to OpenMetadata, DataHub and Apache Atlas as examples.

> **Not legal advice.** Enum values follow the Law's wording (checked against the consolidated
> text of Law No. 6698). Which value applies to which activity, and how long data is kept, is for
> counsel and the data owner to decide.

## Design rules

1. **Record per processing activity, attach to assets.** Purpose, legal basis and retention belong
   together (D14.2 level 4: several purpose–basis pairs per asset, each with its own retention).
   Keep them as one composite record per activity, not as three loose fields.
2. **Closed lists, never free text,** for anything an auditor or a rule will read: legal basis,
   destruction method, consent state, special category, transfer mechanism, counterparty role.
3. **Column overrides asset.** A table can be "contract" while one column is "explicit consent" or
   special-category. Allow a column-level value that overrides the asset-level one.
4. **Every deadline has its own start field** (application date, learned-at, retention end,
   signature date). A computed due date without its start field cannot be audited.
5. **Records that prove something are append-only:** destruction minutes, breach record, DSR
   closure. Keep them exportable for at least 3 years (Destruction Reg. Art. 7/3).
6. **Enforced vs convention is explicit.** Note for each field whether the platform enforces it
   or whether only a rule check detects violations.

## Field set

| Group | Field | Type / values | Obligation (row in map) | D14 |
|---|---|---|---|---|
| Inventory | `personal_data` | bool, derived from classification | 1 | 14.1 |
| | `data_categories` | list, org-defined enum (VERBİS Reg. Art. 4/1(m)) | 2, 3 | 14.1 |
| | `data_subject_groups` | list, org-defined enum (VERBİS Reg. Art. 4/1(n)) | 2, 3 | 14.1 |
| | `recipient_groups` | list, org-defined enum | 2, 3 | 14.1 |
| | `security_measures` | list: technical / administrative measures | 2, 3 | 14.1 |
| Activity | `purpose` | org-defined enum | 6 | 14.2 |
| | `legal_basis` | closed enum, see below | 6, 7 | 14.2 |
| | `exemption_clause` + `exemption_reason` | Art. 28/1(a)–(d) or 28/2(a)–(ç) + text | Art. 28 | 14.2 |
| | `max_retention` | ISO 8601 duration, per data category | 4, 10 | 14.1, 14.3 |
| | `retention_basis` | statute or purpose reference | 10 | 14.3 |
| Destruction | `destruction_method` | `erasure` / `destruction` / `anonymisation` | 13 | 14.3 |
| | `destruction_cycle` | duration ≤ P6M | 12 | 14.3 |
| | `last_destruction_run` | ref to destruction record | 14 | 14.3 |
| Consent | `consent_state` | `given` / `not_given` / `withdrawn` | 16, 18 | 14.4 |
| | `consent_text_version` + `consent_date` | string + date, separate from notice | 17 | 14.4 |
| Special | `special_category` | list of the 11 Art. 6/1 values | 34 | 14.8 |
| | `special_basis` | Art. 6/3 (a)–(g) | 34, 35 | 14.8 |
| | `extra_measures` | BD 2018/10 checklist status | 36 | 14.8 |
| Transfer | `transfers[]` | see transfer record | 27–33 | 14.7 |
| Counterparty | `processors[]` | ref to counterparty record | 38–42 | 14.9 |
| Notice | `privacy_notice` | ref to notice object + version | 43–45 | 14.10 |
| Publication | `published`, `publication_url`, `published_at`, `publication_expiry`, `publication_masking` | bool, url, date, date, enum | 46–49 | 14.11 |
| Location | `storage_country`, `storage_region`, `hosting` (`on_prem` / `domestic_cloud` / `foreign_cloud`) | enum | 27, 33 | 14.7 |

### Legal-basis enum (Law Art. 5, Art. 28)

| Value | Law text |
|---|---|
| `art5_1_explicit_consent` | Art. 5/1, the default rule |
| `art5_2_a_provided_by_law` | expressly provided for by law |
| `art5_2_b_vital_interest` | life or physical integrity of a person unable to consent |
| `art5_2_c_contract` | directly related to forming or performing a contract with the subject |
| `art5_2_c2_legal_obligation` | Art. 5/2(ç): necessary for the controller's legal obligation |
| `art5_2_d_made_public` | made public by the data subject |
| `art5_2_e_legal_claims` | establishing, exercising or protecting a right |
| `art5_2_f_legitimate_interest` | controller's legitimate interest, without harming fundamental rights |
| `out_of_scope_art28_1` | full exemption; `exemption_clause` names (a)–(d), e.g. (b) official statistics after anonymisation |

Special categories use **Art. 6/3 (a)–(g)** instead (after Law No. 7499, one list for all
categories): (a) explicit consent, (b) provided by law, (c) vital interest, (ç) made public by the
subject in line with that intent, (d) a right, (e) health services by persons under secrecy or
authorised bodies, (f) employment, OHS, social security, social services and assistance
obligations, (g) foundations, associations and similar bodies, limited to members.

**Special categories (Art. 6/1), one value each:** race, ethnic origin, political opinion,
philosophical belief, religion/sect/other belief, dress, membership of an association, foundation
or trade union, health, sex life, criminal conviction and security measures, biometric data,
genetic data. The research counts these as 11 categories, grouping "race and ethnic origin" and
"religion, sect and other beliefs". Keep them separable either way. A breach notice must split
special from ordinary data (BD 2019/271).

**DSR types (Art. 11/1):** (a) learn whether processed, (b) request information, (c) learn purpose
and use, (ç) know domestic/foreign recipients, (d) rectify, (e) erase/destroy under Art. 7,
(f) notify (d)/(e) to recipients, (g) object to an adverse result of solely automated analysis,
(ğ) claim damages.

### Transfer record (one per recipient relationship)

`recipient`, `recipient_role` (`controller` / `processor`), `country`, `abroad` (bool),
`mechanism` (`adequacy`, `safeguard_agreement`, `safeguard_bcr`, `safeguard_standard_contract`,
`safeguard_undertaking`, `incidental_art9_6_a` … `_f`), `mechanism_document`, `permit_date`,
`valid_until`, `signed_at` (standard contract, for the 5-business-day notice), `notified_at`,
`onward_transfer_of` (ref), `includes_special_category` (bool), `access_mode` (`push` / `read_grant`).
Do not offer consent as a general mechanism. It is `incidental_art9_6_a` only, and that value
must be unavailable for public-law activities of public institutions (Art. 9/7).

### Counterparty, notice, destruction, breach and DSR records

- **Counterparty:** identity, role (`controller` / `processor` / `joint_controller`, decided by
  who sets purposes and means, not by the org chart), contract reference, contract date,
  `valid_until`.
- **Privacy notice:** id, version, effective date, superseded-by; linked assets with link dates,
  so you can answer "which version applied on date X".
- **Destruction record:** see [processes-and-evidence.md](processes-and-evidence.md).
- **Breach record:** `occurred_at`, `learned_at`, `created_at` (three separate fields), affected
  assets, categories split personal / special, subject groups, consequences, measures,
  `board_notified_at`, `delay_reason`.
- **DSR record:** `application_date`, `channel` (Application Comm. Art. 5/1), `right` (Art. 11/1
  letter), owner, affected assets, reply, `closed_at`.

## Example (YAML, tool-neutral)

```yaml
asset: warehouse.sales.customer_orders
kvkk:
  personal_data: true
  data_categories: [identity, contact, customer_transaction]
  data_subject_groups: [customer]
  recipient_groups: [authorised_public_bodies, processors]
  security_measures: [access_logging, encryption_at_rest, role_based_access]
  activities:
    - purpose: order_fulfilment
      legal_basis: art5_2_c_contract
      max_retention: P10Y
      retention_basis: "statutory bookkeeping period (counsel to confirm)"
    - purpose: marketing_analytics
      legal_basis: art5_1_explicit_consent
      consent: {state: given, text_version: "v3", date: 2026-03-01}
      max_retention: P2Y
  destruction: {method: anonymisation, cycle: P6M, last_run: dr-2026-09-30-017}
  privacy_notice: {id: notice-customers, version: "4.1"}
  processors: [cp-cloud-backup]
  transfers:
    - recipient: cp-cloud-backup
      recipient_role: processor
      country: TR
      abroad: false
columns:
  national_id: {special_category: [], data_categories: [identity]}
  health_note: {special_category: [health], special_basis: art6_3_a_explicit_consent}
```

## Enforced vs convention

| Property | Who enforces it | How to close the gap |
|---|---|---|
| Value is from the closed list | Platform (enum/allowed values) in all three products | — |
| Field is filled | Convention in OpenMetadata and DataHub. Atlas validates `isOptional=false` entity-type attributes on write | Rule check + owner assignment (DataHub Forms; OpenMetadata workflow rules; any orchestrator) |
| Value is correct | Nobody | Steward review, approval workflow on change |
| Retention is acted on | Nobody in the catalog | Destruction job in the engine/orchestrator |
| 6-month cycle holds | Nobody | Scheduled sweep + alert on overdue |
| Notice linked and current | Nobody | Drift rule: purpose/recipients changed after the notice's effective date |
| Change history | Platform (versioning: D1.4 OM 3, DH 3, Atlas 2) | Export diffs for the 7-day VERBİS rule |

## Mapping to three products (examples, not prescriptions)

| Model element | OpenMetadata 1.13.3 | DataHub v1.7.0 | Atlas 2.5.0 |
|---|---|---|---|
| Typed field on asset/column | Custom property (only on entities with `extension`: table, column, database, schema, container, glossary term, data product, …) | Structured property (any entity incl. `schemaField`) | Business Metadata; entity-type attribute (mandatory possible) |
| Closed list | Enum custom property | `allowedValues` | `AtlasEnumDef` |
| Special category | Own 11-tag classification (built-in `SpecialCategory` is one tag) | Tags or glossary terms | Classification types with hierarchy |
| Notice / counterparty / transfer / breach / DSR object | No new entity types; glossary terms with custom properties as stand-ins | `document` for notices, `corpGroup` + structured property for parties; custom entity types possible via model extension (D1.1 = 4) | Custom entity types via typedef REST (D1.1 = 4) |
| Empty-field detection | Workflow rule / attribute check | `IS_NULL` filter + Forms | `IS_NULL` search |
| Export | Recursive CSV export | API / GraphQL | CSV download, REST |

Custom entity types (DataHub, Atlas) are a real route to transfer, breach and DSR objects. They
were **not** counted in the D14 scores, which measure what the product ships. Budget for building
and upgrading them as your own model.
