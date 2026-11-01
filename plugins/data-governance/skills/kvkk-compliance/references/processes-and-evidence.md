# Processes and Evidence

A catalog records and detects. The engine, storage and orchestrator act. People decide. This
file describes the KVKK processes that need all three, and the evidence each one leaves. Field
names refer to [metadata-model.md](metadata-model.md). Obligation numbers refer to
[obligations-map.md](obligations-map.md).

> **Not legal advice.** The steps show the technical side. Counsel decides on replies, legal
> bases, exemptions and notifications.

## 1. Data subject request (DSR) — 30 days

Law Art. 11, 13/2; Application Comm. Art. 5/1, 6/5. Obligations 19–22.

1. **Open a DSR record** with `application_date`, `channel`, `right` (Art. 11/1 letter) and an
   owner. The 30-day clock runs from the application date. Application Comm. Art. 5/1 explicitly
   allows applications through "software or an application developed for the purpose".
2. **Find the person's data.** Search the catalog for columns classified as the identifier used
   in the request, such as a national ID or customer number (D6.1 classification + column search).
   This is the level-2 capability that all three measured products have.
3. **Expand with lineage.** Walk downstream from every hit, at column level where available, to
   reach derived tables, extracts, reports and published outputs. Walk upstream to the system of
   record. Lineage coverage is the limit: a copy made by an unlineaged script is invisible.
4. **List recipients** (Art. 11/1(ç)): processors and transfer records attached to every asset in
   the expanded set, domestic and abroad.
5. **Act in the engine.** Rectification or erasure runs on the data platform. If the erased data
   had been transferred, notify the recipients (Art. 11/1(f); Destruction Reg. Art. 12/1(b)).
6. **Close** with the reply, actions per asset and `closed_at`. Alert when the request nears
   30 days.

**Evidence:** DSR record (dates, right, owner, reply), the affected-asset list exported at the
time, and the engine job logs for rectification or erasure.

**Market state:** OpenMetadata `partial` on rows 19–21 (column search + PII tags + workflows), DataHub
`weak` (approval workflow D7.2 = 1), Atlas `none` (D7.2 = 0). No product has a request type
mapped to Art. 11, an application date or a remaining-time indicator. A generic ticket or
access-request flow does not meet D14.5 level 3.

## 2. Consent withdrawal

Law Art. 7/1; Destruction Reg. Art. 7/1; Information Comm. Art. 5/1(f). Obligations 16–18.

1. A consent store, usually outside the catalog, emits a withdrawal for a person and purpose.
2. Find the assets whose activity has `legal_basis = art5_1_explicit_consent` for that purpose.
   Expand downstream via lineage.
3. For each asset, check whether **another** valid basis covers the processing. If it does,
   record that basis and the reasoning, and leave the asset out of destruction scope. If not, the
   conditions have ceased and the data must be erased, destroyed or anonymised ex officio.
4. Track closure per affected asset (D14.4 level 4).

## 3. Breach scoping — 72 hours from learning

Law Art. 12/5; BD 2019/10, 2019/271. Obligations 23–26.

1. **Open a breach record** with `learned_at` separate from `occurred_at` and `created_at`.
   BD 2019/10 starts the 72 h from **learning**, and late notices must explain the delay.
   A data-quality "incident" object is not a breach record.
2. **Scope:** start from the compromised assets (access logs, D6.6). Add downstream copies via
   lineage. Pull data categories and subject groups from the inventory fields.
3. **Split the report by category and by personal vs special-category** (BD 2019/271). A flat
   asset list does not satisfy the notice content.
4. Record consequences, measures taken, `board_notified_at` and any `delay_reason`. Keep the record
   immutable and exportable for Board review (BD 2019/10).

**Market state:** every product reaches only level 2 (category report on a selected asset set). None
adds lineage copies automatically, splits special categories in the report, or has a learned-at
countdown. Atlas is `none` on rows 24 and 26 (alerting 0, PII detection 0).

## 4. Periodic destruction — at most every 6 months

Destruction Reg. Art. 4/1(c), 6/1(g),(ğ), 7/3, 7/4, 11/2, 11/3, 12/1(b). Obligations 10–15.

1. **Policy:** the retention-destruction policy contains the retention table and the periodic
   destruction periods (Art. 6/1(g),(ğ)) and explains the methods used (Art. 7/4). Generate the
   table from `max_retention` + `destruction_method` instead of maintaining it by hand.
2. **Sweep** at least every 6 months (Art. 11/2): select assets and partitions whose retention has
   ended. Run it from the orchestrator or from the catalog's scheduled workflows where they exist.
   A controller with no policy obligation must destroy within 3 months of the duty arising
   (Art. 11/3).
3. **Execute in the engine** with the recorded method: erasure, destruction or anonymisation
   (Art. 8, 9, 10). Anonymisation is a legal destruction method. Its technique and measurement
   are covered in the `data-anonymization` skill.
4. **Wait for physical completion.** In table formats with snapshots, a `DELETE` is logical. In
   Apache Iceberg, the bytes go only after `rewrite_data_files → expire_snapshots →
   remove_orphan_files`. Defaults put a floor of about 8 days on this (snapshot age 5 days +
   orphan-file age 3 days), and Trino rejects snapshot expiry under 7 days by default. A tag kept
   "for audit" pins personal data indefinitely, because tag max-ref-age defaults to infinite.
   Close the record when the chain completes, not when the `DELETE` returns.
5. **Write the destruction record** back to the catalog: run id, run timestamp, scope (assets,
   partitions, row counts), method, policy version, engine job reference, chain-completion time,
   and the operator. Append-only, exportable, kept **≥ 3 years** (Art. 7/3).
6. **Notify recipients** of destroyed data found via lineage and transfer records (Art. 12/1(b)).
7. **Alert** when a sweep misses its window or expired data remains after a run.

**Anonymisation caveats** (technical, from the research's anonymisation work):
- The anonymised output is a new dataset. The **source stays personal data** and needs its own
  destruction. That makes two operations and two records.
- A record proves a **measurement** (e.g. k measured on a snapshot with a given quasi-identifier
  set and policy version), not the legal conclusion "this is anonymous". k-anonymity alone is not
  considered sufficient (Article 29 Working Party Opinion 05/2014, WP216).
- Snapshot history in table formats is not cryptographically chained and expires with the
  snapshot. Copy the measurement result into the catalog, and use WORM storage or an external
  audit log if tamper-evidence is required.

**Market state:** all three products are `partial` on recording the method and exporting the table.
None has a data-destruction job, a 6-month bound or destruction minutes. OpenMetadata's
`DataRetention` app cleans catalog-internal records only. Applying anonymisation (D6.8) is 0 in
all three.

## 5. VERBİS inventory and the 7-day change rule

VERBİS Reg. Art. 4/1(h), 5/1(ç), 9/2, 9/4, 13; Law Art. 16/3. Obligations 1–5.

1. **Export** one row per (asset, activity) with: purpose, legal basis, data category, subject
   group, recipient group, max retention per data category, transfers abroad and measures. This is
   the VERBİS Reg. Art. 4/1(h) field set. Registry disclosures must be based on the inventory
   (Art. 5/1(ç), 9/2), so keep the catalog as the single source.
2. **Snapshot and diff** the export on a schedule. A changed purpose, a new category or a new
   transfer becomes a dated diff for an owner. Registered information must be updated in VERBİS
   within **7 days** of a change (Art. 13).
3. **Feed discoveries in:** a new personal-data column found by a scan opens an inventory task
   (D14.1 level 4). No product does this automatically today.
4. **Scope by activity:** activities exempt under Art. 28/2 may drop out of the registry export,
   but they stay in the catalog with their exemption clause.

## 6. Publication of personal data (public bodies)

PD 2026/1301; Law Art. 4/2(ç),(d). Obligations 46–49. Flag published outputs, record the masking
or anonymisation method and the publication period, list expired publications for removal and
**scan already-published content retrospectively**. A forward-only rule engine is not enough.
Row 47 is `none` in all three products, because no product applies masking or anonymisation to a
published output.

## 7. What a catalog can prove to an auditor

| Question | Catalog can prove (if modelled and filled) | Needs a process or another system |
|---|---|---|
| What personal data do we hold, where? | Classified assets/columns, inventory export, lineage | Coverage of unscanned and unlineaged stores |
| Why and on what basis? | Purpose + legal basis per activity, change history | That the basis is legally right (counsel) |
| How long, and was it destroyed? | Retention fields, retention table, destruction records written back | That the engine actually deleted the bytes; the 6-month schedule ran |
| Who received it? | Transfer and counterparty records, lineage to outputs | Contracts, Board permissions, standard-contract notifications |
| Were people informed? | Notice version linked to assets on a date | That the notice was actually given (burden on the controller, Information Comm. Art. 5/1(e)) |
| Was consent obtained or withdrawn? | Consent state and version per asset | The consent records themselves (consent store) |
| Was a DSR answered in 30 days? | DSR record with dates, affected assets | Reply letter, identity check, engine actions |
| Was a breach notified in 72 h? | Breach record with learned-at, scope report | Board form submission, notices to subjects |
| Are special categories protected? | Category tags, linked policies, access logs | Encryption, 2FA, periodic access review (BD 2018/10) in source systems |

The catalog never proves **enforcement on data reads**. In all three products the built-in
authorisers decide access to catalog entities and actions, not to rows in the warehouse.
Data-plane enforcement happens elsewhere: query-engine or policy-engine rules (which may use
catalog tags), masking views and storage ACLs. Evidence it separately and link that evidence from
the catalog.
