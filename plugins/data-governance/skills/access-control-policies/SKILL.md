---
name: access-control-policies
description: "Vendor-neutral sensitive-data classification and access control for a data platform, with source-code evidence: the D6 security/privacy/compliance rubric (8 capabilities, 0-4 levels); PII auto-classification and tag propagation through lineage; RBAC vs ABAC; policy store vs policy decision/enforcement point (the catalog holds classification and policy metadata, query engines and Ranger-style tools enforce); row/column-level security, dynamic masking, tokenisation and where each is enforced; audit logging and access reviews; illustrative policy-as-code (SQL secure views, tag-based masking, Ranger-style JSON, drift check); and how OpenMetadata 1.13.3, DataHub 1.7.0 and Apache Atlas 2.5.0 score on D6 and why, incl. the catalog-to-enforcer bridge and which direction is hard. Use when designing a classification taxonomy, choosing where a control is enforced, writing masking or row-filter policies, syncing catalog tags to an enforcement tool, planning audit or access reviews, or judging a catalog's security claims."
---

# Access Control Policies

Access control on a data platform has two halves that are often confused: **knowing** which data is
sensitive (classification, held in the catalog) and **enforcing** who may read it (row/column
security, masking, held in the query engine or a Ranger-style authorisation tool). The bottom line
from measuring three open-source catalogs against an 8-capability rubric (domain D6):

- **No measured catalog sits in the data path.** Query-time masking (D6.3) and row/column security
  (D6.4) score 0-1 in OpenMetadata 1.13.3, DataHub v1.7.0 and Apache Atlas 2.5.0. Their policy
  engines govern **metadata** (who sees or edits a catalog entry), not data reads.
- **Classification belongs in the catalog; enforcement belongs in the engine.** Expect the catalog
  to *derive and verify* enforcement, not to perform it.
- **The bridge is the real gap.** Pushing catalog tags into a tag-based policy store already exists
  (Atlas -> Ranger tagsync is mature). The reverse — reading what the enforcer actually applies and
  reporting "tagged sensitive but unprotected" drift — was found in **no** product.

Anonymisation and privacy models (k-anonymity, differential privacy, D6.8 = 0 everywhere) are out of
scope here; see the `data-anonymization` skill.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. All D6 scores below are **K** unless marked. `?` or "not verified" means not
measured or not proven absent — never read it as 0.

## When to Use This Skill

- Designing a sensitivity taxonomy and an auto-classification pipeline with steward review.
- Deciding whether a control (mask, row filter, column deny, tokenise) lives in the catalog, the
  engine, the ingestion pipeline or an external policy server.
- Writing masking views, row filters or tag-based policies as code.
- Wiring catalog classifications to Ranger/OPA/engine-native policies and detecting drift.
- Specifying audit logs and periodic access reviews an auditor will accept.
- Reading a catalog's "security" or "governance" claims critically (metadata plane vs data plane).

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| Taxonomy, detection levels, false-positive loop | Sensitivity levels + data-type tags, confidence + review | [classification.md](references/classification.md) |
| Keep tags right after copies and joins | Lineage-based propagation as a reviewed hint | [classification.md](references/classification.md) |
| RBAC or ABAC | Roles for coarse grants, attributes (tag, domain, region) for the rest | [policy-enforcement.md](references/policy-enforcement.md) |
| Hide columns / filter rows | Engine-native policy or Ranger row filter; secure view as fallback | [policy-enforcement.md](references/policy-enforcement.md) |
| Mask, tokenise, pseudonymise | Dynamic mask at the engine; tokenise at ingest; audited re-identification | [policy-enforcement.md](references/policy-enforcement.md) |
| Catalog tags -> enforcement tool | Forward sync, then drift report, delegation last | [policy-enforcement.md](references/policy-enforcement.md) |
| Audit and access reviews | Decision log + engine access log + recertification | [policy-enforcement.md](references/policy-enforcement.md) |
| Rubric levels and product scores | D6.1-D6.8 definitions, scores, evidence, why | [d6-comparison.md](references/d6-comparison.md) |

## Where Each Control Lives

| Control | Decided / stored in | Enforced in | Catalog's job |
|---|---|---|---|
| Sensitivity classification | Catalog (tags, classifications) | — | Detect, review, version, propagate |
| Policy definition | Policy store (Ranger, OPA, engine-native) | — | Hold policy *metadata*: intent, owner, link |
| Access decision (PDP) | Ranger / OPA / engine | — | Explain, not decide |
| Column deny, row filter | Policy store | Query engine / warehouse (PEP) | Verify coverage against tags |
| Dynamic masking | Policy store | Query engine at read time | Verify coverage; record method |
| Tokenisation / static masking | Pipeline config + key vault | Ingestion / transformation job | Record method, key domain, lineage |
| Catalog sample data masking | Catalog | Catalog UI/API only | Its own previews — not the data |
| Data-access audit | Engine / Ranger audit | — | Join with tags for reporting |
| Metadata-change audit | Catalog | — | Append-only log, export |

## D6 at a Glance

| ID | Capability | OM 1.13.3 | DataHub 1.7.0 | Atlas 2.5.0 |
|---|---|---|---|---|
| D6.1 | PII detection and auto-classification | **3** | 1 | 0 |
| D6.2 | Policy engine (RBAC/ABAC) — metadata plane | **4** | 3 | 2 |
| D6.3 | Masking and pseudonymisation | 1 | 1 | 0 |
| D6.4 | Row- and column-level access control | 0 | 1 (D) | 1 |
| D6.5 | Retention policy definition | 2 | 2 | 2 |
| D6.6 | Audit trail and immutability | 3 | 3 | 2 |
| D6.7 | External authorisation engine | 1 | 2 | **3** |
| D6.8 | Anonymisation / PETs (see `data-anonymization`) | 0 | 0 | 0 |

Reading: OpenMetadata is strongest at **knowing** (detection, ABAC on metadata, decision
explanation); Atlas is strongest at **delegating** (`atlas.authorizer.impl=RANGER`); nobody
enforces on data. Details and evidence: [d6-comparison.md](references/d6-comparison.md).

## Core Rules

1. **Classify by sensitivity and by data type, separately.** A level tag (`Sensitive`) drives the
   default control; a type tag (`EMAIL`, `NATIONAL_ID`) drives the masking method.
2. **Every tag that implies a control needs a matching enforcer policy** — or a recorded reason why
   not. A tag alone protects nothing.
3. **Prefer tag-based (ABAC) enforcer policies** over per-column policies: new tagged columns are
   covered without a policy change.
4. **Enforce once, at the engine.** Secure views and pipeline masking are fallbacks for engines
   without native policies, not a second layer to keep in sync by hand.
5. **Reconcile continuously.** Schedule a drift job: tagged-but-unprotected, protected-but-untagged,
   tag-policy referencing a tag nobody uses.
6. **Log decisions, not just changes.** Catalog audit logs record metadata edits; data access is
   logged by the engine/enforcer. An auditor needs both, joined.
7. **Time-box and recertify grants.** Access with no expiry and no periodic review fails most
   special-category requirements (e.g. KVKK Board Decision 2018/10).

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Treating catalog ABAC as data security | OM/DataHub/Atlas policies gate catalog operations, not `SELECT` | Enforce in engine/Ranger; catalog verifies |
| A column shows "Sensitive" so it is assumed masked | No product reconciles tag vs enforcer policy | Drift report on a schedule |
| Trusting `dataContract.rowFilters` (OM) | Documentation only; nothing evaluates it | Generate the engine row filter from it |
| Relying on default PII tiers | OM maps phone, location, date, NRP, URL to `NonSensitive`; its masker only masks `PII.Sensitive` | Review the type -> level mapping |
| Auto-tags applied without review | False positives erode trust; false negatives leave data open | Confidence threshold + steward approval |
| Propagating tags through every lineage edge | Aggregates and hashes inherit tags they no longer need; over-masking | Propagate on identity-preserving edges; review |
| A second, hand-written policy store per engine | Inconsistent decisions, unauditable | One PDP or one generator feeding all PEPs |
| Hash as "anonymisation" | Deterministic hashes are linkable and reversible by dictionary | Treat as pseudonymisation; see `data-anonymization` |
| Expecting the catalog's audit to show reads | Catalogs log metadata changes; reads are in engine logs | Ship engine/Ranger audit to the SIEM |

## References

- [references/classification.md](references/classification.md): taxonomy, detection levels,
  measured detectors, propagation through lineage, review loop.
- [references/policy-enforcement.md](references/policy-enforcement.md): RBAC/ABAC, PEP/PDP/PAP,
  row/column security, masking and tokenisation, illustrative policy-as-code, the catalog-enforcer
  bridge, audit and access reviews.
- [references/d6-comparison.md](references/d6-comparison.md): D6.1-D6.8 level definitions, scores
  and evidence for the three products, cross-product findings.
- [references/sources.md](references/sources.md): research files, product versions, standards.
- Related skills: `data-catalog-lineage` (column lineage that propagation depends on),
  `data-contracts` (where row filters and classification are declared), `data-anonymization`
  (privacy models, D6.8), `kvkk-compliance` (special categories, legal duties); OpenMetadata-specific:
  `openmetadata-gap-closing` (closing the Ranger link in OpenMetadata), `openmetadata-kvkk-compliance`.
