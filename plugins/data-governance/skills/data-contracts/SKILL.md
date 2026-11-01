---
name: data-contracts
description: "Vendor-neutral guide to versioned data contracts between data producers and consumers: what a contract holds (owner, schema, semantics, quality rules, freshness and availability SLA, security and terms of use, lifecycle status), an illustrative ODCS-style contract YAML, schema compatibility modes (BACKWARD, FORWARD, FULL), a breaking-change table mapped to semantic versioning, deprecation windows with consumer sign-off, and a CI contract-check outline. Grounded in a source-code comparison on rubric capabilities D9.3 data contracts, D9.1-D9.2 domains and data products (ownership, input/output ports, visibility, lifecycle), D1.3 schema openness, D1.4 version history, D1.6 change events and D7.2 approval workflows, with path:line evidence for OpenMetadata 1.13.3, DataHub 1.7.0 and Apache Atlas 2.5.0. Research-measured facts are tagged; general practice is marked as such. Use when publishing a new dataset or data product, changing a schema, or deciding where contracts live and how they are enforced."
---

# Data Contracts

A data contract is a versioned, machine-readable promise from a producer to its consumers. It
covers what the data looks like (schema), what it means (semantics), how good and how fresh it
will be (quality and SLA), who may use it and how (security, terms), and how it may change. This
skill gives a vendor-neutral contract format and change process. It also shows how far three
open-source catalogs go in storing and checking contracts, measured against their source code.

**Bottom line.** Store the contract in Git, check it in CI, and register it in the catalog.

- OpenMetadata 1.13.3 has a first-class contract entity. It validates the schema, semantic rules
  and quality tests, and imports/exports ODCS (D9.3 = 3).
- DataHub v1.7.0 stores contracts but does not validate them in OSS (D9.3 = 2).
- Apache Atlas has no contract concept (D9.3 = 1).
- **No product reaches level 4.** None analyses whether a change is backward-compatible, and none
  blocks an incompatible change until consumers agree. That gate is yours to build, in CI.

Evidence tags: **K** = verified in source code, **D** = official docs, **İ** = issue/roadmap,
**B** = vendor claim. **Untagged statements are general practice**: established industry
convention, not measured by the research. `?`/`null` ≠ 0.

## When to Use This Skill

- Publishing a new dataset, topic or data product that other teams will consume.
- Reviewing a schema change (column drop, rename, type change) before it ships.
- Designing the contract template, the review/approval flow and the deprecation policy.
- Choosing which catalog features to rely on and which to build outside (CI, orchestrator).
- Assessing products on D9.3 and the capabilities a contract programme depends on.

## Decision Table

| Need | Technique | Reference |
|---|---|---|
| What goes into a contract; a template to copy | Contract anatomy + illustrative ODCS-style YAML | [contract-spec.md](references/contract-spec.md) |
| Map the template onto a catalog's contract object | OM `DataContract` / DH `DataContractProperties` / ODCS field map | [contract-spec.md](references/contract-spec.md) |
| Is this change breaking? Which version bump? | Change-classification table, compatibility modes, semver | [change-management.md](references/change-management.md) |
| How long to keep a deprecated field | Deprecation windows by consumer tier | [change-management.md](references/change-management.md) |
| Block incompatible changes | CI contract-check outline (diff → classify → gate → sign-off) | [change-management.md](references/change-management.md) |
| Who owns a contract; where it hangs in a mesh | Domain → data product → ports → contract; approval workflows | [data-products-and-ownership.md](references/data-products-and-ownership.md) |
| Cite a source | Research files, versions, standards | [sources.md](references/sources.md) |

## Contract Anatomy (general practice)

| Section | Holds | Checked by |
|---|---|---|
| Identity | id, name, version (semver), status (draft/active/deprecated/retired), effective dates | CI lint |
| Ownership | producer team, owner, support channel, domain, data product | Catalog owners (D7.1) |
| Schema | columns/fields, logical + physical type, required, unique, primary key, classification | CI diff + runtime schema check |
| Semantics | business definitions, glossary links, units, allowed values, invariants | Review + rule checks |
| Quality | rules per column/table with thresholds and tier (error/warn) | Pipeline gate + scheduled suite (see `data-quality-slas`) |
| SLA | freshness / max latency, availability time, retention, support hours | Freshness sensor; incident timers |
| Security and terms | classification, access policy reference, permitted use, row filters | Data-plane enforcement, not the catalog |
| Consumers | known consumers and their pinned major version | Change-impact notice |
| Change policy | compatibility mode, deprecation window, approvers | CI gate |

## Measured at a Glance (0–4)

| ID | Capability | OM 1.13.3 | DH v1.7.0 | Atlas 2.5.0 |
|---|---|---|---|---|
| D9.3 | Data contract definition and validation | **3** (K) | 2 (K) | 1 (K) |
| D9.2 | Data product as a first-class entity | **3** (K) | 2 (K) | 1 (İ) |
| D9.1 | Domain modelling and ownership | 3 (K) | 3 (K) | 1 (K) |
| D1.3 | Schema standard and openness | 3 (K) | 3 (K) | 2 (K) |
| D1.4 | Metadata versioning and history | 3 (K) | 3 (K) | 2 (K) |
| D1.6 | Change events / webhooks | 3 (K) | 3 (K) | 3 (K) |
| D7.2 | Approval and review workflow | **3** (K) | 1 (K) | 0 (İ) |
| D11.6 | Governance as code (GitOps) | 2 (K) | **3** (K) | 2 (K) |

Notable verified details (K):
- OM contract validation counts **missing and duplicate fields as failures only**. A type mismatch
  is recorded "for informational purposes" and does not fail the check
  (`DataContractRepository.java:456-458`).
- OM's contract SLA (`refreshFrequency`, `maxLatency`, `availabilityTime`) is stored but not
  evaluated.
- DataHub classifies a column removal or type change as `MAJOR` (backwards-incompatible) and an
  addition as `MINOR`. It also labels a **rename** `MINOR` ("forwards & backwards compatible"),
  even though a rename breaks every SQL consumer. It uses these labels only for change events,
  not as a gate.

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Contract lives only in the catalog UI | No review, no diff, no CI gate | Git is the source of truth; catalog is the registry |
| Relying on catalog schema validation to catch type changes | OM treats type mismatch as non-failing (K) | CI diff with your own type-compatibility rules |
| Writing an SLA field and assuming it is monitored | OM does not evaluate contract SLA (K); DH OSS does not validate (K) | Freshness sensor + incident timer outside |
| `rowFilters` / access policy in the contract read as enforcement | Documentation only in OM; catalogs do not sit in the data path (K) | Enforce in engine/storage; reference from contract |
| Renaming a column as "minor" | Every consumer query breaks | Treat rename as add-new + deprecate-old (major on removal) |
| No consumer list | Cannot notify or get sign-off | Register consumers; use lineage to find the unregistered ones |
| Deprecation with no end date | Old fields live forever | Fixed window by consumer tier; announce, warn, remove |
| Using the DataHub `datacontract` CLI | Deprecated in v1.7.0 (K) | Manage contracts through your own YAML + API |

## References

- [references/contract-spec.md](references/contract-spec.md): anatomy, illustrative YAML, field mapping to OM/DH/ODCS, what each product validates.
- [references/change-management.md](references/change-management.md): compatibility modes, change table, semver, deprecation, CI check outline, measured versioning/events.
- [references/data-products-and-ownership.md](references/data-products-and-ownership.md): D9/D7 scores, data product model, ports, visibility, approval workflows.
- [references/sources.md](references/sources.md): research files, product versions, standards.
- Related skills: `data-quality-slas` (quality rules and SLOs referenced by a contract),
  `data-catalog-lineage` (finding consumers for change impact), `access-control-policies`
  (enforcing the contract's security section).
