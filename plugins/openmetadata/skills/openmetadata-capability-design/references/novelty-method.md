# Novelty Check Method

"No product does X" is the most attractive and the weakest sentence in a capability proposal.
The research treated it as a claim that must be earned: every candidate was searched in four
places, misses were recorded as misses, and a candidate whose distinguishing constraints all
turned up elsewhere was eliminated rather than reworded. Product facts are for
OpenMetadata 1.13.3 (`1.13.3-release`, `255f6694913b`), DataHub v1.7.0, Apache Atlas 2.5.0.

## Where Ideas Come From: the Class 1 Opportunity Space

Gaps were split into five non-overlapping classes (rule applied in order, first match wins):

| Class | Rule | Count |
|---|---|---|
| 5 - KVKK & regulatory | capability is in the `D14` domain | 11 |
| 2 - competitor has it, OM does not | `max(competitor) − OM ≥ 2` | 5 (after verification) |
| 1 - absent in all products | `max(all products) ≤ 1` | 9 (after verification) |
| 3 - present but weak | everything else | 20 |
| 4 - platform-specific | separate axis: only visible against the target stack | 8 |

The Class 1 threshold is deliberately strict: no product reaches level 2 ("mechanism exists, you
build it yourself"). A looser `≤ 2` would have put 41 capabilities in "absent everywhere" and made
the claim meaningless. Level-2 items are "automated nowhere", not "absent everywhere".

Class 1 after issue/roadmap verification:

| Capability | OM | DataHub | Atlas | Verification |
|---|---|---|---|---|
| D6.8 Anonymisation / privacy-enhancing techniques | 0 | 0 | 0 | no record anywhere (OM #23135 is preview scrambling only) |
| D8.4 Golden record / entity resolution | 0 | 0 | 0 | no record anywhere |
| D8.3 MDM integration | 1 | 1 | 0 | no record (OM #12558 closed) |
| D12.5 Multilingual *user content* | 1 | 1 | 0 | no record - every hit is UI translation |
| D6.4 Row/column-level access control | 0 | 1 | 1 | planned - column only, OM PR #27396 (2.0-targeted); row level nowhere |
| D6.3 Masking / pseudonymisation | 1 | 1 | 0 | planned (partial) - reversible pseudonymisation nowhere |
| D9.6 External data sharing agreements | 1 | 1 | 1 | planned - DataHub RFC #17242 (internal contracts) |
| D12.3 Multi-tenancy | 1 | 1 | 1 | planned in all three (ATLAS-872, DataHub PR #15762, OM #26202) |
| D5.6 Anomaly detection | 1 | 0 | 0 | commercial - DataHub Cloud `saasOnly`; OM leads in OSS |

D9.4 (access request workflow) moved from Class 1 to Class 2 during verification: DataHub Cloud
documents exactly the level-3 behaviour. So "absent and unplanned everywhere" shrinks to **four**:
D6.8, D8.4, D8.3, D12.5 - and D8.3/D8.4 only matter if the platform has an MDM system. D6.3/D6.4
share a root: **no catalog sits in the data path**; enforcement belongs to Ranger, and the catalog's
job is to derive policy and report drift (C01).

Beyond Class 1, the three themes fit naturally where (a) a field exists but nothing happens when
a deadline passes or a definition drifts (agentic), (b) the regulation is a concept scheme, not a
field (ontology - OM's single strongest area, `D4.4 = 4`, but bound to the glossary and off by
default), (c) structured fields must be extracted from unstructured text (LLM).

## The Four Sources

| # | Source | What to search | Notes |
|---|---|---|---|
| 1 | OpenMetadata issues / PRs / release notes, **plus the schema on the pinned tag vs `main`** | Feature keywords, schema file names, extension-point class names | A file on `main` but not on the tag = "planned/post-version", not "exists in the studied version" |
| 2 | Competitor products - DataHub, Atlas; commercial catalogs (Collibra, Alation, Purview, Collate) | Feature docs, OSS-vs-commercial matrices | Commercial boundary from markers, not marketing: DataHub `<FeatureAvailability saasOnly />` in `docs/managed-datahub/`; "Collate Only" in OM docs |
| 3 | Academic literature | arXiv, Semantic Scholar, OpenAlex | Surveys count; note result counts |
| 4 | General product space | Web search for vendors and open-source tools outside catalogs | DSPM/GRC/privacy tools often have the idea outside a catalog |

The methodological floor is **three** sources (product docs, issue tracker, literature); the
research used four. No candidate may pass with "not searched".

## Evidence Rules

- Record **query and finding per source**, including "searched, found nothing".
- Only sources actually opened go into the evidence list; unreachable sources are marked
  "could not verify" in the finding and kept out of the evidence list.
- Each evidence item: source title, URL, the quoted sentence or file fact that matters.
- Label the evidence type: `K` code, `D` docs, `İ` issue/PR/RFC/roadmap, `B` vendor claim.
  A `B`-only "exists" is weaker than a `K` or `D` one - say so.
- Pin versions and dates; governance tools move fast and a verdict is dated.

Record format used by the research (per candidate; keys translated from the Turkish originals):

```yaml
novelty:
  status: partial            # new | partial | planned | exists
  searched:
    - source: "OpenMetadata issues/PR + 1.13.3 and main schemas"
      query: "ranger; securityService; securityServiceMetadataPipeline"
      finding: "Only #8271 relevant (closed). Pipeline schema is a one-field stub, unchanged on main."
  evidence:
    - source: "Collate - Apache Ranger connector"
      url: "https://docs.getcollate.io/connectors/security/ranger"
      finding: "'Reverse Metadata (Collate Only)'; 'one-way sync'; no drift detection."
  differentiator: "Reading actual Ranger policy back and reporting drift exists in no product."
```

## Verdict Categories and What to Do

| Verdict | Meaning | Action |
|---|---|---|
| **new** | No product, plan or paper found in any source | Keep; state the searches in the proposal |
| **partial** | Pieces exist (often commercial, or outside catalogs) | Keep, but **rewrite the claim to the surviving differentiator**; drop the parts that exist |
| **planned** | Open issue/RFC/PR, not shipped | Keep with caution; coordinate upstream or wait |
| **exists** | All distinguishing constraints already shipped | **Eliminate**; list under "evaluated and eliminated" with the evidence |

"Partial" rewrites seen in the research: C04 - the special-category taxonomy is not new (W3C DPV
has it), only category→safeguard inference is; C08 - LLM PII classification is not new
(Databricks, Snowflake, arXiv 2506.22305), only the chain to a processing-inventory filing is;
C11 - not a research contribution at all, a port of what DataHub and Atlas have in OSS, valued
because C01/C02/C09 need it. C19 carries an open caveat: if commercial automated factsheets with
EU AI Act classification are confirmed, its position narrows to "commercial only, not OSS".

Eliminated: **C17** time-bound access requests - column scope, expiry with auto-revoke and
mandatory justification all shipped after 1.13.3 (`main` `dataAccessRequestPayload.json` requires
`reason`; policy-agent PRs; Collate 2.0 Data Marketplace) and UI logic moved to Collate (#28922).

Result: new 2 · partial 16 · planned 0 · exists 1 → 18 candidates enter scoring.

## Cross-Cutting Finding: Why Empty Extension Points Are Empty

Most candidates targeted hooks that OpenMetadata defines but leaves empty. The common reason
found during verification: **they are not forgotten - they are reserved for the commercial
edition (Collate).** Seen four times:

| Hook in 1.13.3 OSS | State (`K`) | Where the working version is |
|---|---|---|
| Ranger: `securityService.json` enum, `rangerConnection.json`, `securityServiceMetadataPipeline.json` | Pipeline schema is a one-field stub, unchanged on `main`; no connector under `ingestion/.../source/security/` | Collate docs only, "Reverse Metadata (Collate Only)"; OSS docs 404 on all version paths (`D`) |
| AI layer: `llmService`, `agentExecution`, `promptTemplate`, `aiApplication` | Schemas, persistence and REST exist; no generation engine. `configuration/aiPlatformConfiguration.json` is a gRPC *client* config, `enabled` default `false`, a field description names Collate; `app.json` `agentType` enum lists `CollateAI`, `CollateAITierAgent`, `CollateAIQualityAgent`, `Metadata` | 2.0 docs: "AgentExecution stores runtime observations; it isn't the agent definition"; AI SDK under the Collate Community License 1.0 (`D`) |
| `NoOpNLQService` | Only OSS implementation, returns `null`; provider loaded by reflection | Collate AI Analytics / AskCollate (`D`) |
| `entity/datacontract/slaValidation.json` | `refreshFrequencyMet`, `latencyMet`, `availabilityMet`, `actualLatency` defined; zero Java/Python references | Result schema without evaluator |

Same boundary elsewhere: Data Access Request UI logic moved OSS → Collate (#28922 / PR #28984);
the AI Governance Studio UI (~15,000 lines) removed from the OSS build in PR #29734, schema types
left behind (`İ`).

**Consequence.** An empty hook usually means "the vendor already built it, commercially". For any
open-source contribution this is both a **risk** (same space as the vendor) and an
**opportunity** (no on-prem, external-API-free, Apache-2.0 equivalent). In the research this
finding never removed a surviving differentiator; in C17 it caused elimination.

Practical check before designing on an empty hook:

1. Search the vendor's docs for the feature name and "Collate Only".
2. Diff the schema between the pinned tag and `main`.
3. Search closed issues/PRs for "move ... to Collate" style changes.
4. Decide the license home of your code (Apache-2.0 core vs plug-in) - see `openmetadata-architecture`.

## Other Ideas Considered and Dropped

Only C17 was formally eliminated. Ideas that were reframed rather than dropped: the Ranger
time-bound policy back-end (folded into C01), category taxonomy alone (folded into C04's
inference), and plain LLM classification (folded into C08's inventory chain).
