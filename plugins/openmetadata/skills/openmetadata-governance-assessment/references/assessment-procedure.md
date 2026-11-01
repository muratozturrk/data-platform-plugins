# Assessment Procedure

How the OpenMetadata / DataHub / Atlas study was run, written as a repeatable procedure. Use it
for a new product, a newer OpenMetadata release, or an audit of your own installation.

## 1. Freeze the Rubric

1. Write domains and capabilities from frameworks (DMBOK2, DCAM, ISO/IEC 38505-1, ISO 8000, DGI,
   data-mesh governance, EU AI Act, local law) **before** opening any product. See `rubric.md`.
2. Review level texts: no subjective adjectives, no product names, monotonic levels, verified
   numbers, at least one source per capability. In the study a domain expert reviewed D1-D13 and
   an independent legal review checked D14 (it caught double counting with D6, a wrong start
   point for the 72-hour breach clock and an incomplete list of data-subject rights).
3. Set `frozen: true` and a date. From then on every change goes to a changelog entry listing the
   changed capability IDs, what changed, why, and which evidence files must be re-filled.
4. Keep rubric parts editable and generate the merged file from them; never hand-edit the merged file.

Why: inspecting a product and then adjusting a level fits the yardstick to that product and voids
the comparison. Some level-4 texts may match no existing product; that is the definition of a
differentiator, not a defect.

## 2. Pin What You Assess

- Clone the released tag, not main. Record repo URL, tag, full commit hash, clone date, and
  whether the clone equals the latest release. Study pins: OpenMetadata `1.13.3-release`
  `255f6694913b84797064a42859cda3f2a3425dc6`; DataHub `v1.7.0` `7f81ccbfe27b9acc947f5f600fcf9ddb72138a80`;
  Atlas `release-2.5.0` `234608315f29b643c2dd7632cad0e8ef2a15c696`.
- Assessment basis: what an organisation can install from the release. Features only on main or
  in a roadmap are recorded with type `İ`.
- Note open-core splits: a capability only in the vendor's commercial edition is scored on the
  open-source release, and the note says what the paid edition adds.

## 3. Fill One Evidence File per Product

Template (one row per capability; every rubric ID exactly once):

```yaml
product: openmetadata
version: "1.13.3-release"
commit: "255f6694913b84797064a42859cda3f2a3425dc6"
inspected_at: "2026-08-11"
assessments:
  - capability: D1.2
    score: 2                      # 0-4, or null when unknown
    evidence_type: K              # K | D | İ | B
    evidence: "openmetadata-spec/src/main/resources/json/schema/type/customProperty.json:1-71; openmetadata-service/src/main/java/org/openmetadata/service/jdbi3/TypeRepository.java:164-192"
    note: "Typed custom properties (string, number, date, enum, entityReference, table) via UI and API, JSON Schema validated. Only entity-category types (not user/team/service/policy); no required flag."
  - capability: D8.4
    score: 0
    evidence_type: D
    evidence: "https://docs.open-metadata.org/latest/how-to-guides/data-governance"
    note: "No schema/code for 'golden record', 'entity resolution', 'survivorship' in the repo AND no such topic in the official governance docs."   # two places
  - capability: D12.6             # illustrative null row (not a study value for OpenMetadata)
    score: null
    evidence_type: B
    evidence: ""
    note: "searched: public repo (none), vendor site, release notes (not published), issue tracker (none)"   # must start with the marker; the study's tooling used the Turkish "arandı:"
```

Rules per row:

| Situation | Score | Note must say |
|---|---|---|
| Behaviour read in code at the pinned commit | 0-4 with `K`, `path:line` | What the code does and what the next level would need |
| Only documented | 0-4 with `D`, URL | Which page; that code was not checked |
| Only an issue/RFC/roadmap | usually 0-1 with `İ` | Issue state (open = "not yet") |
| Only a vendor statement | with `B` | That it is unverified |
| Not found, searched two places | 0 | Both places, e.g. "not in source (grep X, Y) and not in docs (page Z)" |
| Not found, only one place, or unsure | `null` | Starts with "searched:" and lists the places |

Good notes name the gap to the next level ("no required flag", "decisions are not written to the
audit log"); that sentence later becomes the gap catalogue (see `openmetadata-gap-closing`).

## 4. Parallel Assessors

Assessors of different products must not see each other's files. Consistency comes from: the
frozen rubric, **one identical prompt template**, **one identical output format**, and a
cross-check pass afterwards. Give each assessor a search-depth budget per capability so one
product is not inspected more deeply than another.

## 5. Validate Mechanically

The study's validator checks (re-implement in any language):

| Target | Check |
|---|---|
| Rubric | Domain ID `^D\d+$`, capability ID `^D\d+\.\d+$`, both unique; capability prefix equals its domain |
| Rubric | `name`, `definition`, `rationale` non-empty; at least one source; all five levels `"0"`-`"4"` present |
| Sources | Every `ref:` starts with `ref:`, resolves to a source entry, and that entry has a non-empty URL |
| Evidence | `product`, `version`, `commit`, `inspected_at` present |
| Evidence | Every row's capability exists in the rubric, appears once; every rubric capability is assessed |
| Evidence | Score in {0..4} or null; evidence type in {K, D, İ, B} |
| Evidence | Non-null score has non-empty `evidence`; null score has a note starting with the "searched:" marker |
| Report | Prints the vendor-claim ratio: rows with type `B` / all rows |

Exit codes: 0 valid, 1 validation errors, 2 file not found. Add your own grep for banned
adjectives and product names in level texts; the study's README says these are scanned, but the
validator inspected does not implement that scan (see `sources.md`).

A source entry that cannot be reached is not written with an empty URL; it is left out and
reported as a gap. Each source note records which sections were verified and how (URL, HTTP
status, page count, workaround), so someone else can repeat it.

## 6. Cross-Check Before Publishing

Automatic flags (a heuristic that produces work, not verdicts):

| Flag | Rule | Possible cause |
|---|---|---|
| All zero | Every product scored, and all scores are 0 | Rubric item unrealistic for the category |
| All unknown | No product has a score | Capability definition ambiguous |
| Excess spread | At least two scores and max - min >= 4 | One assessor read the levels differently |
| Weak zero | Score 0 and the note has no sign of a second search place | "Absent" not proven |

Then read every flagged row by hand and record the decision. Study outcome: one spread flag
(D11.4: OpenMetadata 4, DataHub 2, Atlas 0) judged a real difference; two weak-zero flags (Atlas
D3.5, D7.2) were false alarms because the notes cited the code search and the JIRA search, which
the keyword heuristic did not recognise. All 21 Atlas zeros were then read by hand: each documents
two search places. No evidence file needed changes. Details in `openmetadata-results.md`.

## 7. Publish

| Table | Content | Format rule |
|---|---|---|
| Executive summary | Domain x product mean | `mean (scored/total)`; mean over scored rows only; add a spread column (max - min of the means) to show which domains separate products |
| Full matrix | Capability x product | `score (type)`, `?` for null; header lists version, short commit, B ratio per product |
| DMBOK coverage | DMBOK2 KA x product | Map at **capability** level through each capability's `ref:dmbok2-kaN` sources (one capability can count for several KAs; the study had 95 mappings for 90 capabilities); unmapped KA = "not covered", never 0 |
| Community | Stars, forks, contributors, commits, releases, issues, licence, governance model | Date of pull, API endpoint per number; same method across products; `?` where not computable |
| Gaps | Low scores turned into prioritised gaps | See `openmetadata-gap-closing` |
| Regulation matrix | Obligation x product | See `openmetadata-kvkk-compliance` |

Community metrics lessons: count default-branch commits for cross-product comparison (all-branch
stats differ by method); contributor counts from the API are cumulative; first-response time
needs per-issue comment timestamps and was not computable with unauthenticated API quotas; for an
Apache Software Foundation project the GitHub repo is a mirror (issues in JIRA, releases on ASF
dist), so stars and GitHub issue counts are misleading; a closed-source product's community rows
are "not assessable", not zero.

Always state: inspection date, versions, that findings are dated, and that regulatory mapping is
not legal advice.

## 8. Assessing Your Own OpenMetadata Installation

The study scored the release. For an installation, re-score capabilities that depend on
configuration, and record the configuration as evidence (`conf/openmetadata.yaml` key, app
installed, connector deployed):

| Capability | Release score | What changes it in an installation |
|---|---|---|
| D4.4 RDF/SPARQL | 4 | Off by default (`RDF_ENABLED=false`, needs Fuseki); disabled = not available to users |
| D6.7 External authoriser | 1 | Only a custom `authorizerConfiguration.className` implementation changes it |
| D12.4 SSO | 3 | Which IdP is actually wired |
| D12.8 Backup | 0 | Your own database backup procedure is outside the product; record it separately, do not raise the product score |
| D3.1-D3.4 / D11.1 | 3 | Which connectors and lineage sources are deployed (see `openmetadata-platform-integration`) |

Keep "product capability" and "our operational practice" in separate columns so a later upgrade
assessment stays comparable.
