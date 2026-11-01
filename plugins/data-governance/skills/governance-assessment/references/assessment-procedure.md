# Assessment Procedure

How to score one product, compare several, or measure the governance coverage of a whole
platform with the frozen rubric ([rubric.md](rubric.md)). The procedure is the one the research
used for OpenMetadata, DataHub and Apache Atlas (inspected 2026-08-11); sections marked
**(adaptation)** extend it to platforms and organisations and were not exercised in the research.

## 1. Freeze the rubric first

Change the yardstick after looking at a product and you have shaped it around that product. So:

1. Write and review the rubric **before** any product is opened. Mark it `frozen: true` with a
   `frozen_at` date.
2. Every later change goes into a changelog entry: changed capability IDs, what changed, why, and
   **which evidence files must be re-scored**. Re-score all of them; old scores were given against
   a different yardstick.
3. Review the draft against the level-writing rules before freezing:

| Rule | Check |
|---|---|
| Observability | Two people looking at the product reach the same level. Banned subjective adjectives: good, advanced, comprehensive, mature, strong, sufficient, rich (Turkish originals: iyi, gelişmiş, kapsamlı, olgun, güçlü, yeterli, zengin). |
| Product neutrality | No product name in any level text. Open standards (OpenLineage in D3.5) are allowed. |
| Monotonicity | Level n contains all of n-1; levels 3 and 4 say "in addition to level 2/3". |
| 0 = absent, 4 = differentiator | Level 4 is rare by definition; no product is expected to score 4 everywhere. |
| Verified numbers | Every duration, count or article number in a level is checked against the official source. |
| Sources | At least one, preferably two `sources` per capability, each resolving to a registry entry with a non-empty URL. Unreachable sources are left out and reported, never written with `url: ""`. |

Before freezing, the research ran a domain-expert review on D1-D13 (adjectives removed, product
names removed, monotonic wording, consultancy blogs replaced by DAMA chapter summaries, ISO pages
and primary papers) and an independent legal review on D14 (double counting with D6/D9 removed,
a legally wrong 72-hour start fixed, three capabilities added). Budget for both.

**(adaptation) Other jurisdictions.** D14 is a Turkish-law slice. To assess under GDPR or another
regime, replace D14 with a local slice written to the same rules (regulatory delta only, rationale
names the D6/D9 capability it builds on), log it as a rubric version, and do not compare D14
numbers across different slices.

## 2. Pin what you assess

- Assess **released** versions: the code an organisation can actually install. Record product,
  version tag, full commit hash and inspection date in the evidence file header.
- A feature that exists only on the main branch is recorded with evidence type **İ** (issue /
  roadmap), not **K**.
- Open-core products: assess the open-source distribution. A feature gated to the commercial
  edition counts as absent for the OSS score and is named as such in the note (the research did
  this for DataHub Cloud-only approval workflows and assertion monitors).
- Closed-source products: everything is **B** (vendor claim). Report the B ratio next to every
  average; a product with a high B ratio or many `null`s is not comparable on the same footing.

## 3. Run assessors in parallel, blind to each other

One assessor (person or agent) per product, all with the **same brief and output format**, none
seeing another's results. Each fills **all 90 capabilities**. The brief says:

- read the full level text, then find the highest level whose every clause is met (levels are
  conjunctive; one missing clause drops the score one level);
- cite one traceable piece of evidence per score, prefer code (`path:line`) over docs;
- write what was searched and where for every 0 and every `null`.

The price of blind parallel work is interpretation drift. It is controlled by the frozen rubric,
the shared brief, and the cross-check in step 6.

## 4. Score with evidence

| Type | Meaning | Format | Confidence |
|---|---|---|---|
| **K** | Read in source code | `path:line` in the pinned repo | Highest; independent of marketing |
| **D** | Official documentation | URL (+ version/date) | High |
| **İ** | Issue, RFC, design discussion | URL | Medium; usually proves *planned* or *not yet*; an open request is good evidence of absence |
| **B** | Vendor claim | URL or quote | Lowest; not independently verifiable; ratio reported |

### Absence is a weak claim: `null` is not 0

A 0 says "this product does not do it", the hardest claim to prove, because it may sit where you
did not look.

1. A **0** needs searches in **at least two different places** (code **and** docs, or docs **and**
   issue tracker), and the note names both.
2. If unsure, write `score: null` ("unknown"), not 0.
3. Every `null` row's note **starts with `arandı:`** ("searched:") and lists where. Keep the
   literal prefix if you use the research validator; it checks it.
4. Every non-null score carries evidence. A score without evidence is an error.

Averages are computed over known scores only and always printed with coverage
(`known / total`). A domain mean from 1 of 6 capabilities is not a domain mean.

## 5. Evidence file

One YAML file per product. Template: [../assets/evidence-template.yaml](../assets/evidence-template.yaml).

```yaml
product: <name>
version: "<release tag>"
commit: "<full 40-char hash>"
inspected_at: "YYYY-MM-DD"
assessments:
  - capability: D7.2
    score: 3                      # 0-4 or null
    evidence_type: K              # K | D | İ | B
    evidence: "path/to/File.java:120-180; path/to/flow.json"
    note: "What was seen, which level clause failed, what was not found."
```

Keep a separate `versions` file with repo URL, tag, commit, clone date and whether the clone
equals the release.

## 6. Validate, then cross-check

### Automated validation (research `validate.py`)

Checks rubric: domain/capability ID format `^D\d+$` / `^D\d+\.\d+$`, uniqueness, ID prefix
matches its domain, non-empty `name`/`definition`/`rationale`, >= 1 source, all five levels.
Checks sources: every `ref:` resolves and has a URL. Checks evidence: required header
(`product`, `version`, `commit`, `inspected_at`), score in 0-4 or null, evidence type in
{K, D, İ, B}, no score without evidence, `null` note starts with `arandı:`, no capability missing
or duplicated, no unknown capability. Prints the B ratio per file. Exit codes: 0 valid,
1 errors, 2 file not found.

**Gap:** the research README says subjective adjectives and product names in level texts are
"scanned by the validation script"; the script contains no such check (verified in
`research/tools/validate.py`). Run the scan yourself; a plain regex over level texts works, then
read each hit (the research rubric gives 5 false positives on word stems such as "iyileştirme",
"yeterlilik kararı", "zenginleştirme", "olgunluğu", and the standard name OpenLineage).

### Cross-check (research `crosscheck.py`)

| Flag | Condition | Likely cause |
|---|---|---|
| All-zero | every product scored 0 | level 1 unrealistic for the product category, or a shared blind spot |
| All-unknown | every product `null` | capability definition ambiguous |
| Spread >= 4 | max - min of known scores >= 4 (needs >= 2 known) | one assessor read the level differently |
| Weak zero | a 0 whose note lacks signs of two search places | absence not proven |

Known tool limits, both seen in the research:

- **All-zero is suppressed by any `null`.** The rule requires every product to be known. With a
  null-heavy fourth product in the set, D6.8 (anonymisation) and D8.4 (golden record) were not
  flagged although all three open-source products scored 0. Re-run the check on the subset of
  products with full coverage.
- **Weak zero is a keyword heuristic.** It flagged Atlas D3.5 and D7.2, whose notes do name two
  places (repository + JIRA); it does not recognise JIRA as a search place. Treat flags as a
  reading list, not a verdict.

### Manual verification

For each flag, re-read all products' evidence for that capability against the level text and
record a decision with reasons. Research example: **D11.4 (OpenMetadata 4, DataHub 2, Atlas 0)**
was kept: OpenMetadata meets all three level-4 clauses (central API-managed routing, retries,
delivery status), DataHub lacks severity filtering (level 3 clause), Atlas has no outbound
notification at all (two places searched). After the flags, the research also hand-read all 21
Atlas zeros; all named two search places, no change needed. Spot-check zeros this way whenever
one product has many.

## 7. Aggregate and report

| Measure | Formula | Read it as |
|---|---|---|
| Domain mean | mean of known scores in the domain, shown as `mean (known/total)` | maturity in that area |
| Spread | max - min of product domain means | near 0 = domain does not separate the products |
| Distribution | count of 0/1/2/3/4/null per product | shape behind a mean |
| B ratio | share of rows with type B | how much rests on vendor claims |
| DMBOK view | capability-level mapping via `ref:dmbok2-kaN` | see [dmbok-coverage.md](dmbok-coverage.md) |

Means are unweighted; weighting by importance is a separate, explicit step (the research weighted
candidate features, not rubric domains). State the version pins and inspection date in every table.

## 8. (adaptation) Assessing a platform or an organisation

The rubric measures **tool capability**. To measure an organisation's governance coverage:

1. **Platform score.** For a stack of tools (catalog + quality engine + policy engine + MDM ...),
   score each capability for the composed platform and name the component in `evidence`. A
   capability split across tools only reaches level 3+ if the hand-off is automatic (e.g.
   quality results land in the catalog's incident flow, D5.7 level 3).
2. **Practice overlay.** For process capabilities (D7, D9.1, D9.4, D14), record separately whether
   the organisation **uses** the mechanism: owners assigned (% of critical assets), approval flow
   active, policies with review dates, campaigns run. Keep it as a second column; do not mix it
   into the 0-4 tool score.
3. **Unmeasured areas.** DMBOK2 Documents and Content (KA7) has no capability; assess it with a
   separate checklist or declare it out of scope. Never print it as 0.

## 9. Community sustainability metrics (D12.6 and the T5 table)

| Metric | Source (unauthenticated GitHub REST v3 unless noted) | Caveat |
|---|---|---|
| Stars, forks | `repos/{repo}` | Mirrors (e.g. Apache projects) under-count; do not compare across |
| Contributors | `contributors?per_page=1&anon=false`, last page from `Link` | Cumulative, all time |
| Commits, last 12 months | `commits?since=...&per_page=1`, last page | Default branch only; use this for comparisons |
| Commits, all branches | `stats/commit_activity` (52 weeks) | Can return empty (`{}`), as for Atlas |
| Stable releases, 12 months | `releases`, minus prerelease/draft | Apache projects publish tags + `downloads.apache.org`, 0 GitHub releases |
| Open / closed issues | `search/issues?q=repo:..+type:issue+state:..` | Apache projects use JIRA (`rest/api/2/search` with JQL) |
| First-response time | per-issue comments | Not computable within the 60 requests/hour unauthenticated quota |
| Organisation diversity | commit authors' affiliations, 12 months | Not computable from shallow clones; leave `?` |
| Licence, governance model | `LICENSE` files per module; foundation membership | Check sub-module licences (one product's ingestion module had a different licence) |

Unmeasurable cells are `?` / "not assessed", never 0.

## Pitfalls

| Pitfall | Why it hurts | Instead |
|---|---|---|
| Editing levels after seeing a product | Comparison becomes invalid | Changelog + re-score every file |
| 0 after one grep | Absence unproven | Two places, or `null` with `arandı:` |
| Mean without coverage | Hides how few cells it rests on | `mean (known/total)` |
| Scoring the vendor's cloud edition as the OSS product | Inflates what can be self-hosted | Score OSS; name gated features |
| Trusting the cross-check output as a verdict | Heuristic flags false alarms and misses all-zeros next to nulls | Manual decision per flag; re-run on full-coverage subsets |
| Comparing stars of a mirror repo | Different accumulation period, no issues on GitHub | Use foundation health indicators |
| Treating "not measured" as "weak" | Reports a result that does not exist | "No data" row with the reason |
