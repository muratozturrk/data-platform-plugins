# Measured Results: a Worked Example

The procedure in [assessment-procedure.md](assessment-procedure.md) applied to three open-source
catalog/governance products. This is an illustration of what the method produces and how to
read it, not a ranking for every context: weight the domains by your own needs before choosing.

## Pins

| Product | Release | Commit | Inspected | Evidence mix (90 rows) | `null` | B ratio |
|---|---|---|---|---|---|---|
| OpenMetadata | `1.13.3-release` | `255f6694913b` | 2026-08-11 | K 87 · D 3 | 0 | 0 % |
| DataHub | `v1.7.0` | `7f81ccbfe27b` | 2026-08-11 | K 86 · D 4 | 0 | 0 % |
| Apache Atlas | `release-2.5.0` | `234608315f29` | 2026-08-11 | K 84 · D 3 · İ 3 | 0 | 0 % |

All three clones equal the release tag. Released code only; main-branch features would be İ.

## Score distribution

| Product | 0 | 1 | 2 | 3 | 4 | Mean (90/90) |
|---|---|---|---|---|---|---|
| OpenMetadata | 4 | 13 | 28 | 39 | 6 | 2.33 |
| DataHub | 4 | 16 | 33 | 30 | 7 | 2.22 |
| Apache Atlas | 21 | 29 | 29 | 9 | 2 | 1.36 |

OpenMetadata and DataHub are close overall (0.11 apart) and differ by domain; Atlas sits about a
full level lower.

## Executive summary by domain (T1)

Cell = domain mean; every domain has full coverage (known = total). **Spread** = max - min across
the three products, recomputed for this three-product set (the research table also included a
null-heavy fourth product, so its spread column differs for D1, D2, D9, D11, D14).

| Domain | OM | DH | Atlas | Spread |
|---|---|---|---|---|
| D1 Metadata model | 2.7 | **3.3** | 3.0 | 0.6 |
| D2 Discovery and search | 3.0 | **3.2** | 1.5 | 1.7 |
| D3 Lineage | 2.8 | **3.0** | 1.8 | 1.2 |
| D4 Glossary and semantics | **3.4** | 2.4 | 1.8 | 1.6 |
| D5 Data quality | **2.4** | 1.4 | 0.3 | **2.1** |
| D6 Security and privacy | **1.8** | 1.6 | 1.2 | 0.6 |
| D7 Governance processes | **2.5** | 1.8 | 0.5 | **2.0** |
| D8 Master/reference data | **1.2** | 1.0 | 1.0 | 0.2 |
| D9 Data products/mesh | **2.2** | 1.8 | 0.7 | 1.5 |
| D10 Observability | **2.7** | 2.3 | 1.0 | 1.7 |
| D11 Integration | **2.8** | 2.7 | 1.5 | 1.3 |
| D12 Platform maturity | 1.9 | 1.9 | 1.4 | 0.5 |
| D13 AI/ML governance | 2.2 | **2.8** | 1.0 | 1.8 |
| D14 KVKK compliance | 1.7 | **2.1** | 1.8 | 0.4 |

How to read it:

- **Separating domains** (spread >= 1.5): data quality, governance processes, AI/ML, discovery,
  observability, glossary, data products. Choose on these.
- **Non-separating domains** (spread <= 0.6): master/reference data, KVKK, platform maturity,
  security, metadata model. All three are similar there, so the choice of product will not fix
  a weakness in these domains.
- **Category ceiling:** D6, D8, D12 and D14 stay below 2.2 for every product. A catalog records
  and evidences controls; it does not enforce them on data reads, master the data, or run
  destruction. Plan those in the data plane, MDM and orchestration layers.
- **Atlas** keeps a strong type system (D1 = 3.0, D1.1 and D1.2 = 4) but lacks most governance
  workflow and quality features.

## Full matrix (T2), compact

Each entry is `capability:OM DH Atlas`. Evidence type is K unless listed after the table.

| Domain | Scores |
|---|---|
| D1 | 1:3 4 4 · 2:2 4 4 · 3:3 3 2 · 4:3 3 2 · 5:3 3 3 · 6:3 3 3 · 7:2 3 3 |
| D2 | 1:4 3 2 · 2:3 4 2 · 3:2 2 1 · 4:2 3 1 · 5:3 3 1 · 6:4 4 2 |
| D3 | 1:3 3 3 · 2:3 4 3 · 3:3 3 1 · 4:3 3 2 · 5:2 2 0 |
| D4 | 1:3 3 1 · 2:4 3 3 · 3:3 1 0 · 4:4 2 2 · 5:3 3 3 |
| D5 | 1:3 1 0 · 2:3 3 1 · 3:2 0 0 · 4:2 2 0 · 5:3 1 0 · 6:1 0 0 · 7:3 3 1 |
| D6 | 1:3 1 0 · 2:4 3 2 · 3:1 1 0 · 4:0 1 1 · 5:2 2 2 · 6:3 3 2 · 7:1 2 3 · 8:0 0 0 |
| D7 | 1:3 3 1 · 2:3 1 0 · 3:1 2 0 · 4:3 2 1 · 5:2 2 1 · 6:3 1 0 |
| D8 | 1:2 2 2 · 2:2 1 2 · 3:1 1 0 · 4:0 0 0 |
| D9 | 1:3 3 1 · 2:3 2 1 · 3:3 2 1 · 4:1 1 0 · 5:2 2 0 · 6:1 1 1 |
| D10 | 1:1 2 1 · 2:3 2 1 · 3:3 3 2 · 4:3 3 1 · 5:3 3 0 · 6:3 1 1 |
| D11 | 1:3 4 1 · 2:3 2 2 · 3:2 3 2 · 4:4 2 0 · 5:3 2 2 · 6:2 3 2 |
| D12 | 1:3 3 1 · 2:3 3 2 · 3:1 1 1 · 4:3 1 2 · 5:1 1 0 · 6:2 2 2 · 7:2 2 2 · 8:0 2 1 |
| D13 | 1:2 3 1 · 2:2 4 2 · 3:2 3 1 · 4:2 2 1 · 5:3 2 0 |
| D14 | 1:2 2 2 · 2:2 3 2 · 3:2 2 2 · 4:2 2 2 · 5:2 2 2 · 6:2 2 2 · 7:1 2 1 · 8:2 2 3 · 9:1 2 1 · 10:1 2 1 · 11:2 2 2 |

Non-K rows: OpenMetadata D8.3, D8.4, D9.6 (D); DataHub D6.4, D12.2, D12.3, D12.7 (D); Atlas D1.7,
D12.6, D12.8 (D) and D3.5, D7.2, D9.2 (İ).

### Level-4 and level-0 cells

| Product | Level 4 | Level 0 |
|---|---|---|
| OpenMetadata | D2.1, D2.6, D4.2, D4.4, D6.2, D11.4 | D6.4, D6.8, D8.4, D12.8 |
| DataHub | D1.1, D1.2, D2.2, D2.6, D3.2, D11.1, D13.2 | D5.3, D5.6, D6.8, D8.4 |
| Apache Atlas | D1.1, D1.2 | 21 cells: D3.5, D4.3, D5.1, D5.3-D5.6, D6.1, D6.3, D6.8, D7.2, D7.3, D7.6, D8.3, D8.4, D9.4, D9.5, D10.5, D11.4, D12.5, D13.5 |

- **Shared zeros:** D6.8 (anonymisation/PETs) and D8.4 (golden record). Both belong to other
  product categories (privacy engineering, MDM); they are real absences, not rubric errors.
- **DataHub's zeros are edition effects:** D5.3 test execution and D5.6 anomaly detection are
  Cloud-only in the vendor's own OSS/Cloud matrix
  (`docs/managed-datahub/managed-datahub-overview.md:30-33`); so are approval workflows (line 48),
  which caps D7.2 at 1. Scores describe the self-hostable OSS release.

## Cross-check outcome

| Flag | Result | Decision |
|---|---|---|
| Spread >= 4 | D11.4 only (OM 4, DH 2, Atlas 0) | Real difference, kept. OM: subscription types Webhook, Slack, MsTeams, GChat, Email (`eventSubscription.json:98-110`), retry default 3 (`:334`), delivery status `active/failed/awaitingRetry/retryLimitReached` (`:180-185`). DH: Actions pipeline with `retry_count`/`failure_mode` (`datahub-actions/.../pipeline/pipeline.py:89-90`) but no severity filtering. Atlas: no webhook/SMTP/chat code (re-checked: zero matches outside a bundled Swagger UI file). |
| Weak zeros | Atlas D3.5, D7.2 | False alarms: both notes name repository **and** JIRA. |
| All-zero | none reported | Missed D6.8 and D8.4 because the fourth product was `null` there; re-run on the three products flags both. |
| Manual audit | all 21 Atlas zeros read | All document two search places; no change. |

## Community sustainability (T5, fetched 2026-08-11)

| Metric | OpenMetadata | DataHub | Apache Atlas |
|---|---|---|---|
| Repository | `open-metadata/OpenMetadata` | `datahub-project/datahub` | `apache/atlas` (**mirror**) |
| Stars / forks | 14 848 / 2 309 | 12 516 / 3 655 | 2 132 / 903 (not comparable) |
| Contributors (all time) | 424 | 411 | 145 |
| Commits, 12 months, default branch | 4 414 | 3 140 | **178** |
| Stable releases, 12 months | 59 (two maintenance lines, ~6 days apart) | 15 (~24 days; majors ~2.5 months) | **1** (2.5.0; gaps of ~25 and ~16 months before) |
| Open / closed issues | 724 / 9 788 (93 % closed) | 518 / 2 283 (82 %) | 1 053 / 4 266 in JIRA (80 %) |
| Issues opened / resolved, 12 months | ? | ? | 283 / 251 |
| First-response time | ? | ? | ? |
| Licence | Apache-2.0 core; `ingestion/` under Collate Community License (`ingestion/LICENSE:1-2`) | Apache-2.0 | Apache-2.0 |
| Governance | Single vendor, no foundation | Single-vendor led, no foundation | ASF top-level project, PMC-voted releases |
| Commercial split | Hosted/enterprise edition | Cloud edition gates key governance features | None |
| D12.6 score | 2 | 2 | 2 |

Read Atlas through foundation indicators (JIRA activity, `downloads.apache.org` releases, PMC
process), not GitHub stars: the mirror was created in 2017 for a project incubated in 2015, has 0
GitHub issues and 0 GitHub releases, and its `open_issues_count` counts pull requests. Atlas is
maintained but not actively developed; it is also the only one without single-vendor dependency.
OpenMetadata's release cadence means speed **and** upgrade load.

## Known limitations of these results

1. One assessor per product; no inter-rater statistic. Cross-check plus manual audit is the only
   consistency control.
2. Means are unweighted and treat level steps as equal intervals.
3. Snapshot of 2026-08-11; all three ship often (Atlas 2.6.0-rc1 was tagged, not released).
4. Open-core products scored on OSS; their commercial editions would score higher in D5, D7.
5. Organisation diversity (D12.6 level 3) and first-response time were not measurable
   (unauthenticated API quota, shallow clones).
6. DMBOK2 Documents and Content is not measured; DW/BI and Data Architecture rest on 2-3
   capabilities ([dmbok-coverage.md](dmbok-coverage.md)).
7. The book text of DMBOK2 was not available; knowledge-area structure comes from DAMA's public
   context-diagram summaries, triangulated with DCAM, ISO and DGI.
8. Small inconsistencies inside the evidence: Atlas D12.6 dates 2.4.0 to "2025-01" while the tag
   date is 2024-12-20; one DataHub D11.4 citation (`teams.py:44-47`) shows the webhook config, not
   the retry setting, which lives in `pipeline.py:89-90`.
