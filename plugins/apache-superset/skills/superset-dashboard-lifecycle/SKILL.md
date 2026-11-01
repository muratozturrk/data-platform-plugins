---
name: superset-dashboard-lifecycle
description: "Dashboard lifecycle for Apache Superset after Dashboards That Deliver (Wexler, Shaffer, Cotgreave, Makulec) and the Big Book of Dashboards: spark, brief and discovery, prototype (wireframe, mockup, working draft), development and user testing, release QA (ACT), adoption measured from the Superset /api/v1/log/ access log, maintenance backlog and retirement, plus scenario patterns (KPI cards and BANs, RFM, metric tree, mobile twin layout, security, RLS and embedding touchpoints). Includes a table mapping lifecycle stage to artefact to Superset feature or API. Use when planning a new dashboard, writing a dashboard brief, running user tests, measuring adoption, deciding on enhancement requests, or retiring a dashboard."
---

# Dashboard Lifecycle (Superset)

Process rules are paraphrased from DTD ch. 2-11 and scenario chapters 14-28; BBD for patterns. Superset facts marked **[4.1.2 lab]** were verified on 4.1.2 only. Companion skills: `superset-dashboard-design` (visual standard), `superset-storytelling-declutter`, `superset-dashboards-as-code`, `superset-security-model`.

## Principles
1. **Framework as a plan, not a bureaucracy**: seven phases (spark, discovery, prototyping, development + user testing, release, adoption, enhancements); "just enough" of each; improvement over perfection (DTD ch. 2).
2. **Purpose before data**: 2-3 sentences, clear, specific (who, which metrics), achievable (time, tech, budget). "Not now" is not "never" (ch. 5).
3. **Two diamonds**: diverge then converge, once in discovery, once in prototyping. Park nice-to-haves on the backlog to stop scope creep (ch. 5, 7).
4. **Cheapest change first**: wireframe -> mockup -> working prototype; show chart options side by side; use Five Whys on complaints; test with real data early because idealised data hides outliers (ch. 7).
5. **Define success twice**: little "s" (countable: users, views, requests) and big "S" (decisions enabled, fewer "let me get back to you", cost saved); review at least quarterly (ch. 5, 10).
6. **Ship, then evolve**: data must be solid at release, design can follow; one backlog, one point of contact, say no to sprawl (ch. 10, 11).

## Brief (discovery output)
Audience and personas; decisions ("As a ... I need ... in order to ..."); KPIs with definition, grain, target, comparison; data sources and a minimum viable dataset at the needed grain; medium and devices; refresh cadence; security needs; success measures (s and S); owner and data steward; test plan. Use command `superset-dashboard-brief`.

## User testing
- Short demo, 3-5 task scenarios, think-aloud, one observer, clear scope for feedback (DTD ch. 9).
- Performance requirement: the book quotes 5-10 s maximum as one practitioner benchmark; set your own budget in the brief.
- Requests from several users or priority groups become change requests, ranked on impact vs effort.
- Peer review by another dashboard builder: chart choice, optimisation, shared learning.

## Release QA (ACT)
Accuracy against source systems, Completeness across metrics, Timeliness of date range. Check filters and the user path (login steps kill adoption). **[4.1.2 lab]** a load missing 27.8 % of rows still passed filter checks because slices summed to the flawed total: always reconcile totals to source. A published dashboard with no charts returns 404 to a normal user.
Minimum docs: data dictionary, business logic, governance/security, data quality log; release notes list data source changes, quality issues, UX changes.

## Adoption measurement from `/api/v1/log/`
- Admin-only; count events with `action` = `DashboardRestApi.get`, grouped by `user`, dashboard id and week. This counts API reads, not browser page views **[4.1.2 lab]**.
- Page with `order_column:dttm,order_direction:asc`. The listing is itself logged and defaults to newest first, so plain paging returns duplicates; `id` is not an allowed order column (HTTP 400) **[4.1.2 lab]**.
- Rows with identical `dttm` can be repeated or skipped at page boundaries (counts off by one in the lab); dedupe or treat small counts as approximate **[4.1.2 lab]**.
- History cannot be created through the API; the lab back-dated rows in the metadata `logs` table. In production the log retention is whatever you keep.
- Read the numbers as questions: a cliff after a login change, super-users vs casual viewers, last view per dashboard.

## Maintenance and retirement
- One backlog (form or weekly session), one point of contact, prioritise on impact vs effort, release notes every round, QA each change (ch. 11).
- Monitor data quality from the data: value out of range, break in trend, two metrics that should move together but do not.
- Retire on evidence: not needed, not used, not worth the effort. Ask users first, keep an export, set `published: false` before deleting **[4.1.2 lab]** (`published` round-trips; draft is a label, not access control).

## Lifecycle stage -> artefact -> Superset feature
| Stage | Artefact | Superset feature / API |
|---|---|---|
| Spark | Problem statement, sponsor | None; tracker outside Superset |
| Discovery | Brief, user stories, success measures, minimum viable dataset | Virtual dataset + saved metrics to test grain; `/api/v1/log/` of related dashboards |
| Prototype | Wireframe, mockup, prototype | Markdown rows and `position_json` via `PUT /api/v1/dashboard/{id}` (stored verbatim [4.1.2 lab]) |
| Development | Datasets, charts, dashboard, data dictionary | Dataset `PUT` with column/metric descriptions in one call [4.1.2 lab]; certification fields (`certified_by`, free text, no reviewer workflow) |
| User testing | Task scenarios, findings, change requests | Test user with the target role; performance via chart data API timing |
| Release | QA log, release notes, access | Roles on dashboard (`GET /api/v1/dashboard/{id}` returns `roles`, `owners`), `published`, RLS rules, export bundle (dashboards-as-code) |
| Adoption | Training, champions, quarterly review | `/api/v1/log/` as above |
| Enhancements | Backlog, prioritisation, release notes | Git PR on exported assets; CI sync |
| Retirement | Archive export, comms | Export, `published: false`, delete |

## Scenario patterns
| Pattern | Rule | Superset note |
|---|---|---|
| BAN / KPI card (DTD ch. 27, BBD) | Never leave a number alone: add target, prior period, status dot, bar or sparkline; when the number is no longer the biggest thing it is a KPI card; first BANs may double as colour legend | Big Number with Trendline; ratio BAN needs `SUM/SUM` ad hoc metric, not an average of rates; `conditional_formatting` thresholds are constants, so a changed goal needs a chart change; sub-queries in custom SQL are refused (HTTP 403/400) so carry flags such as `is_latest` in the dataset **[4.1.2 lab]**; new dataset columns are ignored in filters until `PUT /api/v1/dataset/{id}/refresh` **[4.1.2 lab]** |
| RFM (ch. 15) | Score recency, frequency, monetary 1-5, group into segments, one row per segment with action; templates only if you can maintain their logic | Virtual dataset with `NTILE()` needs a tie-breaker; the SQL runs on every query, so materialise for large customer bases **[4.1.2 lab]** |
| Metric tree (ch. 20) | Root KPI with children; solid line = arithmetic parent, dotted = context; cards link to detail dashboards | Hierarchy as a dataset with parent keys; verify parent = sum of children in SQL; cost nodes need reversed colour rules (constants) **[4.1.2 lab]** |
| Mobile (ch. 8, 27) | Fewer charts, one tall column, fewer filters | No per-device layout in 4.1.2: build a mobile twin dashboard (width-12 rows) sharing the same charts **[4.1.2 lab]** |
| Executive monitor / email (ch. 14) | Static daily page may beat an interactive one for execs | Alerts and reports (email/Slack screenshots) are Superset features; not covered by the lab |
| Dynamic duos (ch. 28) | Pair charts that answer two facets; keep sort order aligned | `x_axis_sort` on a ranked label; totals as column on every dataset row [4.1.2 lab] |
| Build-your-own (ch. 22) | High-literacy users only; defaults in a config table | Native filters + cross-filter scoping (`chart_configuration`) [4.1.2 lab] |

## Security, RLS and embedding touchpoints
- Decide in the brief: who sees which rows (RLS rule per role), which datasets are certified, whether the dashboard is embedded.
- Role on dashboard + dataset permissions; RLS rules created via API/YAML (see `superset-security-model`, `superset-dashboards-as-code`). Personalisation by role/location keeps newcomers from being overwhelmed (DTD ch. 10).
- Embedding and guest tokens: see `superset-multi-tenancy-embedding`. Embedding reduces friction (ch. 22) but is not covered by the lab verification here. Test as the real role, not as admin; **[4.1.2 lab]** `PUT /api/v1/security/users/{id}` with only `roles` locks the user out unless `password` is sent too.

See `references/sources.md`.
