---
name: superset-dashboard-design
description: Information-design standard for Superset dashboards and charts, grounded in Few (Information Dashboard Design), Knaflic (Storytelling with Data), Tufte and Cairo: purpose and audience, single-screen rule, KPI definition, chart-type selection, colour and formatting, native filters, layout grid, accessibility, review checklist. Use when building or reviewing any dashboard or chart.
---

# Dashboard Design Standard

## Principles (sources in references)
1. **Purpose first** — a dashboard supports specific decisions for a specific audience; write the questions before the charts. Monitoring dashboards (at-a-glance), analytical dashboards (explore) and reports are different artefacts; do not mix.
2. **Single screen** (Few) — everything important visible without scrolling; use tabs for secondary views. Few's thirteen mistakes are the review checklist (see examples/dashboard-review-checklist.md).
3. **Data-ink ratio** (Tufte) — remove gridlines, borders, 3D, shadows, redundant legends/titles; let data carry the ink.
4. **Pre-attentive encoding** — position and length for quantities (bar/line), hue for category (≤ 6), intensity for ordered values; never encode quantity with area/angle (pie, bubble) when precision matters.
5. **Context** (Few, Knaflic) — every KPI shows comparison (target, previous period, benchmark) and direction; sparkline/trendline beats a lone number.
6. **One message per chart** — chart title states the insight ("Returns up 12% after Q3 policy change"), subtitle states metric/grain; description gives source and refresh.
7. **Consistency** — same colour for the same entity across the dashboard; same number formats; same time grain; aligned axes.
8. **Accessibility** — colour-blind-safe palettes, contrast ≥ 4.5:1, text ≥ 12 px, no colour-only meaning; theme tokens for light/dark.

## Chart selection (Superset types)
| Question | Use | Avoid |
|---|---|---|
| How is a KPI doing? | Big Number with Trendline (comparison to target/previous) | Gauge, speedometer |
| Trend over time | Line / Area (stacked only for parts of a whole) | Bar for long series |
| Compare categories | Horizontal Bar sorted | Pie, radar, word cloud |
| Part-to-whole (few parts) | Stacked bar 100% / Pie ≤ 3 slices | Donut with many slices |
| Distribution | Histogram, Box plot | |
| Relationship | Scatter with trend | Bubble unless 3rd variable essential |
| Lookup/detail | Table with conditional formatting, pagination | Pivot with > 15 columns |
| Funnel/flow | Funnel, Sankey (few stages) | |
| Geo | Country/Deck.gl with sparse colour | Chloropleth for counts not normalised |
| Matrix comparisons | Matrixify (6.1) small multiples | Dual-axis mixed charts |

## Layout and filters
- 12-column grid; KPIs top row (4–6 tiles), trends second, breakdowns third, detail tables last/tab; markdown headers per section; whitespace over borders.
- Native filters: ≤ 6 visible, sensible defaults (bounded time range), scoping per section, dependent filters where cardinality is high; cross-filters only for exploratory dashboards; filter bar collapsed for embedded viewers if the host controls context.
- Drill-to-detail enabled for aggregated charts; drill-by for hierarchies.
- Naming per `pipeline-naming`/`data-asset-naming`; slugs stable; certification + owner + tags required for production.

## Definition of done
Audience and questions documented; KPIs defined with grain/target; every chart has title-as-message, description, format; passes thirteen-mistakes review; p95 load within budget; synced from Git; accessibility check; dark/light theme verified.

See `references/sources.md` and `examples/dashboard-review-checklist.md`.
