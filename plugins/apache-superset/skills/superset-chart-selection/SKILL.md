---
name: superset-chart-selection
description: "Chart-type choice, scale rules and number/date formatting rules for Superset, distilled from Practical Charts, Storytelling with Data, Data Visualisation (Kirk) and Information Dashboard Design, each mapped to a Superset viz_type that actually exists (checked on 4.1.2) with the settings to use and what Superset cannot do. Covers categorical, quantitative and time scales, zero baseline, dual axes, part-to-whole, maps, tables, combo charts, charts to avoid, number formats (d3 strings), date formats. Use when choosing a viz_type for a question, reviewing a chart's axes or formats, or explaining why a requested chart type is unavailable."
---

# Chart selection, scales and formatting for Superset

Rules are paraphrased from the books (chapter cited); the Superset column is our mapping, verified on 4.1.2 only (6.x may differ). For API limits see `superset-chart-api-limits`; for colour see `superset-color-accessibility`. Rule checkers: `examples/rules.py`.

## 1. Choose by the question (PC ch 6-12, SWD ch 2, IDD ch 6)
| Question | Book guidance | Superset viz_type / setting | Cannot do |
|---|---|---|---|
| One or two numbers | Text beats a chart (SWD 2) | `big_number_total`; `big_number` (with trendline) for context | Gauge-style dials: avoid (`gauge_chart` exists, discouraged) |
| Exact lookup, export | Table alone only for lookup/import (PC 10) | `table`, `pivot_table_v2`; set d3 formats per column | Zebra striping control |
| Compare categories | Bars, sorted by value unless ordinal (PC 2, 9) | `echarts_timeseries_bar` (horizontal for long names), `dist_bar` | Bar width, per-bar colour (use two CASE metrics) |
| Change over time, regular intervals | Line or bar (PC 6) | `echarts_timeseries_line`; `echarts_area` only for stacked parts | Per-series line style, bump chart |
| Irregular persistent values | Step chart (PC 6) | `echarts_timeseries_step` (confirm via `registered_viz_keys`) | |
| Rank over time | Bumps chart (PC 6) | None; lines on a rank column | Bump chart |
| Two periods | Arrow chart rather than slope (PC 6, 12) | Not available; two-series bar or table with delta | Slope, arrow, dumbbell |
| Part of a whole, few parts | Pie only when the point is a fraction; stacked bar for subtotal; bars to compare (PC 7) | `pie` (<= 6-7 slices, no donut), stacked bar, `treemap_v2` (one metric) | Exploded slices; pie of rates is accepted by the API but invalid |
| Parts with negatives or overlap | Not a valid pie/stack; use bars or waterfall (PC 7) | `waterfall` | Running total comes from the browser |
| Distribution | Histogram / box plot (DVH) | `histogram_v2`, `box_plot` | |
| Relationship | Scatter | `echarts_timeseries_scatter`, `bubble_v2` | Trend line control on bubble |
| Many series over time | Up to 4-5 lines; else small multiples or heatmap (PC 6) | `heatmap_v2` | Small multiples (none in 4.1.2) |
| Geography | Map only if place matters; choropleth least recommended; add a bar chart of same data (PC 8) | `country_map`, `world_map` (no token); deck.gl needs Mapbox token | Tile maps, mini-line maps |
| Bar plus line | Combo only for pattern vs periods, distinguish by colour not type (PC 11) | `mixed_timeseries` | |
| Wanted, unavailable | Avoid for everyday charts: word cloud, packed bubble, bullet, funnel, radar, 3-D (PC 12) | `word_cloud`, `radar`, `funnel` exist; do not recommend; replace with bar / heatmap / 2-D | Packed bubble, true bullet (legacy `bullet` deprecated) |

## 2. Scales (PC ch 2, SWD ch 2)
| Rule | Superset setting | Note |
|---|---|---|
| Bar charts start at zero, never truncated; use an inset chart for small differences | Keep `truncateYAxis: false`; `y_axis_bounds` only works with `truncateYAxis: true` | No inset or broken axis |
| Default to zero baseline for lines too; truncate only when zero is meaningless and say so | `truncateYAxis`, `y_axis_bounds` | Check `rules.check_zero_baseline` |
| Sort non-sequential categories by value; keep ordinal order | `x_axis_sort`, `x_axis_sort_asc`; text sorts byte-wise, so add a rank column | Sort only by x column or a drawn metric |
| Avoid dual axes; use merged or index charts | `mixed_timeseries` is the only dual-axis chart | Prefer two charts |
| Log scale only for audiences that read it | `logAxis: true` (rendering only) | |
| Time runs left to right with equal intervals; drop or mark incomplete periods; gaps for missing values | Time axis column + grain; filter the latest partial period in SQL | Missing dates are bridged unless a NULL row exists |
| Units once, on the axis or title; avoid mixed units on one scale | Axis title, dataset metric label | |
| Rotate the chart, not the text, for long category names | `orientation: horizontal` | |

## 3. Number and date formats (PC ch 5, 10)
| Rule | Setting |
|---|---|
| Show only the digits the insight needs; hidden digits must be visible (K, M) | `y_axis_format: ".3s"` or `SMART_NUMBER`; billions as `B` with `SMART_NUMBER` (SI writes `G`) |
| Group full digits by three | `,.0f`; per table column `column_config[<metric>].d3NumberFormat` |
| Regional separators | `D3_FORMAT` in `superset_config.py` (one locale per server) |
| Dates in the audience's format, four-digit years, alphabetic months | `table_timestamp_format`, `x_axis_time_format` (strftime-style d3 strings) |
| State units once; align by comparability in tables; tone down borders | Column titles with units; right-align numbers |
| Keep the same format for the same measure on every chart | Set the format on the dataset metric (`d3format`) once |

## 4. Review order
1. Is a chart needed at all (section 1 row 1, table rows)? 2. Does the chosen type exist (`registered_viz_keys`)? 3. Scales (section 2). 4. Formats (section 3). 5. Colour (sibling skill). 6. Remove ink: gridlines, borders, redundant legends (PC ch 1, 4).

See `examples/README.md` and `references/sources.md`.
