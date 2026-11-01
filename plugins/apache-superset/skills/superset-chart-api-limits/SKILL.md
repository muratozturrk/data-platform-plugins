---
name: superset-chart-api-limits
description: "Verified facts and limits of the Superset REST API and chart builder, as tested on Superset 4.1.2: which viz_types exist (and how to check), which controls do not exist (gridlines, bar width, per-series style, per-bar colour) and workarounds, POST /api/v1/chart/data quirks, virtual dataset behaviour, dashboard json_metadata and chart-to-dashboard linking, security and log endpoints, colour scheme access. Use when creating or testing charts and dashboards through the API, when asked whether Superset can draw or configure something, or before promising a chart feature to a stakeholder."
---

# Superset chart and API limits (verified on 4.1.2)

Scope: everything below was observed on `apache/superset:4.1.2` with a Postgres metadata DB and warehouse, via REST. Items marked *bundle* were read from the served frontend JavaScript, not exercised in a browser; rendering was never checked visually. **Superset 6.x may differ** (new controls, theming, Matrixify): re-verify any "not available" row before relying on it. Runnable helpers are in `examples/`.

## Principles
1. The API validates almost nothing. `POST /api/v1/chart/` stores any `viz_type` and any `params`; "it saved" proves nothing. Validate viz_types against the frontend registry (`examples/viz_registry.py`, 63 keys on 4.1.2).
2. Assert on data, not pixels: `POST /api/v1/chart/data` returns `rowcount`, `data` and the generated SQL.
3. The absence of a control cannot be detected through the API; check the docs or the bundle.
4. Writes need the JWT bearer token and the CSRF token from `/api/v1/security/csrf_token/`, on the same session.

## viz_type facts
| Fact | Detail |
|---|---|
| Gauge | `gauge_chart` (not `echarts_gauge`) |
| Donut | `pie` with `donut: true` |
| Exist | `word_cloud`, `tree_chart`, `graph_chart`, `bubble_v2`, `mixed_timeseries` (the only dual-axis chart), `country_map`, `world_map`, `treemap_v2`, `heatmap_v2`, `waterfall`, `box_plot`, `radar`, `pivot_table_v2` |
| Do not exist | bump, slope, lollipop, packed bubble, Gantt, small multiples, cycle, animated charts |
| Deprecated | `bullet`, `time_table`: data only via `/superset/explore_json/` with a form-login cookie |
| Mapbox token needed | deck.gl charts, `deck_scatter`; not needed for `country_map`, `world_map`, `radar`, `box_plot`, `pivot_table_v2`, `heatmap_v2` |
| Silent truncation | `row_limit` cuts `pie`, `treemap_v2`, `heatmap_v2`, `bubble_v2` with no error; `treemap_v2` takes one metric |
| Quirks | `heatmap_v2.groupby` is a string; `waterfall` data = per-step deltas (running total is browser-side); `y_axis_bounds` needs `truncateYAxis: true`; `logAxis` changes rendering only (same SQL); `stack: "Expand"` is a browser-side 100% stack (data stays raw); `x_axis_sort` can name only the x column or a drawn metric; text categories sort byte-wise (`100+` before `20-29`), so use a rank column or date axis |
| Table | `conditional_formatting` stores constants only (no column-to-column rule) and any hex string |

## Missing controls and workarounds
| No form_data control for | Workaround |
|---|---|
| Per-bar colour on one metric | Two stacked CASE metrics, or a dimension column plus dashboard `label_colors` |
| Per-series line style / marker, per-point labels | None in Explore; use separate series or annotations |
| Major gridlines (only `minorTicks`, `minorSplitLine`) | Accept defaults; dashboard `css` at best |
| Hide y-axis on ECharts charts, bar width/gap, tick interval | None; dashboard `css` at best |
| Labels inside bars or at line ends, broken or inset axes, multi-level time axes | Separate inset chart; text annotation |
| Slopegraph, square-area, 3-D, textures, gridline/axis colours | Not available |
| Top-N plus "Other" | Merge the tail in SQL (virtual dataset); `series_limit` + `series_limit_metric` keeps top-N series only |

## Chart-data API quirks
- Sub-query in an ad hoc SQL metric: HTTP 400. The same text in an ad hoc filter or a column name such as `Amount ($)`: HTTP 403, issue 1037. Duplicate column/metric labels: 400.
- A columns-only query returns raw rows (no GROUP BY); raw-mode `table` still groups, and every `orderby` column must be in `columns`.
- Time grain needs a `BASE_AXIS` column object (`{"columnType": "BASE_AXIS", "sqlExpression": "<col>", "timeGrain": "P1M"}`) plus `extras.time_grain_sqla`. `PT15M` returns 500 on Postgres. Timestamps return as epoch ms; rows are not chronological (sort in tests). Missing dates are absent (the line bridges them); a NULL row draws a gap.
- Post-processing: rolling windows follow row order (send `orderby`); `pivot` then `contribution` needs `flatten` or HTTP 500; a cumulative line needs `cum` plus `orderby`; `boxplot` uses medians of halves.
- `result_type`: `drill_detail` (pages with `row_limit`/`row_offset`), `samples` (all dataset columns), `query` (SQL); `result_format: "csv"` downloads. A chart created by API has no `query_context` until a `PUT` saves one; CSV export returns 400 before that.
- NATIVE annotation layers need `showMarkers` and `hideLine` and return `annotation_data[name] = {columns, records}`; `FORMULA` layers return nothing (the browser draws them). A layer holding annotations cannot be deleted.
- A native filter on a nonexistent column is saved, then silently dropped by chart data (no WHERE, no error).
- `GET /chart/{id}` has no `datasource_id` (parse `params.datasource`, `"<id>__table"`); `GET /dashboard/{id}/charts` returns `form_data` but no top-level `viz_type`.

## Datasets
- Window functions pass through to Postgres but run before dashboard filters. A virtual dataset cannot reference another by name (inline a CTE); nested sub-selects in `FROM` work. `random()` re-rolls on every query.
- The column list is frozen: `PUT /dataset/{id}/refresh` after table changes. `PUT /dataset/{id}` with `metrics` replaces the whole list and needs ids for existing metrics (resend `count`).
- Jinja needs `ENABLE_TEMPLATE_PROCESSING`; `url_param()` reads the chart-data POST query string, then `template_params`, then a default. No live what-if sliders.

## Dashboards and json_metadata
- Link a chart via the chart's `dashboards` field (`PUT /chart/{id}`); `position_json` only places linked charts and is not validated (a 20-wide chart publishes silently; it holds string entries such as `DASHBOARD_VERSION_KEY`, so guard with `isinstance(node, dict)`). `CHART-<id>` appears once per dashboard. `SupersetClient.attach_charts` does both.
- `PUT /dashboard/{id}` rejects unknown `json_metadata` keys (400). These round-trip: `label_colors`, `color_scheme`, `refresh_frequency`, `roles`, `certified_by`, `certification_details`, native filters (cascade, defaults, `scope.excluded`), `chart_configuration`, cross-filter scope, `css`, `cross_filters_enabled`.
- API-written layouts leave `uuid: null` on CHART nodes, so exports differ: stamp each node with the chart uuid. Import with `overwrite` restores layout and filters but keeps charts; use `/chart/import/` for chart edits.
- Not present: dashboard theme, device layout, overlays, comments. Chrome only via `css`; `css_template` stores but does not apply. `HTML_SANITIZATION` strips inline `<span style>` in Markdown.
- `create_chart(replace=True)` replaces by name and can delete a same-named chart on another dashboard: use distinct names. `ALERT_REPORTS` was off in the image (`/api/v1/report/` returned 404).

## Security, logs, colours
- With `FAB_ADD_SECURITY_API = True`, users and roles are creatable; `PUT /security/users/{id}` with only `roles` locks the user out (401): send `password` too. Gamma sees a dashboard only if listed in its `roles`. RLS via `/api/v1/rowlevelsecurity/` works; `EMBEDDED_SUPERSET` guest tokens carry RLS rules.
- `GET /api/v1/log/` records every API read, including its own listing: order by `dttm` ascending. Back-dated history needs a direct insert into the metadata `logs` table.
- Colours: scheme hex values live only in the frontend bundle; `EXTRA_*_COLOR_SCHEMES` appear only in the HTML bootstrap payload (`SupersetClient.frontend_bootstrap()`). No per-chart label colours: pin via dashboard `label_colors`. `supersetColors` has 20 entries, the 5th is grey `#666666`, and 5 of 20 reach 3:1 on white. See `superset-color-accessibility`.

See `examples/README.md` and `references/sources.md`.
