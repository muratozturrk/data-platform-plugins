---
name: superset-storytelling-declutter
description: "Storytelling and decluttering standard for Superset charts and dashboards, after Knaflic (Storytelling with Data and Let's Practice): the six lessons (understand the context, choose an effective visual, eliminate clutter, focus attention, think like a designer, tell a story), the Big Idea, the storyboard, and a before/after declutter sequence mapped to what Superset 4.1.2 controls can and cannot do, with an offline declutter lint over chart YAML. Use when turning a dashboard into an explanatory deliverable, writing chart titles as takeaways, decluttering a chart, deciding where to focus attention, or checking which clutter Superset lets you remove."
---

# Storytelling and Declutter in Superset

Rules are in my own words from Knaflic (SWD, SWDLP). Superset behaviour is labelled **[4.1.2 lab]** where verified on 4.1.2 only; re-check on your version.

## The six lessons -> what you do in Superset
| Lesson | Rule | Superset lever |
|---|---|---|
| 1 Context | Name audience, action and Big Idea before any chart; narrow a wide audience to the one decision maker | Brief (see `superset-dashboard-lifecycle`); Markdown header states audience + takeaway |
| 2 Choose a visual | Compare on a common baseline; bars start at zero, lines may zoom; avoid dual y-axes and mixed year/quarter axes; sketch on paper first | Chart type per `superset-dashboard-design`; y-axis bounds only on lines |
| 3 Declutter | Every element must earn its place; use Gestalt (proximity, similarity, enclosure, closure, continuity, connection) to group and to drop borders | Table below; dashboard CSS template removes card chrome |
| 4 Focus | Push context to grey, one accent hue, sparingly; same colour = same entity everywhere | `label_colors` in dashboard `json_metadata` (dashboard-wide); table conditional formatting |
| 5 Design | Affordances (say how to read it), aesthetics (align, round consistently), accessibility (alt text, contrast, direct labels), acceptance (start familiar, show side by side) | Chart `description`, number/date formats, Markdown "how to read" |
| 6 Story | Takeaway in a sentence-case title; exploring is not explaining; plot -> tension -> action arc for presentations | Markdown rows in `position_json`; tabs as story steps; annotation layers for events |

## Big Idea and storyboard
- **Big Idea**: one complete sentence with your point of view and what is at stake. If the "why" is missing it is a topic, not an idea.
- **Storyboard**: brainstorm 20+ notes, edit with a discard pile, test on a colleague before touching Superset. It is the cheapest place to change structure.
- Say the data in three sentences, pick one: it becomes the chart title and sometimes changes the chart (dual axis -> single-axis 100 % area).
- Store the Big Idea where it travels: dashboard title + top Markdown row. **[4.1.2 lab]** `json_metadata` rejects unknown keys, so audience/decision metadata cannot live on the dashboard object.

## Declutter sequence mapped to Superset (ECharts timeseries)
| Step (SWDLP ch3, 15-step example) | Setting | Possible? |
|---|---|---|
| 2 Gridlines | `minorSplitLine: false` | Minor only; major cannot be switched off |
| 3 Trailing zeros, axis title | `y_axis_format` (`,d` or `SMART_NUMBER`), `y_axis_title` | Yes |
| 4 Diagonal text, repeated year | `xAxisLabelRotation: 0`, `x_axis_time_format: "%b"`, year in `x_axis_title` | Yes |
| 7 Axis or labels, not both | `show_value: false` | Keep the axis; no "hide y-axis" |
| 8 Line instead of bars over time | `viz_type: echarts_timeseries_line` | Yes |
| 13-14 Thin dashed goal line | annotation layer `FORMULA`, `style: dashed`, `width: 1`, grey | Yes |
| 15 Remove colour | same grey for every series via `label_colors` | Dashboard level only |
| 1, 5, 6, 9-12 (heavy lines, bar width, labels in bars, direct labels, title colour) | none | **Not available**; card borders only via dashboard CSS |

Other limits **[4.1.2 lab]**:
- `show_value` and markers are chart-wide (cannot label only the last point or the focus line).
- `label_colors` is dashboard-wide: one label cannot be blue on one tab and grey on another; add a second grouping column for the background version.
- Inline colour in Markdown is stripped by `HTML_SANITIZATION` at render time (stored text is kept).
- Light grey for context #bfbfbf measures 1.84:1 against white and fails 3:1; #8c8c8c (3.36:1) passes. Focus and accessibility pull apart, so measure the trade-off.
- Footnote text lives in the chart `description` (metadata, not canvas).

## Titles and words
- Title = takeaway in sentence case; description = metric, grain, source, refresh.
- When presenting: at most two visuals per view; repeat one short phrase so the audience can retell it.
- Dashboards explore; to communicate, export a few annotated views (or tabs) and drop uniform or empty series.

## Offline lint
`examples/declutter_lint.py` checks an exported chart YAML (`python declutter_lint.py chart.yaml [max_decimals]`; PyYAML only, no Superset client). `max_decimals` depends on the audience (0 for day counts, 1 for a 1-5 score). Findings are prompts to look, not verdicts. Pair with the thirteen-mistakes lint in `superset-dashboard-design/examples` (command `superset-dashboard-lint`).

## Definition of done
Big Idea written; storyboard reviewed by one person; every chart title is a takeaway; lint clean or exceptions noted; one accent hue maximum; "so what?" answered aloud.

See `references/sources.md` and `examples/README.md`.
