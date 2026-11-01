# declutter_lint.py

Lint over Superset chart `params` (ECharts timeseries form_data) that flags clutter the Storytelling with Data decluttering sequence would remove: minor gridlines, trailing zeros, missing axis title, rotated x labels, repeated year, redundant value labels, bars over time, legend on a single series, markers on every point.

Origin: Superset 4.1.2 course lab (SWDLP chapter 3, lab 3.2). Form_data keys come from the 4.1.2 ECharts Timeseries control panel; other versions may name them differently.

## Dependencies
- Library use `lint_chart(viz_type, params, temporal_x, max_decimals)`: standard library only.
- `lint_superset_chart(client, chart_id)` needs a client exposing `chart()`, `chart_params()` and `dataset()` (the lab's `SupersetClient`). The import is type-check only, so the module imports cleanly without it.
- CLI on an exported chart YAML: `python declutter_lint.py path/to/chart.yaml 0` (needs PyYAML).

Findings are heuristics. Steps with no chart-level control in 4.1.2 (heavy lines, bar width, labels inside bars, direct labels) are not covered; see the skill.
