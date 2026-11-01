# Examples

Copied unchanged from a Superset 4.1.2 lab (Practical Charts, chapter 2 "Scales").

- `rules.py`: stdlib-only checks over a chart's saved `params` (the form_data returned by `GET /api/v1/chart/{id}`) and optionally the rows from `POST /api/v1/chart/data`. Each `check_*` returns a list of `Finding(rule, message)`; an empty list means the chart follows the guideline. Covers category sort, long labels, units, merged charts, number format, log axis, zero baseline, scale range, time direction, equal time intervals and incomplete periods.
- `chartdata.py`: small helpers for time-axis queries (`time_axis`, `temporal_filter`, `as_timestamp`); needs `pandas`.

Usage (run from this folder):

```python
from rules import check_zero_baseline, rule_ids
params = {"viz_type": "echarts_timeseries_bar", "truncateYAxis": True, "y_axis_bounds": [50, 100]}
print(rule_ids(check_zero_baseline(params)))
```

The form_data keys match the ECharts plugins of Superset 4.1.2; re-check them on other versions.
