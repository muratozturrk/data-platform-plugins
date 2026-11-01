"""The decluttering steps of Exercise 3.4 (and the chapter intro) as a lint over Superset chart params.

Only steps that map to a form_data key of the ECharts timeseries charts (Superset 4.1.2) are
covered. Steps with no chart-level knob (heavy title lines, bar thickness, labels inside bars,
direct labels at line ends, title alignment and color) are documented in the chapter summary.

Form_data keys come from the plugin's control panel:
https://github.com/apache/superset/tree/4.1.2/superset-frontend/plugins/plugin-chart-echarts/src/Timeseries
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # only needed for the live-API helpers; offline linting does not import it
    from common.superset_client import SupersetClient

_DECIMALS = re.compile(r"\.(\d+)f")
BAR_VIZ = {"echarts_timeseries_bar", "dist_bar", "bar"}


@dataclass(frozen=True)
class Finding:
    rule: str
    step: str       # Exercise 3.4 step, or "intro" for the chapter's opening list of clutter
    message: str


def lint_chart(viz_type: str, params: dict, temporal_x: bool = False, max_decimals: int = 0) -> list[Finding]:
    """Return the clutter the book would remove from a chart saved with these params.

    `max_decimals` is the context-dependent precision the audience needs (step 6 says this is
    always context-dependent): 0 for day counts, 1 for a 1-5 satisfaction score, and so on.
    """
    p = params
    out: list[Finding] = []
    if p.get("minorSplitLine"):
        out.append(Finding("minor-gridlines", "2", "minor gridlines add lines that carry no information"))
    fmt = p.get("y_axis_format") or ""
    m = _DECIMALS.search(fmt)
    if m and int(m.group(1)) > max_decimals:
        out.append(Finding("trailing-zeros", "3", f"y_axis_format {fmt!r} prints {m.group(1)} decimals; "
                                                  f"the audience needs at most {max_decimals}"))
    if not p.get("y_axis_title"):
        out.append(Finding("axis-title", "3", "no y-axis title: the audience has to guess what is plotted"))
    if p.get("xAxisLabelRotation") not in (None, 0):
        out.append(Finding("diagonal-text", "4", f"x labels rotated {p['xAxisLabelRotation']} degrees are slower to read"))
    tf = p.get("x_axis_time_format") or ""
    if temporal_x and ("%Y" in tf or "%y" in tf):
        out.append(Finding("repeated-year", "4", "the year repeats on every x label; use %b and put the year in x_axis_title"))
    if p.get("show_value"):
        out.append(Finding("redundant-labels", "7", "value labels and a y-axis together are redundant; keep one"))
    if viz_type in BAR_VIZ and temporal_x:
        out.append(Finding("bars-over-time", "8", "data over time drawn as bars; a line uses less ink per series"))
    single_series = len(p.get("metrics", [])) == 1 and not p.get("groupby")
    if single_series and p.get("show_legend"):
        out.append(Finding("legend-single-series", "9", "a legend for one series is a search with nothing to find"))
    if p.get("markerEnabled"):
        out.append(Finding("data-markers", "intro", "a marker on every point competes with the line"))
    return out


def lint_superset_chart(superset: "SupersetClient", chart_id: int, max_decimals: int = 0) -> list[Finding]:
    """Read a saved chart and its dataset from the API and lint it."""
    chart = superset.chart(chart_id)
    params = superset.chart_params(chart_id)
    x_axis = params.get("x_axis")
    dataset_id = int(params["datasource"].split("__")[0])      # "12__table", as Explore stores it
    temporal = {c["column_name"] for c in superset.dataset(dataset_id)["columns"] if c["is_dttm"]}
    return lint_chart(chart["viz_type"], params, temporal_x=x_axis in temporal, max_decimals=max_decimals)


if __name__ == "__main__":
    # Offline use on an exported chart YAML:  python declutter_lint.py chart.yaml [max_decimals]
    import sys
    import yaml  # PyYAML

    doc = yaml.safe_load(open(sys.argv[1], encoding="utf8"))
    params = doc.get("params", {})
    for f in lint_chart(doc.get("viz_type", ""), params, temporal_x=bool(params.get("x_axis")),
                        max_decimals=int(sys.argv[2]) if len(sys.argv) > 2 else 0):
        print(f"[{f.rule}] (step {f.step}) {f.message}")
