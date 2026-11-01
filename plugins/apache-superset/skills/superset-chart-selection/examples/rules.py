"""Chapter 2 ("Scales", Practical Charts) encoded as reusable checks over Superset charts.

Every check takes the chart's saved `params` (the Explore form_data that
`GET /api/v1/chart/{id}` returns) and, where the rule depends on the data, the rows that
`POST /api/v1/chart/data` returned. A check returns a list of `Finding`s; an empty list means
the chart follows the book's guideline. The rule ids reference the book's key takeaways.

The form_data keys used here are the ones the ECharts plugins in Superset 4.1.2 store
(superset-frontend/plugins/plugin-chart-echarts/src/Timeseries and MixedTimeseries controls).
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class Finding:
    rule: str
    message: str

    def __str__(self) -> str:
        return f"[{self.rule}] {self.message}"


def rule_ids(findings: Iterable[Finding]) -> set[str]:
    return {f.rule for f in findings}


# ----------------------------------------------------------------------------------------
# form_data helpers
# ----------------------------------------------------------------------------------------

BAR_VIZ_TYPES = {"echarts_timeseries_bar", "dist_bar"}


def metric_labels(metrics: Sequence) -> list[str]:
    """Labels of ad hoc metrics (dicts) or saved metrics (strings) as Superset names the series."""
    out = []
    for m in metrics or []:
        if isinstance(m, dict):
            out.append(m.get("label") or f"{m.get('aggregate')}({m['column']['column_name']})")
        else:
            out.append(str(m))
    return out


def is_bar_chart(params: dict) -> bool:
    return params.get("viz_type") in BAR_VIZ_TYPES or params.get("seriesType") == "bar"


def starts_at_zero(params: dict) -> bool:
    """ECharts timeseries: the axis includes zero unless `truncateYAxis` is on, and then
    `y_axis_bounds[0]` (null = ECharts picks a "nice" minimum near the data) decides."""
    if not params.get("truncateYAxis", False):
        return True
    lower, upper = params.get("y_axis_bounds") or [None, None]
    # The baseline is inside the range: lower <= 0 <= upper (an open upper bound ends above the data).
    return lower is not None and lower <= 0 and (upper is None or upper >= 0)


# ----------------------------------------------------------------------------------------
# Categorical scales (book pp. 55-58)
# ----------------------------------------------------------------------------------------

def check_categorical_sort(params: dict, ordinal_order: Sequence[str] | None = None,
                           labels: Sequence[str] | None = None) -> list[Finding]:
    """Non-sequential categories: sort by value. Sequential categories: built-in order."""
    findings: list[Finding] = []
    labels_of_metrics = metric_labels(params.get("metrics", []))
    sort_key = params.get("x_axis_sort")
    if ordinal_order is None:
        if sort_key not in labels_of_metrics:
            findings.append(Finding("cat.sort_by_value",
                                    f"non-sequential categories should be sorted by value "
                                    f"(x_axis_sort is {sort_key!r}, metrics are {labels_of_metrics})"))
    else:
        if sort_key != params.get("x_axis"):
            findings.append(Finding("cat.built_in_order",
                                    "sequential categories must keep their built-in order "
                                    f"(x_axis_sort is {sort_key!r}, expected the x-axis column)"))
        if labels is not None:
            ascending = params.get("x_axis_sort_asc", True)
            shown = sorted(labels, reverse=not ascending)
            expected = [lbl for lbl in ordinal_order if lbl in labels]
            if shown != expected:
                findings.append(Finding("cat.built_in_order",
                                        f"axis order {shown} differs from built-in order {expected}"))
    return findings


def check_categorical_axis_label(params: dict, categories_obvious: bool) -> list[Finding]:
    title = (params.get("x_axis_title") or "").strip()
    if categories_obvious and title:
        return [Finding("cat.axis_label_redundant", f"axis label {title!r} repeats what the categories already say")]
    if not categories_obvious and not title:
        return [Finding("cat.axis_label_missing", "categories are not self-explanatory: add an axis label")]
    return []


def check_long_category_names(params: dict, labels: Sequence[str], max_len: int = 12) -> list[Finding]:
    """Long names: rotate the chart 90 degrees (horizontal bars) instead of rotating the text."""
    findings: list[Finding] = []
    longest = max((len(x) for x in labels), default=0)
    if longest > max_len and params.get("orientation", "vertical") != "horizontal":
        findings.append(Finding("cat.rotate_chart",
                                f"longest category name has {longest} characters: use horizontal bars"))
    findings += check_horizontal_text(params)
    return findings


def check_horizontal_text(params: dict) -> list[Finding]:
    rotation = params.get("xAxisLabelRotation", 0)
    if rotation not in (0, None):
        return [Finding("text.horizontal", f"x-axis labels are rotated by {rotation} degrees")]
    return []


# ----------------------------------------------------------------------------------------
# Quantitative scales: units, dual axes, number formats, log scales (book pp. 60-82)
# ----------------------------------------------------------------------------------------

def check_units_on_scale(params: dict, units: dict[str, str],
                         equivalent_units: Iterable[frozenset[str]] = ()) -> list[Finding]:
    """One unit of measurement per quantitative scale; dual axes only for equivalent units.

    `units` maps metric label -> unit of measurement (the chart creator's declaration).
    """
    findings: list[Finding] = []
    equivalents = {frozenset(e) for e in equivalent_units}

    def units_of(labels: Sequence[str]) -> set[str]:
        missing = [lbl for lbl in labels if lbl not in units]
        if missing:
            raise KeyError(f"no unit declared for metrics {missing}")
        return {units[lbl] for lbl in labels}

    a = units_of(metric_labels(params.get("metrics", [])))
    if params.get("viz_type") == "mixed_timeseries":
        b = units_of(metric_labels(params.get("metrics_b", [])))
        dual = params.get("yAxisIndex", 0) != params.get("yAxisIndexB", 0)
        if dual:
            pair = frozenset(a | b)
            if len(pair) > 1 and pair not in equivalents:
                findings.append(Finding("scale.dual_axis",
                                        f"dual-axis chart with non-equivalent units {sorted(pair)}: "
                                        "use a merged chart or an index chart"))
        else:
            a = a | b
    if len(a) > 1:
        findings.append(Finding("scale.mixed_units",
                                f"different units on the same scale {sorted(a)}: split into one chart per unit"))
    return findings


def check_merged_charts(params_list: Sequence[dict]) -> list[Finding]:
    """A merged chart = separate charts that share the horizontal scale (same x, grain, range)."""
    keys = ("x_axis", "time_grain_sqla", "time_range", "datasource")
    first = {k: params_list[0].get(k) for k in keys}
    findings = []
    for p in params_list[1:]:
        for k in keys:
            if p.get(k) != first[k]:
                findings.append(Finding("scale.merged_mismatch", f"{k} differs: {p.get(k)!r} vs {first[k]!r}"))
    return findings


MAGNITUDE_WORDS = ("thousand", "million", "billion", "trillion", "000s", " k", "(k", " m", "(m", " bn")
COMPACT_FORMATS = {"SMART_NUMBER"}


def is_compact_format(fmt: str | None) -> bool:
    """d3 formats with an SI (`s`) type, or Superset's adaptive SMART_NUMBER, hide the zeros."""
    if not fmt:
        return False
    return fmt in COMPACT_FORMATS or re.search(r"\.\d*~?s$", fmt) is not None or fmt.endswith("s")


def check_number_format(params: dict, data_max: float, large_from: float = 1e5) -> list[Finding]:
    findings: list[Finding] = []
    fmt = params.get("y_axis_format") or ""
    title = (params.get("y_axis_title") or "").lower()
    if abs(data_max) >= large_from:
        if not is_compact_format(fmt):
            findings.append(Finding("format.hide_zeros",
                                    f"values reach {data_max:,.0f}: hide the zeros (format {fmt!r} prints them all)"))
        if not any(w in title for w in MAGNITUDE_WORDS):
            findings.append(Finding("format.magnitude_label",
                                    f"label the order of magnitude in the axis title (title is {title!r})"))
    if "~" in fmt or fmt == "SMART_NUMBER":
        findings.append(Finding("format.inconsistent_decimals",
                                f"format {fmt!r} trims trailing zeros, so labels get different numbers of decimals"))
    if not title:
        findings.append(Finding("format.unit_label", "no unit of measurement on the quantitative scale"))
    return findings


def orders_of_magnitude(values: Sequence[float]) -> float:
    positive = [v for v in values if v > 0]
    return math.log10(max(positive) / min(positive)) if positive else 0.0


def check_log_axis(params: dict, values: Sequence[float], audience_data_savvy: bool = False) -> list[Finding]:
    findings: list[Finding] = []
    if params.get("logAxis", False):
        span = orders_of_magnitude(values)
        if not audience_data_savvy:
            findings.append(Finding("scale.log_axis",
                                    f"logarithmic scale on an everyday chart (data spans {span:.1f} orders of magnitude)"))
        if span < 2:
            findings.append(Finding("scale.log_pointless",
                                    f"data spans only {span:.1f} orders of magnitude: a log scale buys nothing"))
    return findings


def check_outlier_handling(big_picture: dict, detail: dict, detail_excludes: Sequence[str]) -> list[Finding]:
    """Our stand-in for the book's inset chart: a full chart plus a zoomed companion on the dashboard.
    Both must keep a zero baseline; the detail chart must drop the outliers through a filter."""
    findings = []
    for name, p in (("big picture", big_picture), ("detail", detail)):
        if not starts_at_zero(p):
            findings.append(Finding("zero.bar_must_start_at_zero", f"{name} chart does not start at zero"))
        if p.get("logAxis"):
            findings.append(Finding("scale.log_axis", f"{name} chart uses a log scale"))
    filtered = {str(v) for f in detail.get("adhoc_filters", []) for v in (f.get("comparator") or [])}
    missing = [x for x in detail_excludes if x not in filtered]
    if missing:
        findings.append(Finding("outlier.detail_filter", f"detail chart does not exclude outliers {missing}"))
    return findings


# ----------------------------------------------------------------------------------------
# Quantitative scales: zero baseline and range (book pp. 83-92)
# ----------------------------------------------------------------------------------------

def check_zero_baseline(params: dict, has_zero_based_companion: bool = False,
                        distance_to_zero_irrelevant: bool = False) -> list[Finding]:
    if starts_at_zero(params):
        return []
    if is_bar_chart(params):
        return [Finding("zero.bar_must_start_at_zero",
                        f"bar chart truncated at {params.get('y_axis_bounds')}: bar length must mean value")]
    if has_zero_based_companion or distance_to_zero_irrelevant:
        return []
    return [Finding("zero.default_extend_to_zero",
                    "truncated scale without a zero-based companion chart: small changes will look volatile")]


def scale_limits(values: Sequence[float], theoretical_min: float | None = None,
                 theoretical_max: float | None = None, breathing: float = 0.15,
                 lower_mode: str = "zero") -> tuple[float, float]:
    """Lower and upper limit of a quantitative scale, pp. 89-91.

    All positive: lower = 0 (default), the theoretical minimum, or 10-20% below the spread;
    upper = lower + (max - lower) * (1 + breathing), cut at the theoretical maximum.
    All negative: the same upside down. Mixed signs: breathing room above and below the spread.
    """
    if not 0.10 <= breathing <= 0.20:
        raise ValueError("breathing room should be 10% to 20%")
    lo, hi = min(values), max(values)
    spread = hi - lo

    def cap(lower: float, upper: float) -> tuple[float, float]:
        if theoretical_min is not None:
            lower = max(lower, theoretical_min)
        if theoretical_max is not None:
            upper = min(upper, theoretical_max)
        return lower, upper

    if lo >= 0:
        if lower_mode == "zero":
            lower = 0.0
        elif lower_mode == "theoretical":
            if theoretical_min is None:
                raise ValueError("lower_mode='theoretical' needs theoretical_min")
            lower = theoretical_min
        elif lower_mode == "spread":
            lower = lo - spread * breathing
        else:
            raise ValueError(lower_mode)
        return cap(lower, lower + (hi - lower) * (1 + breathing))
    if hi <= 0:
        upper = 0.0 if lower_mode == "zero" else (theoretical_max if lower_mode == "theoretical" else hi + spread * breathing)
        return cap(upper - (upper - lo) * (1 + breathing), upper)
    return cap(lo - spread * breathing, hi + spread * breathing)


def check_scale_range(params: dict, values: Sequence[float], theoretical_min: float | None = None,
                      theoretical_max: float | None = None) -> list[Finding]:
    findings: list[Finding] = []
    lower, upper = params.get("y_axis_bounds") or [None, None]
    if upper is None:
        return [Finding("range.upper_unset", "upper limit left to the renderer: breathing room is not controlled")]
    if theoretical_max is not None and upper > theoretical_max:
        findings.append(Finding("range.beyond_theoretical_max", f"upper limit {upper} exceeds {theoretical_max}"))
    if theoretical_min is not None and lower is not None and lower < theoretical_min:
        findings.append(Finding("range.beyond_theoretical_min", f"lower limit {lower} is below {theoretical_min}"))
    base = lower if lower is not None else min(0.0, min(values))
    room = (upper - max(values)) / (max(values) - base) if max(values) > base else 0.0
    capped = theoretical_max is not None and upper == theoretical_max
    if not capped and not 0.10 <= room <= 0.20:
        findings.append(Finding("range.breathing_room", f"breathing room is {room:.0%}, aim for 10% to 20%"))
    return findings


# ----------------------------------------------------------------------------------------
# Time scales (book pp. 98-109)
# ----------------------------------------------------------------------------------------

def check_time_direction(params: dict, x_is_temporal: bool) -> list[Finding]:
    findings = []
    if x_is_temporal and params.get("orientation", "vertical") == "horizontal":
        findings.append(Finding("time.left_to_right", "time runs top to bottom: keep time on the horizontal axis"))
    return findings + check_horizontal_text(params)


_PERIOD_RE = re.compile(r"^\s*(-?\d+)\s*-\s*(-?\d+)\s*$")


def check_equal_time_intervals(labels: Sequence[str]) -> list[Finding]:
    """Interval labels such as '1960-1969' must all cover the same number of years."""
    widths = []
    for lbl in labels:
        m = _PERIOD_RE.match(str(lbl))
        if not m:
            return [Finding("time.unparsable_interval", f"cannot read interval label {lbl!r}")]
        widths.append(int(m.group(2)) - int(m.group(1)) + 1)
    if len(set(widths)) > 1:
        return [Finding("time.equal_intervals", f"intervals have different lengths: {dict(zip(labels, widths))}")]
    return []


def check_incomplete_period(params: dict, period_starts: Sequence[str], grain: str, as_of: str) -> list[Finding]:
    """The last period must be complete at `as_of`, or be visibly marked through an annotation layer."""
    import pandas as pd
    offset = {"P1D": pd.offsets.Day(1), "P1W": pd.offsets.Week(1), "P1M": pd.offsets.MonthBegin(1),
              "P3M": pd.offsets.QuarterBegin(startingMonth=1), "P1Y": pd.offsets.YearBegin(1)}[grain]
    last_start = max(pd.Timestamp(p) for p in period_starts)
    last_end = last_start + offset
    if last_end <= pd.Timestamp(as_of):
        return []
    marked = [layer for layer in params.get("annotation_layers", [])
              if layer.get("show", True) and layer.get("annotationType") == "INTERVAL"]
    if marked:
        return []
    return [Finding("time.incomplete_period",
                    f"period starting {last_start.date()} is incomplete at {as_of} and is shown like a complete one")]
