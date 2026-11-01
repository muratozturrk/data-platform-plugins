"""Dashboard design lint for Superset 4.1.2: one rule per mistake in chapter 3 of
Stephen Few, "Information Dashboard Design" (3.1 .. 3.13).

The rules only read what the REST API returns for a dashboard: `position_json`
(layout tree), `json_metadata` (colors), `css` (visual chrome) and every linked chart's
`params` (form_data). They are heuristics: a rule firing means "look at this", not a
verdict. Each rule's docstring names the book page it comes from.
"""
from __future__ import annotations

import colorsys
import json
import re
from dataclasses import dataclass, field
from statistics import median
from typing import Callable

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # only needed for the live-API helpers; offline linting does not import it
    from common.superset_client import SupersetClient

GRID_UNIT_PX = 8          # superset-frontend/src/dashboard/util/constants.ts GRID_BASE_UNIT
ROW_GUTTER_PX = 16        # GRID_GUTTER_SIZE
# ponytail: one screen = 880 px of grid (1080p minus browser and Superset chrome);
# make it a parameter per audience/screen if the lint is reused.
SCREEN_PX = 880

INAPPROPRIATE_VIZ = {"pie": "pie (p. 47-48)", "radar": "radar (p. 50)",
                     "bubble_v2": "bubble (p. 49)", "world_map": "map (p. 51)",
                     "country_map": "map (p. 51)"}
BAR_VIZ = {"echarts_timeseries_bar", "dist_bar"}
LINE_VIZ = {"echarts_timeseries_line", "echarts_timeseries_smooth", "echarts_area"}
D3_DECIMALS = re.compile(r"\.(\d+)f")


@dataclass
class Finding:
    mistake: str      # "3.1" .. "3.13"
    where: str        # chart name or dashboard component
    detail: str


@dataclass
class DashboardCtx:
    title: str
    position: dict
    metadata: dict
    css: str
    charts: list[dict]  # [{"id", "name", "viz_type", "params"}]
    chart_data: Callable[[dict], dict] | None = None  # runs a chart's query (for data rules)
    findings: list[Finding] = field(default_factory=list)

    @classmethod
    def load(cls, superset: "SupersetClient", dashboard_id: int) -> "DashboardCtx":
        dash = superset.dashboard(dashboard_id)
        charts = []
        for c in superset.dashboard_charts(dashboard_id):
            params = superset.chart_params(c["id"])
            charts.append({"id": c["id"], "name": c["slice_name"], "viz_type": params.get("viz_type", ""),
                           "params": params})

        def run(query: dict, dataset_id: int) -> dict:
            return superset.chart_data(dataset_id, query)

        return cls(title=dash["dashboard_title"],
                   position=json.loads(dash.get("position_json") or "{}"),
                   metadata=json.loads(dash.get("json_metadata") or "{}"),
                   css=dash.get("css") or "", charts=charts, chart_data=run)

    @classmethod
    def from_export(cls, bundle_dir: str) -> "DashboardCtx":
        """Offline: build the context from an unzipped `superset export` / dashboards-as-code folder
        (dashboards/*.yaml + charts/*.yaml). Data rules (pie sums) are skipped: no chart_data."""
        import pathlib
        import yaml  # PyYAML

        root = pathlib.Path(bundle_dir)
        dash = yaml.safe_load(next(root.glob("**/dashboards/*.yaml")).read_text(encoding="utf8"))
        by_uuid = {}
        for f in root.glob("**/charts/*.yaml"):
            c = yaml.safe_load(f.read_text(encoding="utf8"))
            by_uuid[str(c.get("uuid"))] = c
        position = dash.get("position") or {}
        charts = []
        for comp in position.values():
            if isinstance(comp, dict) and comp.get("type") == "CHART":
                c = by_uuid.get(str(comp["meta"].get("uuid")))
                if c:
                    params = c.get("params") or {}
                    charts.append({"id": comp["meta"]["chartId"], "name": c.get("slice_name", ""),
                                   "viz_type": c.get("viz_type", params.get("viz_type", "")), "params": params})
        return cls(title=dash.get("dashboard_title", ""), position=position,
                   metadata=dash.get("metadata") or {}, css=dash.get("css") or "", charts=charts)

    # ---- layout helpers ----
    def components(self, kind: str) -> list[dict]:
        return [c for c in self.position.values() if isinstance(c, dict) and c.get("type") == kind]

    def rows_in(self, container_id: str) -> list[dict]:
        out = []
        for child in self.position.get(container_id, {}).get("children", []):
            comp = self.position.get(child, {})
            if comp.get("type") == "ROW":
                out.append(comp)
            elif comp.get("type") in ("TABS", "TAB"):
                out += self.rows_in(child)
        return out

    def row_height_px(self, row: dict) -> int:
        heights = [self.position[c]["meta"].get("height", 0) for c in row["children"]
                   if self.position.get(c, {}).get("type") in ("CHART", "MARKDOWN")]
        return (max(heights) if heights else 0) * GRID_UNIT_PX + ROW_GUTTER_PX

    def chart_boxes(self) -> list[dict]:
        return [c["meta"] for c in self.components("CHART")]

    def chart_name(self, chart_id: int) -> str:
        return next((c["name"] for c in self.charts if c["id"] == chart_id), f"chart {chart_id}")

    def viz_of(self, chart_id: int) -> str:
        return next((c["viz_type"] for c in self.charts if c["id"] == chart_id), "")


# ---------------------------------------------------------------- 3.1 .. 3.4: scope and context

def exceeds_single_screen(ctx: DashboardCtx) -> list[Finding]:
    """3.1 (p. 39-42): everything should be visible at once; no scrolling, no fragmentation."""
    out = []
    containers = [t["id"] for t in ctx.components("TAB")] or ["GRID_ID"]
    for cid in containers:
        px = sum(ctx.row_height_px(r) for r in ctx.rows_in(cid))
        if px > SCREEN_PX:
            out.append(Finding("3.1", cid, f"grid is {px} px tall, one screen is {SCREEN_PX} px: scrolling required"))
    if ctx.components("TABS"):
        names = [t["meta"].get("text", t["id"]) for t in ctx.components("TAB")]
        out.append(Finding("3.1", "TABS", f"data fragmented into tabs {names}; seen-together comparisons are lost"))
    return out


def inadequate_context(ctx: DashboardCtx) -> list[Finding]:
    """3.2 (p. 43-45): a lone number ("$736,502") means nothing; needs target, prior period, trend."""
    out = []
    for c in ctx.charts:
        p, name = c["params"], c["name"]
        if c["viz_type"] == "big_number_total" and not p.get("subheader"):
            out.append(Finding("3.2", name, "single value without target, prior period or trend"))
        if c["viz_type"] == "big_number" and not (p.get("compare_lag") or p.get("show_trend_line")):
            out.append(Finding("3.2", name, "big number without comparison lag or trend line"))
        if c["viz_type"] == "gauge_chart":
            if not p.get("intervals"):
                out.append(Finding("3.2", name, "gauge without good/bad ranges (intervals)"))
            if not p.get("show_axis_tick"):
                out.append(Finding("3.2", name, "gauge scale without labeled tick marks"))
    return out


def excessive_precision(ctx: DashboardCtx) -> list[Finding]:
    """3.3 (p. 45): $3,848,305.93 vs $3.8M; time to the second; 4-10 decimals."""
    out = []
    for c in ctx.charts:
        p, name = c["params"], c["name"]
        for key in ("y_axis_format", "number_format", "yAxisFormat"):
            m = D3_DECIMALS.search(str(p.get(key, "")))
            if m and int(m.group(1)) > 2:
                out.append(Finding("3.3", name, f"{key}={p[key]!r}: {m.group(1)} decimals"))
        for col, cfg in (p.get("column_config") or {}).items():
            m = D3_DECIMALS.search(str(cfg.get("d3NumberFormat", "")))
            if m and int(m.group(1)) > 2:
                out.append(Finding("3.3", name, f"column {col}: {m.group(1)} decimals"))
        if p.get("time_grain_sqla") in ("PT1S", "PT1M", "PT1H"):
            out.append(Finding("3.3", name, f"time grain {p['time_grain_sqla']} on an overview dashboard"))
    return out


def _metric_labels(p: dict) -> list[str]:
    ms = p.get("metrics") or ([p["metric"]] if p.get("metric") else [])
    return [m["label"] if isinstance(m, dict) else str(m) for m in ms]


def deficient_measure(ctx: DashboardCtx) -> list[Finding]:
    """3.4 (p. 46): show the variance (-9%), not actual and budget side by side."""
    out = []
    for c in ctx.charts:
        labels = [l.lower() for l in _metric_labels(c["params"])]
        has_actual = any("revenue" in l and "budget" not in l and "variance" not in l for l in labels)
        has_budget = any("budget" in l or "target" in l for l in labels)
        has_variance = any("variance" in l or "%" in l for l in labels)
        if has_actual and has_budget and not has_variance:
            out.append(Finding("3.4", c["name"], f"actual and budget as separate series {labels}; viewer must compute the gap"))
    return out


# ---------------------------------------------------------------- 3.5 .. 3.8: display media

def inappropriate_media(ctx: DashboardCtx) -> list[Finding]:
    """3.5 (p. 47-51): pies, bubbles, radars and maps for plain comparisons; pie slices must sum to 100%."""
    out = []
    for c in ctx.charts:
        p, name = c["params"], c["name"]
        if c["viz_type"] in INAPPROPRIATE_VIZ:
            out.append(Finding("3.5", name, f"{INAPPROPRIATE_VIZ[c['viz_type']]} where a bar graph or table works better"))
        if c["viz_type"] == "pie" and ctx.chart_data and p.get("datasource"):
            label = _metric_labels(p)[0]
            dataset_id = int(str(p["datasource"]).split("__")[0])
            res = ctx.chart_data({"columns": p["groupby"], "metrics": [p["metric"]], "row_limit": 100}, dataset_id)
            total = sum(r[label] for r in res["data"])
            if "%" in label and abs(total - 100) > 1:
                out.append(Finding("3.5", name, f"pie of a rate: slices sum to {total:.1f}%, not 100%"))
    return out


def meaningless_variety(ctx: DashboardCtx) -> list[Finding]:
    """3.6 (p. 51-52): the same question answered with different media for variety's sake."""
    by_dims: dict[tuple, set] = {}
    for c in ctx.charts:
        p = c["params"]
        if c["viz_type"] in ("table", "big_number", "big_number_total"):
            continue  # a table answers lookups, a graph answers comparisons: different questions (p. 47)
        dims = tuple(sorted(set((p.get("groupby") or []) + [p[k] for k in ("x_axis", "entity") if p.get(k)])))
        if dims:
            by_dims.setdefault(dims, set()).add(c["viz_type"])
    return [Finding("3.6", f"charts over {list(dims)}", f"{len(v)} different media: {sorted(v)}")
            for dims, v in by_dims.items() if len(v) > 1]


def poorly_designed_media(ctx: DashboardCtx) -> list[Finding]:
    """3.7 (p. 52-55): legends instead of direct labels, unsorted slices/bars, odometer numbers."""
    out = []
    for c in ctx.charts:
        p, name, vt = c["params"], c["name"], c["viz_type"]
        if vt == "pie" and p.get("show_legend") and not p.get("show_labels"):
            out.append(Finding("3.7", name, "legend instead of direct labels: eyes bounce between slices and legend"))
        if vt == "pie" and not p.get("sort_by_metric"):
            out.append(Finding("3.7", name, "slices not ordered by size"))
        if vt in BAR_VIZ and not p.get("time_grain_sqla") and not p.get("x_axis_sort"):
            out.append(Finding("3.7", name, "categorical bars not sorted by value"))
        fmt = str(p.get("y_axis_format") or p.get("number_format") or "")
        if fmt and fmt != "SMART_NUMBER" and "," not in fmt and "%" not in fmt and "s" not in fmt:
            out.append(Finding("3.7", name, f"format {fmt!r} has no thousands separator (odometer numbers)"))
    return out


def inaccurate_encoding(ctx: DashboardCtx) -> list[Finding]:
    """3.8 (p. 56): bar lengths lie when the scale does not start at zero."""
    out = []
    for c in ctx.charts:
        p = c["params"]
        lo = (p.get("y_axis_bounds") or [None, None])[0]
        if c["viz_type"] in BAR_VIZ and p.get("truncateYAxis") and lo not in (None, 0):
            out.append(Finding("3.8", c["name"], f"bar scale starts at {lo}, not 0"))
    return out


# ---------------------------------------------------------------- 3.9 .. 3.13: arrangement and look

def poor_arrangement(ctx: DashboardCtx) -> list[Finding]:
    """3.9 (p. 56-57): top-left is prime real estate; trends to compare belong over-under."""
    out = []
    rows = ctx.rows_in("GRID_ID")
    if rows:
        first = ctx.position.get(rows[0]["children"][0] if rows[0]["children"] else "", {})
        if first.get("type") == "MARKDOWN":
            out.append(Finding("3.9", "ROW-0", "top-left corner holds a logo/markdown block, not data"))
    for row in rows:
        lines = [ctx.chart_name(ctx.position[c]["meta"]["chartId"]) for c in row["children"]
                 if ctx.position[c]["type"] == "CHART" and ctx.viz_of(ctx.position[c]["meta"]["chartId"]) in LINE_VIZ]
        if len(lines) >= 2:
            out.append(Finding("3.9", row["id"], f"trend lines side by side {lines}; stack them to compare"))
    return out


def _saturation(hex_color: str) -> float:
    h = hex_color.lstrip("#")
    if len(h) != 6:
        return 0.0
    r, g, b = (int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))
    return colorsys.rgb_to_hsv(r, g, b)[1]


def _hue(hex_color: str) -> float:
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) / 255 for i in (0, 2, 4))
    return colorsys.rgb_to_hsv(r, g, b)[0] * 360


def ineffective_highlighting(ctx: DashboardCtx) -> list[Finding]:
    """3.10 (p. 57-58): when everything is equally prominent, nothing stands out."""
    boxes = ctx.chart_boxes()
    colors = ctx.metadata.get("label_colors") or {}
    highlighted = [k for k, v in colors.items() if _saturation(v) > 0.7]
    muted = [k for k, v in colors.items() if _saturation(v) < 0.35]
    singled_out_by_color = 1 <= len(highlighted) <= 2 and bool(muted)
    if len(boxes) >= 4 and len({(b["width"], b["height"]) for b in boxes}) == 1 and not singled_out_by_color:
        return [Finding("3.10", "GRID", f"all {len(boxes)} charts have the same size and no series is singled out by color")]
    return []


def useless_decoration(ctx: DashboardCtx) -> list[Finding]:
    """3.11 (p. 58-61): gradients, background images, ornate logos, control-panel chrome."""
    out = []
    seen = set()
    for pattern, what in (("gradient", "color gradient"), ("background-image", "background image"),
                          ("url(", "background image"), ("box-shadow", "drop shadow")):
        if pattern in ctx.css and what not in seen:
            seen.add(what)
            out.append(Finding("3.11", "css", f"{what} in dashboard CSS"))
    for md in ctx.components("MARKDOWN"):
        code = md["meta"].get("code", "")
        if "<img" in code and md["meta"].get("height", 0) >= 20:
            out.append(Finding("3.11", md["id"], "large logo/image block"))
    return out


def color_misuse(ctx: DashboardCtx) -> list[Finding]:
    """3.12 (p. 53, 61-62): bright colors only for what must stand out; red/green excludes the colour-blind."""
    out = []
    colors = ctx.metadata.get("label_colors") or {}
    bright = {k: v for k, v in colors.items() if _saturation(v) > 0.7}
    if len(bright) > 2:
        out.append(Finding("3.12", "label_colors", f"{len(bright)} saturated series colors: {sorted(bright)}"))
    hues = [_hue(v) for v in bright.values()]
    if any(h < 15 or h > 345 for h in hues) and any(90 < h < 150 for h in hues):
        out.append(Finding("3.12", "label_colors", "red vs green as the only good/bad cue (10% of men are colour-blind)"))
    return out


def unattractive_display(ctx: DashboardCtx) -> list[Finding]:
    """3.13 (p. 62-63): sections stretched to fill space; backgrounds too dark."""
    out = []
    areas = [b["width"] * b["height"] for b in ctx.chart_boxes()]
    if len(areas) >= 3:
        med = median(areas)
        for b in ctx.chart_boxes():
            if b["width"] * b["height"] > 3 * med:
                out.append(Finding("3.13", ctx.chart_name(b["chartId"]), f"stretched to {b['width']}x{b['height']}, 3x the median chart"))
    for m in re.finditer(r"background(?:-color)?:\s*(#[0-9a-fA-F]{6})", ctx.css):
        h = m.group(1).lstrip("#")
        lum = sum(int(h[i:i + 2], 16) for i in (0, 2, 4)) / (3 * 255)
        if lum < 0.35:
            out.append(Finding("3.13", "css", f"background {m.group(1)} is too dark (luminance {lum:.2f})"))
    return out


RULES = [exceeds_single_screen, inadequate_context, excessive_precision, deficient_measure,
         inappropriate_media, meaningless_variety, poorly_designed_media, inaccurate_encoding,
         poor_arrangement, ineffective_highlighting, useless_decoration, color_misuse,
         unattractive_display]


def lint(ctx: DashboardCtx, rules=RULES) -> list[Finding]:
    ctx.findings = [f for rule in rules for f in rule(ctx)]
    return ctx.findings


def mistakes(findings: list[Finding]) -> set[str]:
    return {f.mistake for f in findings}


if __name__ == "__main__":
    # python dashboard_lint.py <unzipped-export-dir>
    import sys

    for f in lint(DashboardCtx.from_export(sys.argv[1])):
        print(f"[Few {f.mistake}] {f.where}: {f.detail}")
