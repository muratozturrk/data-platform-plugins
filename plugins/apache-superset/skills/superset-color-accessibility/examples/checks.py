"""Colour checks the book asks for but Superset does not perform.

Chapter 9 of "Data Visualisation: A Handbook for Data Driven Design" (Kirk, 2019) reasons
about colour in HSL terms (hue, saturation, lightness) and warns about rainbow scales,
indistinct dark ends of diverging scales, too many categorical hues, greyscale printing
and deuteranopia. Superset stores colour choices (scheme names, label_colors, hex values)
but never evaluates them, so these checks run in Python over the hex values.

Conversions follow the sRGB / CIE definitions:
  - relative luminance and contrast ratio: WCAG 2.1, https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
  - CIE L*a*b* (D65) from linear sRGB: Bruce Lindbloom's reference formulas, http://www.brucelindbloom.com/
  - deuteranopia simulation: Machado, Oliveira & Fernandes (2009), severity 1.0 matrix,
    applied in linear RGB (the same matrix the "Coblis"-style simulators use).
"""
from __future__ import annotations

import json
import math
from itertools import combinations
from pathlib import Path

CSS_NAMES = {"white": "#FFFFFF", "black": "#000000", "yellow": "#FFFF00", "red": "#FF0000"}
SCHEMES_FILE = Path(__file__).with_name("superset_schemes.json")

# ---------- basic conversions ----------


def normalize_hex(color: str) -> str:
    c = CSS_NAMES.get(color.strip().lower(), color.strip())
    if not c.startswith("#"):
        raise ValueError(f"not a hex colour: {color!r}")
    c = c[1:]
    if len(c) == 3:
        c = "".join(ch * 2 for ch in c)
    if len(c) != 6:
        raise ValueError(f"not a hex colour: {color!r}")
    return "#" + c.upper()


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    c = normalize_hex(color)[1:]
    return int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)


def rgb_to_hex(rgb: tuple[float, float, float]) -> str:
    return "#" + "".join(f"{max(0, min(255, round(v))):02X}" for v in rgb)


def _to_linear(v: float) -> float:
    v = v / 255.0
    return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4


def _from_linear(v: float) -> float:
    v = max(0.0, min(1.0, v))
    return 255.0 * (12.92 * v if v <= 0.0031308 else 1.055 * v ** (1 / 2.4) - 0.055)


def linear_rgb(color: str) -> tuple[float, float, float]:
    r, g, b = hex_to_rgb(color)
    return _to_linear(r), _to_linear(g), _to_linear(b)


def relative_luminance(color: str) -> float:
    """WCAG relative luminance Y in [0, 1] (0 = black, 1 = white)."""
    r, g, b = linear_rgb(color)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast_ratio(a: str, b: str) -> float:
    """WCAG contrast ratio in [1, 21]. 3:1 is the floor for graphical objects (SC 1.4.11)."""
    la, lb = relative_luminance(a), relative_luminance(b)
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


def hsl(color: str) -> tuple[float, float, float]:
    """Hue (degrees 0-360), saturation (0-1), lightness (0-1): the book's working model (9.1)."""
    r, g, b = (v / 255.0 for v in hex_to_rgb(color))
    mx, mn = max(r, g, b), min(r, g, b)
    light = (mx + mn) / 2
    if mx == mn:
        return 0.0, 0.0, light
    d = mx - mn
    sat = d / (2 - mx - mn) if light > 0.5 else d / (mx + mn)
    if mx == r:
        h = ((g - b) / d) % 6
    elif mx == g:
        h = (b - r) / d + 2
    else:
        h = (r - g) / d + 4
    return (h * 60) % 360, sat, light


def lab(color: str) -> tuple[float, float, float]:
    """CIE L*a*b* under D65. L* is perceptual lightness (0 black .. 100 white)."""
    r, g, b = linear_rgb(color)
    x = (0.4124564 * r + 0.3575761 * g + 0.1804375 * b) / 0.95047
    y = (0.2126729 * r + 0.7151522 * g + 0.0721750 * b) / 1.00000
    z = (0.0193339 * r + 0.1191920 * g + 0.9503041 * b) / 1.08883

    def f(t: float) -> float:
        return t ** (1 / 3) if t > (6 / 29) ** 3 else t / (3 * (6 / 29) ** 2) + 4 / 29

    fx, fy, fz = f(x), f(y), f(z)
    return 116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz)


def lightness(color: str) -> float:
    return lab(color)[0]


def delta_e(a: str, b: str) -> float:
    """CIE76 colour difference. Roughly: < 2 invisible, < 10 hard to tell apart at a glance."""
    return math.dist(lab(a), lab(b))


def hue_distance(h1: float, h2: float) -> float:
    d = abs(h1 - h2) % 360
    return min(d, 360 - d)


def is_grey(color: str, max_saturation: float = 0.12) -> bool:
    """Grey = the unsaturated form of a hue (book, p. 262)."""
    return hsl(color)[1] <= max_saturation


# ---------- scale checks (quantitative colour, book 9.2) ----------


def lightness_steps(colors: list[str]) -> list[float]:
    ls = [lightness(c) for c in colors]
    return [b - a for a, b in zip(ls, ls[1:])]


def is_lightness_monotonic(colors: list[str], min_step: float = 1.0) -> bool:
    """True when every step moves lightness the same way (a usable sequential scale)."""
    steps = lightness_steps(colors)
    return all(s <= -min_step for s in steps) or all(s >= min_step for s in steps)


def hue_span(colors: list[str], min_saturation: float = 0.2) -> float:
    """Largest hue distance between any two saturated colours of the scale (0 = single hue)."""
    hues = [hsl(c)[0] for c in colors if hsl(c)[1] >= min_saturation]
    return max((hue_distance(a, b) for a, b in combinations(hues, 2)), default=0.0)


def diverging_report(colors: list[str]) -> dict:
    """How well a diverging scale meets the book's two rules (p. 255):
    mirrored classes carry the same intensity, and the two dark ends stay distinguishable."""
    n = len(colors)
    ls = [lightness(c) for c in colors]
    mirror = [abs(ls[i] - ls[n - 1 - i]) for i in range(n // 2)]
    mid = colors[n // 2] if n % 2 else None
    left = colors[: n // 2 + (n % 2)]  # odd length: the neutral midpoint belongs to both halves
    right = colors[n // 2:]
    return {
        "max_mirror_lightness_gap": max(mirror) if mirror else 0.0,
        "end_delta_e": delta_e(colors[0], colors[-1]),
        "end_hue_distance": hue_distance(hsl(colors[0])[0], hsl(colors[-1])[0]),
        "min_end_lightness": min(ls[0], ls[-1]),
        "midpoint_is_neutral": is_grey(mid) if mid else None,
        "left_monotonic": is_lightness_monotonic(left),
        "right_monotonic": is_lightness_monotonic(right),
    }


# ---------- categorical checks (book 9.2) ----------

MAX_NOMINAL_CATEGORIES = 12  # book, p. 259: beyond 12 there are no sufficiently different hues


def assign_scheme_colors(labels: list[str], scheme_colors: list[str]) -> dict[str, str]:
    """Simulates Superset's CategoricalColorScale: labels get scheme colours in order of
    first appearance and wrap around. The browser does this at render time; the lab
    reproduces it from the chart-data row order (an inference, not an API read)."""
    return {lbl: normalize_hex(scheme_colors[i % len(scheme_colors)]) for i, lbl in enumerate(labels)}


def lint_categorical(colors_by_label: dict[str, str], context_labels: tuple[str, ...] = ("Other",),
                     min_hue_gap: float = 20.0, max_categories: int = MAX_NOMINAL_CATEGORIES) -> list[str]:
    """Design lint over a label -> colour map for nominal data. Returns findings (empty = OK).

    Rules from the book: too many categories (p. 259); nominal categories must differ by
    hue, not by lightness of the same hue (p. 257, Figure 9.9); context categories such as
    'Other' belong in grey (p. 259, p. 263)."""
    findings: list[str] = []
    data = {k: normalize_hex(v) for k, v in colors_by_label.items() if k not in context_labels}
    if len(data) > max_categories:
        findings.append(f"{len(data)} data categories exceed the {max_categories}-hue budget")
    for lbl, col in data.items():
        if is_grey(col):
            findings.append(f"data category '{lbl}' is grey ({col}) and reads as context")
    for (la, ca), (lb, cb) in combinations(data.items(), 2):
        ha, sa, _ = hsl(ca)
        hb, sb, _ = hsl(cb)
        if sa < 0.2 or sb < 0.2:
            continue
        if hue_distance(ha, hb) < min_hue_gap:
            findings.append(f"'{la}' {ca} and '{lb}' {cb} share a hue (lightness-only difference)")
    for lbl in context_labels:
        if lbl in colors_by_label and not is_grey(colors_by_label[lbl]):
            findings.append(f"context category '{lbl}' is not grey: {colors_by_label[lbl]}")
    return findings


def emphasis_report(colors_by_label: dict[str, str]) -> dict:
    """Foreground vs background split (book p. 262-264): saturated labels vs grey labels."""
    fg = [k for k, v in colors_by_label.items() if not is_grey(v)]
    bg = [k for k, v in colors_by_label.items() if is_grey(v)]
    return {"foreground": fg, "background": bg}


# ---------- accessibility and medium (book 9.3) ----------

_DEUTAN = (  # Machado et al. 2009, deuteranopia, severity 1.0 (linear RGB in, linear RGB out)
    (0.367322, 0.860646, -0.227968),
    (0.280085, 0.672501, 0.047413),
    (-0.011820, 0.042940, 0.968881),
)


def simulate_deuteranopia(color: str) -> str:
    r, g, b = linear_rgb(color)
    sim = [m[0] * r + m[1] * g + m[2] * b for m in _DEUTAN]
    return rgb_to_hex(tuple(_from_linear(v) for v in sim))


def deutan_distance(a: str, b: str) -> tuple[float, float]:
    """(CIE76 distance as seen by a deuteranope, fraction of the normal-vision distance kept)."""
    normal = delta_e(a, b)
    sim = delta_e(simulate_deuteranopia(a), simulate_deuteranopia(b))
    return sim, (sim / normal if normal else 1.0)


def deutan_confusion_pairs(colors: list[str], max_delta_e: float = 15.0,
                           min_retained: float = 0.3) -> list[tuple[str, str, float, float]]:
    """Pairs a deuteranope would struggle with: too close after simulation, or a pair that
    looks far apart to normal vision but keeps less than `min_retained` of that distance
    (the red/green collapse of Figure 9.28)."""
    out = []
    for a, b in combinations(colors, 2):
        sim, kept = deutan_distance(a, b)
        if sim < max_delta_e or kept < min_retained:
            out.append((normalize_hex(a), normalize_hex(b), round(sim, 1), round(kept, 2)))
    return out


def greyscale_conflicts(colors: list[str], min_delta_l: float = 10.0) -> list[tuple[str, str, float]]:
    """Pairs that collapse when printed in black and white (book p. 273): lightness too close."""
    out = []
    for a, b in combinations(colors, 2):
        d = abs(lightness(a) - lightness(b))
        if d < min_delta_l:
            out.append((normalize_hex(a), normalize_hex(b), round(d, 1)))
    return out


# ---------- Superset scheme table ----------


def load_schemes() -> dict[str, dict]:
    """Scheme definitions extracted from the Superset 4.1.2 static bundle
    (superset-ui-core colorSchemes). lab_9_1 verifies them against the running server."""
    with SCHEMES_FILE.open(encoding="utf-8") as fh:
        return json.load(fh)


def scheme_colors(scheme_id: str) -> list[str]:
    return [normalize_hex(c) for c in load_schemes()[scheme_id]["colors"]]
