---
name: superset-color-accessibility
description: "Colour and accessibility rules for Superset charts and dashboards: categorical vs sequential vs diverging palettes, how many hues, grey for context, one colour per meaning, contrast thresholds (3:1 graphics, 4.5:1 text), colour-blind safety and traffic-light alternatives, Superset colour schemes, label_colors pinning, custom schemes via config, theming and what cannot be set. Includes a dependency-free colour-maths module. Use when choosing or reviewing chart colours, building a brand palette for Superset, fixing red/green status colours, or checking contrast and colour-blind safety."
---

# Colour and accessibility in Superset

Rules are paraphrased from Kirk (DVH ch 9) and Desbarats (PC ch 3); Superset behaviour is verified on 4.1.2 only (6.x adds theming and may differ). Checks: `examples/checks.py`.

## Principles
1. Colour carries information; if one neutral colour is clear, use it. Vary colour only to encode something (PC 3, DVH 9).
2. Match palette type to data: **categorical** (hue, nominal), **sequential** (lightness in one hue, darker = more), **diverging** (two hues around a meaningful breakpoint). Wrong type is the commonest error (PC 3).
3. Nominal: at most 7-8 colours legible, about 12 absolute ceiling; beyond that filter, merge into "Other" (grey) or facet. Do not vary lightness for nominal classes (DVH 9).
4. Grey/black/white are special: totals, averages, "Other", missing, context. Highlight = one saturated colour on grey.
5. One colour, one meaning, across every chart on the dashboard. Respect known meanings (red bad, green good) but never use them alone.
6. Avoid the rainbow scale for quantities; use discrete classes and a justified breakpoint for diverging data.
7. Measure lightness as CIE L*, not HSL L (perceptual).

## Contrast and colour-blind safety
| Check | Threshold | Tool |
|---|---|---|
| Graphical objects/data marks vs background | >= 3:1 (WCAG 2.2 non-text) | `contrast_ratio(a, b)` |
| Text vs background | >= 4.5:1 (3:1 large text) | `contrast_ratio` |
| Adjacent lightness steps in a sequential scale | >= 10 L* apart (greyscale print) | `greyscale_conflicts`, `is_lightness_monotonic` |
| Colour pairs under deuteranopia (about 5% of people, DVH 9) | CIE76 distance should not collapse | `deutan_confusion_pairs`, `simulate_deuteranopia` |
| Secondary encoding for status | Text/symbol column beside any red/green | add to `groupby` or SQL |
Lab numbers (4.1.2): Superset's red/green (`#E04355`/`#5AC189`) keeps 19% of its distance under a deuteranopia simulation; orange/blue (`#E69F00`/`#0072B2`) keeps 101%. The Okabe-Ito palette is not perfect either: bluish green vs reddish purple keeps 21%, and three pairs differ mainly by lightness.

## Superset colour features (verified on 4.1.2)
| Need | Mechanism | Limit |
|---|---|---|
| Categorical scheme | `color_scheme` on chart; dashboard `json_metadata.color_scheme` | Colours assigned by first appearance |
| Sequential / diverging scale | `linear_color_scheme` (heatmap, country map, deck.gl); diverging via schemes like `schemeRdBu` plus symmetric `value_bounds: [-b, b]` | No "centre at zero" switch |
| Pin colours (highlight, grey "Other", Likert shades) | Dashboard `json_metadata.label_colors` (label to hex; `rgba(0,0,0,0)` hides a series) | None per chart: Explore and dashboard can look different |
| Brand palette | `EXTRA_CATEGORICAL_COLOR_SCHEMES`, `EXTRA_SEQUENTIAL_COLOR_SCHEMES` (`isDiverging`) in `superset_config.py`, restart | Visible only in the HTML bootstrap payload, not the REST API |
| Status colours | Table / Big Number `conditional_formatting`, `color_pn` | Any hex accepted; nothing warns about red/green |
| Background / chrome | Dashboard `css` | No dashboard theme in 4.1.2; check 6.x theming |
| Textures/patterns | none | |
Scheme facts: 72 schemes in the 4.1.2 bundle (18 categorical, 54 sequential), hex values only in the bundle (`examples/superset_schemes.json`). `supersetColors` has 20 entries but colours 11-20 are tints of 1-10 (10 hues really), the 5th is grey `#666666`, only 5 of 20 reach 3:1 on white. `schemeSpectral` (UI label "rainbow") is a rainbow: avoid. `schemeBlues` is monotonic in L*. `shared_label_colors`, `map_label_colors`, `color_scheme_domain` are frontend-managed; edit only `color_scheme` and `label_colors`.

## Workflow
1. Decide data type, then palette type. 2. Cap categories (top 7 + "Other" in SQL). 3. Pin meaning-bearing colours in `label_colors`. 4. Lint with `checks.py` (contrast, deutan pairs, greyscale). 5. Squint test and a greyscale print (DVH 9). 6. Re-check dark/light viewing manually; no test covered it.

See `examples/README.md`, `references/sources.md`, and `superset-chart-api-limits`.
