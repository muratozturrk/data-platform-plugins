# Examples

Copied unchanged from a Superset 4.1.2 lab (`colourkit`, Data Visualisation chapter 9). Python stdlib only, no Superset needed.

- `checks.py`: hex to RGB/HSL/CIE L*a*b*, WCAG `relative_luminance` and `contrast_ratio`, `delta_e`, `is_lightness_monotonic`, `diverging_report`, `lint_categorical`, `emphasis_report`, `simulate_deuteranopia`, `deutan_confusion_pairs`, `greyscale_conflicts`, `load_schemes`, `scheme_colors`.
- `superset_schemes.json`: 72 colour schemes (colours, kind, `is_diverging`) extracted from the Superset 4.1.2 static bundle; other versions may differ.

```python
from checks import contrast_ratio, deutan_confusion_pairs, scheme_colors
print(contrast_ratio("#666666", "#FFFFFF"))
print(deutan_confusion_pairs(["#E69F00", "#0072B2", "#E04355", "#5AC189"]))
print(scheme_colors("supersetColors")[:5])
```

Run from this folder (`checks.py` reads the JSON next to it).
