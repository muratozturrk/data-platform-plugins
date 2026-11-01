# dashboard_lint.py

Heuristic lint, one rule per mistake in Few's *Information Dashboard Design*, chapter 3 (3.1 to 3.13). Rules read `position_json` (layout tree), `json_metadata`, `css` and each chart's `params`. A finding means "look at this", not a verdict. Complements `dashboard-review-checklist.md` (manual scoring).

Origin: Superset 4.1.2 course lab (IDD chapter 3). Constants assume the 4.1.2 grid (8 px unit, 16 px gutter) and one screen = 880 px; adjust `SCREEN_PX` for your audience.

## Offline use (no Superset needed)
```
pip install pyyaml
python dashboard_lint.py <unzipped-export-or-assets-dir>
```
`DashboardCtx.from_export(dir)` reads `dashboards/*.yaml` and `charts/*.yaml` from an export bundle. The pie-sum data rule is skipped offline (it needs a chart query).

## Live use
`DashboardCtx.load(client, dashboard_id)` needs a client exposing `dashboard()`, `dashboard_charts()`, `chart_params()` and `chart_data()` (the lab's `SupersetClient`); the import is type-check only.

Chart-level declutter checks live in `superset-storytelling-declutter/examples/declutter_lint.py`. `layout.py` from the lab is not shipped (it depends on the lab client).
