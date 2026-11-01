"""Thin client for the Superset REST API (v1) used by every lab.

Covers the calls the labs need: login, CSRF, databases, datasets (physical and virtual),
charts, chart data, dashboards and export. Modeled on the API reference
(https://superset.apache.org/docs/api/) and on how the Superset frontend talks to the
backend (JWT bearer token + CSRF token + session cookie).
"""
from __future__ import annotations

import html
import io
import json
import re
import os
import time
import zipfile
from typing import Any

import requests


class SupersetError(RuntimeError):
    pass


class SupersetClient:
    def __init__(self, base_url: str | None = None, username: str | None = None,
                 password: str | None = None, timeout: int = 60):
        self.base_url = (base_url or os.environ.get("SUPERSET_URL", "http://superset:8088")).rstrip("/")
        self.username = username or os.environ.get("SUPERSET_USER", "admin")
        self.password = password or os.environ.get("SUPERSET_PASSWORD", "admin")
        self.timeout = timeout
        self.session = requests.Session()
        self._csrf: str | None = None

    # ---------- auth ----------

    def wait_until_healthy(self, max_wait_s: int = 300) -> None:
        deadline = time.time() + max_wait_s
        last = None
        while time.time() < deadline:
            try:
                r = self.session.get(f"{self.base_url}/health", timeout=5)
                if r.ok:
                    return
                last = f"{r.status_code} {r.text[:100]}"
            except requests.RequestException as exc:  # server still booting
                last = repr(exc)
            time.sleep(3)
        raise SupersetError(f"Superset not healthy after {max_wait_s}s: {last}")

    def login(self) -> "SupersetClient":
        r = self.session.post(f"{self.base_url}/api/v1/security/login", json={
            "username": self.username, "password": self.password,
            "provider": "db", "refresh": True}, timeout=self.timeout)
        self._raise(r, "login")
        self.session.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
        r = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/", timeout=self.timeout)
        self._raise(r, "csrf")
        self._csrf = r.json()["result"]
        self.session.headers["X-CSRFToken"] = self._csrf
        self.session.headers["Referer"] = self.base_url
        return self

    def frontend_bootstrap(self, page: str = "/superset/welcome/") -> dict:
        """The `data-bootstrap` JSON Superset embeds in its HTML pages (common.conf, feature
        flags, extra colour schemes from superset_config.py ...). HTML pages need a browser
        style session, so this logs in through the /login/ form with its own requests.Session."""
        s = requests.Session()
        r = s.get(f"{self.base_url}/login/", timeout=self.timeout)
        self._raise(r, "login page")
        m = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', r.text)
        r = s.post(f"{self.base_url}/login/", timeout=self.timeout, data={
            "username": self.username, "password": self.password, "csrf_token": m.group(1) if m else ""})
        self._raise(r, "form login")
        r = s.get(f"{self.base_url}{page}", timeout=self.timeout)
        self._raise(r, f"GET {page}")
        m = re.search(r'data-bootstrap="([^"]+)"', r.text)
        if not m:
            raise SupersetError(f"no data-bootstrap payload on {page}")
        return json.loads(html.unescape(m.group(1)))

    # ---------- low level ----------

    @staticmethod
    def _raise(r: requests.Response, what: str) -> None:
        if not r.ok:
            raise SupersetError(f"{what}: HTTP {r.status_code} {r.text[:800]}")

    def get(self, path: str, **params: Any) -> dict:
        r = self.session.get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        self._raise(r, f"GET {path}")
        return r.json()

    def post(self, path: str, payload: dict) -> dict:
        r = self.session.post(f"{self.base_url}{path}", json=payload, timeout=self.timeout)
        self._raise(r, f"POST {path}")
        return r.json()

    def put(self, path: str, payload: dict) -> dict:
        r = self.session.put(f"{self.base_url}{path}", json=payload, timeout=self.timeout)
        self._raise(r, f"PUT {path}")
        return r.json()

    def delete(self, path: str) -> None:
        r = self.session.delete(f"{self.base_url}{path}", timeout=self.timeout)
        if r.status_code != 404:
            self._raise(r, f"DELETE {path}")

    def list_all(self, resource: str, page_size: int = 100) -> list[dict]:
        """GET /api/v1/<resource>/ with rison paging; returns every row."""
        rows, page = [], 0
        while True:
            q = f"(page:{page},page_size:{page_size})"
            data = self.get(f"/api/v1/{resource}/", q=q)
            rows += data["result"]
            if len(rows) >= data["count"] or not data["result"]:
                return rows
            page += 1

    def find(self, resource: str, **match: Any) -> dict | None:
        for row in self.list_all(resource):
            if all(row.get(k) == v for k, v in match.items()):
                return row
        return None

    # ---------- databases ----------

    def ensure_database(self, database_name: str, sqlalchemy_uri: str | None = None, **extra: Any) -> int:
        """Register the warehouse once; return its id (idempotent by name)."""
        found = self.find("database", database_name=database_name)
        if found:
            return found["id"]
        payload = {
            "database_name": database_name,
            "sqlalchemy_uri": sqlalchemy_uri or os.environ["WAREHOUSE_SQLALCHEMY_URI"],
            "expose_in_sqllab": True,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            **extra,
        }
        return self.post("/api/v1/database/", payload)["id"]

    # ---------- datasets ----------

    def ensure_dataset(self, database_id: int, table_name: str, schema: str = "public",
                       sql: str | None = None) -> int:
        """Physical dataset (table) or virtual dataset (sql). Idempotent by table_name."""
        found = self.find("dataset", table_name=table_name)
        if found:
            return found["id"]
        payload: dict[str, Any] = {"database": database_id, "schema": schema, "table_name": table_name}
        if sql:
            payload["sql"] = sql
        return self.post("/api/v1/dataset/", payload)["id"]

    def dataset(self, dataset_id: int) -> dict:
        return self.get(f"/api/v1/dataset/{dataset_id}")["result"]

    def update_dataset(self, dataset_id: int, **fields: Any) -> dict:
        return self.put(f"/api/v1/dataset/{dataset_id}", fields)

    # ---------- charts ----------

    def create_chart(self, slice_name: str, viz_type: str, dataset_id: int, params: dict,
                     dashboards: list[int] | None = None, description: str | None = None,
                     replace: bool = True) -> int:
        """Create a chart whose form_data is `params` (what the Explore UI would save)."""
        if replace:
            old = self.find("chart", slice_name=slice_name)
            if old:
                self.delete(f"/api/v1/chart/{old['id']}")
        form = {"viz_type": viz_type, "datasource": f"{dataset_id}__table", **params}
        payload: dict[str, Any] = {
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(form),
        }
        if dashboards:
            payload["dashboards"] = dashboards
        if description:
            payload["description"] = description
        return self.post("/api/v1/chart/", payload)["id"]

    def chart(self, chart_id: int) -> dict:
        return self.get(f"/api/v1/chart/{chart_id}")["result"]

    def chart_params(self, chart_id: int) -> dict:
        return json.loads(self.chart(chart_id)["params"] or "{}")

    def chart_data(self, dataset_id: int, query: dict, result_type: str = "full") -> dict:
        """POST /api/v1/chart/data: run one query and return the first result block
        (keys: data, rowcount, colnames, query, ...)."""
        payload = {
            "datasource": {"id": dataset_id, "type": "table"},
            "queries": [query],
            "result_format": "json",
            "result_type": result_type,
        }
        return self.post("/api/v1/chart/data", payload)["result"][0]

    # ---------- dashboards ----------

    def ensure_dashboard(self, title: str, slug: str, position_json: dict | None = None,
                         json_metadata: dict | None = None, published: bool = True,
                         replace: bool = True) -> int:
        if replace:
            old = self.find("dashboard", slug=slug)
            if old:
                self.delete(f"/api/v1/dashboard/{old['id']}")
        payload: dict[str, Any] = {"dashboard_title": title, "slug": slug, "published": published}
        if position_json is not None:
            payload["position_json"] = json.dumps(position_json)
        if json_metadata is not None:
            payload["json_metadata"] = json.dumps(json_metadata)
        return self.post("/api/v1/dashboard/", payload)["id"]

    def dashboard(self, dashboard_id: int) -> dict:
        return self.get(f"/api/v1/dashboard/{dashboard_id}")["result"]

    def dashboard_charts(self, dashboard_id: int) -> list[dict]:
        return self.get(f"/api/v1/dashboard/{dashboard_id}/charts")["result"]

    def update_dashboard(self, dashboard_id: int, **fields: Any) -> dict:
        for key in ("position_json", "json_metadata"):
            if key in fields and not isinstance(fields[key], str):
                fields[key] = json.dumps(fields[key])
        return self.put(f"/api/v1/dashboard/{dashboard_id}", fields)

    def attach_charts(self, dashboard_id: int, chart_ids: list[int], columns: int = 2,
                      height: int = 50) -> dict:
        """Link charts to a dashboard and lay them out in a grid.

        The chart<->dashboard relation lives on the chart (`dashboards` field), the grid
        lives on the dashboard (`position_json`); both must agree, like the UI's "Save" does.
        """
        for cid in chart_ids:
            current = [d["id"] for d in self.chart(cid).get("dashboards", [])]
            if dashboard_id not in current:
                self.put(f"/api/v1/chart/{cid}", {"dashboards": sorted(current + [dashboard_id])})
        layout = chart_layout(chart_ids, columns=columns, height=height)
        return self.update_dashboard(dashboard_id, position_json=layout)

    def export_dashboard(self, dashboard_id: int) -> dict[str, str]:
        """Export-as-code: returns {path_in_zip: yaml_text} for the dashboard bundle."""
        r = self.session.get(f"{self.base_url}/api/v1/dashboard/export/",
                             params={"q": f"!({dashboard_id})"}, timeout=self.timeout)
        self._raise(r, "export dashboard")
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            return {n: zf.read(n).decode("utf-8") for n in zf.namelist()}


# ---------- helpers for building form_data ----------

def metric(column: str, aggregate: str = "SUM", label: str | None = None) -> dict:
    """Ad hoc metric exactly as the Explore UI serializes it."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column},
        "aggregate": aggregate,
        "label": label or f"{aggregate}({column})",
        "hasCustomLabel": label is not None,
    }


def sql_metric(sql: str, label: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label, "hasCustomLabel": True}


def chart_layout(chart_ids: list[int], columns: int = 2, width: int | None = None, height: int = 50) -> dict:
    """Minimal dashboard position_json: a grid with `columns` charts per row.

    Mirrors the layout tree Superset's dashboard builder stores
    (ROOT_ID -> GRID_ID -> ROW -> CHART), see superset-frontend/src/dashboard/util/constants.ts.
    """
    width = width or 12 // columns
    rows: list[list[int]] = [chart_ids[i:i + columns] for i in range(0, len(chart_ids), columns)]
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for r, ids in enumerate(rows):
        row_id = f"ROW-{r}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for cid in ids:
            key = f"CHART-{cid}"
            layout[row_id]["children"].append(key)
            layout[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                           "meta": {"chartId": cid, "width": width, "height": height, "uuid": None}}
    return layout


def row_layout(rows: list[list[tuple[int, int]]], height: int = 50) -> dict:
    """position_json for rows of charts with explicit widths: [[(chart_id, width), ...], ...].

    Same tree as chart_layout() but each chart gets its own width (12-column grid), which is
    what dashboard-design chapters need (a BAN beside a wide trend chart, etc.).
    """
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for r, cells in enumerate(rows):
        if sum(w for _, w in cells) > 12:
            raise ValueError(f"row {r} wider than 12 columns: {cells}")
        row_id = f"ROW-{r}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for cid, width in cells:
            key = f"CHART-{cid}"
            layout[row_id]["children"].append(key)
            layout[key] = {"type": "CHART", "id": key, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                           "meta": {"chartId": cid, "width": width, "height": height, "uuid": None}}
    return layout
