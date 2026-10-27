"""Minimal client for Trino's REST client protocol (POST /v1/statement, then follow nextUri).

https://trino.io/docs/current/develop/client-protocol.html
This is what every Trino client (JDBC, ODBC, Python) speaks underneath; custom apps can use it
directly without a driver.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

import requests

BASE = f"http://{os.getenv('TRINO_HOST', 'trino')}:{os.getenv('TRINO_PORT', '8080')}"


class TrinoRestError(RuntimeError):
    pass


@dataclass
class RestResult:
    columns: list[str]
    rows: list[list]
    http_requests: int
    query_id: str


def execute(sql: str, user: str, source: str = "custom-app") -> RestResult:
    headers = {"X-Trino-User": user, "X-Trino-Source": source}
    session = requests.Session()
    body = session.post(f"{BASE}/v1/statement", data=sql.encode(), headers=headers, timeout=30).json()
    calls, columns, rows = 1, [], []
    while True:
        if "error" in body:
            raise TrinoRestError(body["error"].get("message"))
        if body.get("columns") and not columns:
            columns = [c["name"] for c in body["columns"]]
        rows.extend(body.get("data", []))
        next_uri = body.get("nextUri")
        if not next_uri:
            return RestResult(columns, rows, calls, body["id"])
        body = session.get(next_uri, headers=headers, timeout=30).json()
        calls += 1
