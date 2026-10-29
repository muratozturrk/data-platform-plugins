"""Send a commonly written ("naive") REST request, then the corrected call, and record both.

Every example becomes one row of the request-check table written to measurements.json:
  example id, method + path, HTTP status of the naive request, verdict, fix.
Host, token and catalog names are filled in with the local server, a real token and unique
names; nothing else in the naive request is changed.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import requests

from common.polaris import POLARIS_URI, ROOT, token

MGMT = f"{POLARIS_URI}/api/management/v1"
ICEBERG = f"{POLARIS_URI}/api/catalog"


@dataclass
class Call:
    method: str
    url: str
    body: dict | None = None
    data: dict | None = None          # form body (OAuth)
    headers: dict = field(default_factory=dict)

    def send(self, bearer: str | None = None) -> requests.Response:
        headers = {"Content-Type": "application/json", **self.headers}
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        if self.data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        return requests.request(self.method, self.url, json=self.body, data=self.data, headers=headers, timeout=30)


@dataclass
class Row:
    example: str
    request: str
    naive_status: int
    verdict: str          # "as-is" | "as-is, response differs" | "fails" | "silent difference"
    fix: str


class RequestCheck:
    def __init__(self):
        self.rows: list[Row] = []
        self._token = None

    @property
    def bearer(self) -> str:
        if self._token is None:
            self._token = token(*ROOT)
        return self._token

    def run(self, example: str, call: Call, verdict: str, fix: str = "", bearer: str | None = None,
            expect_status: int | tuple[int, ...] | None = None) -> requests.Response:
        """Send the naive request, assert the observed status, record the row."""
        r = call.send(bearer or self.bearer)
        if expect_status is not None:
            ok = (expect_status,) if isinstance(expect_status, int) else expect_status
            assert r.status_code in ok, f"{example}: {r.status_code} {r.text[:300]}"
        path = call.url.replace(MGMT, "/api/management/v1").replace(ICEBERG, "/api/catalog")
        self.rows.append(Row(example, f"{call.method} {path.split('?')[0]}", r.status_code, verdict, fix))
        return r

    def as_table(self) -> list[dict]:
        return [r.__dict__ for r in self.rows]
