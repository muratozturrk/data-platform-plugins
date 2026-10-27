"""Plain-HTTP client for the Apache Iceberg REST Catalog API (no Iceberg library involved).

Paths, payloads and error model follow open-api/rest-catalog-open-api.yaml in
https://github.com/apache/iceberg. Works unchanged against every compliant server; the labs
use it against the REST reference server and against Apache Polaris.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import quote

import requests

from common.lakehouse import POLARIS_URI, REST_URI

UNIT_SEPARATOR = "\x1f"   # multi-level namespaces are joined with 0x1F in URL paths


class CatalogError(Exception):
    def __init__(self, status: int, body: dict | str):
        self.status = status
        err = body.get("error", {}) if isinstance(body, dict) else {}
        self.type = err.get("type", "")
        self.message = err.get("message", str(body))
        super().__init__(f"{status} {self.type}: {self.message}")


@dataclass
class Timing:
    method: str
    path: str
    status: int
    ms: float


class IcebergRest:
    """One client = one catalog (base URI + optional OAuth2 token + warehouse)."""

    def __init__(self, base_uri: str, warehouse: str | None = None, token: str | None = None,
                 headers: dict | None = None):
        self.base = base_uri.rstrip("/")
        self.s = requests.Session()
        if token:
            self.s.headers["Authorization"] = f"Bearer {token}"
        self.s.headers.update(headers or {})
        self.timings: list[Timing] = []
        self.config = self.get_config(warehouse)
        self.prefix = self.config.get("overrides", {}).get("prefix")

    # ---- constructors ----------------------------------------------------------------------
    @classmethod
    def reference(cls) -> "IcebergRest":
        return cls(REST_URI)

    @classmethod
    def polaris(cls, catalog: str, client_id: str, client_secret: str,
                vended_credentials: bool = False) -> "IcebergRest":
        from common.polaris import token
        headers = {"X-Iceberg-Access-Delegation": "vended-credentials"} if vended_credentials else {}
        return cls(f"{POLARIS_URI}/api/catalog", warehouse=catalog,
                   token=token(client_id, client_secret), headers=headers)

    # ---- transport -------------------------------------------------------------------------
    def _url(self, path: str) -> str:
        return f"{self.base}/v1/{self.prefix}{path}" if self.prefix else f"{self.base}/v1{path}"

    def _req(self, method: str, url: str, **kw):
        t0 = time.perf_counter()
        r = self.s.request(method, url, timeout=60, **kw)
        self.timings.append(Timing(method, url.removeprefix(self.base), r.status_code,
                                   (time.perf_counter() - t0) * 1000))
        if r.status_code >= 400:
            try:
                body = r.json()
            except ValueError:
                body = r.text
            raise CatalogError(r.status_code, body)
        return r.json() if r.content else {}

    @staticmethod
    def ns_path(namespace: str | list[str]) -> str:
        parts = namespace.split(".") if isinstance(namespace, str) else namespace
        return quote(UNIT_SEPARATOR.join(parts), safe="")

    # ---- spec operations ---------------------------------------------------------------------
    def get_config(self, warehouse: str | None = None) -> dict:
        params = {"warehouse": warehouse} if warehouse else None
        return self._req("GET", f"{self.base}/v1/config", params=params)

    def list_namespaces(self, parent: str | None = None) -> list[str]:
        params = {"parent": UNIT_SEPARATOR.join(parent.split("."))} if parent else None
        res = self._req("GET", self._url("/namespaces"), params=params)
        return [".".join(n) for n in res["namespaces"]]

    def create_namespace(self, namespace: str, properties: dict | None = None) -> dict:
        return self._req("POST", self._url("/namespaces"),
                         json={"namespace": namespace.split("."), "properties": properties or {}})

    def drop_namespace(self, namespace: str) -> None:
        self._req("DELETE", self._url(f"/namespaces/{self.ns_path(namespace)}"))

    def list_tables(self, namespace: str) -> list[str]:
        res = self._req("GET", self._url(f"/namespaces/{self.ns_path(namespace)}/tables"))
        return [i["name"] for i in res["identifiers"]]

    def create_table(self, namespace: str, name: str, schema: dict, properties: dict | None = None) -> dict:
        return self._req("POST", self._url(f"/namespaces/{self.ns_path(namespace)}/tables"),
                         json={"name": name, "schema": schema, "properties": properties or {}})

    def load_table(self, namespace: str, name: str) -> dict:
        """LoadTableResult: metadata-location (the pointer), full metadata, optional config."""
        return self._req("GET", self._url(f"/namespaces/{self.ns_path(namespace)}/tables/{name}"))

    def table_exists(self, namespace: str, name: str) -> bool:
        r = self.s.head(self._url(f"/namespaces/{self.ns_path(namespace)}/tables/{name}"), timeout=60)
        return r.status_code in (200, 204)

    def commit(self, namespace: str, name: str, requirements: list[dict], updates: list[dict]) -> dict:
        """CommitTableRequest. The server checks every requirement against the current metadata and
        swaps the pointer atomically, or answers 409 CommitFailedException."""
        return self._req("POST", self._url(f"/namespaces/{self.ns_path(namespace)}/tables/{name}"),
                         json={"requirements": requirements, "updates": updates})

    def commit_transaction(self, changes: list[tuple[str, str, list[dict], list[dict]]]) -> None:
        """CommitTransactionRequest: several tables, all-or-nothing."""
        body = {"table-changes": [
            {"identifier": {"namespace": ns.split("."), "name": t}, "requirements": req, "updates": upd}
            for ns, t, req, upd in changes]}
        self._req("POST", self._url("/transactions/commit"), json=body)

    def register_table(self, namespace: str, name: str, metadata_location: str) -> dict:
        return self._req("POST", self._url(f"/namespaces/{self.ns_path(namespace)}/register"),
                         json={"name": name, "metadata-location": metadata_location})

    def drop_table(self, namespace: str, name: str, purge: bool = False) -> None:
        self._req("DELETE", self._url(f"/namespaces/{self.ns_path(namespace)}/tables/{name}"),
                  params={"purgeRequested": str(purge).lower()})

    def endpoints(self) -> list[str]:
        """Endpoints the server advertises in /v1/config (spec 1.6+). Empty for older servers."""
        return self.config.get("endpoints", [])


# ---- builders for commit payloads ----------------------------------------------------------
def schema(fields: list[tuple[str, str, bool]], schema_id: int = 0) -> dict:
    return {"type": "struct", "schema-id": schema_id,
            "fields": [{"id": i + 1, "name": n, "type": t, "required": r}
                       for i, (n, t, r) in enumerate(fields)]}


def add_column(meta: dict, name: str, type_: str) -> tuple[list[dict], list[dict]]:
    """Requirements + updates for 'ALTER TABLE ADD COLUMN', built from the metadata we loaded.

    assert-current-schema-id makes the commit fail if someone changed the schema meanwhile.
    """
    current = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    new_fields = current["fields"] + [{"id": meta["last-column-id"] + 1, "name": name,
                                       "type": type_, "required": False}]
    new_schema = {"type": "struct", "schema-id": current["schema-id"] + 1, "fields": new_fields}
    requirements = [{"type": "assert-table-uuid", "uuid": meta["table-uuid"]},
                    {"type": "assert-current-schema-id", "current-schema-id": meta["current-schema-id"]}]
    updates = [{"action": "add-schema", "schema": new_schema, "last-column-id": meta["last-column-id"] + 1},
               {"action": "set-current-schema", "schema-id": -1}]   # -1 = the schema added above
    return requirements, updates


def main_snapshot_id(meta: dict) -> int | None:
    return meta.get("refs", {}).get("main", {}).get("snapshot-id")


def current_columns(meta: dict) -> list[str]:
    current = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
    return [f["name"] for f in current["fields"]]
