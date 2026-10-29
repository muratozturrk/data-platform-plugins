"""Clients for the two Apache Polaris APIs.

- Management API v1 (spec/polaris-management-service.yml in github.com/apache/polaris):
  catalogs, principals, principal roles, catalog roles, grants.
- Iceberg REST catalog API (open-api/rest-catalog-open-api.yaml in github.com/apache/iceberg):
  namespaces, tables, views, as seen by one principal.

Every call returns (or raises with) the real HTTP status so tests can assert on 200/403/404.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import boto3
import requests

POLARIS_URI = os.getenv("POLARIS_URI", "http://polaris:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
ROOT = (os.getenv("POLARIS_ROOT_CLIENT_ID", "root"), os.getenv("POLARIS_ROOT_CLIENT_SECRET", "s3cr3t"))
MGMT = f"{POLARIS_URI}/api/management/v1"
CATALOG_API = f"{POLARIS_URI}/api/catalog"
# Role SeaweedFS STS lets Polaris assume (config/iam.json). Polaris adds a session policy scoped
# to one table location and to read or read+write, so vended keys only fit that table.
VENDOR_ROLE_ARN = "arn:aws:iam::000000000000:role/lake-vendor"
US = "\x1f"   # multi-level namespaces are joined with 0x1F in REST paths


class ApiError(Exception):
    def __init__(self, method: str, url: str, status: int, text: str):
        self.status = status
        self.text = text
        super().__init__(f"{method} {url.removeprefix(POLARIS_URI)} -> {status} {text[:300]}")


def token_response(client_id: str, client_secret: str, scope: str = "PRINCIPAL_ROLE:ALL") -> requests.Response:
    """OAuth2 client-credentials grant against Polaris' built-in token endpoint."""
    return requests.post(f"{CATALOG_API}/v1/oauth/tokens", timeout=30, data={
        "grant_type": "client_credentials", "client_id": client_id,
        "client_secret": client_secret, "scope": scope})


def token(client_id: str, client_secret: str, scope: str = "PRINCIPAL_ROLE:ALL") -> str:
    r = token_response(client_id, client_secret, scope)
    if r.status_code != 200:
        raise ApiError("POST", f"{CATALOG_API}/v1/oauth/tokens", r.status_code, r.text)
    return r.json()["access_token"]


class _Http:
    def __init__(self, bearer: str):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {bearer}"

    def call(self, method: str, url: str, ok=(200, 201, 204), **kw) -> requests.Response:
        r = self.s.request(method, url, timeout=60, **kw)
        if r.status_code not in ok:
            raise ApiError(method, url, r.status_code, r.text)
        return r


# ------------------------------------------------------------------------------------------
# Management API
# ------------------------------------------------------------------------------------------
class Admin(_Http):
    """Management API client. Created with root credentials unless told otherwise."""

    def __init__(self, client_id: str = ROOT[0], client_secret: str = ROOT[1], scope: str = "PRINCIPAL_ROLE:ALL"):
        super().__init__(token(client_id, client_secret, scope))

    def m(self, method: str, path: str, **kw) -> requests.Response:
        return self.call(method, f"{MGMT}{path}", **kw)

    # catalogs ---------------------------------------------------------------------------------
    def create_catalog(self, name: str, base_location: str, allowed: list[str] | None = None,
                       vend: bool = True, properties: dict | None = None) -> dict:
        storage = {"storageType": "S3", "allowedLocations": allowed or [base_location],
                   "region": "us-east-1", "endpoint": S3_ENDPOINT, "pathStyleAccess": True}
        storage |= ({"roleArn": VENDOR_ROLE_ARN, "stsEndpoint": S3_ENDPOINT} if vend
                    else {"stsUnavailable": True})
        props = {"default-base-location": base_location,
                 "polaris.config.drop-with-purge.enabled": "true", **(properties or {})}
        return self.m("POST", "/catalogs", json={"catalog": {
            "name": name, "type": "INTERNAL", "properties": props, "storageConfigInfo": storage}}).json()

    def catalogs(self) -> list[str]:
        return [c["name"] for c in self.m("GET", "/catalogs").json()["catalogs"]]

    def drop_catalog(self, name: str) -> None:
        """Polaris refuses to drop a catalog that still has content or custom roles."""
        if name not in self.catalogs():
            return
        root_view = CatalogClient(name, *ROOT)
        for ns in sorted(root_view.list_namespaces_deep(), key=len, reverse=True):
            for view in root_view.list_views(ns):
                root_view.drop_view(ns, view)
            for table in root_view.list_tables(ns):
                root_view.drop_table(ns, table, purge=True)
            root_view.drop_namespace(ns)
        for role in self.catalog_roles(name):
            if role != "catalog_admin":
                self.m("DELETE", f"/catalogs/{name}/catalog-roles/{role}")
        self.m("DELETE", f"/catalogs/{name}")

    # principals -------------------------------------------------------------------------------
    def create_principal(self, name: str, properties: dict | None = None) -> "Principal":
        res = self.m("POST", "/principals",
                     json={"principal": {"name": name, "properties": properties or {}}}).json()
        return Principal(name, res["credentials"]["clientId"], res["credentials"]["clientSecret"])

    def principal(self, name: str) -> dict:
        return self.m("GET", f"/principals/{name}").json()

    def principals(self) -> list[dict]:
        return self.m("GET", "/principals").json()["principals"]

    def reset_credentials(self, name: str) -> "Principal":
        """Admin-side credential reset: new secret, the old one stops working immediately."""
        res = self.m("POST", f"/principals/{name}/reset", json={}).json()
        return Principal(name, res["credentials"]["clientId"], res["credentials"]["clientSecret"])

    def drop_principal(self, name: str) -> None:
        self.m("DELETE", f"/principals/{name}", ok=(204, 404))

    # principal roles ------------------------------------------------------------------------
    def create_principal_role(self, name: str) -> None:
        self.m("POST", "/principal-roles", json={"principalRole": {"name": name}})

    def principal_roles(self) -> list[str]:
        return [r["name"] for r in self.m("GET", "/principal-roles").json()["roles"]]

    def drop_principal_role(self, name: str) -> None:
        self.m("DELETE", f"/principal-roles/{name}", ok=(204, 404))

    def assign_principal_role(self, principal: str, role: str) -> None:
        self.m("PUT", f"/principals/{principal}/principal-roles", json={"principalRole": {"name": role}})

    def revoke_principal_role(self, principal: str, role: str) -> None:
        self.m("DELETE", f"/principals/{principal}/principal-roles/{role}")

    def principal_roles_of(self, principal: str) -> list[str]:
        return [r["name"] for r in self.m("GET", f"/principals/{principal}/principal-roles").json()["roles"]]

    def principals_with_role(self, role: str) -> list[str]:
        return [p["name"] for p in self.m("GET", f"/principal-roles/{role}/principals").json()["principals"]]

    # catalog roles and grants -----------------------------------------------------------------
    def create_catalog_role(self, catalog: str, name: str) -> None:
        self.m("POST", f"/catalogs/{catalog}/catalog-roles", json={"catalogRole": {"name": name}})

    def catalog_roles(self, catalog: str) -> list[str]:
        return [r["name"] for r in self.m("GET", f"/catalogs/{catalog}/catalog-roles").json()["roles"]]

    def drop_catalog_role(self, catalog: str, name: str) -> None:
        self.m("DELETE", f"/catalogs/{catalog}/catalog-roles/{name}", ok=(204, 404))

    def assign_catalog_role(self, principal_role: str, catalog: str, catalog_role: str) -> None:
        self.m("PUT", f"/principal-roles/{principal_role}/catalog-roles/{catalog}",
               json={"catalogRole": {"name": catalog_role}})

    def revoke_catalog_role(self, principal_role: str, catalog: str, catalog_role: str) -> None:
        self.m("DELETE", f"/principal-roles/{principal_role}/catalog-roles/{catalog}/{catalog_role}")

    def catalog_roles_of(self, principal_role: str, catalog: str) -> list[str]:
        return [r["name"] for r in
                self.m("GET", f"/principal-roles/{principal_role}/catalog-roles/{catalog}").json()["roles"]]

    def principal_roles_with_catalog_role(self, catalog: str, catalog_role: str) -> list[str]:
        return [r["name"] for r in self.m(
            "GET", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/principal-roles").json()["roles"]]

    def grant(self, catalog: str, catalog_role: str, grant: dict) -> None:
        self.m("PUT", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/grants", json={"grant": grant})

    def revoke(self, catalog: str, catalog_role: str, grant: dict) -> None:
        """Revoke = POST on the same grants resource (PUT adds, POST removes)."""
        self.m("POST", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/grants", json={"grant": grant})

    def grants(self, catalog: str, catalog_role: str) -> list[dict]:
        return self.m("GET", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/grants").json()["grants"]


@dataclass
class Principal:
    name: str
    client_id: str
    client_secret: str

    def catalog(self, catalog: str, scope: str = "PRINCIPAL_ROLE:ALL", vended: bool = True) -> "CatalogClient":
        return CatalogClient(catalog, self.client_id, self.client_secret, scope, vended)

    def pyiceberg(self, catalog: str, scope: str = "PRINCIPAL_ROLE:ALL"):
        """PyIceberg engine with NO storage keys: S3 access only through vended credentials."""
        from pyiceberg.catalog.rest import RestCatalog
        return RestCatalog(f"{self.name}-{catalog}", **{
            "uri": CATALOG_API, "warehouse": catalog,
            "credential": f"{self.client_id}:{self.client_secret}",
            "oauth2-server-uri": f"{CATALOG_API}/v1/oauth/tokens", "scope": scope,
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            "s3.endpoint": S3_ENDPOINT, "s3.path-style-access": "true", "s3.region": "us-east-1"})


# ------------------------------------------------------------------------------------------
# Iceberg REST catalog API (as one principal)
# ------------------------------------------------------------------------------------------
class CatalogClient(_Http):
    def __init__(self, catalog: str, client_id: str, client_secret: str,
                 scope: str = "PRINCIPAL_ROLE:ALL", vended: bool = True):
        super().__init__(token(client_id, client_secret, scope))
        if vended:
            self.s.headers["X-Iceberg-Access-Delegation"] = "vended-credentials"
        cfg = self.call("GET", f"{CATALOG_API}/v1/config", params={"warehouse": catalog}).json()
        self.base = f"{CATALOG_API}/v1/{cfg['overrides']['prefix']}"

    @staticmethod
    def ns(namespace: str) -> str:
        return quote(namespace.replace(".", US), safe="")

    def c(self, method: str, path: str, **kw) -> requests.Response:
        return self.call(method, f"{self.base}{path}", **kw)

    def list_namespaces(self, parent: str | None = None) -> list[str]:
        params = {"parent": parent.replace(".", US)} if parent else None
        return [".".join(n) for n in self.c("GET", "/namespaces", params=params).json()["namespaces"]]

    def list_namespaces_deep(self) -> list[str]:
        out, todo = [], self.list_namespaces()
        while todo:
            ns = todo.pop()
            out.append(ns)
            todo += self.list_namespaces(ns)
        return out

    def create_namespace(self, namespace: str, properties: dict | None = None) -> None:
        self.c("POST", "/namespaces", json={"namespace": namespace.split("."), "properties": properties or {}})

    def namespace_properties(self, namespace: str) -> dict:
        return self.c("GET", f"/namespaces/{self.ns(namespace)}").json()["properties"]

    def set_namespace_properties(self, namespace: str, updates: dict) -> None:
        self.c("POST", f"/namespaces/{self.ns(namespace)}/properties", json={"updates": updates, "removals": []})

    def drop_namespace(self, namespace: str) -> None:
        self.c("DELETE", f"/namespaces/{self.ns(namespace)}")

    def list_tables(self, namespace: str) -> list[str]:
        return [i["name"] for i in self.c("GET", f"/namespaces/{self.ns(namespace)}/tables").json()["identifiers"]]

    def create_table(self, namespace: str, name: str, location: str | None = None) -> dict:
        body = {"name": name, "schema": SIMPLE_SCHEMA}
        if location:
            body["location"] = location
        return self.c("POST", f"/namespaces/{self.ns(namespace)}/tables", json=body).json()

    def load_table(self, namespace: str, name: str) -> dict:
        return self.c("GET", f"/namespaces/{self.ns(namespace)}/tables/{name}").json()

    def set_table_properties(self, namespace: str, name: str, updates: dict) -> None:
        self.c("POST", f"/namespaces/{self.ns(namespace)}/tables/{name}",
               json={"requirements": [], "updates": [{"action": "set-properties", "updates": updates}]})

    def drop_table(self, namespace: str, name: str, purge: bool = False) -> None:
        self.c("DELETE", f"/namespaces/{self.ns(namespace)}/tables/{name}",
               params={"purgeRequested": str(purge).lower()})

    def list_views(self, namespace: str) -> list[str]:
        return [i["name"] for i in self.c("GET", f"/namespaces/{self.ns(namespace)}/views").json()["identifiers"]]

    def create_view(self, namespace: str, name: str, sql: str) -> dict:
        body = {"name": name, "schema": SIMPLE_SCHEMA, "properties": {},
                "view-version": {"version-id": 1, "timestamp-ms": 0, "schema-id": 0,
                                 "summary": {"engine-name": "lab"}, "default-namespace": namespace.split("."),
                                 "representations": [{"type": "sql", "sql": sql, "dialect": "spark"}]}}
        return self.c("POST", f"/namespaces/{self.ns(namespace)}/views", json=body).json()

    def load_view(self, namespace: str, name: str) -> dict:
        return self.c("GET", f"/namespaces/{self.ns(namespace)}/views/{name}").json()

    def drop_view(self, namespace: str, name: str) -> None:
        self.c("DELETE", f"/namespaces/{self.ns(namespace)}/views/{name}")


SIMPLE_SCHEMA = {"type": "struct", "schema-id": 0, "fields": [
    {"id": 1, "name": "id", "type": "long", "required": False},
    {"id": 2, "name": "amount", "type": "double", "required": False}]}


# ------------------------------------------------------------------------------------------
# Storage access with vended credentials
# ------------------------------------------------------------------------------------------
def vended_config(load_result: dict) -> dict:
    cfg = dict(load_result.get("config", {}))
    for cred in load_result.get("storage-credentials", []):
        cfg |= cred["config"]
    return cfg


def vended_s3(load_result: dict):
    """boto3 client built ONLY from the credentials in a LoadTable response, or None."""
    cfg = vended_config(load_result)
    if "s3.access-key-id" not in cfg:
        return None
    return boto3.client("s3", endpoint_url=S3_ENDPOINT, region_name="us-east-1",
                        aws_access_key_id=cfg["s3.access-key-id"],
                        aws_secret_access_key=cfg["s3.secret-access-key"],
                        aws_session_token=cfg.get("s3.session-token"))


def split_uri(uri: str) -> tuple[str, str]:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")
