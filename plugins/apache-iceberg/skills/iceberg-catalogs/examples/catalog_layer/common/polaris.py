"""Minimal Apache Polaris management client (Management API v1, OAuth2 client credentials).

Endpoints follow spec/polaris-management-service.yml in https://github.com/apache/polaris.
"""
from __future__ import annotations

import requests

from common.lakehouse import BUCKET, POLARIS_URI, ROOT_CLIENT, S3_ENDPOINT

MGMT = f"{POLARIS_URI}/api/management/v1"
CATALOG_API = f"{POLARIS_URI}/api/catalog"
# Role that SeaweedFS STS lets Polaris assume (config/iam.json). Polaris adds a session policy
# scoped to the table location, so the vended keys only work for that table.
VENDOR_ROLE_ARN = "arn:aws:iam::000000000000:role/lake-vendor"


def token(client_id: str, client_secret: str, scope: str = "PRINCIPAL_ROLE:ALL") -> str:
    r = requests.post(f"{CATALOG_API}/v1/oauth/tokens", timeout=30, data={
        "grant_type": "client_credentials", "client_id": client_id,
        "client_secret": client_secret, "scope": scope})
    r.raise_for_status()
    return r.json()["access_token"]


class PolarisAdmin:
    def __init__(self, client_id: str = ROOT_CLIENT[0], client_secret: str = ROOT_CLIENT[1]):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token(client_id, client_secret)}"

    def _call(self, method: str, path: str, ok=(200, 201, 204), **kw) -> requests.Response:
        r = self.s.request(method, f"{MGMT}{path}", timeout=30, **kw)
        if r.status_code not in ok:
            raise RuntimeError(f"{method} {path}: {r.status_code} {r.text}")
        return r

    # ---- catalogs -------------------------------------------------------------------------
    def catalog_exists(self, name: str) -> bool:
        return self.s.get(f"{MGMT}/catalogs/{name}", timeout=30).status_code == 200

    def create_catalog(self, name: str, vend_credentials: bool = True,
                       extra_locations: tuple[str, ...] = ()) -> dict:
        """Internal catalog on SeaweedFS. vend_credentials=False mimics a store without STS.

        extra_locations: further prefixes tables may live in (needed to register tables that
        were created by another catalog under a different prefix).
        """
        location = f"s3://{BUCKET}/polaris/{name}"
        storage = {"storageType": "S3", "allowedLocations": [location, *extra_locations],
                   "region": "us-east-1",
                   "endpoint": S3_ENDPOINT, "pathStyleAccess": True}
        if vend_credentials:
            storage |= {"roleArn": VENDOR_ROLE_ARN, "stsEndpoint": S3_ENDPOINT}
        else:
            storage |= {"stsUnavailable": True}
        properties = {"default-base-location": location,
                      # DROP ... PURGE is disabled by default in Polaris
                      "polaris.config.drop-with-purge.enabled": "true"}
        if extra_locations:
            # By default a table must live under its namespace's location; registered tables
            # created by another catalog do not.
            properties["polaris.config.allow.unstructured.table.location"] = "true"
        body = {"catalog": {"name": name, "type": "INTERNAL", "properties": properties,
                            "storageConfigInfo": storage}}
        cat = self._call("POST", "/catalogs", json=body).json()
        # The bootstrap catalog_admin role can manage grants but not table data: add it.
        self.grant(name, "catalog_admin", {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        return cat

    def ensure_catalog(self, name: str, vend_credentials: bool = True,
                       extra_locations: tuple[str, ...] = ()) -> None:
        if not self.catalog_exists(name):
            self.create_catalog(name, vend_credentials, extra_locations)

    def drop_catalog_cascade(self, name: str) -> None:
        """Polaris refuses to drop non-empty catalogs: drop tables and namespaces first."""
        from common.iceberg_http import IcebergRest
        if not self.catalog_exists(name):
            return
        rest = IcebergRest.polaris(name, *ROOT_CLIENT)
        for ns in rest.list_namespaces():
            for t in rest.list_tables(ns):
                rest.drop_table(ns, t, purge=True)
            rest.drop_namespace(ns)
        for role in self._call("GET", f"/catalogs/{name}/catalog-roles").json()["roles"]:
            if role["name"] != "catalog_admin":
                self._call("DELETE", f"/catalogs/{name}/catalog-roles/{role['name']}")
        self._call("DELETE", f"/catalogs/{name}")

    # ---- principals and roles ----------------------------------------------------------------
    def create_principal(self, name: str) -> tuple[str, str]:
        """Returns (client_id, client_secret). Polaris shows the secret only once."""
        creds = self._call("POST", "/principals", json={"principal": {"name": name}}).json()["credentials"]
        return creds["clientId"], creds["clientSecret"]

    def drop_principal(self, name: str) -> None:
        self._call("DELETE", f"/principals/{name}", ok=(204, 404))

    def create_principal_role(self, name: str) -> None:
        self._call("POST", "/principal-roles", json={"principalRole": {"name": name}})

    def drop_principal_role(self, name: str) -> None:
        self._call("DELETE", f"/principal-roles/{name}", ok=(204, 404))

    def assign_principal_role(self, principal: str, principal_role: str) -> None:
        self._call("PUT", f"/principals/{principal}/principal-roles",
                   json={"principalRole": {"name": principal_role}})

    def create_catalog_role(self, catalog: str, name: str) -> None:
        self._call("POST", f"/catalogs/{catalog}/catalog-roles", json={"catalogRole": {"name": name}})

    def assign_catalog_role(self, principal_role: str, catalog: str, catalog_role: str) -> None:
        self._call("PUT", f"/principal-roles/{principal_role}/catalog-roles/{catalog}",
                   json={"catalogRole": {"name": catalog_role}})

    def revoke_catalog_role(self, principal_role: str, catalog: str, catalog_role: str) -> None:
        self._call("DELETE", f"/principal-roles/{principal_role}/catalog-roles/{catalog}/{catalog_role}")

    def grant(self, catalog: str, catalog_role: str, grant: dict) -> None:
        self._call("PUT", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/grants", json={"grant": grant})

    def grants(self, catalog: str, catalog_role: str) -> list[dict]:
        return self._call("GET", f"/catalogs/{catalog}/catalog-roles/{catalog_role}/grants").json()["grants"]


def pyiceberg_client(catalog: str, client_id: str, client_secret: str):
    """PyIceberg engine connected to Polaris with NO storage keys: S3 access comes only from the
    short-lived credentials Polaris vends in each LoadTable/CreateTable response."""
    from pyiceberg.catalog.rest import RestCatalog
    return RestCatalog(f"polaris-{client_id[:8]}", **{
        "uri": CATALOG_API,
        "warehouse": catalog,
        "credential": f"{client_id}:{client_secret}",
        "oauth2-server-uri": f"{CATALOG_API}/v1/oauth/tokens",
        "scope": "PRINCIPAL_ROLE:ALL",
        "header.X-Iceberg-Access-Delegation": "vended-credentials",
        "s3.endpoint": S3_ENDPOINT,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    })
