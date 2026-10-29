"""Measure what each Polaris privilege really allows.

For every documented privilege, a fresh catalog role holding ONLY that privilege (granted at
catalog level) is attached to a probe principal, which then attempts ~20 operations through
the Iceberg REST and Management APIs. 2xx = allowed, 403 = denied.
"""
from __future__ import annotations

from botocore.exceptions import ClientError

from common.polaris import Admin, ApiError, CatalogClient, Principal, split_uri, vended_s3

DOCUMENTED_PRIVILEGES = [
    "TABLE_CREATE", "TABLE_DROP", "TABLE_LIST", "TABLE_READ_PROPERTIES", "TABLE_WRITE_PROPERTIES",
    "TABLE_READ_DATA", "TABLE_WRITE_DATA", "TABLE_FULL_METADATA",
    "VIEW_CREATE", "VIEW_DROP", "VIEW_LIST", "VIEW_READ_PROPERTIES", "VIEW_WRITE_PROPERTIES",
    "VIEW_FULL_METADATA",
    "NAMESPACE_CREATE", "NAMESPACE_DROP", "NAMESPACE_LIST", "NAMESPACE_READ_PROPERTIES",
    "NAMESPACE_WRITE_PROPERTIES", "NAMESPACE_FULL_METADATA",
    "CATALOG_MANAGE_ACCESS", "CATALOG_MANAGE_CONTENT", "CATALOG_MANAGE_METADATA",
    "CATALOG_READ_PROPERTIES", "CATALOG_WRITE_PROPERTIES",
]

OPERATIONS = [
    "namespace_list", "namespace_read", "namespace_write", "namespace_create", "namespace_drop",
    "table_list", "table_load", "table_write_props", "table_create", "table_drop",
    "data_read", "data_write",
    "view_list", "view_load", "view_create", "view_drop",
    "catalog_list", "catalog_read", "catalog_write", "grant_manage",
]

# What each privilege actually allowed (measured on Polaris 1.8.0, see test_privilege_matrix).
T, V, N = "table_", "view_", "namespace_"
MEASURED = {
    "TABLE_CREATE": {"table_list", "table_create"},
    "TABLE_DROP": {"table_drop"},
    "TABLE_LIST": {"table_list"},
    "TABLE_READ_PROPERTIES": {"table_list", "table_load"},
    "TABLE_WRITE_PROPERTIES": {"table_list", "table_load", "table_write_props"},
    "TABLE_READ_DATA": {"table_list", "table_load", "data_read"},
    "TABLE_WRITE_DATA": {"table_list", "table_load", "table_write_props", "data_read", "data_write"},
    "TABLE_FULL_METADATA": {"table_list", "table_load", "table_write_props", "table_create", "table_drop"},
    "VIEW_CREATE": {"view_list", "view_create"},
    "VIEW_DROP": {"view_drop"},
    "VIEW_LIST": {"view_list"},
    "VIEW_READ_PROPERTIES": {"view_list", "view_load"},
    "VIEW_WRITE_PROPERTIES": {"view_list", "view_load"},
    "VIEW_FULL_METADATA": {"view_list", "view_load", "view_create", "view_drop"},
    "NAMESPACE_CREATE": {"namespace_list", "namespace_create"},
    "NAMESPACE_DROP": {"namespace_drop"},
    "NAMESPACE_LIST": {"namespace_list"},
    "NAMESPACE_READ_PROPERTIES": {"namespace_list", "namespace_read"},
    "NAMESPACE_WRITE_PROPERTIES": {"namespace_list", "namespace_read", "namespace_write"},
    "NAMESPACE_FULL_METADATA": {"namespace_list", "namespace_read", "namespace_write",
                                "namespace_create", "namespace_drop"},
    "CATALOG_MANAGE_ACCESS": {"grant_manage"},
    "CATALOG_MANAGE_CONTENT": set(OPERATIONS) - {"grant_manage", "catalog_list"},
    "CATALOG_MANAGE_METADATA": set(OPERATIONS) - {"grant_manage", "catalog_list", "data_read", "data_write"},
    "CATALOG_READ_PROPERTIES": {"catalog_read"},
    "CATALOG_WRITE_PROPERTIES": {"catalog_read", "catalog_write"},
}


def _allowed(fn) -> bool | str:
    try:
        fn()
        return True
    except ApiError as e:
        if e.status == 403:
            return False
        return f"HTTP {e.status}"
    except ClientError as e:
        if e.response["Error"]["Code"] in ("AccessDenied", "403"):
            return False
        raise


def _require(visible: bool) -> None:
    """GET /catalogs succeeds for everyone but filters the result: treat 'not listed' as denied."""
    if not visible:
        raise ApiError("GET", "/catalogs", 403, "catalog not in listing")


class Fixture:
    """Objects the probe operates on, recreated by root before each privilege run."""

    def __init__(self, admin: Admin, root: Principal, catalog: str):
        self.admin, self.root, self.catalog = admin, root, catalog
        self.rc = CatalogClient(catalog, root.client_id, root.client_secret)
        self.data_file = None

    def reset(self) -> None:
        rc = self.rc
        for ns in ("scratch_drop",):
            if ns not in rc.list_namespaces():
                rc.create_namespace(ns)
        if "t_drop" not in rc.list_tables("sales"):
            rc.create_table("sales", "t_drop")
        if "v_drop" not in rc.list_views("sales"):
            rc.create_view("sales", "v_drop", "SELECT 1")
        for ns in rc.list_namespaces():
            if ns.startswith("probe_ns_"):
                rc.drop_namespace(ns)
        for t in rc.list_tables("sales"):
            if t.startswith("probe_t_"):
                rc.drop_table("sales", t, purge=True)
        for v in rc.list_views("sales"):
            if v.startswith("probe_v_"):
                rc.drop_view("sales", v)
        if "probe_cr" not in self.admin.catalog_roles(self.catalog):
            self.admin.create_catalog_role(self.catalog, "probe_cr")
        if self.data_file is None:
            from common.polaris import vended_s3 as vs
            loc = rc.load_table("sales", "orders")["metadata"]["location"]
            s3 = vs(rc.load_table("sales", "orders"))
            bucket, prefix = split_uri(loc + "/data/")
            self.data_file = (bucket, s3.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"][0]["Key"])
            self.table_location = loc


def probe(fx: Fixture, who: Principal, n: int) -> dict[str, bool | str]:
    c = CatalogClient(fx.catalog, who.client_id, who.client_secret, vended=False)
    cv = CatalogClient(fx.catalog, who.client_id, who.client_secret, vended=True)
    mgmt = Admin(who.client_id, who.client_secret)

    def data(op: str):
        s3 = vended_s3(cv.load_table("sales", "orders"))
        if s3 is None:                      # loaded, but no credentials were vended
            raise ApiError("GET", "vended", 403, "no storage credentials vended")
        bucket, key = fx.data_file
        if op == "read":
            s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        else:
            b, k = split_uri(fx.table_location + f"/data/probe-{n}.bin")
            s3.put_object(Bucket=b, Key=k, Body=b"x")

    def catalog_write():
        cur = mgmt.m("GET", f"/catalogs/{fx.catalog}").json()
        mgmt.m("PUT", f"/catalogs/{fx.catalog}", json={
            "currentEntityVersion": cur["entityVersion"], "properties": {**cur["properties"], "probe": str(n)}})

    ops = {
        "namespace_list": lambda: c.list_namespaces(),
        "namespace_read": lambda: c.namespace_properties("sales"),
        "namespace_write": lambda: c.set_namespace_properties("sales", {"probe": str(n)}),
        "namespace_create": lambda: c.create_namespace(f"probe_ns_{n}"),
        "namespace_drop": lambda: c.drop_namespace("scratch_drop"),
        "table_list": lambda: c.list_tables("sales"),
        "table_load": lambda: c.load_table("sales", "orders"),
        "table_write_props": lambda: c.set_table_properties("sales", "orders", {"probe": str(n)}),
        "table_create": lambda: c.create_table("sales", f"probe_t_{n}"),
        "table_drop": lambda: c.drop_table("sales", "t_drop"),
        "data_read": lambda: data("read"),
        "data_write": lambda: data("write"),
        "view_list": lambda: c.list_views("sales"),
        "view_load": lambda: c.load_view("sales", "v_orders"),
        "view_create": lambda: c.create_view("sales", f"probe_v_{n}", "SELECT 2"),
        "view_drop": lambda: c.drop_view("sales", "v_drop"),
        "catalog_list": lambda: _require(fx.catalog in [x["name"] for x in mgmt.m("GET", "/catalogs").json()["catalogs"]]),
        "catalog_read": lambda: mgmt.m("GET", f"/catalogs/{fx.catalog}"),
        "catalog_write": catalog_write,
        "grant_manage": lambda: mgmt.grant(fx.catalog, "probe_cr", {"type": "catalog", "privilege": "TABLE_LIST"}),
    }
    return {name: _allowed(ops[name]) for name in OPERATIONS}


def run_matrix(fx: Fixture, who: Principal, principal_role: str, privileges: list[str]) -> dict[str, dict]:
    admin, cat = fx.admin, fx.catalog
    matrix = {}
    for n, priv in enumerate(privileges):
        fx.reset()
        role = f"only_{priv.lower()}"
        admin.create_catalog_role(cat, role)
        admin.grant(cat, role, {"type": "catalog", "privilege": priv})
        admin.assign_catalog_role(principal_role, cat, role)
        try:
            matrix[priv] = probe(fx, who, n)
        finally:
            admin.revoke_catalog_role(principal_role, cat, role)
            admin.drop_catalog_role(cat, role)
    return matrix
