"""The initial access setup (catalog role, principal, principal role, assignments, grants),
first as a sequence of HTTP calls, then with quickstart_env/bootstrap.py.

The quickstart server keeps state in memory, so each path starts from a restarted, empty server.
"""
import pathlib
import subprocess

import pyarrow as pa
import pytest
import requests
from pyiceberg.catalog.rest import RestCatalog

from common.servers import QUICKSTART, MEM, restart

FILE_CATALOG = {"name": "polariscatalog", "type": "INTERNAL", "properties": {"default-base-location": "file:///data"},
                "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///data"]}}


def call(method, path, body=None):
    return requests.request(method, f"{QUICKSTART.mgmt}{path}", json=body, headers=QUICKSTART.headers(), timeout=30)


def user_can_write(client_id, secret, table):
    subprocess.run(["python", "/labs/quickstart_env/icebergdata/table_setup.py", f"db.{table}"], cwd="/data", check=True,
                   capture_output=True)
    cat = RestCatalog("u", uri=QUICKSTART.catalog_api, warehouse="polariscatalog", credential=f"{client_id}:{secret}",
                      scope="PRINCIPAL_ROLE:ALL")
    if ("db",) not in cat.list_namespaces():
        cat.create_namespace("db")
    t = cat.create_table(f"db.{table}", schema=pa.schema([("id", pa.int64())]))
    t.append(pa.table({"id": pa.array([1, 2], pa.int64())}))
    return cat.load_table(f"db.{table}").scan().to_arrow().num_rows


def test_access_setup_as_http_sequence(example_rows, measure):
    restart(QUICKSTART)
    steps = [
        ("Create catalog (local file system)", "POST", "/catalogs", FILE_CATALOG, 201),
        ("Create catalog role", "POST", "/catalogs/polariscatalog/catalog-roles", {"catalogRole": {"name": "polariscatalogrole"}}, 201),
        ("List catalog roles", "GET", "/catalogs/polariscatalog/catalog-roles", None, 200),
        ("Create principal", "POST", "/principals", {"name": "polarisuser", "type": "user"}, 201),
        ("List principals", "GET", "/principals", None, 200),
        ("Create principal role", "POST", "/principal-roles", {"principalRole": {"name": "polarisuserrole"}}, 201),
        ("Assign principal role to principal", "PUT", "/principals/polarisuser/principal-roles",
         {"principalRole": {"name": "polarisuserrole"}}, 201),
        ("Verify role assignment", "GET", "/principals/polarisuser/principal-roles", None, 200),
        ("Assign catalog role to principal role", "PUT", "/principal-roles/polarisuserrole/catalog-roles/polariscatalog",
         {"catalogRole": {"name": "polariscatalogrole"}}, 201),
        ("Grant CATALOG_MANAGE_CONTENT", "PUT", "/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants",
         {"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}, 201),
        ("Verify grants", "GET", "/catalogs/polariscatalog/catalog-roles/polariscatalogrole/grants", None, 200),
    ]
    secret = None
    for name, method, path, body, expected in steps:
        r = call(method, path, body)
        assert r.status_code == expected, f"{name}: {r.status_code} {r.text[:200]}"
        if name == "Create principal":
            secret = (r.json()["credentials"]["clientId"], r.json()["credentials"]["clientSecret"])
        example_rows.append({"example": name, "server": "quickstart image", "status": r.status_code, "verdict": "as-is",
                          "fix": "" if name != "Create principal" else "body is accepted without the 'principal' wrapper; 'type' is ignored"})
    rows = user_can_write(*secret, "events")
    assert rows == 2
    measure("curl_sequence", {"steps": len(steps), "all_as_expected": True, "polarisuser_rows_written": rows})


def test_access_setup_with_bootstrap_script(example_rows, measure):
    """Run quickstart_env/bootstrap.py unchanged except for the host
    (the script targets localhost:8181 from the host machine)."""
    restart(QUICKSTART)
    source = pathlib.Path("/labs/quickstart_env/bootstrap.py").read_text(encoding="utf-8")
    patched = source.replace("http://localhost:8181", QUICKSTART.base)
    assert patched.count(QUICKSTART.base) == 8 and patched.replace(QUICKSTART.base, "http://localhost:8181") == source
    run = subprocess.run(["python", "-c", patched], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stdout + run.stderr
    lines = [l for l in run.stdout.splitlines() if l.startswith("✅")]
    assert len(lines) == 8                                 # token, catalog, catalog role, principal, ...
    secret = next(l.split(":", 1)[1].strip() for l in run.stdout.splitlines() if l.startswith("Client Secret:"))
    client = next(l.split(":", 1)[1].strip() for l in run.stdout.splitlines() if l.startswith("Client ID  :"))
    assert user_can_write(client, secret, "clicks") == 2
    example_rows.append({"example": "quickstart_env/bootstrap.py", "server": "quickstart image", "status": 0,
                      "verdict": "as-is", "fix": "host only (localhost -> service name)"})
    measure("bootstrap_output", [l[:60] for l in lines])


@pytest.mark.parametrize("privilege", ["CATALOG_MANAGE_PRIVILEGES", "TABLE_DELETE"])
def test_nonexistent_privilege_names_are_rejected(privilege, example_rows, measure):
    """CATALOG_MANAGE_PRIVILEGES and TABLE_DELETE are often named in guides but do not exist; Polaris has
    CATALOG_MANAGE_ACCESS and TABLE_DROP instead."""
    body = {"name": f"probe_{privilege.lower()}", "type": "INTERNAL", "properties": {"default-base-location": "s3://warehouse/p"},
            "storageConfigInfo": {"storageType": "S3", "allowedLocations": ["s3://warehouse/p"], "stsUnavailable": True}}
    requests.post(f"{MEM.mgmt}/catalogs", json=body, headers=MEM.headers(), timeout=30)
    r = requests.put(f"{MEM.mgmt}/catalogs/{body['name']}/catalog-roles/catalog_admin/grants",
                     json={"grant": {"type": "catalog", "privilege": privilege}}, headers=MEM.headers(), timeout=30)
    assert r.status_code == 400
    measure(f"privilege_{privilege}", r.status_code)
