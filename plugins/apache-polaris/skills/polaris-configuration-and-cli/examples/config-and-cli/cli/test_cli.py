"""A Polaris CLI session with the PyPI package apache-polaris 1.8.0, including syntax from older
docs that 1.8.0 rejects; only --host/--port/--client-id/--client-secret are inserted."""
import json

import pytest

from common.polaris import CatalogClient
from conftest import cli

SEQUENCE = [   # (command line, expected exit code, fix if it fails on 1.8.0)
    ("polaris catalogs create --storage-type file --default-base-location file:///tmp/polaris my_catalog", 0, ""),
    ("polaris catalogs create --storage-type s3 --default-base-location s3://my-bucket/polaris-data "
     "--role-arn arn:aws:iam::1234567890:role/my-polaris-role my_cloud_catalog", 0, ""),
    ("polaris catalogs list", 0, ""),
    ("polaris catalogs update my_catalog --property environment=dev", 2, "--set-property environment=dev"),
    ("polaris principals create data_engineer_1", 0, ""),
    ("polaris principals create --property team=analytics data_scientist_1", 0, ""),
    ("polaris principals rotate-credentials data_engineer_1", 1,
     "root may only reset: principals reset data_engineer_1; rotate-credentials is for the principal itself"),
    ("polaris principals list", 0, ""),
    ("polaris principals get data_engineer_1", 0, ""),
    ("polaris principal-roles create analyst_role", 0, ""),
    ("polaris principal-roles grant --principal data_scientist_1 analyst_role", 0, ""),
    ("polaris catalog-roles create --catalog my_catalog limited_reader", 0, ""),
    ("polaris catalog-roles grant --catalog my_catalog --principal-role analyst_role limited_reader", 0, ""),
    ("polaris namespaces create --catalog my_catalog analytics.q1", 1, "create the parent first: namespaces create --catalog my_catalog analytics"),
    ("polaris namespaces list --catalog my_catalog", 0, ""),
    ("polaris privileges catalog grant --catalog my_catalog --catalog-role limited_reader CATALOG_MANAGE_CONTENT", 0, ""),
    ("polaris privileges table grant --catalog my_catalog --catalog-role limited_reader --namespace analytics.q1 "
     "--table sales_data TABLE_READ_DATA", 0, ""),
    ("polaris privileges table revoke --catalog my_catalog --catalog-role limited_reader --namespace analytics.q1 "
     "--table sales_data --cascade TABLE_READ_DATA", 1, "drop --cascade (server answers 501 Not Implemented)"),
]
FIXES = {
    "polaris catalogs update my_catalog --property environment=dev": ["polaris catalogs update my_catalog --set-property environment=dev"],
    "polaris principals rotate-credentials data_engineer_1": ["polaris principals reset data_engineer_1"],
    "polaris namespaces create --catalog my_catalog analytics.q1": ["polaris namespaces create --catalog my_catalog analytics",
                                                                  "polaris namespaces create --catalog my_catalog analytics.q1"],
}


def before_table_grant():
    """Table grants need an existing table; sales_data is created here (FILE catalog)."""
    CatalogClient("my_catalog", "root", "s3cr3t", vended=False).create_table("analytics.q1", "sales_data")


def test_cli_entity_session(example_rows, measure):
    log = []
    for line, expected, fix in SEQUENCE:
        if "privileges table grant" in line:
            before_table_grant()
        r = cli(line)
        log.append({"line": line, "exit": r.returncode, "last": r.out.splitlines()[-1][:160] if r.out else ""})
        example_rows.append({"example": line, "status": r.returncode, "verdict": "as-is" if r.returncode == 0 else "fails", "fix": fix})
        assert r.returncode == expected, f"{line}: {r.out[-300:]}"
        for fixed in FIXES.get(line, []):
            f = cli(fixed)
            assert f.returncode == 0, f"{fixed}: {f.out[-300:]}"
    revoke = cli(SEQUENCE[-1][0].replace(" --cascade", ""))
    assert revoke.returncode == 0
    measure("sequence", log)


def test_entities_ended_up_as_described(measure):
    cat = json.loads(next(l for l in cli("polaris catalogs get my_catalog").out.splitlines() if l.startswith("{")))
    scientist = json.loads(cli("polaris principals get data_scientist_1").out)
    grants = [json.loads(l) for l in cli("polaris privileges list --catalog my_catalog --catalog-role limited_reader").out.splitlines()]
    namespaces = cli("polaris namespaces list --catalog my_catalog").out
    measure("end_state", {"catalog_properties": cat["properties"], "data_scientist_1_properties": scientist["properties"],
                               "limited_reader_grants": grants, "namespaces": namespaces})
    assert cat["properties"].get("environment") == "dev"
    assert scientist["properties"] == {"team": "analytics"}
    assert grants == [{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}]   # table grant revoked


def test_file_storage_is_refused_by_default(example_rows, measure):
    """The same FILE line against a server with default settings (polaris-durable, realm my_realm)."""
    r = cli("polaris catalogs create --storage-type file --default-base-location file:///tmp/polaris my_catalog",
            conn=["--host", "polaris-durable", "--port", "8181", "--client-id", "client-id", "--client-secret", "my-secret"])
    measure("file_on_defaults", r.out[-200:])
    assert r.returncode == 1 and "Unsupported storage type: FILE" in r.out


def test_authentication_and_connection_options(measure):
    out = {}
    out["no_host (default localhost:8181)"] = cli("polaris principals list", conn=["--client-id", "root", "--client-secret", "s3cr3t"]).returncode
    out["CLIENT_ID / CLIENT_SECRET env"] = cli("polaris principals list", conn=["--host", "polaris", "--port", "8181"],
                                               env={"CLIENT_ID": "root", "CLIENT_SECRET": "s3cr3t"}).returncode
    both = cli("polaris principals list", conn=["--host", "polaris", "--port", "8181", "--client-id", "root",
                                                "--client-secret", "s3cr3t", "--access-token", "x"])
    out["client-id + access-token"] = both.returncode
    out["--base-url .../api/catalog/v1 (api path)"] = cli("polaris principals list", conn=[
        "--base-url", "http://polaris:8181/api/catalog/v1", "--client-id", "root", "--client-secret", "s3cr3t"]).returncode
    out["--base-url http://polaris:8181"] = cli("polaris principals list", conn=[
        "--base-url", "http://polaris:8181", "--client-id", "root", "--client-secret", "s3cr3t"]).returncode
    measure("auth_connection", {**out, "both_message": both.out[-160:]})
    assert out == {"no_host (default localhost:8181)": 1, "CLIENT_ID / CLIENT_SECRET env": 0,
                   "client-id + access-token": out["client-id + access-token"],
                   "--base-url .../api/catalog/v1 (api path)": out["--base-url .../api/catalog/v1 (api path)"],
                   "--base-url http://polaris:8181": 0}
    assert out["client-id + access-token"] != 0 and out["--base-url .../api/catalog/v1 (api path)"] != 0


def test_profiles(measure):
    cli("polaris profiles delete dev", conn=[])             # profiles live in ~/.polaris across runs
    created = cli("polaris profiles create dev", conn=[],
                  stdin="root\ns3cr3t\npolaris\n8181\nhttp\nPOLARIS\nPolaris-Realm\n")
    by_flag = cli("polaris catalogs list", conn=["--profile", "dev"])      # polaris --profile dev catalogs list
    by_env = cli("polaris catalogs list", conn=[], env={"CLIENT_PROFILE": "dev"})
    measure("profiles", {"create": created.returncode, "--profile dev": by_flag.returncode, "CLIENT_PROFILE=dev": by_env.returncode})
    assert (created.returncode, by_flag.returncode, by_env.returncode) == (0, 0, 0)
    assert "my_catalog" in by_flag.out
