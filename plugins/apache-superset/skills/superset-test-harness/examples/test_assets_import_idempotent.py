"""T-bi-002: sync native twice → same UUIDs and counts; nothing duplicated."""
import subprocess, os, pytest

def snapshot(client):
    out = {}
    for kind, key in (("dataset", "table_name"), ("chart", "slice_name"), ("dashboard", "slug")):
        r = client.get(f"/api/v1/{kind}/?q=(page_size:500)"); r.raise_for_status()
        out[kind] = sorted((x[key], x.get("uuid")) for x in r.json()["result"])
    return out

@pytest.mark.integration
def test_sync_is_idempotent(superset_client, superset_url):
    before = snapshot(superset_client)
    subprocess.run(["superset-cli", "--jwt-token", superset_client._login("admin"), superset_url, "sync", "native", "fixtures/assets/t-acme",
                    "--option", "tenant=t-acme", "--option", "tenant_catalog=t_acme", "--disallow-edits", "--overwrite"], check=True, env=os.environ)
    after = snapshot(superset_client)
    assert before == after, "second sync changed UUIDs or counts"
