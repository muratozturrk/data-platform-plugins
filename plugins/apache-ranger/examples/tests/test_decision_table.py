"""Decision-table tests through the engines (T-ranger-001, T-trino-001/002). Reads examples/policies/decision-table.csv.
Cases that need an open activation ("after activation") run only when the harness has applied it (ACTIVATIONS_APPLIED=1);
then the "not activated" cases on the same resource are skipped."""
import csv, os, pathlib, re, pytest, trino

TABLE = pathlib.Path(__file__).parents[1] / "policies" / "decision-table.csv"
CASES = [r for r in csv.DictReader(TABLE.open()) if r["engine"] in ("trino", "trino-etl") and r["action"] in ("select", "insert")]
HOSTS = {"trino": (os.environ.get("TRINO_HOST", "localhost"), int(os.environ.get("TRINO_PORT", "8080"))),
         "trino-etl": (os.environ.get("TRINO_ETL_HOST", "localhost"), int(os.environ.get("TRINO_ETL_PORT", "8081")))}
ACTIVATED = os.environ.get("ACTIVATIONS_APPLIED") == "1"
HEX = re.compile(r"[0-9a-fA-F]{32,128}")

def run(case, sql):
    # harness uses header auth + a file group provider mapping principals to groups; production uses Keycloak JWTs per principal
    host, port = HOSTS[case["engine"]]
    cur = trino.dbapi.connect(host=host, port=port, user=case["principal"], extra_credential=[("groups", case["groups"])]).cursor()
    cur.execute(sql); return cur.fetchall()

def sql_for(case):
    parts = case["resource"].split(".")
    if case["action"] == "insert":
        return f"INSERT INTO {'.'.join(parts[:3])} SELECT * FROM {'.'.join(parts[:3])} WHERE 1=0"
    if len(parts) == 4:
        return f"SELECT {parts[3]} FROM {'.'.join(parts[:3])} LIMIT 5"
    return f"SELECT * FROM {case['resource']} LIMIT 50"

MASKED = {"MASK_SHOW_LAST_4": lambda v: v is not None and str(v)[:-4].strip("xX") == "",
          "MASK_HASH": lambda v: v is not None and bool(HEX.fullmatch(str(v))),
          "MASK_NULL": lambda v: v is None}

@pytest.mark.decision
@pytest.mark.parametrize("case", CASES, ids=[c["case"] for c in CASES])
def test_decision(case):
    if ("after activation" in case["detail"] and not ACTIVATED) or ("not activated" in case["detail"] and ACTIVATED):
        pytest.skip("depends on the harness activation state")
    sql = sql_for(case)
    if case["expected"] == "deny":
        with pytest.raises(trino.exceptions.TrinoUserError) as e:
            run(case, sql)
        assert "Access Denied" in str(e.value)
    elif case["expected"] == "allow":
        rows = run(case, sql)
        if case["detail"].startswith("unmasked"):
            assert rows and all(r[0] is not None and not HEX.fullmatch(str(r[0])) for r in rows), "expected clear values"
    elif case["expected"] == "mask":
        mask = case["detail"].split()[0]                     # mask type is the first word of `detail` (F24)
        rows = run(case, sql)
        assert rows and all(MASKED[mask](r[0]) for r in rows), f"expected {mask}"
    elif case["expected"] == "filter":
        if "'" not in case["detail"]:                        # e.g. "1 = 1": unfiltered, must not hit the public 1 = 0 item
            assert run(case, f"SELECT count(*) FROM {case['resource']}")[0][0] > 0
            return
        col = case["detail"].split("=")[0].strip(); want = case["detail"].split("'")[1]
        rows = run(case, f"SELECT DISTINCT {col} FROM {case['resource']}")
        assert rows == [[want]]
