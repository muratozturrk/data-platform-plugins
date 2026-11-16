"""Offline check of the manifest compiler (no engine needed): python -m pytest examples/tests/test_compile_manifest.py"""
import copy, json, pathlib, sys, pytest

EX = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(EX / "automation"))
from compile_policies import SHA, compile_docs, load

DOCS = load([EX / "policies" / "policy-manifest.yaml"])
POL = {p["name"]: p for p in compile_docs(DOCS)}

def spec(docs, rid): return next(d for d in docs if d["metadata"].get("id") == rid)["spec"]
def items(p): return p.get("policyItems", []) + p.get("rowFilterPolicyItems", []) + p.get("dataMaskPolicyItems", [])
def raises(rid, match, **changes):
    bad = copy.deepcopy(DOCS); spec(bad, rid).update(changes)
    with pytest.raises(AssertionError, match=match): compile_docs(bad)

def test_domain_scope_is_published_layer_only():
    assert POL["t-acme-ra-0001-trino-interactive"]["resources"]["schema"]["values"] == ["gold_sales"]
    assert POL["t-acme-ra-0001-kafka-main"]["resources"]["topic"]["values"] == ["t.acme.sales.pub.*"]
    assert POL["t-acme-ra-0001-spark-sql"]["resources"]["database"]["values"] == ["t_acme_lake_gold_sales"]   # D5

def test_dataset_and_column_scope():
    r = POL["t-acme-ra-0006-trino-interactive"]["resources"]
    assert (r["table"]["values"], r["column"]["values"]) == (["dim_customers"], ["customer_id", "segment", "city"])
    assert POL["t-acme-ra-0006-spark-sql"]["resources"]["database"]["values"] == ["t_acme_lake_gold_sales"]
    assert not any(n.startswith("t-acme-ra-0006-kafka") for n in POL)                     # no topics listed
    assert POL["t-globex-ra-0011-kafka-main"]["resources"]["topic"]["values"] == ["t.globex.risk.pub.loans.v1"]

def test_no_ranger_roles_and_no_delegate_admin():
    for p in POL.values():
        for i in items(p):
            assert i["roles"] == [] and not i.get("delegateAdmin")

def test_writes_only_on_trino_etl_and_spark():                                           # D8
    for n, p in POL.items():
        if p["service"] == "trino-interactive":
            assert all(a["type"] == "select" for i in items(p) for a in i["accesses"]), n
    assert {a["type"] for a in POL["t-acme-ra-0003-trino-etl"]["policyItems"][0]["accesses"]} >= {"insert", "update", "delete"}
    assert "t-acme-ra-0001-trino-etl" not in POL                                           # readers get no ETL policy
    assert POL["t-acme-ra-0003-spark-sql"]["policyItems"][0]["accesses"][-1]["type"] == "update"

def test_workload_identity_is_a_direct_user_and_humans_write_only_staging():
    assert POL["t-acme-ra-0003-trino-etl"]["policyItems"][0]["users"] == ["wi-t-acme-orders-ingest"]
    assert POL["t-acme-ra-0002-trino-etl"]["resources"]["schema"]["values"] == ["silver_sales"]
    raises("ra-0002", "#61", scope="/institutions/t-acme/domains/sales")

def test_humans_do_not_consume_int_topics():                                             # D3
    assert [a["type"] for a in POL["t-acme-ra-0002-kafka-main"]["policyItems"][0]["accesses"]] == ["describe", "publish"]
    assert "t-acme-ra-0002-kafka-main-cg" not in POL

def test_user_principal_and_foreign_group_rejected():
    raises("ra-0001", "groups", principal={"type": "user", "name": "ayse"})
    raises("ra-0007", "agreement", principal={"type": "group", "name": "platform_t-acme_analyst"})

def test_eligible_without_activation_emits_nothing():
    assert not any("assignment:ra-0005" in p["policyLabels"] for p in POL.values())
    static = next(p for p in json.load(open(EX / "policies" / "tag-policies.json", encoding="utf-8")) if p["name"] == "t-acme-tag-mask")
    assert POL["t-acme-tag-mask"]["dataMaskPolicyItems"] == static["dataMaskPolicyItems"]  # static file = compiled zone instance

def test_activation_and_eligibility_limits():                                            # D7
    raises("ra-0009", "PT8H", activation={"start": "2026-11-17T09:00:00+03:00", "end": "2026-11-17T17:01:00+03:00"})
    raises("ra-0009", "3 months", eligibility={"start": "2026-11-16T00:00:00+03:00", "end": "2027-02-17T00:00:00+03:00"})
    raises("ra-0005", "3 months", eligibility={"start": "2026-11-16T00:00:00+03:00", "end": "2027-03-01T00:00:00+03:00"})
    bad = copy.deepcopy(DOCS); s = spec(bad, "ra-0009"); s.pop("assignmentType")
    with pytest.raises(AssertionError, match="eligible-only"): compile_docs(bad)

def test_share_is_trino_interactive_only_with_generated_row_filter():                    # D4, D6
    assert [n for n in POL if "-ra-0010-" in n] == ["t-globex-ra-0010-trino-interactive"]
    rf = POL["t-globex-loans-rowfilter"]
    assert rf["service"] == "trino-interactive" and rf["zoneName"] == "t-globex" and "exemption:owning-pipeline" in rf["policyLabels"]
    got = [(i["groups"], i["users"], i["rowFilterInfo"]["filterExpr"]) for i in rf["rowFilterPolicyItems"]]
    assert got == [(["platform_t-acme_marketing-analyst"], [], "region = 'TR'"),
                   (["platform_t-globex_credit-risk", "platform_t-globex_risk-analyst"], [], "1 = 1"),
                   ([], ["wi-t-globex-loans-ingest"], ""),
                   (["public"], [], "1 = 0")]
    assert rf["rowFilterPolicyItems"][0]["validitySchedules"][0]["endTime"] == "2027/05/15 23:59:59"
    bad = copy.deepcopy(DOCS); spec(bad, "ra-0010").pop("columns")
    with pytest.raises(AssertionError, match="column-scoped"): compile_docs(bad)

def test_pii_reader_activation_lands_in_zone_tag_mask():                                 # D2
    first = POL["t-globex-tag-mask"]["dataMaskPolicyItems"][0]
    assert POL["t-globex-tag-mask"]["zoneName"] == "t-globex"
    assert first["groups"] == ["platform_t-globex_credit-risk"] and first["dataMaskInfo"]["dataMaskType"] == "trino:MASK_NONE"
    assert first["validitySchedules"] == [{"startTime": "2026/11/17 09:00:00", "endTime": "2026/11/17 17:00:00", "timeZone": "Europe/Istanbul"}]
    assert not any("-ra-0009-" in n for n in POL)                                          # never a resource policy

def test_breakglass_allow_inside_every_zone():                                            # F11
    for z in ("t-acme", "t-globex"):
        p = POL[f"{z}-breakglass-trino-interactive"]
        assert p["zoneName"] == z and p["policyItems"][0]["accesses"] == [{"type": "select", "isAllowed": True}]

def test_labels_on_every_policy():                                                        # F28
    for n, p in POL.items():
        keys = {l.split(":", 1)[0] for l in p["policyLabels"]}
        assert keys >= {"source", "institution", "assignment", "role", "owner", "review-by", "manifest"}, n
        assert any(l.startswith("manifest:") and l.endswith(f"@{SHA}") for l in p["policyLabels"]), n
    assert "request:REQ-2026-0412" in POL["t-acme-ra-0004-trino-interactive"]["policyLabels"]

def test_validity_and_consumer_groups():
    assert POL["t-acme-ra-0004-trino-interactive"]["validitySchedules"][0]["endTime"] == "2027/01/31 23:59:59"
    assert "t-acme-ra-0001-kafka-main-cg" in POL
