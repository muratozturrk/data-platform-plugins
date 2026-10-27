import dataclasses
import uuid

import pytest

from common.s3 import client, empty_bucket, ensure_bucket
from storage_requirements_probe.probe import ProbeReport, visibility
from storage_governance.governance import feature_matrix

from .scorecard import SCENARIOS, VENDORS, measured_profile, rank, raw_capacity_tb, shortlist


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.name for s in SCENARIOS])
def test_scorecard_shortlists_the_reference_vendors(scenario, measure):
    short = shortlist(scenario)
    measure("shortlist", f"{short} (reference: {list(scenario.picks)})")
    assert set(scenario.picks) <= set(short)


@pytest.mark.parametrize("scenario", [s for s in SCENARIOS if s.weaker], ids=lambda s: s.name)
def test_vendors_named_weaker_rank_below_the_picks(scenario):
    pos = {x.vendor: i for i, x in enumerate(rank(scenario))}
    for worse in scenario.weaker:
        assert all(pos[worse] > pos[p] for p in scenario.picks)


def test_vendor_table_matches_the_vendor_matrix():
    assert len(VENDORS) == 10
    assert [v.name for v in VENDORS if v.s3_api == "none"] == ["HDFS"]
    assert {v.name for v in VENDORS if v.s3_api == "native"} == {"Amazon S3", "Google Cloud Storage", "Azure Blob/ADLS"}
    assert [v.name for v in VENDORS if "no egress" in v.cost_model] == ["Wasabi"]
    assert {v.name for v in VENDORS if "cloud" in v.deployment} == {
        "Amazon S3", "Google Cloud Storage", "Azure Blob/ADLS", "Wasabi"}


def test_a_changed_constraint_changes_the_answer():
    """The startup again, but legal now requires on-premises storage: Wasabi is filtered out."""
    startup = next(s for s in SCENARIOS if s.name.startswith("Startup"))
    on_prem = dataclasses.replace(startup, deployment=frozenset({"onprem"}))
    assert shortlist(startup)[0] == "Wasabi"
    assert shortlist(on_prem)[0] == "MinIO" and "Wasabi" not in shortlist(on_prem)


@pytest.fixture(scope="module")
def measured():
    """Vendor rows for the two lab stores, built only from live probe results."""
    rows = {}
    for store in ("seaweedfs", "rustfs"):
        s3, uid = client(store), uuid.uuid4().hex[:6]
        ensure_bucket(store, "probes")
        raw, lst = visibility(s3, "probes", f"select-{uid}", rounds=5)
        empty_bucket(store, "probes", f"select-{uid}")
        rows[store] = measured_profile(store, feature_matrix(s3, f"sel-{uid}"),
                                       ProbeReport(store, read_after_write=raw, list_after_write=lst))
    return rows


def test_measured_profiles_qualify_only_where_evidence_exists(measured, measure):
    hipaa = next(s for s in SCENARIOS if "HIPAA" in s.name)
    retention = next(s for s in SCENARIOS if "retention" in s.name)
    lab = list(measured.values())
    hipaa_scores = {x.vendor: x.missing for x in rank(hipaa, lab)}
    measure("hipaa_missing", hipaa_scores)
    measure("measured_caps", {k: sorted(v.caps) for k, v in measured.items()})
    assert hipaa_scores == {"seaweedfs": ("audit_logging", "encryption_at_rest"),
                            "rustfs": ("audit_logging",)}            # audit logs were never probed
    assert all(x.eligible for x in rank(retention, lab))                # object lock was enforced


def test_durability_scheme_drives_raw_capacity():
    assert raw_capacity_tb(100, replicas=3) == 300                      # HDFS default
    assert raw_capacity_tb(100, ec=(8, 4)) == 150                       # assumption: EC 8+4
