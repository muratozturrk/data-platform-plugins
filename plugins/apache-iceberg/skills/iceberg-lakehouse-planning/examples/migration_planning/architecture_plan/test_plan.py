from requirements_validation.requirements import load
from acceptance_probes.probes import Result
from architecture_plan.plan import briefing, load_decisions, migration_waves, render_guide, uncovered


def test_migration_waves_follow_access_sensitivity_and_complexity(audit_report):
    waves = [(w.wave, w.dataset, w.strategy, w.complexity) for w in migration_waves(audit_report)]
    assert waves == [
        (1, "pg:reporting.daily_positions", "rewrite", 2),          # 48% of reads
        (1, "pg:reporting.v_desk_exposure", "recreate_view", 2),    # 32% of reads
        (1, "s3:legacy-lake/trades_history", "add_files", 1),       # metadata-only move
        (2, "pg:trading.trades", "rewrite", 2),
        (2, "s3:legacy-lake/vendor_feeds", "rewrite", 2),
        (2, "pg:staging.vendor_feed_raw", "rewrite", 3),            # jsonb payload
        (2, "s3:legacy-lake/market_ticks", "rewrite", 3),           # JSON files
        (3, "pg:crm.clients", "rewrite", 3),                        # PII + no primary key
        (3, "s3:legacy-lake/compliance_records", "rewrite", 2),     # PII
    ]


def test_traceability_finds_requirements_without_a_decision():
    reqs, decisions = load(), load_decisions()
    assert uncovered(reqs, decisions) == ["ING-04"]                 # lab-derived, not in the scenario's plan
    no_catalog = [d for d in decisions if d.id != "D-CATALOG"]
    assert uncovered(reqs, no_catalog) == ["ING-04", "CAT-01", "CAT-02", "CAT-03"]


def test_briefings_are_role_specific():
    reqs, decisions = load(), load_decisions()
    compliance = dict(briefing("compliance", reqs, decisions))
    assert compliance["STO-01"] == "D-STORAGE" and "CON-04" not in compliance
    assert len(compliance) == 6
    assert dict(briefing("data_engineering", reqs, decisions))["ING-04"] == "OPEN"


def test_guide_contains_every_section_and_open_items(audit_report, tmp_path):
    acceptance = [Result("CAT-02", "probe", "fail", {}), Result("CAT-01", "probe", "pass", {})]
    guide = render_guide(load(), load_decisions(), audit_report, acceptance)
    (tmp_path / "architecture_guide.md").write_text(guide)
    for heading in ("## Data flow", "## Requirement traceability", "## Open items", "## Dataset map",
                    "## Staged migration plan", "## Road-show briefings"):
        assert heading in guide
    diagram = guide.split("```mermaid\n", 1)[1].split("```", 1)[0]
    assert diagram.startswith("flowchart TD") and diagram.count("subgraph ") == 5
    assert diagram.count('["') == 10 + 2 + 5                         # decisions, legacy systems, subgraphs
    assert sum(1 for line in guide.splitlines() if line.startswith("| ") and "-0" in line.split("|")[1]) == 18
    assert "- ING-04: no decision addresses it" in guide
    assert "- CAT-02: acceptance check failed on the evaluated platform" in guide
    assert "| CAT-01 | critical | catalog | D-CATALOG | pass |" in guide
    assert "| s3:legacy-lake/compliance_records | csv | regulated (PII) |" in guide
