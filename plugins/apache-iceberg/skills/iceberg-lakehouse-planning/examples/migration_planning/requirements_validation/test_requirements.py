from collections import Counter
from dataclasses import replace

import pytest

from requirements_validation.requirements import evaluate_metric, load, prioritized, validate


@pytest.fixture(scope="module")
def reqs():
    return load()


def errors(issues):
    return [(i.requirement, i.message) for i in issues if i.level == "error"]


def test_hamerliva_requirements_validate_against_the_audit(reqs, audit_report):
    assert len(reqs) == 18
    assert sum(r.source == "scenario" for r in reqs) == 16          # from the reference scenario
    assert Counter(r.component for r in reqs) == {"storage": 4, "ingestion": 4, "catalog": 3,
                                                  "federation": 3, "consumption": 4}
    assert errors(validate(reqs, audit_report)) == []


def test_requirements_that_name_products_are_flagged_for_review(reqs):
    warned = {i.requirement for i in validate(reqs) if i.level == "warning"}
    assert warned == {"ING-02", "CAT-03", "FED-01"}      # Spark/Airflow, Active Directory, HDFS


def test_untestable_and_untraced_requirements_are_rejected(reqs):
    bad = replace(reqs[0], acceptance={}, traces=())
    msgs = [m for rid, m in errors(validate([bad] + reqs[1:])) if rid == "STO-01"]
    assert len(msgs) == 2
    assert msgs[0].startswith("not testable") and msgs[1].startswith("not traced")


def test_unaddressed_high_severity_finding_blocks_the_set(reqs, audit_report):
    without_key_rule = [r for r in reqs if r.id != "ING-04"]
    assert ("*", "high-severity finding F-NO-PRIMARY-KEY is not addressed by any requirement") in \
        errors(validate(without_key_rule, audit_report))


def test_missing_component_and_unknown_trace_are_reported(reqs, audit_report):
    no_federation = [r for r in reqs if r.component != "federation"]
    assert ("*", "component 'federation' has no requirement") in errors(validate(no_federation))
    typo = [replace(reqs[0], traces=("F-PIII",))] + reqs[1:]
    assert ("STO-01", "traces unknown finding 'F-PIII'") in errors(validate(typo, audit_report))


def test_prioritized_puts_critical_first(reqs):
    order = [r.priority for r in prioritized(reqs)]
    assert order == sorted(order, key=["critical", "nice-to-have", "future-facing"].index)
    assert Counter(order) == {"critical": 13, "nice-to-have": 4, "future-facing": 1}
    assert prioritized(reqs)[0].id == "STO-01"


def test_metric_acceptance_is_evaluated(reqs):
    con01 = next(r for r in reqs if r.id == "CON-01")                    # "within 5 seconds"
    assert evaluate_metric(con01, 3.2) and not evaluate_metric(con01, 7.5)
