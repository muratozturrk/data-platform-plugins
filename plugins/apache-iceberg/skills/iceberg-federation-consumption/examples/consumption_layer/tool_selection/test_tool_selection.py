"""A tested selection helper that reproduces ten reference scenario decisions."""
import re
from dataclasses import replace

import pytest

from tool_selection.selector import REFERENCE_PICKS, SCENARIOS, recommend


@pytest.mark.parametrize("scenario", list(SCENARIOS))
def test_recommendation_covers_the_reference_choice(scenario):
    rec = recommend(SCENARIOS[scenario])
    missing = REFERENCE_PICKS[scenario] - set(rec.components)
    assert not missing, f"{scenario}: missing {missing}"


def test_startup_gets_no_bi_stack_yet():
    rec = recommend(SCENARIOS["startup data science"])
    assert not {"semantic_layer", "jdbc_odbc", "enterprise_bi", "embedded_bi"} & set(rec.components)


def test_every_reason_names_its_source():
    for needs in SCENARIOS.values():
        for reasons in recommend(needs).components.values():
            for reason in reasons:
                assert re.search(r"\[(guideline|lab assumption)\]$", reason), reason


def test_single_requirements_toggle_single_components():
    base = SCENARIOS["media self-service"]
    assert "mcp_server" not in recommend(base)
    assert "mcp_server" in recommend(replace(base, personas=base.personas | {"ai_agents"}))
    assert "arrow_flight" not in recommend(base)
    assert "arrow_flight" in recommend(replace(base, bulk_programmatic_access=True))
    assert "region_bound_deployment" in recommend(replace(base, data_residency=True))


def test_precision_against_the_reference_picks():
    picked = sum(len(recommend(n).components) for n in SCENARIOS.values())
    matched = sum(len(REFERENCE_PICKS[s] & set(recommend(n).components)) for s, n in SCENARIOS.items())
    print(f"recommended {picked} components, {matched} in the reference picks ({matched / picked:.0%})")
    # Extras are components the reference picks do not name for that scenario (e.g. a semantic layer for the
    # government agency): additions to review, not contradictions. Recall is 100% (test above).
    assert matched / picked >= 0.6
