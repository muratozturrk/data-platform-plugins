"""Production-readiness checks: Polaris 1.8.0 checks its own configuration at startup."""
import json

from conftest import container


def test_readiness_checks_list_the_production_gaps(measure):
    """Polaris 1.8.0 checks its own configuration at startup and logs what is not production-ready."""
    lines = container("polaris-hard").logs().decode("utf-8", "replace").splitlines()
    msgs = [json.loads(l)["message"] for l in lines if l.startswith("{") and "ProductionReadinessChecks" in l]
    clean = [m.replace("\u0000", "").strip() for m in msgs]
    measure("readiness_checks", clean)
    assert any("Production readiness checks failed" in m for m in clean) and len(clean) > 1
