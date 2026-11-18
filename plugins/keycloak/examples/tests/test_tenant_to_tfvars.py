"""T-iam-030..032: Tenant manifest -> Terraform variables, with platform rules enforced."""
import copy

import pytest
import yaml


def doc(fixtures):
    return yaml.safe_load((fixtures / "tenant-acme.yaml").read_text(encoding="utf-8"))


def test_converts_valid_tenant(tenant, fixtures):
    tid, value = tenant.convert(doc(fixtures))
    assert tid == "t-acme"
    assert value["admin_group"] == "admin"
    assert value["idp"]["trust_email"] is True
    assert value["idp"]["group_map"] == {"HKM-Analistler": "analyst", "HKM-Yoneticiler": "admin"}
    assert "pii-reader" in value["roles"]


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d["metadata"].update(name="ACME"), "tenant id"),
    (lambda d: d["spec"].update(roles=["analyst"]), "admin"),
    (lambda d: d["spec"].update(roles=["analyst", "admin", "superuser"]), "unknown roles"),
    (lambda d: d["spec"]["identity"]["idp"]["groupMap"].update({"X": "engineer-lead"}), "groupMap"),
    (lambda d: d["spec"]["identity"].update(domains=[]), "domain"),
    (lambda d: d["spec"]["identity"]["idp"].update(issuer="http://adfs.acme.gov.tr"), "https"),
])
def test_rejects_rule_violations(tenant, fixtures, mutate, message):
    d = copy.deepcopy(doc(fixtures))
    mutate(d)
    with pytest.raises(tenant.ManifestError, match=message):
        tenant.convert(d)


def test_cli_outputs_json(tenant, fixtures, capsys):
    assert tenant.main([str(fixtures / "tenant-acme.yaml")]) == 0
    out = capsys.readouterr().out
    assert '"t-acme"' in out and '"institutions"' in out
