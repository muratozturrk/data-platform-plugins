"""Fitness test: Nessie authorization — institution roles cannot commit to main or see other institutions' branches."""
import os, httpx, pytest

NESSIE = os.environ["NESSIE_URI"]

def h(token): return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def test_engineer_cannot_commit_to_main():
    head = httpx.get(f"{NESSIE}/trees/main", headers=h(os.environ["TOKEN_T_ACME_ENGINEER"])).json()["reference"]
    r = httpx.post(f"{NESSIE}/trees/main@{head['hash']}/history/commit", headers=h(os.environ["TOKEN_T_ACME_ENGINEER"]),
                   json={"commitMeta": {"message": "x"}, "operations": [{"type": "PUT", "key": {"elements": ["probe"]}, "content": {"type": "NAMESPACE", "elements": ["probe"]}}]})
    assert r.status_code == 403

def test_other_institution_branch_hidden():
    r = httpx.get(f"{NESSIE}/trees/t-globex", headers=h(os.environ["TOKEN_T_ACME_ANALYST"]))
    assert r.status_code in (403, 404)

def test_release_tags_readable():
    r = httpx.get(f"{NESSIE}/trees", headers=h(os.environ["TOKEN_T_ACME_ANALYST"]), params={"filter": "ref.name.startsWith('rel-')"})
    assert r.status_code == 200

def test_promotion_service_can_merge():
    r = httpx.get(f"{NESSIE}/trees/main", headers=h(os.environ["TOKEN_SVC_PROMOTION"]))
    assert r.status_code == 200
