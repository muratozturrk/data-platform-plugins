"""Control-plane endpoint: mint Superset guest tokens with policy-derived RLS.
Signs the JWT locally (GUEST_TOKEN_JWT_SECRET shared via Vault) — no round-trip to Superset."""
from __future__ import annotations
import time, jwt
from dataclasses import dataclass

@dataclass(frozen=True)
class EmbedRequest:
    tenant_id: str
    user_id: str
    username: str
    roles: list[str]
    dashboard_uuid: str

def rls_rules_for(req: EmbedRequest, policy) -> list[dict]:
    """Derive RLS from the platform Policy, never from client input."""
    rules = [{"clause": f"tenant_id = '{req.tenant_id}'"}]            # tenant fence on shared datasets
    for grant in policy.grants_for(req.tenant_id, req.roles):          # e.g. region restrictions
        if grant.row_filter:
            rules.append({"dataset": grant.dataset_id, "clause": grant.row_filter})
    return rules

def mint_guest_token(req: EmbedRequest, policy, secret: str, audience: str = "superset", ttl: int = 300) -> str:
    if not policy.can_view_dashboard(req.tenant_id, req.roles, req.dashboard_uuid):
        raise PermissionError("dashboard not granted to tenant/role")
    now = int(time.time())
    payload = {
        "user": {"username": f"{req.tenant_id}:{req.username}", "first_name": req.username, "last_name": ""},
        "resources": [{"type": "dashboard", "id": req.dashboard_uuid}],
        "rls_rules": rls_rules_for(req, policy),
        "iat": now, "exp": now + ttl, "aud": audience, "type": "guest",
    }
    return jwt.encode(payload, secret, algorithm="HS256")
