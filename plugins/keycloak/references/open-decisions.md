# Open owner decisions

These topics have no approved platform decision yet (status 2026-11-18). The plugin writes no rule or number for them.

**When a request touches one of them:**
- Tell the user it is still open.
- Ask whether the platform owner or legal team has decided it since.
- If they have, ask the user to record the decision in the `keycloak-workspace` as an answer to the open item. That workspace turns it into a rule.
- Until then, give options, never a number or legal position presented as decided.

| Topic | Owner | Affects |
| --- | --- | --- |
| KVKK legal basis (Art. 5) for workforce, institution and `hkm-public` data; whether explicit consent (açık rıza) records are needed and where they live | legal | audit-compliance (privacy), identity-federation (registration) |
| Data subject rights process (Art. 11): intake channel, who executes access, correction and deletion in Keycloak, audit-record exceptions | legal, platform owner | audit-compliance |
| Breach notification runbook (72 h to the Board) | legal, security | audit-compliance, operations-upgrades |
| Retention periods: user events, admin events, OpenSearch, immudb hashes, closed-account attributes. Keycloak keeps events forever when no expiration is set | platform owner (destruction policy) | audit-compliance |
| Inactivity threshold for disabling accounts | platform owner | identity-federation, `/keycloak-access-review` |
| Grace period before self-deletion in `hkm-public` completes | platform owner | audit-compliance |
| Renewal interval for temporary (time-limited) accounts | platform owner | identity-federation |
| Whether a disabled account keeps its group memberships | platform owner | identity-federation, `/keycloak-access-review` |
| Which organization holds vendor and auditor accounts | platform owner | architecture-tenancy, identity-federation |
| Named assurance profile for government logins (e-Devlet, institution IdPs) | platform owner | authentication-assurance |
| Review cadence for the account inventory and hygiene report | platform owner | `/keycloak-access-review` |
