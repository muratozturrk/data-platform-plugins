---
name: keycloak-audit-compliance
description: Evidence and compliance for Keycloak on the HKM platform - user events and admin events (which types, with or without representation), event listeners and shipping JSON logs to OpenSearch, tamper evidence in immudb, retention, KVKK (personal data held in Keycloak, minimisation, erasure requests), access reviews of groups and admin rights, and control mapping to the Bilgi ve İletişim Güvenliği Rehberi 1.1 (3.1.8, 3.1.12, 3.2.1-3.2.3), NIST SP 800-63-4, OWASP ASVS 5.0 V6-V10 and ISO/IEC 27001 A.5.15-A.5.18/A.8.5. Use when designing audit, answering an auditor, preparing a KVKK request or running an access review.
---

# Audit and Compliance

## Events to capture
| Kind | Setting | Destination | Retention |
|---|---|---|---|
| User events: `LOGIN`, `LOGIN_ERROR`, `LOGOUT`, `CODE_TO_TOKEN(_ERROR)`, `REFRESH_TOKEN_ERROR`, `CLIENT_LOGIN(_ERROR)`, `TOKEN_EXCHANGE`, `IDENTITY_PROVIDER_LOGIN(_ERROR)`, `UPDATE_CREDENTIAL`, `REMOVE_CREDENTIAL`, `RESET_PASSWORD`, `UPDATE_PROFILE`, `IMPERSONATE`, `USER_DISABLED_BY_*`, `USER_SESSION_DELETED` (26.5) | realm events enabled, listener `jboss-logging` (success level `info`, errors `warn`) | JSON logs → Fluent Bit → OpenSearch index `kc-events-<realm>-*` | explicit event expiration in every realm (DB store for troubleshooting only); OpenSearch period per the destruction policy — retention still open |
| Admin events (with representation; verify no secrets reach it) | admin events enabled, include representation, representation size capped | same pipeline; immudb gets event id, time, actor `sub` and a hash | retention still open |
| Server logs, HTTP access log (dedicated file, tokens redacted since 26.6) | `log-console-output=json`, `http-access-log-enabled` | OpenSearch | retention still open |
| Metrics (`keycloak_*`, event metrics) | management interface | Prometheus | retention still open |

Every event carries `realmId`, `clientId`, `userId`, `ipAddress`, `sessionId`, `details` (`username`, `identity_provider`, `auth_method`, `impersonator` since 26.8). Enrich in the pipeline with `organization` (tenant id) and `preferred_username`.

## Tamper evidence
The full event lives in OpenSearch (restricted to authorised staff, deleted when its retention ends); immudb receives only event id, time, actor `sub` and a hash of the full event, for user, admin and data-access logs alike (HKM D19). BG Rehberi 3.1.8.4 requires integrity protection of log records.

## KVKK
- Keycloak is a data processor component holding identity data of staff and public users: inventory it in the VERBİS records of each institution.
- Minimise: user profile only with required attributes; no TCKN in `hkm`; no hidden history of earlier email addresses or phone numbers (HKM D28); IP addresses in events are personal data and stay unobfuscated → retention schedule and access restriction to authorised staff (HKM D20).
- Erasure (public realm): delete user → sessions/credentials removed; events keep `userId` — pseudonymise or expire per retention; document in the DSR runbook. Workforce users: disable on leave, delete after the institution's retention period.
- Consent screen off for first-party clients, on for third-party clients (HKM D20).
- Still open (no rules yet): KVKK legal basis, retention periods, the data subject rights process and the breach notification runbook (`synthesis/platform-decisions.md`).

## Access reviews
Quarterly (every authorisation at least once a year, BG 3.1.12.20): export group memberships of `platform_*` groups per institution, institution admins attest (keep / remove), changes applied via Terraform/SCIM, evidence stored (report + signed approval) — `/keycloak-access-review`. Also review: realm-management roles, FGAP permissions, service-account roles, clients with `full scope allowed`, IdP mappers granting groups.

## Control mapping (excerpt)
| Control | Requirement | Keycloak evidence |
|---|---|---|
| BG 3.1.12.2 / 3.2.1.2 | unique accounts, no shared accounts | no shared users; service accounts per component |
| BG 3.1.12.3 | brute-force protection, failed logins logged | brute force settings; `LOGIN_ERROR` events |
| BG 3.1.12.5 / .11 | separate admin accounts, MFA, admin actions audited | `master` break-glass with WebAuthn; admin events |
| BG 3.1.12.6 / 3.2.2.3 | idle timeout, logout | SSO idle/max settings; back-channel logout |
| BG 3.1.12.8 / .10 | periodic review, disable unused accounts | access review records; Workflows |
| BG 3.1.12.13 / .14 | inventory of authentication systems; centralised authentication | client inventory from Terraform; SSO coverage list |
| BG 3.2.1.13 | central identity management or e-Devlet integration (level 3) | Keycloak as central IdP; e-Devlet broker for public realm if adopted |
| BG 3.2.2.5 | users can list/terminate sessions | Account Console sessions page |
| NIST 800-63B-4 | AAL2/3 authenticators, reauth limits | flows, WebAuthn policy, session settings |
| ASVS 5.0 V10 | OAuth/OIDC hardening | client policy `hkm-secure`, realm audit report |

## Open owner decisions
Still undecided: KVKK legal basis and consent records, data subject rights, breach notification, retention periods and the self-deletion grace period. When a request depends on one of them, say so, ask whether it has been decided since, and point to `../../references/open-decisions.md`. Give options, never a value presented as decided.

## References
- `references/audit-and-compliance.md` — merged rules: what to log, detection and response, access reviews, privacy, tenant exit, regulatory mapping, identity policies.
- `examples/automation/realm_audit.py` — checks a realm export against these controls.
