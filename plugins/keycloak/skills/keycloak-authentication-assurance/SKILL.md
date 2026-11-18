---
name: keycloak-authentication-assurance
description: Authentication strength for the HKM platform in Keycloak 26.x - authentication flows, passkeys/WebAuthn (discoverable credentials), OTP, conditional and step-up authentication with ACR/LoA mapping, brute-force detection, password policy for local accounts, SSO and client session timeouts, remember-me, and how these map to NIST SP 800-63B-4 AAL levels, OWASP ASVS 5.0 and the Bilgi ve İletişim Güvenliği Rehberi (3.1.12, 3.2.1, 3.2.2). Use when designing login flows, MFA policy, session lifetimes or answering an audit question about authentication.
---

# Authentication and Assurance

## Assurance targets (platform)
| Population / action | Target | Mechanism |
|---|---|---|
| Staff via institution IdP | AAL2 equivalent, MFA on every login (HKM D9) | institution IdP MFA, asserted via `acr`/`amr`; if the IdP cannot prove MFA, Keycloak adds a second factor after brokering (post-broker-login flow); passkey is the target factor, TOTP only until a passkey is enrolled |
| Platform operators, institution admins, `platform_admins` | AAL3-like: phishing-resistant | separate admin accounts signing in only with passkeys (User Verification `required`, two passkeys per administrator, device-bound where AAL3 is claimed) |
| Sensitive actions (PII unmask role use, export, admin console) | step-up to `acr=gold` | step-up flow with LoA mapping; client requests `acr_values` or `claims` |
| Public portal users (`hkm-public`) | AAL1 for low-risk actions, AAL2 otherwise | password-only (`silver`) only for low-risk actions; passkey or OTP as second factor, recovery codes as backup |
| Machines | not interactive | client authentication (see clients-and-tokens) |

ACR mapping in realm: `silver` → LoA 1, `gold` → LoA 2. In `hkm`, `silver` means password plus a second factor and `gold` always requires a phishing-resistant passkey; password-only `silver` exists only in `hkm-public` (HKM D9). Clients enforce their floor with *Minimum ACR Value* (not *Default ACR values*, which a client can lower) and request `gold` with `acr_values` for sensitive actions; resource servers check `acr` (ASVS 10.3.4).

## Session and reauthentication limits
| Setting | Workforce realm `hkm` | `master` / admin | `hkm-public` | Basis |
|---|---|---|---|---|
| SSO session idle | 30 min | 15 min (also the `hkm` admin console client) | set explicitly; no platform value yet | NIST 800-63B-4 AAL2 inactivity ≤ 1 h (SHOULD), AAL3 ≤ 15 min; BG 3.1.12.6; HKM D10, D38 |
| SSO session max | 10 h | 12 h | set explicitly; no platform value yet | AAL2 overall ≤ 24 h, AAL3 ≤ 12 h |
| Client session idle/max | ≤ SSO values | ≤ SSO values | ≤ SSO values | override per client only to shorten |
| Access token lifespan | about 15 min (about 5 min for high-security clients) | about 5 min | about 15 min | short bearer exposure (oauth-oidc-profile starter rule 6) |
| Remember-me | off | off | off | HKM D62 |
| Concurrent sessions | unlimited, visible and endable in the account console | 1 per administrator | unlimited, visible and endable | HKM D63 |
| Offline sessions | only for named clients, Offline Session Max Limited on | off | off | ASVS 10.4.8 absolute expiry; async jobs use service accounts (HKM D16) |

## Brute force and passwords
- Brute-force detection on in `master`, `hkm` and `hkm-public`: temporary per-user lockout with increasing waits plus IP rate limiting at the ingress; no automatic permanent lockout — a permanent lock (BG 3.2.1.1) means an administrator disables the user (HKM D30). Count second-factor failures too (Max Secondary Authentication Failures non-zero, at most 100) and keep Failure Reset Time longer than Max Wait. Password and OTP counters are separated (default since 26.6), login failures persisted in DB (26.8). Start from the 26.8.2 defaults; HKM lockout values are still to be set from observed events.
- Local-account password policy: length ≥ 15 when the password is the only factor, ≥ 8 only where MFA is always enforced, max length ≥ 64, no composition rules, denylist file with breached, common and context words (auto-reload since 26.6), not username/email, hashing Argon2 (default) (HKM D6). Maximum age long (365 days suggested) to meet BG Rehberi 3.2.1.8, with a forced change on evidence of compromise or a breached-password hit (HKM D5).
- Account recovery as strong as login (BG 3.2.1.11): workforce users recover through their institution IdP or help-desk re-verification; `hkm-public` users by email link plus a second factor; administrators register two passkeys; no SMS anywhere (HKM D7, D48, D64). Administrators never type a user's password; they send an execute-actions email (HKM D8).

## Flows
- Browser flow (a Terraform copy of the built-in flow, never the original): Cookie → Identity Provider Redirector (organization identity-first) → Username form → Password → REQUIRED second-factor subflow for every `hkm` user (passkey; TOTP only until a passkey is enrolled); a user without a second factor is forced to enrol or denied.
- Post-broker-login flow on each institution IdP: Conditional OTP/WebAuthn when the IdP's `amr` does not show MFA.
- Passkeys: WebAuthn Passwordless policy with User Verification `required` and discoverable credentials `required` (HKM D37); administrators register through the WebAuthn Register Passwordless required action at first login.

## Anti-patterns
- Password grant (direct access grants) for apps — forbidden (ASVS 10.4.4, RFC 9700).
- Long-lived SSO sessions or Remember Me (off in every realm, HKM D62).
- SMS, voice or email as an authentication or recovery factor (HKM D64).
- MFA only at the institution with no evidence in the assertion.
- Permanent lockouts that let an attacker disable every admin.

## Open owner decisions
Still undecided: the named assurance profile for government logins. When a request depends on one of them, say so, ask whether it has been decided since, and point to `../../references/open-decisions.md`. Give options, never a value presented as decided.

## References
- `references/authentication-and-assurance.md` — merged rules: flows and factors, passkeys, step-up and LoA, sessions and logout, brute force, passwords and recovery.
- Standards: NIST SP 800-63B-4 §2 and session chapter; OWASP ASVS 5.0 V6, V7, V10; BG Rehberi 1.1 sections 3.1.12, 3.2.1, 3.2.2 — see `../../references/standards.md`.
