---
name: keycloak-operations-upgrades
description: Operating Keycloak 26.x for the HKM platform - SLIs/SLOs and Prometheus alerts, failure modes (database, cache, IdP outage, key rotation, clock skew, lockouts), backup/restore and DR drills, signing-key and client-secret rotation, CVE and advisory watch, patch vs minor upgrade procedure (zero-downtime patch updates, full shutdown for minor upgrades), the 26.8 breaking-change checklist, load testing with Keycloak Benchmark, and runbooks. Use when running, upgrading or troubleshooting Keycloak in production.
---

# Operations and Upgrades

## SLIs and alerts
| SLI | Source | Alert |
|---|---|---|
| Login success ratio per realm/IdP | events (`LOGIN` vs `LOGIN_ERROR`) in OpenSearch | drop > 20 % in 10 min → page; per-IdP drop → institution IdP down |
| Token endpoint latency p99 | HTTP metrics | > 1 s for 10 min |
| Ready pods | `/health/ready`, kube-state-metrics | < 2 ready |
| DB connections / pool wait | `agroal_*` metrics | pool exhausted |
| JVM heap, GC pause | JVM metrics | heap > 85 % for 15 min |
| Brute-force lockouts | `USER_DISABLED_BY_*` / `LOGIN_ERROR user_temporarily_disabled` | spike → credential stuffing |
| Admin events outside change windows | admin events | any realm-wide change not from the Terraform client → security alert |
| Certificate / key expiry | realm keys, TLS secrets | < 30 days |

## Failure modes and runbooks
- **DB down**: Keycloak cannot issue tokens; existing access tokens work until expiry (about 15 min; about 5 min for high-security clients). Restore DB/failover via CNPG; Keycloak reconnects.
- **Institution IdP down**: only that organization's users fail; communicate; break-glass local login for that institution's admins only if pre-approved.
- **Signing key rotation**: on a dated schedule, add a new active key with higher priority, keep the old one passive until all tokens and JWKS caches expired (≥ access token + refresh lifetimes), then disable, then delete; disable at once only on suspected compromise. Resource servers must refresh JWKS on unknown `kid`. Protect realm signing keys tightly and evaluate HSM/KMS storage until the 26.8 docs confirm a supported key provider; keep every other private key in OpenBao.
- **Client key rotation**: clients use `client-jwt` keys published at a JWKS URL with stable `kid` values (no client secrets, HKM D12); publish the new key, switch signing, then remove the old key. Machine secrets in OpenBao rotate on a schedule and after any exposure (HKM D33); workload secrets are mounted as read-only files, not environment variables.
- **Clock skew**: NTP on nodes; 26.6 default not-before tolerance 10 s.
- **Mass lockout**: check source IPs in events; block at ingress; unlock via Admin API script, not by disabling brute force.
- **Lost admin**: use a personal `master` break-glass account (at least two named operators, enabled, alert on every login; two-person release of passkey or OpenBao password, rotated after use; console changes written back to Terraform within 24 h — HKM D65). A temporary `bootstrap-admin` service account is the last resort under the same two-person, alerted procedure and is deleted immediately afterwards.

## Backup and DR
CNPG continuous archiving + base backups; quarterly restore drill into an isolated namespace, start Keycloak against it, run claims-contract check. Keep the exact image and Terraform state version with the backup. RPO = WAL archiving interval; RTO target 1 h.

## Upgrades
| Type | Example | Procedure |
|---|---|---|
| Patch (same major.minor) | 26.8.1 → 26.8.2 | operator `update.strategy: Auto` → rolling if compatible; still read the release notes (26.8.2 changed Admin API audience, protocol mapper rights, SAML and token exchange rules) |
| Minor | 26.7.x → 26.8.x | full shutdown (recreate) required; DB backup; staging rehearsal on prod copy; first node migrates schema; then the others; no downgrade — roll back only by restoring the pre-upgrade backup (HKM D42) |
| Provider/Terraform | provider v5.x, `keycloak_version` pin | upgrade with server, re-plan |

### 26.8 checklist (from release notes and upgrading guide)
1. IdP mappers no longer grant admin roles by default — verify none needed.
2. Client secrets need `manage-clients` (not `view-clients`).
3. `initiating_idp` logout parameter ignored — use OIDC back-/front-channel logout.
4. Disabled clients removed from `aud`/`resource_access`.
5. Group claims: bare names resolve to top-level groups only — platform groups are top-level; check any nested-group policies.
6. Authorization Services resource URIs normalised — re-test Airflow permissions.
7. `--features=preview` no longer enables `stateless`; set a cache cluster name.
8. 26.8.1: StartTLS strict for SMTP; back-channel logout tokens always signature-validated (configure JWKS for IdPs).
9. 26.8.2: Admin REST clients need the `kc-admin-api` scope (Terraform, Ranger sync, scripts); protocol mapper management restricted; organization domain membership requires email verification; exchanged tokens capped by subject token lifetime.
10. Rebuild custom providers/themes against the new Quarkus; check login theme overrides (social buttons redesigned).

## CVE watch
Keycloak publishes fixes in patch releases (26.8.2 fixed 33 CVEs). Subscribe to keycloak.org release posts and GitHub security advisories; patch within 14 days for High, 30 days otherwise (platform policy, plugin v1); track in the platform vulnerability register.

## Load testing
Keycloak Benchmark (Gatling) scenarios: authorization code login, client credentials, refresh; run before go-live and after minor upgrades; compare against sizing formulas.

## References
- `references/operations-and-upgrades.md` — merged rules: incident signals, key/certificate/secret rotation, backup and DR, upgrade strategy, IdP threat model and hardening, troubleshooting, exception runbooks, image supply chain.
