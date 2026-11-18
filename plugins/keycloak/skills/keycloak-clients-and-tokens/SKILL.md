---
name: keycloak-clients-and-tokens
description: The OAuth 2.0 / OpenID Connect profile of the HKM platform in Keycloak 26.x - client archetypes (web app with backend, SPA via BFF, CLI/native, resource server, service/job, automation), authorization code + PKCE S256, confidential client authentication (private_key_jwt, federated client authentication with Kubernetes service-account tokens, client secret rotation), DPoP sender-constrained tokens, client scopes and audience, token lifetimes and refresh-token rotation, standard token exchange (RFC 8693) and delegation, JWT authorization grant, device grant, client policies and profiles, back-channel logout. Use when registering a client, reviewing a token, or deciding how a workload authenticates.
---

# Clients and Tokens

## Client archetypes
| Archetype | Examples | Keycloak client | Grant | Client auth | Notes |
|---|---|---|---|---|---|
| Server-side web app | Superset, Airflow UI, Grafana, OpenMetadata, Trino Web UI, Argo CD | confidential, standard flow | code + PKCE S256 | `client-jwt` (signed JWT, keys from a JWKS URL); no client secrets | exact redirect URIs; back-channel logout URL; acts as its own BFF |
| Browser SPA | portal front-ends | **BFF** in front; the BFF is a confidential client | code + PKCE | via BFF | tokens never in the browser (ASVS 10.1.1); an existing public SPA stays only with DPoP and refresh rotation (HKM D14) |
| Native/CLI | Trino CLI (external auth), `kubectl`, `bao login -method=oidc` | public | code + PKCE (loopback) or device grant | none | refresh rotation plus DPoP (HKM D11); loopback redirect with variable port is the only exact-match deviation (HKM D13) |
| Resource server / API | Trino coordinator, Nessie, Kafka brokers, WSO2-published APIs | no login; validates tokens via JWKS | — | — | checks `iss`, `aud`, `exp`, `acr` |
| Workload in Kubernetes | Spark/Flink jobs, Airflow workers, control-plane controller | confidential, service account on | client credentials | **federated client authentication** (`federated-jwt`) with Kubernetes SA token (supported 26.6) | no static secret; no refresh tokens for client credentials; SPIFFE not used (preview, HKM D40) |
| Automation | Terraform, Ranger sync, CI | confidential in `hkm` (not `master` unless managing realms) | client credentials | `client-jwt` | needs `kc-admin-api` scope for Admin REST since 26.8.2 |

## Token rules
1. Authorization code + PKCE S256 for every interactive client; implicit and password grants off (RFC 9700; ASVS 10.4.4, 10.4.6).
2. Exact redirect URI match; no wildcards in production (ASVS 10.4.1), except a variable port on native/CLI loopback redirects and the one documented host-limited `airflow` exception.
3. Every API client gets an explicit **audience** via an `aud-{resource}` client scope; resource servers reject other audiences (ASVS 10.3.1). Since 26.8 disabled clients drop out of `aud`/`resource_access`.
4. Access tokens about 15 min, about 5 min for high-security clients, set at realm level and overridden per client only to shorten; refresh tokens rotated (Revoke Refresh Token on, max reuse 0); absolute expiry via client session max (ASVS 10.4.5, 10.4.8). Authorization code lifetime at most 1 min, target 10 s (HKM D23).
5. DPoP (supported 26.4) required for every public client and every ASVS L3 client; confidential clients of other sensitive APIs move to DPoP when their libraries support it. No mTLS client authentication or certificate-bound tokens (ASVS 10.3.5, 10.4.14; HKM D11, D46).
6. "Full scope allowed" off (deprecated 26.8): grant only the role scopes a client needs (ASVS 10.4.11).
7. Confidential clients authenticate with `client-jwt` or `federated-jwt` only; no client secrets and no HS256 client-secret JWTs (HKM D12). Tokens are signed with RS256 or ES256 (HKM D15).
8. Token exchange: **standard token exchange** (RFC 8693, supported since 26.2) for service-to-service down-scoping (e.g. Superset → Trino on behalf of user), only narrowing scope and audience (HKM D50); legacy V1 is deprecated; exchanged tokens cannot outlive the subject token (26.8.2). Token exchange delegation (`delegation:client:<id>` scope) is preview in 26.8 — not for production.
9. External-to-internal: JWT authorization grant (RFC 7523, supported 26.6) instead of V1 external exchange, only for a reviewed cross-domain bridge.
10. Back-channel logout URLs for server-side apps so revocation propagates.

## Client policies
Enable a realm client profile `hkm-secure` with executors: secure client authenticator (`client-jwt`/`federated-jwt`), PKCE enforcer (S256), secure redirect URIs (exact, https), confidential client required for standard flow except allow-listed public CLIs, `allowed-protocol-mappers` (26.8) to stop delegated admins adding mappers; apply via a policy with condition "any client" and a negative client-role condition for the built-in console clients. Apply the FAPI 2 Security Profile (PAR, DPoP, `client-jwt`) to the ASVS L3 clients: admin console, Ranger Admin UI, Terraform service account, break-glass (HKM D23).

## Anti-patterns
- One shared client for several apps; client secrets of any kind (HKM D12).
- Tokens with every role of every client (`full scope allowed`).
- Resource servers that accept any token signed by the realm.
- Using ID tokens as API credentials.

## Examples and references
- `examples/terraform/clients/` and `examples/terraform/scopes.tf`
- `references/oauth-oidc-profile.md` — merged rules: discovery and ID token, client types and grants, client authentication, access token design, refresh and logout, DPoP, token exchange, browser and native apps.
