# OAuth 2.0 / OIDC profile

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (RFC 9700, NIST SP 800-63C-4, OWASP ASVS 5.0) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action, Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Use authorization code with PKCE S256 (a verifier of 43 to 128 characters) and a random, session-bound `state` for every interactive client (web, desktop, CLI), set the challenge method explicitly on every client (never blank, never `plain`) and enforce it with a client policy. Why: the blank default makes PKCE optional and lets an attacker strip the challenge and inject an unbound code, PKCE makes an intercepted code useless, and `state` stops login CSRF. (research: RFC 9700 §2.1.1; OWASP ASVS 5.0, 10.4.6; RFC 9700 §4.8; Keycloak docs 26.8, p. 11, p. 13; Keycloak IAM ch. 3, 6, 14; Cloud Native OAuth ch. 2, 12, 13; Solving IdM ch. 5, 6; API Security in Action ch. 7; OAuth 2 in Action ch. 2, 7, 10; HKM D45)
2. Register full, exact HTTPS redirect URIs per client, compared as exact case-sensitive strings with no `*`, path, subdomain or context-path wildcards, list custom schemes explicitly, allow no onward redirect from the callback, keep DNS for redirect hosts free of dangling CNAMEs, and enforce this with the secure redirect URIs client policy executor; the only deviation is a variable port on loopback redirects of native and CLI clients. Why: vague redirect URIs and taken-over subdomains enable client impersonation, open redirects and leaked codes. (research: RFC 9700; OWASP ASVS 5.0, 10.4.1; RFC 9700 §2.1, §2.6, §4.1.1, §4.1.3; Keycloak docs 26.8, p. 11, p. 13; Keycloak IAM ch. 6, 14; Cloud Native OAuth ch. 2; Solving IdM ch. 5; API Security in Action ch. 7; HKM D13) The only other exception is `airflow` (Client types and grant selection, rule 12; OAuth 2 in Action ch. 7, 9).
3. Give every resource server its own audience through an optional `aud-*` client scope with an Audience mapper, issue each token for one resource server (or a small documented set), make every API reject tokens without its own `aud`, and never accept an ID token as an API credential. Why: a token stolen from or replayed by one API must not work at another, and an issuer that shares keys across audiences can only be told apart by `aud`. (research: OWASP ASVS 5.0, 10.3.1; RFC 9700 §2.3; OWASP ASVS 5.0, 9.2.4; Keycloak docs 26.8, p. 11, p. 13; Cloud Native OAuth ch. 5, 6; Solving IdM ch. 5; API Security in Action ch. 6, 7; OAuth 2 in Action ch. 10, 13; Identity Attack Vectors ch. 19; Learning Digital Identity ch. 4; HKM D15)
4. Decide per client whether it gets refresh tokens at all; for every public client rotate them on every use (Revoke Refresh Token on, the client stores the new token after each refresh, reuse revokes the active one) and bind them with DPoP. Why: refresh-token replay is the main residual risk of public clients, and rotation alone does not stop an attacker who uses a leaked token first. (research: RFC 9700 §4.14; OWASP ASVS 5.0, 10.4.5; RFC 9700 §2.2.2, §4.14.1, §4.14.2; Keycloak docs 26.8, p. 5; Keycloak IAM ch. 6, 12; Cloud Native OAuth ch. 7, 13; Solving IdM ch. 5, 9, 10; Identity Attack Vectors ch. 14; HKM D11, D14)
5. Authenticate confidential clients, backend-for-frontend clients included, with Signed JWT (`client-jwt`, public key from a JWKS URL) or Signed JWT issued by an identity provider (`federated-jwt`) on every back-channel request (token, PAR, revocation), enforce the allowed authenticators with a client policy, and use no client secrets and no HS256 client-secret JWTs. Why: Keycloak then holds no shared secret, a public-key assertion never travels and cannot be phished, and JWKS keys are re-imported when they change. (research: OWASP ASVS 5.0, 10.4.16; Keycloak 26.6 release notes, federated client authentication; RFC 9700 §2.5; OWASP ASVS 5.0, 10.4.10; Keycloak docs 26.8, p. 11; Cloud Native OAuth ch. 10, 13; API Security in Action ch. 11; Identity Attack Vectors ch. 6, 14, 19; Zero Trust Networks ch. 10; HKM D12)
6. Keep access tokens short-lived, sized to the data's sensitivity and to how fast a dismissed user must lose access: about 15 minutes, about 5 minutes for high-security clients, never hours, with Access Token Lifespan set at realm level and overridden per client only to shorten it. Why: bearer tokens cannot be recalled once issued, signing out or disabling does not revoke them, claim-based decisions keep stale group membership until expiry, and tokens derived by exchange are not revoked with their subject token. (plugin v1; Keycloak docs 26.8, p. 5, p. 13, p. 14; Keycloak IAM ch. 12; OWASP ASVS 5.0, 8.3.2; Cloud Native OAuth ch. 7; Solving IdM ch. 5, 9, 13; API Security in Action ch. 6, 7; OAuth 2 in Action ch. 2, 8, 10; Identity Attack Vectors ch. 10, 14; Learning Digital Identity ch. 13)

## Rules by topic

### Discovery, sign-in and ID token

Source: Keycloak – IAM for Modern Applications, ch. 3–4; RFC 9700 §2.6, §4.5, §4.6; NIST SP 800-63C-4 §4.9–4.11; OWASP ASVS 5.0, 10.5.1–10.5.4; Cloud Native Data Security with OAuth, ch. 2, 6, 7 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 4, 6, 8, 9, 11, 14, 18; API Security in Action, ch. 7; OAuth 2 in Action (Richer, Sanso, 2017), ch. 2, 9, 11, 13, 15; Learning Digital Identity, ch. 10, 13 (merged 2026-11-18). New section: no pending heading covered discovery, ID token and UserInfo.

1. Configure every relying party with the realm issuer URL only (for example `https://sso.hkm.example/realms/hkm`), load endpoints from the discovery document, and require the discovery `issuer` to equal the configured issuer exactly. Why: endpoints and supported features then come from Keycloak rather than hand-copied values, and an exact issuer stops provider impersonation. (Keycloak IAM ch. 4; RFC 9700 §2.6; OWASP ASVS 5.0, 10.5.3; API Security in Action ch. 7; OAuth 2 in Action ch. 13; Learning Digital Identity ch. 10)
2. Sign users in with OIDC: request the `openid` scope plus only the profile scopes the client needs, and build sign-in on the validated ID token, never on a bare OAuth access token, a successful API call or a UserInfo answer; call UserInfo only with the access token, never with the ID token. Why: OAuth delegates access but does not authenticate the user, without `openid` no ID token is issued, and access tokens and UserInfo answers can exist without the user being present. (Keycloak IAM ch. 3, 4; Solving IdM ch. 6; OAuth 2 in Action ch. 13; Learning Digital Identity ch. 13)
3. Validate every ID token in the client before use: signature, `iss`, `aud` equal to the client's own `client_id`, `azp`, `exp`, the `nonce` returned by the token endpoint (even if one came in the front channel), `at_hash` in hybrid flows, and `acr` or `auth_time` when the client needs them; keep the accepted validity window only as long as processing plus clock drift. Why: these checks stop injected codes, replayed ID tokens from other clients and long replay windows. (Keycloak IAM ch. 4; RFC 9700 §4.5.3.2, §4.6.1; OWASP ASVS 5.0, 10.5.1, 10.5.4; NIST SP 800-63C-4 §4.9, §4.10; Cloud Native OAuth ch. 2, 6; Solving IdM ch. 6, 8; API Security in Action ch. 7; OAuth 2 in Action ch. 13)
4. Key the user on `iss` + `sub`, never on email or another editable name, and never show `sub` to users as their user name. Why: email and display names can change or be reassigned, and the issuer scopes the subject so the pair does not collide across issuers. (Keycloak IAM ch. 4; Solving IdM ch. 4, 6, 14, 18; OAuth 2 in Action ch. 13; HKM D1)
5. Do not let clients parse access tokens; clients read user data from the ID token or UserInfo, pick up changed profile data by refreshing tokens rather than forcing a new login or a silent `prompt=none` redirect, and re-read it before sensitive actions. Why: a client that depends on access-token content turns the token into an interface that cannot change, Keycloak rebuilds claims from the current user on refresh, and group or role changes otherwise stay invisible during a long session. (Cloud Native OAuth ch. 2, 7; Keycloak IAM ch. 4; Solving IdM ch. 14; OAuth 2 in Action ch. 2, 11, 13, 15)
6. Redeem an authorization code once, right after the redirect, and restart the login on `invalid_grant` instead of retrying; verify by test in a non-production realm that Keycloak refuses a second redemption of a code, a redemption by any client other than the one the code was issued to, and a token request whose `redirect_uri` differs from the one in the authorization request. Why: codes are single-use and very short-lived, codes left in browser history can be replayed by the next user of a shared computer, and client authentication alone does not stop a stolen code from being injected through the honest client. (Keycloak IAM ch. 4; RFC 9700 §4.5.2; NIST SP 800-63C-4 §4.11.1; Solving IdM ch. 6; OAuth 2 in Action ch. 9)
7. Before a sensitive or destructive action, send `max_age` (or `prompt=login`) and verify `auth_time` in the returned ID token. Why: otherwise Keycloak silently reuses the existing SSO session however old it is. (Keycloak IAM ch. 4; Solving IdM ch. 9, 11)

Example:

```python
# superset_config.py fragment: only the issuer's discovery document is configured
OAUTH_PROVIDERS = [{
    "name": "keycloak",
    "remote_app": {
        "client_id": "superset",
        "server_metadata_url": "https://sso.hkm.example/realms/hkm/.well-known/openid-configuration",
        "client_kwargs": {"scope": "openid aud-trino", "code_challenge_method": "S256"},
    },
}]
# Admin "delete dashboard" route: redirect with max_age=300, then require auth_time >= now - 300.
```

```http
# Test in realm hkm-test (CI): one code, three redemption attempts
POST /realms/hkm-test/protocol/openid-connect/token
grant_type=authorization_code
&code=<code issued to superset>
&redirect_uri=https://superset.hkm.example/oauth-authorized/keycloak
&code_verifier=<verifier of this login>
# client authentication: superset, client-jwt assertion
# attempt 1: 200 with tokens
# attempt 2, same request: 400 invalid_grant
# attempt 3, same code with a different redirect_uri or sent by svc-spark-etl: 400
```

### Client types and grant selection

Source: Keycloak 26.8.2 documentation, Server Admin: Managing OpenID Connect and SAML clients, Managing user sessions, Mitigating security threats; Securing Applications: client registration; Keycloak – IAM for Modern Applications, ch. 3, 5, 6, 8, 14; RFC 9700 §2.1, §2.4, §2.6, §4.2.4, §4.11.2; OWASP ASVS 5.0 V10; Cloud Native Data Security with OAuth, ch. 2, 3, 12 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 5, 7; API Security in Action, ch. 7, 11; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 6, 14; OAuth 2 in Action, ch. 6, 9, 12, 13 (merged 2026-11-18).

1. Choose the grant from the caller: authorization code + PKCE S256 for every user-facing application, client credentials only for a workload acting for itself where no user is involved (`svc-spark-etl`) and never for an API call that names the user it acts for, and the device authorization grant only on named clients for headless or input-limited hosts. Why: each grant fits one situation, and a client-credentials token carries no user, so using it for a user loses per-user consent and audit. (Keycloak IAM ch. 3, 6; Cloud Native OAuth ch. 2, 12; Solving IdM ch. 5; API Security in Action ch. 11; OAuth 2 in Action ch. 6)
2. Enable on each client only the capability it needs (Standard Flow for interactive applications, Service account roles for machines), keep Implicit Flow and Direct Access Grants off on every client including test clients, and prefer the `code` response type. Why: every enabled grant adds attack surface, front-channel access tokens leak and cannot be sender-constrained, and the password grant exposes passwords and cannot do MFA. (Keycloak docs 26.8, p. 11; Keycloak IAM ch. 3, 14; RFC 9700 §2.1.2, §2.4; OWASP ASVS 5.0, 10.4.4, 10.6.1; HKM D45)
3. Prefer confidential clients, and register a user-facing client as public only when no backend can hold its credentials. Why: a public client cannot prove its identity. (Cloud Native OAuth ch. 2, 3; Keycloak IAM ch. 6, 14; API Security in Action ch. 7)
4. Do not use one client as both front end and REST service. Why: the access token then omits the requesting client from `aud`. (Keycloak docs 26.8, p. 11)
5. Set the authorization code lifetime to at most 1 minute, target 10 seconds, for every client, and make a second redemption of a code revoke the tokens already issued from it. Why: a leaked code then fails or burns the attacker's tokens; ASVS allows 10 minutes at L1/L2 and 1 minute at L3, NIST suggests at most 5 minutes. (Keycloak docs 26.8, p. 13; RFC 9700 §4.2.4; OWASP ASVS 5.0, 10.4.2, 10.4.3; NIST SP 800-63C-4 §4.11.1; OAuth 2 in Action ch. 9; HKM D23)
6. Label every client first-party (run by the platform or an HKM institution) or third-party; keep the consent screen off for first-party clients and on for third-party clients. Why: administrators approve trusted applications, while the user must approve what a third-party application receives. (Keycloak IAM ch. 5, 6, 8; HKM D20)
7. Use OIDC for every new application, and SAML 2.0 only when the application supports SAML and not OIDC. Why: OIDC fits web, SPA, native and API clients, and Keycloak serves both protocols in one SSO session. (Keycloak IAM ch. 3, 6; Solving IdM ch. 7; OAuth 2 in Action ch. 13)
8. Enforce the client profile with one client policy (`hkm-secure`) per realm (PKCE S256, exact redirect URIs, implicit and direct grants off, full scope off, allowed client authenticators), bound to Any Client with a negative client-role condition for the built-in console clients, apply the FAPI 2 Security Profile (PAR, DPoP, `client-jwt`) to the ASVS L3 clients (admin console, Ranger Admin UI, Terraform service account, break-glass), and test executors in a non-production realm first. Why: per-application discipline is unreliable, higher-risk clients need a stricter profile, and a wrong pattern blocks client updates. (Keycloak docs 26.8, p. 11, p. 13; Keycloak IAM ch. 3, 14; OWASP ASVS 5.0, 10.4.12, 10.4.13, 10.4.15; HKM D23, D34)
9. Create clients only from Terraform, keep dynamic client registration closed, leave the default registration policies in place with Client Disabled for anonymous registration as a safety net, and never accept a `client_id` that can equal a user name. Why: the registration service creates clients outside Git, allowed mappers let a registrant shape token claims, and a colliding client can be taken for a user. (Keycloak docs 26.8, p. 11, p. 14; RFC 9700 §2.6; OWASP ASVS 5.0, 10.4.7; OAuth 2 in Action ch. 9, 12; HKM D17)
10. Make sure Keycloak shows an error page instead of redirecting on an invalid `client_id`/`redirect_uri` pair, authenticates the user before any redirect, and redirects automatically only to registered URIs, including after logout. Why: error, consent-decline and `prompt=none` paths otherwise turn the IdP into a phishing redirector. (RFC 9700 §2.1, §4.11.2; OAuth 2 in Action ch. 9)
11. Register every machine identity (workload, bot or RPA tool) as its own client, one per workload and per environment, such as `svc-spark-etl`, and run it on client credentials with every user flow off, no interactive login and only its own application's roles; keep Use refresh tokens for client credentials grant off and make it request a new token at expiry. Why: such a client can only obtain tokens for itself, one client per workload lets forensics tell instances apart, automation tools tend to collect excessive rights, the refresh option is deprecated, and service-account logins use transient sessions with empty `sid`. (Keycloak docs 26.8, p. 5; Keycloak IAM ch. 6; OWASP ASVS 5.0, 10.4.4; API Security in Action ch. 11; Identity Attack Vectors ch. 6, 14; OAuth 2 in Action ch. 6)
12. Register `airflow` with the redirect URI `<base_url>/*` limited to the Airflow host as the platform's one documented wildcard exception, keep Direct Access Grants off on it, and let the Airflow CLI use `AIRFLOW_CLI_TOKEN` or the JWT bearer grant; replace the wildcard once the provider documents a single callback path. Why: the Airflow Keycloak auth manager documents no single callback path, and limiting the wildcard to one host keeps redirects inside Airflow. (research: Airflow Keycloak auth manager guide; RFC 9700 §2.1, §4.1.3; OWASP ASVS 5.0, 10.4.1; decision 2026-11-17)

Example:

```hcl
resource "keycloak_openid_client" "superset" {
  realm_id                        = keycloak_realm.hkm.id
  client_id                       = "superset"
  access_type                     = "CONFIDENTIAL"
  client_authenticator_type       = "client-jwt"
  standard_flow_enabled           = true
  implicit_flow_enabled           = false
  direct_access_grants_enabled    = false
  service_accounts_enabled        = false
  pkce_code_challenge_method      = "S256"
  consent_required                = false   # first-party
  full_scope_allowed              = false
  valid_redirect_uris             = ["https://superset.hkm.example/oauth-authorized/keycloak"]
  valid_post_logout_redirect_uris = ["https://superset.hkm.example/logout/done"]
  web_origins                     = ["https://superset.hkm.example"]
  extra_config = {
    "use.jwks.url" = "true"
    "jwks.url"     = "https://superset.hkm.example/.well-known/jwks.json"
  }
}

resource "keycloak_openid_client" "svc_spark_etl" {
  realm_id                     = keycloak_realm.hkm.id
  client_id                    = "svc-spark-etl"
  access_type                  = "CONFIDENTIAL"
  client_authenticator_type    = "federated-jwt"   # projected Kubernetes service-account token
  service_accounts_enabled     = true
  standard_flow_enabled        = false
  direct_access_grants_enabled = false
  full_scope_allowed           = false
}
```

### Client authentication and secrets

Source: Keycloak 26.8.2 documentation, Server Admin: Managing OpenID Connect and SAML clients, Integrating identity providers (Kubernetes); Keycloak – IAM for Modern Applications, ch. 6, 14; Cloud Native Data Security with OAuth, ch. 12 (merged 2026-11-17); API Security in Action, ch. 7, 11; OAuth 2 in Action, ch. 2, 6, 7; Learning Digital Identity, ch. 14; Zero Trust Networks, 2nd ed., ch. 6, 7 (merged 2026-11-18). The credential type is starter rule 5.

1. Register an application as a confidential client whenever its credential can stay on a server, and keep PKCE on for it as well. Why: client authentication proves which application redeems the code, and PKCE still binds the code to the request that started the flow. (Keycloak IAM ch. 6, 14; OAuth 2 in Action ch. 2, 7)
2. Never ship a client credential inside a distributed binary or browser code; register CLI, desktop and mobile tools, and any existing public SPA, as public clients. Why: anything shipped to users can be decompiled or read, and every installed copy would share the same extractable secret. (Cloud Native OAuth ch. 12; API Security in Action ch. 7; OAuth 2 in Action ch. 6, 7; HKM D14)
3. Generate client key pairs outside Keycloak and register a JWKS URL with stable `kid` values, never a pasted static public key. Why: key generation in the console is deprecated, Keycloak then never holds the private key, and a pasted key must be replaced by hand at each rotation while a JWKS URL lets keys rotate behind a stable reference. (Keycloak docs 26.8, p. 11; Learning Digital Identity ch. 14)
4. Authenticate in-cluster workloads through the Kubernetes identity provider: one dedicated service account per client (never `default`), a projected token with the realm issuer as audience and a short lifetime (at most 3600 s), and the client authenticator Signed JWT - Federated. Why: the subject is namespace plus service account, and Keycloak accepts the same projected token again until it expires (it does not enforce `jti` for these assertions), so only a short lifetime limits a replay. (Keycloak docs 26.8, p. 8; API Security in Action ch. 11; Zero Trust Networks ch. 6, 7; research: Keycloak 26.8.2 documentation and source, 2026-11-18)
5. Make every `client-jwt` assertion carry `iss` and `sub` equal to the client ID, exactly one `aud`, preferably the realm issuer URL (Keycloak also accepts its token, introspection, PAR or CIBA endpoint), a short `exp` and a unique `jti`, signed with the client's own key whose public half Keycloak reads from the client's JWKS URL. Why: these claims bind the assertion to one client and one endpoint, a unique `jti` lets Keycloak refuse a replay, Keycloak rejects assertions older than 60 s by default, and only a public key is stored on the Keycloak side. (API Security in Action ch. 11; HKM D12; research: Keycloak 26.8.2 documentation and source, 2026-11-18)

Example:

```yaml
# Pod spec excerpt for svc-spark-etl: projected token for Keycloak federated client authentication
serviceAccountName: svc-spark-etl
volumes:
  - name: kc-token
    projected:
      sources:
        - serviceAccountToken:
            audience: https://sso.hkm.example/realms/hkm
            expirationSeconds: 600
            path: token
```

```json
{
  "iss": "superset",
  "sub": "superset",
  "aud": "https://sso.hkm.example/realms/hkm/protocol/openid-connect/token",
  "iat": 1795000000,
  "exp": 1795000060,
  "jti": "5f1c2a9e-7d4b-4c0e-9a51-2b8e6f3d1c77"
}
```
(Claim set of a `client-jwt` assertion that `superset` signs with its own key; the `kid` is published at its JWKS URL.)

### Access token design (format, claims, audience, lifetime)

Source: Keycloak 26.8.2 documentation, Server Admin: Roles and groups, Managing OIDC and SAML clients, Mitigating security threats, Upgrading Guide; Keycloak – IAM for Modern Applications, ch. 3, 4, 5, 12, 14; RFC 9700 §2.3, §4.3, §4.6; OWASP ASVS 5.0 V9, 10.2.3, 10.3.2, 10.4.11; Cloud Native Data Security with OAuth, ch. 1, 2, 5, 6, 7 (merged 2026-11-17); API Security in Action, ch. 6, 7; OAuth 2 in Action, ch. 2, 7, 8, 10, 11, 13; Learning Digital Identity, ch. 9, 12, 13 (merged 2026-11-18).

1. Limit every token to the least scope, claims and audience the client needs: assign each client only the client scopes it uses (claims every token needs in Default scopes, per-call audiences in Optional `aud-*` scopes), give every token a limited scope, make APIs reject tokens that carry none, and send the user through a new authorization request when a client needs a scope it was not granted. Why: a stolen least-privilege token has limited impact, optional scopes apply only when requested and permitted, and scope is the main limit on what a client can do with a token. (Keycloak docs 26.8, p. 11; RFC 9700 §2.3; OWASP ASVS 5.0, 10.2.3, 10.4.11; Cloud Native OAuth ch. 1, 6, 7; API Security in Action ch. 7; OAuth 2 in Action ch. 2, 8, 10; Learning Digital Identity ch. 12, 13)
2. Turn Full Scope Allowed off on every client except `security-admin-console` and `admin-cli`, grant roles through explicit role scope mappings, and enforce it with the `full-scope-disabled` executor, excluding the console clients by a marker client role with negative logic. Why: the switch is deprecated, a full-scope token carries every user role so one compromised client exposes every system, and the console breaks without the exclusion. (Keycloak docs 26.8, p. 6, p. 11, p. 13, p. 17; HKM D34)
3. Configure both service-account roles and role scope mappings for machine clients such as `svc-spark-etl`. Why: their tokens hold only the intersection of the two. (Keycloak docs 26.8, p. 11)
4. Sign tokens only with RS256 or ES256, never with HS256 or another shared-secret algorithm, and move the realm to ES256 only after every consumer (Trino, Kafka, Nessie, ClickHouse) confirms it can verify it. Why: with a symmetric key every API holds a key that can mint tokens, and a switch that a consumer cannot verify breaks it. (Keycloak IAM ch. 14; Cloud Native OAuth ch. 5, 7; OAuth 2 in Action ch. 11; HKM D12, D15)
5. Keep a tested procedure to replace signing keys and algorithms quickly. Why: key theft or a broken algorithm needs immediate change. (Cloud Native OAuth ch. 7)
6. Validate tokens at every resource server with a vetted, maintained JWT library in a fixed order: parse; compare the header with expected values (an algorithm allowlist that excludes `none`, a `kid` looked up only in the key set loaded from the configured realm JWKS URL, `jku`, `x5u`, `x5c` and `jwk` headers ignored or rejected); verify the signature; check `iss`, `aud`, `exp`, `nbf` and `typ`; and only then read the other claims the service relies on. Never paste production tokens or keys into online JWT debuggers. Why: hand-written checks miss algorithm confusion, cross-type and cross-service reuse and attacker-supplied keys, claims are untrusted until the signature is verified, and tokens are live credentials. (Keycloak IAM ch. 3, 5, 14; OWASP ASVS 5.0, 9.1.1–9.1.3, 9.2.1–9.2.3; API Security in Action ch. 6, 7; OAuth 2 in Action ch. 11; Learning Digital Identity ch. 9)
7. Accept access tokens only from the token endpoint and only in the `Authorization` header, never in a URL query parameter. Why: URLs end up in logs, history and Referer headers, and front-channel tokens can be injected. (Keycloak IAM ch. 3; RFC 9700 §4.3.1, §4.3.2, §4.6; OAuth 2 in Action ch. 7, 8, 13; HKM D45)
8. Let resource servers decide from `sub`, `scope` and `authorization_details`, while data rights stay in Ranger and never in scopes. Why: scopes describe what the client may ask for, not which data the user may read. (OWASP ASVS 5.0, 10.3.2; RFC 9700 §2.3; API Security in Action ch. 7; HKM D2)
9. Issue JWT access tokens only, and use introspection instead of local validation for critical operations that must not accept a token whose session has ended, caching the result for a few minutes at most and never longer than the revocation delay the operation tolerates. Why: local validation sees only signature and time claims, while introspection reflects logout at the cost of a call to Keycloak, and every cached second is a second of delayed revocation. (Keycloak IAM ch. 4, 5, 12; API Security in Action ch. 6, 7; OAuth 2 in Action ch. 10, 11; HKM D16)
10. Keep Allow token introspection without audience check off and use none of the deprecated compatibility options; give introspecting clients their audience scope. Why: since 26.6.2 introspection is limited to the `aud` client, and refresh tokens to their `azp` client. (Keycloak docs 26.8, p. 11, p. 17)
11. Do not encrypt access tokens unless regulation requires it, and prefer leaving a confidential claim out over encrypting it; where encryption is unavoidable, use authenticated encryption only (AES-GCM), sign before encrypting with separate signing and encryption keys, never use `RSA1_5` key transport or JWE compression, and plan encryption key rotation. Why: encryption adds key distribution, weaker library support and room for mistakes, unauthenticated ciphertext can be altered, weak padding leaks keys, and compressed length can reveal content. (Cloud Native OAuth ch. 7; API Security in Action ch. 6, 7; HKM D54)
12. When a gateway or resource server introspects a token, authenticate it as its own client (`client-jwt`), reject malformed or oversized tokens before calling Keycloak, and treat `active=false` or a response without `sub` or `scope` as a rejection. Why: the presented token is untrusted input, and most fields of an introspection response are optional. (API Security in Action ch. 7; OAuth 2 in Action ch. 11; HKM D12)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm                       = "hkm"
  default_signature_algorithm = "RS256"   # ES256 only after Trino, Kafka, Nessie and ClickHouse confirm support
  access_token_lifespan       = "15m"
}

resource "keycloak_openid_client_scope" "aud_trino" {
  realm_id               = keycloak_realm.hkm.id
  name                   = "aud-trino"
  include_in_token_scope = true
}

resource "keycloak_openid_audience_protocol_mapper" "aud_trino" {
  realm_id                 = keycloak_realm.hkm.id
  client_scope_id          = keycloak_openid_client_scope.aud_trino.id
  name                     = "aud-trino"
  included_custom_audience = "trino"
  add_to_access_token      = true
  add_to_id_token          = false
}

resource "keycloak_openid_client_optional_scopes" "superset" {
  realm_id        = keycloak_realm.hkm.id
  client_id       = keycloak_openid_client.superset.id
  optional_scopes = [keycloak_openid_client_scope.aud_trino.name]
}
```

```http
POST /realms/hkm/protocol/openid-connect/token/introspect
Content-Type: application/x-www-form-urlencoded

token=<access token presented to trino, length and format checked first>
&client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer
&client_assertion=<trino's signed client-jwt assertion>
# trino rejects unless the response has "active": true, a "sub" and a "scope" containing aud-trino
```

### Refresh tokens, revocation and logout

Source: Keycloak 26.8.2 documentation, Server Admin: Managing user sessions, Managing OIDC and SAML clients, Mitigating security threats; Keycloak – IAM for Modern Applications, ch. 4, 12, 14; RFC 9700 §4.14; NIST SP 800-63C-4 §3.12.3, §4.7; OWASP ASVS 5.0, 10.4.8, 10.4.9, 10.5.5, 10.6.2; Cloud Native Data Security with OAuth, ch. 2, 5, 7, 12, 13 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 5, 9, 10, 13; API Security in Action, ch. 6, 7; Identity Attack Vectors, 2nd ed., ch. 14; OAuth 2 in Action, ch. 2, 6, 11, 13 (merged 2026-11-18).

1. Turn Revoke Refresh Token on with Refresh Token Max Reuse 0, and make every client store the refresh token returned by each refresh and discard the previous one. Why: each refresh token becomes single-use, so a replay is detected and forces re-authentication, and rotation, idle limits and key rotation all act on the newest token. (Keycloak IAM ch. 4, 12; API Security in Action ch. 7; HKM D14)
2. Bind refresh tokens to the consented scope and resource servers, bound how long a client can refresh with Client Session Idle and Max (shorter for public clients), and keep an absolute maximum even when idle expiry slides. Why: idle or endless grants are standing attack surface, and lifetime should follow how safely the client stores tokens. (RFC 9700 §4.14.2; OWASP ASVS 5.0, 10.4.8; Keycloak IAM ch. 12, 14)
3. Keep refresh tokens server-side or in OS-protected storage and send them only to Keycloak's token and revocation endpoints, never to a resource server; at local logout, uninstall, disconnection or suspected compromise delete the application session, its cookies and stored tokens and revoke the refresh token (and the access token where supported) at the revocation endpoint. Why: refresh tokens live long, keeping them away from APIs limits the exposure of each token, revoking one also ends the access tokens of its session, and deleting only the cookie leaves tokens that still call APIs. (Cloud Native OAuth ch. 2, 12; Solving IdM ch. 10, 13; API Security in Action ch. 7; OAuth 2 in Action ch. 2, 6, 11)
4. Make clients retry once after a 401 with a refreshed token, refresh only once at a time per process, and send the user to login when refresh returns `invalid_grant`. Why: tokens expire mid-workflow, and parallel refreshes present an already rotated token that Keycloak rejects. (Cloud Native OAuth ch. 5, 12; OAuth 2 in Action ch. 2)
5. Grant `offline_access` only to named clients, remove the optional `offline_access` scope from all others, and run asynchronous jobs as service accounts with client credentials instead of user offline tokens. Why: offline tokens survive logout and ignore SSO Session Idle and Max. (Keycloak docs 26.8, p. 5, p. 13; HKM D16)
6. Enable Offline Session Max Limited, set Offline Session Max, and shorten Client Offline Session Idle and Max for each client that keeps offline access. Why: with the flag disabled, offline sessions expire only by idle time. (Keycloak docs 26.8, p. 5, p. 11)
7. Start logout as RP-initiated logout at `end_session_endpoint` with `id_token_hint` and a `post_logout_redirect_uri` registered exactly on the client, send users afterwards to a confirmation or home page that does not redirect them back to Keycloak, and keep Keycloak's confirmation prompt for requests without a hint. Why: the hint identifies client, user and session, an auto-redirecting landing page logs the user straight back in, and an unauthenticated logout link must not end a user's session. (Keycloak IAM ch. 4; OWASP ASVS 5.0, 10.6.2; Solving IdM ch. 10, 13)
8. Give every server-side client a back-channel logout URL that the Keycloak pods can reach, manage each client session independently of token lifetime (never extend it because a UserInfo call succeeds), and use short sessions and tokens as the baseline propagation. Why: IdP logout does not end client sessions by itself, back-channel delivery needs direct connectivity, front-channel iframes fail under CSP, and after logout the next refresh fails within minutes. (Keycloak docs 26.8, p. 11, p. 14; Keycloak IAM ch. 4, 12; NIST SP 800-63C-4 §3.12.3, §4.7; Solving IdM ch. 13; OAuth 2 in Action ch. 13; HKM D10)
9. Accept a back-channel logout token only after verifying its signature, type `logout+jwt`, the back-channel logout `events` member and the absence of `nonce`, and end all application sessions tied to its session ID on every replica. Why: forged logout tokens are a denial-of-service lever, and Keycloak posts the token to one instance that may not hold the session. (OWASP ASVS 5.0, 10.5.5; Keycloak IAM ch. 4; Solving IdM ch. 13)
10. Do not use the OIDC session-status iframe, `prompt=none` polling or front-channel logout in new applications to learn about logout, and plan migration away where they exist. Why: browsers cut iframes off from cookies, polling adds traffic and interrupts users, and front-channel logout gives no delivery confirmation. (Keycloak IAM ch. 4; Solving IdM ch. 9, 13)
11. Keep a revocation runbook by scope: not-before for realm-wide invalidation during an incident, ending the user or client session (`sid`) for one user or client, and the RFC 7009 revocation endpoint from clients that no longer need their tokens; prefer per-user over realm-wide sign-out, and pair every method with short access tokens. Why: issued JWT access tokens stay valid until expiry, applications do not learn of a not-before change at once, and realm-wide logout ends sessions one by one. (Keycloak IAM ch. 12; Cloud Native OAuth ch. 7, 13; Solving IdM ch. 5; API Security in Action ch. 6; OAuth 2 in Action ch. 11; Identity Attack Vectors ch. 14)
12. Let users see and revoke their granted applications and refresh tokens in the Account Console. Why: a user who suspects theft must be able to kill the grant. (OWASP ASVS 5.0, 10.4.9)

Example (offline lifespan illustrative):

```hcl
resource "keycloak_realm" "hkm" {
  realm                                = "hkm"
  revoke_refresh_token                 = true
  refresh_token_max_reuse              = 0
  offline_session_max_lifespan_enabled = true
  offline_session_max_lifespan         = "720h"
}

resource "keycloak_openid_client" "grafana" {
  realm_id                            = keycloak_realm.hkm.id
  client_id                           = "grafana"
  access_type                         = "CONFIDENTIAL"
  client_authenticator_type           = "client-jwt"
  standard_flow_enabled               = true
  pkce_code_challenge_method          = "S256"
  valid_redirect_uris                 = ["https://grafana.hkm.example/login/generic_oauth"]
  valid_post_logout_redirect_uris     = ["https://grafana.hkm.example/logout/done"]
  backchannel_logout_url              = "https://grafana.hkm.example/backchannel-logout"
  backchannel_logout_session_required = true
  frontchannel_logout_enabled         = false
}
```

### Sender-constrained tokens (DPoP, mTLS)

Source: Keycloak 26.8.2 documentation, Securing Applications: DPoP, Managing OIDC and SAML clients; Keycloak – IAM for Modern Applications, ch. 12; RFC 9700 §2.2; OWASP ASVS 5.0, 10.3.5, 10.4.5, 10.4.14; Cloud Native Data Security with OAuth, ch. 7, 12 (merged 2026-11-17); API Security in Action, ch. 11; OAuth 2 in Action, ch. 15 (merged 2026-11-18).

1. Require DPoP-bound tokens ("Require DPoP bound tokens" or the `dpop-bind-enforcer` executor) for every public client and every ASVS L3 client, always with the authorization code flow, and move confidential clients of other sensitive APIs to DPoP when their libraries support it. Why: rotation does not stop an attacker who uses a leaked token first, a public client has no credential to bind with mTLS, and DPoP rejects implicit and hybrid flows. (Keycloak docs 26.8, p. 11, p. 14; Keycloak IAM ch. 12; RFC 9700 §2.2.1; OWASP ASVS 5.0, 10.3.5, 10.4.14; Cloud Native OAuth ch. 7, 12; OAuth 2 in Action ch. 15; HKM D11, D23)
2. Make every resource server or sidecar that accepts DPoP tokens verify the proof before normal validation (the proof key matches `cnf.jkt`, and `ath`, `htm` and `htu` match the request), and reject any token that carries a confirmation method it does not support. Why: a binding that the receiver does not check, or silently skips, acts as a bearer token. (Keycloak docs 26.8, p. 14; RFC 9700 §2.2.1; Cloud Native OAuth ch. 7; API Security in Action ch. 11; OAuth 2 in Action ch. 15; HKM D52)
3. Exchange a DPoP-bound token only from its own client with a proof from the same key, and give the exchanged token its own binding. Why: Keycloak rejects any other exchange of a bound token. (Keycloak docs 26.8, p. 14; HKM D52)
4. Do not use mTLS client authentication or certificate-bound tokens. Why: the platform binds with DPoP and authenticates clients with `client-jwt`, which FAPI 2 accepts. (OAuth 2 in Action ch. 15; HKM D11, D46)
5. Let every DPoP client generate its own key pair, keep the private key on the client, and send only the public key inside its proofs. Why: a private key that never crosses the network and is never held by the server cannot be stolen from either. (OAuth 2 in Action ch. 15; HKM D11)

Example:

```hcl
resource "keycloak_openid_client" "hkm_cli" {
  realm_id                   = keycloak_realm.hkm.id
  client_id                  = "hkm-cli"
  access_type                = "PUBLIC"
  standard_flow_enabled      = true
  pkce_code_challenge_method = "S256"
  require_dpop_bound_tokens  = true
}
```

### Token exchange and delegation

Source: Keycloak 26.8.2 documentation, Securing Applications: token exchange, JWT authorization grant; Cloud Native Data Security with OAuth, ch. 1, 6 (merged 2026-11-17); API Security in Action, ch. 7, 11; OAuth 2 in Action, ch. 6, 13 (merged 2026-11-18).

1. Before calling another service, exchange the incoming user token for a narrower one (fewer scopes, the single downstream audience) instead of forwarding it; forward unchanged only within one trust boundary. Why: the user stays verifiable down the chain, while a forwarded token makes every downstream service as powerful as the original client. (Cloud Native OAuth ch. 1, 6; API Security in Action ch. 11; OAuth 2 in Action ch. 13; HKM D15)
2. Use standard token exchange V2 and keep the legacy `token-exchange` feature disabled. Why: V2 is supported and on by default, while V1 is a deprecated preview that needs FGAP v1. (Keycloak docs 26.8, p. 14)
3. Turn Standard token exchange on only for confidential requester clients listed in Terraform, such as `superset`, and make sure the requester is in the subject token's `aud`, for example through an `aud-*` scope. Why: public clients cannot exchange, the switch is per requester, and Keycloak rejects other subject tokens. (Keycloak docs 26.8, p. 14)
4. Send one `audience` value per exchange, attach the `downscope-assertion-grant-enforcer` executor to every requester, and let an exchange only narrow scope and audience, never widen them. Why: `audience` only filters, while `scope` can add optional scopes. (Keycloak docs 26.8, p. 11, p. 14; API Security in Action ch. 7; HKM D50)
5. Keep Allow refresh token in Standard Token Exchange at `No` and Allow exchange token expiration after subject token expiration off. Why: an exchanged refresh token lives as long as the user session, and the cap stops lifetime extension. (Keycloak docs 26.8, p. 14; HKM D16)
6. Restrict sensitive scopes in exchanges with client policy conditions. Why: V2 authorizes exchanges through client policies, not FGAP. (Keycloak docs 26.8, p. 14)
7. Allow the JWT authorization grant only to bridge two identity systems in a reviewed cross-domain case: confidential clients, listed identity providers with one provider per issuer, a JWKS URL, a pinned algorithm, assertion re-use off, the Keycloak user linked to the external identity first, and no custom audience mapping. Why: each bridge widens the issuers Keycloak trusts, the grant finds the provider by `iss`, and one-time assertions limit replay. (Keycloak docs 26.8, p. 14; Cloud Native OAuth ch. 6; API Security in Action ch. 11; OAuth 2 in Action ch. 6)

Example:

```hcl
resource "keycloak_openid_client" "superset" {
  # ... as in "Client types and grant selection"
  extra_config = {
    "standard.token.exchange.enabled"                         = "true"
    "standard.token.exchange.enableRefreshRequestedTokenType" = "NO"
  }
}
```

```http
POST /realms/hkm/protocol/openid-connect/token
grant_type=urn:ietf:params:oauth:grant-type:token-exchange
&subject_token=<superset user access token>
&subject_token_type=urn:ietf:params:oauth:token-type:access_token
&audience=trino
&scope=aud-trino
# client authentication: superset, client-jwt assertion
```

### Browser-based and native applications

Source: Keycloak 26.8.2 documentation, Server Admin: Managing OIDC and SAML clients, Securing Applications: DPoP; Keycloak – IAM for Modern Applications, ch. 6; RFC 9700 §2.1, §2.6, §4.2.4, §4.4, §4.7; OWASP ASVS 5.0, 10.1.1, 10.1.2, 10.2.1, 10.2.2; Cloud Native Data Security with OAuth, ch. 3, 7, 12, 13 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 8, 9, 10; API Security in Action, ch. 5, 7; OAuth 2 in Action, ch. 2, 6, 7, 13; Learning Digital Identity, ch. 13 (merged 2026-11-18).

1. Send every user to Keycloak's own login page by redirect; never keep an application login form that trades passwords for tokens and never embed the login page in an iframe. Why: one compromised app would expose credentials everywhere and block MFA and SSO, and iframes lose cookies. (Keycloak IAM ch. 6; Learning Digital Identity ch. 13)
2. Build every web UI (Superset, Grafana, Airflow, CKAN and every new application) as a backend for frontend: a confidential same-domain backend holds access and refresh tokens in its server-side session, builds the session from the ID token, calls APIs for the browser, and gives the browser only a session cookie. Why: tokens that never reach JavaScript cannot be stolen by XSS, and no CORS is needed. (Keycloak docs 26.8, p. 11, p. 14; Keycloak IAM ch. 6; OWASP ASVS 5.0, 10.1.1; Cloud Native OAuth ch. 3, 7, 13; HKM D14)
3. Keep an existing public SPA only for non-critical use, with PKCE, DPoP, refresh rotation, short client session timeouts, OWASP XSS controls and few third-party scripts. Why: a public client's refresh token is otherwise a bearer credential held where it is easiest to steal. (Keycloak IAM ch. 6; Cloud Native OAuth ch. 13; HKM D11, D14)
4. Never keep tokens or sensitive user data in `localStorage`, `sessionStorage`, IndexedDB or cookies set by JavaScript, and do not renew tokens with `prompt=none` in hidden iframes. Why: injected script can read every store page script can and run its own silent flow, client-side data stays on the device after the session ends, and third-party cookie blocking breaks iframes. (Cloud Native OAuth ch. 13; Solving IdM ch. 9, 10; API Security in Action ch. 5; HKM D14)
5. Generate `state`, `nonce` and the PKCE verifier per transaction, bind them to the browser session, invalidate `state` after first use, and sign or session-bind `state` if it carries application data. Why: constant or reusable values give no CSRF or injection protection. (RFC 9700 §2.1, §2.1.1, §4.2.4, §4.7.1; OWASP ASVS 5.0, 10.1.2, 10.2.1; OAuth 2 in Action ch. 7)
6. In a client that can talk to more than one issuer (for example `hkm` and `hkm-public`), store the issuer with each request and abort when the returned `iss` differs. Why: mix-up pairs an honest authorization endpoint with the attacker's token endpoint. (RFC 9700 §2.1, §4.4.2; OWASP ASVS 5.0, 10.2.2; OAuth 2 in Action ch. 7)
7. Prefer the `form_post` response mode, keep callback paths free of third-party content, user-generated pages and onward redirects, send `Referrer-Policy: no-referrer` on callback pages, and check both sender and receiver origin when a response travels by `postMessage`. Why: codes and `state` leak through history and Referer headers, a user-writable page under the callback path receives the code in its Referer, and loose origin checks leak or inject responses. (RFC 9700 §2.6, §4.2.4; OAuth 2 in Action ch. 7)
8. Use pushed authorization requests (PAR) or a signed request object for backend clients where request integrity matters, never longer scope strings, and require PAR for control-plane (ASVS L3) clients. Why: they keep request parameters out of the browser and safe from tampering. (Cloud Native OAuth ch. 13; Solving IdM ch. 8; OAuth 2 in Action ch. 2, 13; HKM D23)
9. Set each client's Web Origins to its exact origin, never `*`, never on the authorization endpoint, and have browser API calls send a static custom header that forces a CORS preflight, combined with SameSite cookies or Origin checks. Why: clients only redirect to the authorization endpoint, and CORS alone does not protect against CSRF. (RFC 9700 §2.6; Cloud Native OAuth ch. 13; API Security in Action ch. 5)
10. Give each web application and each front end its own client and hostname, web and native variants of one tool included; let microfrontends share one only when they form one logical application. Why: cookies, XSS impact, misconfiguration, access rules and logs then stay separate per front end. (Cloud Native OAuth ch. 13; Solving IdM ch. 10)
11. Authenticate native, mobile and CLI apps with code + PKCE through the system browser or an in-app browser tab, never an embedded web view or the password grant. Why: web views can capture credentials and share no SSO cookies. (Keycloak IAM ch. 6; Cloud Native OAuth ch. 12; API Security in Action ch. 7; OAuth 2 in Action ch. 6, 7)
12. Return the code to native apps through a claimed HTTPS redirect where possible, a reverse-domain custom scheme otherwise, and a loopback address bound to `127.0.0.1` or `[::1]` (never all interfaces) for CLIs, with a variable port as the only deviation from exact matching. Why: claimed HTTPS URIs cannot be taken over by another app, and a wildcard address exposes the callback to the network. (Keycloak IAM ch. 6; Cloud Native OAuth ch. 12; API Security in Action ch. 7; OAuth 2 in Action ch. 6, 7; HKM D13)
13. Keep per-user tokens of native tools in OS-protected storage, and sign desktop tools so corporate laptops run only trusted publishers. Why: a rogue application can claim the same loopback port and harvest codes. (Cloud Native OAuth ch. 12; OAuth 2 in Action ch. 6)
14. Enable the device authorization grant only on clients for headless or input-constrained hosts, poll at the returned interval and handle pending and expired codes. Why: the user approves on a second device while the client waits. (Keycloak IAM ch. 6)

Example:

```hcl
resource "keycloak_openid_client" "hkm_cli" {
  realm_id                                  = keycloak_realm.hkm.id
  client_id                                 = "hkm-cli"
  access_type                               = "PUBLIC"
  standard_flow_enabled                     = true
  pkce_code_challenge_method                = "S256"
  oauth2_device_authorization_grant_enabled = true                             # headless jump hosts only
  valid_redirect_uris                       = ["http://127.0.0.1/callback"]    # loopback; variable port per HKM D13
  require_dpop_bound_tokens                 = true
}
```
