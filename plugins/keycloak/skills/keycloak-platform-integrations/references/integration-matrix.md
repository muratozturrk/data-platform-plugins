# Integration matrix

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (RFC 9700, OWASP ASVS 5.0) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action and Türkiye mevzuatı (BG Rehberi), Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections. "Component notes" under each section collect the summaries' integration notes; items marked "to confirm" need the component's own documentation.

## Starter rules

1. Give every component its own client and its own audience, and make each API require its own value in `aud`. Why: a compromised component must not be able to act as another. (plugin v1; OWASP ASVS 5.0, 10.3.1; Keycloak IAM ch. 5, 6; Cloud Native OAuth ch. 5, 6; API Security in Action ch. 6, 7; HKM D15)
2. Use `preferred_username` as the principal across Trino, Kafka, Superset, Airflow and Ranger. Why: one stable name lets one Ranger policy apply on every path. (plugin v1; HKM D1)
3. Decide data access in Ranger even when a component could read roles from the token. Why: one policy decision point per resource keeps audit and revocation consistent. (plugin v1; apache-ranger plugin; Keycloak IAM ch. 8; HKM D2)
4. Authenticate in-cluster workloads with federated Kubernetes service-account tokens sent as a client assertion instead of client secrets. Why: the platform keeps that token short-lived and renewed, so there is no secret to leak or rotate. (research: Keycloak 26.6 release notes; Keycloak IAM ch. 6; Cloud Native OAuth ch. 10; Zero Trust Networks ch. 7; HKM D12)
5. Validate JWTs locally with a cached JWKS refreshed on a schedule, refetch at most once per interval on an unknown key id before rejecting, and alert on bursts of signature failures. Why: no call to Keycloak per request, key rotation stays invisible to APIs, and refetching on every bad signature lets forged tokens become a denial of service. (research: strimzi-kafka-oauth README; Keycloak IAM ch. 5; Cloud Native OAuth ch. 5; Solving IdM ch. 17; API Security in Action ch. 6)

## Rules by topic

### Web applications and single sign-on

Source: Keycloak – IAM for Modern Applications, ch. 4, 6, 7, 12, 14; RFC 9700 §2, §4; OWASP ASVS 5.0, 10.5; Cloud Native Data Security with OAuth, ch. 7, 13 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 9, 10, 13; OAuth 2 in Action, ch. 7, 9, 13; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 6 (merged 2026-11-18).

1. Register Superset, Grafana, Airflow and CKAN (`hkm-public`) as confidential clients with code + PKCE and exact redirect URIs that keep tokens in their server session and act as their own backend for frontend, each on its own hostname; register OpenMetadata in its server-side mode where the deployed version supports it. Why: these applications already render or proxy on the server, so no token needs to reach the browser. (Keycloak IAM ch. 6; Cloud Native OAuth ch. 7, 13; HKM D14)
2. Keep each application's web session no longer than the SSO session, and register back-channel logout whose handler reaches the session on every replica. Why: Keycloak delivers a logout token to one replica only, and its not-before push does not reach these applications. (Keycloak IAM ch. 4, 12; Keycloak docs 26.8, p. 5; Solving IdM ch. 9, 13; Zero Trust Networks ch. 6; HKM D10)
3. Check for each web application that its OIDC library invalidates `state`, validates `nonce`, `iss` and `aud`, and handles back-channel logout tokens as in oauth-oidc-profile; record "unsupported" where it does not. Why: these checks run in the application, not in Keycloak. (RFC 9700 §4.2.4, §4.4; OWASP ASVS 5.0, 10.5.1–10.5.5; OAuth 2 in Action ch. 7, 13)
4. Record at client registration whether a component is integrated embedded (its own OIDC support) or proxied, and choose proxied only when the code cannot change or the component already sits behind a gateway such as WSO2. Why: embedded changes need a redeploy, while a proxy is one place to manage the integration. (Keycloak IAM ch. 7)
5. Use the OIDC support a component already ships before adding a library, wrap it in a thin identity layer (login, logout, tokens, profile), and accept a library or proxy module only if it is widely used, maintained, current with the specifications and preferably OpenID certified; never hand-write OIDC client logic or use Keycloak-specific adapters. Why: maintained libraries track specification and security changes that home-made code misses, and the thin layer isolates library changes. (Keycloak IAM ch. 7; Solving IdM ch. 10; OAuth 2 in Action ch. 13)
6. Keep XSS, injection and session protection in every web application even behind a WAF. Why: Keycloak and a gateway WAF do not protect the application's own code. (Keycloak IAM ch. 14)
7. Warn users before an application idle timeout and let them extend the session, or keep unsaved work locally and restore it after the next login. Why: losing half-entered work makes users resist sensible timeouts. (Solving IdM ch. 9)
8. If an application keeps state across a timeout to restore it after reauthentication, delete it on explicit logout and cap how long it is kept. Why: state orphaned by deleted cookies would otherwise be stored forever. (Solving IdM ch. 9)
9. Serve anonymous users (for example public CKAN datasets) without any token, and never treat a placeholder bearer value as an identity. Why: anonymous requests are untrusted and their content cannot later be tied to a user. (Solving IdM ch. 10)

Component notes:

- `airflow`: the only client with Authorization Services and client roles deciding application permissions; its group policies take realm groups only; jobs and long DAG runs use `svc-*` service accounts with client credentials, not user offline tokens or long UI sessions; DAG ownership delegation is logged with both identities; no local users except break-glass. (Keycloak docs 26.8, p. 2, p. 5, p. 11, p. 17; Cloud Native OAuth ch. 6; Solving IdM ch. 8, 9, 18; API Security in Action ch. 8; BG Rehberi 3.2.1.2; HKM D2, D16)
- `superset`: confidential, `client-jwt`, back-channel logout URL; local database login disabled, OIDC only; exchanges user tokens with `audience=trino` when calling Trino; requests the organization scope when it reads the tenant; functional access through roles mapped from groups; keeps its own Flask session after login, whose lifetime and end with the Keycloak session must be documented. Superset reads `groups` from UserInfo, not from the token, so the flat-name group mapper must also add to UserInfo; `AUTH_ROLES_MAPPING` maps the names to roles and `AUTH_ROLES_SYNC_AT_LOGIN` re-applies them at every login. `acr_values` can probably be passed through the OAuth `authorize_params` (trial). (Keycloak docs 26.8, p. 2, p. 7, p. 11, p. 14; Solving IdM ch. 8, 9, 13; BG Rehberi 3.2.1.13)
- `ckan`: confidential client in `hkm-public`; UserInfo claims configured on the `ckan` client; users from `hkm-public` self-registration or e-Devlet, never social login, with account linking and unlinking for e-Devlet, self-service closure and a public status page reachable without login; anonymous datasets need no token; share links are issued only after a Ranger allow, for one object and expiring; if it accepts both realms it needs the issuer-bound request pattern. (Keycloak IAM ch. 4, 10; RFC 9700 §4.4.2; Solving IdM ch. 4, 10, 17, 18; API Security in Action ch. 9; BG Rehberi 3.2.1.13; HKM D35)
- `superset`, `grafana`, `trino`, `airflow`, `ckan`: none of Superset, Airflow, Grafana, Trino or OpenMetadata documents OIDC back-channel logout, and Airflow, Grafana and Trino cannot send `acr_values`, so require the level for the whole client and keep each web session short; pin Airflow at 3.2.2 or later (its `_token` cookie fixes); use the file forms for client secrets where a secret is still an exception (Grafana `$__file{}`, Airflow `CLIENT_SECRET_CMD`), since only OpenMetadata lists `private_key_jwt`. CKAN not checked. (Solving IdM ch. 12, 13; research: component documentation (Superset/Flask-AppBuilder, Airflow Keycloak provider 0.12.0, Grafana, Trino 484, OpenMetadata, Strimzi 1.3.1, Apache Ranger, OpenBao 2.7.0), 2026-11-18)
- `superset`, `airflow`, `grafana`: at logout revoke their tokens and end the Keycloak session; never forward their ID or access tokens unchanged to other services. (API Security in Action ch. 6, 7)
- `superset`, `grafana`, `airflow`, `ckan`: user rows created at login must be keyed on an immutable claim and removed or anonymised by the leaver procedure, since deleting the Keycloak user does not reach them; roles refresh only at the next login or token refresh. (Solving IdM ch. 14, 15)
- `openmetadata`: to confirm whether the deployed version keeps tokens server-side.
- `grafana`: shows Keycloak metrics and security alarms; its own login session can refresh tokens, so set and document its lifetime against the `hkm` SSO session, with a short application idle on shared workstations. (Keycloak docs 26.8, p. 16; Cloud Native OAuth ch. 11; Solving IdM ch. 9, 18)
- `superset`: one exact callback per environment, no context-path wildcard; login only from a validated ID token with `nonce`; CORS on API paths only; requests only the scopes for its own API calls and receives `groups` and `preferred_username`, no email or profile attributes unless a feature needs them. To confirm: whether its library checks `azp`. (OAuth 2 in Action ch. 7, 9, 13; Learning Digital Identity ch. 8, 11, 13)
- `ckan`: exact `hkm-public` callbacks kept apart from any user-editable page; public API links never carry tokens in URLs; browsing open data needs no account, and login is needed only where an action requires accountability. (OAuth 2 in Action ch. 7, 8, 9; Learning Digital Identity ch. 8, 11)
- `airflow`: one redirect entry per deployment hostname, never a subdomain wildcard (the host-limited `<base_url>/*` exception of oauth Client types rule 12 stays); workers keep `svc-*` tokens in memory and out of task logs. To confirm: `nonce` and `iss` handling of its library. (OAuth 2 in Action ch. 9, 10, 13; Learning Digital Identity ch. 12)
- `grafana`, `superset`, `airflow`: to confirm `nonce` and `iss` handling; session-lifetime notes above also follow Zero Trust Networks ch. 6. (OAuth 2 in Action ch. 7, 13; Zero Trust Networks ch. 6)

Example:

```ini
# grafana.ini: built-in OIDC support
[auth.generic_oauth]
enabled = true
client_id = grafana
use_pkce = true
# endpoint URLs generated from the realm discovery document at deploy time
auth_url = https://sso.hkm.example/realms/hkm/protocol/openid-connect/auth
scopes = openid hkm-claims
role_attribute_path = contains(groups[*], 'platform_t-acme_analyst') && 'Viewer'
```

### APIs, gateways and resource servers

Source: Keycloak – IAM for Modern Applications, ch. 5, 6, 12, 14; OWASP ASVS 5.0, 9.1–9.2, 10.3; Cloud Native Data Security with OAuth, ch. 1, 2, 3, 5, 6, 7 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 8; API Security in Action, ch. 3, 5, 6, 11; OAuth 2 in Action, ch. 2, 7, 8, 10, 11, 13; Zero Trust Networks, 2nd ed., ch. 4, 9 (merged 2026-11-18).

1. Accept access tokens only as signed JWTs in the `Authorization` header, never in URLs, and serve every API only over TLS with HSTS and no plain-HTTP listener beside it. Why: URLs end up in logs and history, a signature protects integrity but not confidentiality, and a bearer token relies entirely on TLS because OAuth 2.0 has no message-level cryptography of its own. (Cloud Native OAuth ch. 2, 5, 7; OAuth 2 in Action ch. 7, 8, 10)
2. Configure each API at deploy time with a fixed JWKS URI, an allow-list holding only the realm issuer and a fixed list of accepted algorithms; reject any token without a valid signature and any other `alg`, including `none` in any letter case, and never take keys from token headers (`jku`, `x5u`, `x5c`, `jwk`). Why: algorithm substitution and attacker-supplied keys are the standard signature bypasses, and fetching a key URL named in a token also opens SSRF. (Cloud Native OAuth ch. 1, 5; OWASP ASVS 5.0, 9.1.1–9.1.3; API Security in Action ch. 6; OAuth 2 in Action ch. 11)
3. Validate on every request with a mainstream library: signature, `iss`, its own `aud`, token type and `exp`/`nbf` against current UTC, the DPoP proof where DPoP is required, then the scope the endpoint requires. Why: a correctly signed token can still be meant for another API, expired, or issued to a client acting outside its remit, and one unchecked resource server makes every token replayable there. (Cloud Native OAuth ch. 3, 5, 6; OWASP ASVS 5.0, 9.2.1–9.2.4, 10.3.1, 10.3.5; Solving IdM ch. 8; OAuth 2 in Action ch. 2, 10, 11; Zero Trust Networks ch. 9)
4. Validate locally for high request volumes (Trino, Nessie, ClickHouse), and use introspection or very short access tokens for critical operations on Trino and Ranger Admin. Why: local validation scales but does not see logout, which matters most for privileged changes. (Keycloak IAM ch. 5, 12; HKM D16)
5. Return 401 with a Bearer `invalid_token` challenge only for a missing, invalid or expired token, 403 `insufficient_scope` for a missing scope, 403 for missing required claims or a known but unauthorised caller, and 404 for resources the caller may not see; tell the caller nothing more than the error and write the exact rejection reason to an admin-only log. Why: clients treat 401 as the signal to reauthenticate, different codes would let callers probe which resources exist, and detailed validation errors act as an oracle for tampering. (Cloud Native OAuth ch. 5; API Security in Action ch. 3, 5, 6)
6. Keep fine-grained rules in one deny-by-default, unit-testable authorizer; for data engines that authorizer is the Ranger plugin. Why: rules change often, and one place keeps them auditable and testable. (Cloud Native OAuth ch. 5; HKM D2)
7. Enable CORS only on API paths that browser clients call from another origin, never on login, form or other user-facing pages, and return CORS headers only for an exact list of registered origins: echo the matched origin with `Vary: Origin`, refuse other preflights with 403, never combine credentials with a wildcard, and answer preflight requests before authentication; offer no JSONP endpoint. Why: CORS relaxes the same-origin policy, which a token-protected API can afford but an interactive page cannot, a wildcard or blindly reflected origin opens the API to any site, preflights carry no credentials, and JSONP runs data as script outside the same-origin policy. (Keycloak IAM ch. 6; API Security in Action ch. 5; OAuth 2 in Action ch. 8)
8. Test API security with a mock JWKS and locally signed tokens shaped exactly like `hkm` tokens, without any test bypass in the validation code, plus end-to-end tests against a test realm. Why: every claim combination is covered quickly while the real issuer path is still checked. (Cloud Native OAuth ch. 5)
9. Make every API that serves both users and service accounts (for example Trino called by `superset` users and by `svc-spark-etl`) tell their tokens apart from a claim documented in the `hkm-claims` contract, and apply the rules for the right kind of caller. Why: OAuth defines no single marker for service tokens, so without an agreed claim one kind of caller can pose as the other. (API Security in Action ch. 11)
10. Encode every untrusted value that an API echoes back, error messages included, return the exact media type of the payload (`application/json` for JSON, never `text/html`) on success and on error, send `X-Content-Type-Options: nosniff` on every API response, and add a Content-Security-Policy to any response a browser renders; keep output encoding in place behind the JSON content type. Why: one reflected parameter in an error string is enough for a working XSS payload, and the content type and `nosniff` are only second layers that a client pasting the value into HTML would bypass. (OAuth 2 in Action ch. 8)

Component notes:

- `trino`: validates the JWT itself (RS256 or ES256 only, `kid` from the realm JWKS, issuer, audience `trino`, principal `preferred_username`), then the Ranger plugin decides data access; accepts exchanged tokens from Superset, never a multi-audience token, authorizes the user and logs the user's `sub` with the calling client as actor; uses introspection only where immediate revocation matters; stateless per request, so stale identity lasts only for the access token lifetime, and CLI and JDBC users hold tokens, not browser sessions. To confirm: JWKS refetch on unknown `kid`. (Keycloak docs 26.8, p. 11, p. 14; Cloud Native OAuth ch. 5; OWASP ASVS 5.0, 10.3.1; Solving IdM ch. 8, 9, 13, 14; API Security in Action ch. 6, 7, 11; HKM D1, D16)
- `nessie`, `clickhouse`, `wso2`, `opensearch`: own audience, local JWT validation reviewed for issuer, audience and algorithm against oauth-oidc-profile, Access token design rule 6; setting names to confirm per product. (Keycloak IAM ch. 5; API Security in Action ch. 6, 7)
- `trino`, `superset`, `grafana`, `airflow`, `wso2`: cache the realm JWKS with scheduled refresh and a limited forced refresh on unknown `kid` (starter rule 5). (Solving IdM ch. 17)
- `ranger`: policy administration, decision and decision audit in one place; engine plugins enforce from a cached policy copy and return row filters and masks with an allow; usersync pulls users and flat `platform_t-acme_*` groups from Keycloak, so organization group paths or LDAP mapper output must never be used as Ranger names unless they render flat; attributes used in policies must be read-only for users; the plugin never queries the directory at decision time, and scopes carry no data rights; groups go stale between usersync runs (interval to confirm), so leavers must be removed from synced groups; data-level delegation and temporary access are Ranger policies; its policies are part of the authorisation matrix and the yearly review, and Ranger audit is the access record for critical data. (Keycloak docs 26.8, p. 2, p. 3, p. 13; Cloud Native OAuth ch. 9; Solving IdM ch. 8, 14, 15, 18; API Security in Action ch. 7, 8; BG Rehberi 3.1.12.20, 3.2.3.1, 3.2.3.2; HKM D2, D3, D4, D51)
- `opensearch`: receives Keycloak events as JSON, structured logs and Ranger audit so they can be joined, and hosts the `LOGIN_ERROR`, lockout and workflow-failure alerts; it also hosts alerts for admin account creation, deletion and login, role and group changes, login anomalies and event delivery gaps; it must never contain bearer tokens and logs `sub` and `preferred_username`, never the email address, as the user key; Keycloak events with personal data fall under the retention and destruction policy; tenant-scoped access to identity logs needs an organization field and per-tenant filtering (to confirm). immudb gets event id, time, actor `sub` and hash only. (Keycloak docs 26.8, p. 9, p. 12, p. 13; Cloud Native OAuth ch. 7, 9; Solving IdM ch. 4, 15, 18; BG Rehberi 3.1.8.6, 3.1.12.11, 3.1.12.18; HKM D19)
- `grafana`, `opensearch`, `kubernetes`: keep an individual break-glass login that works without Keycloak. (Solving IdM ch. 17)
- `trino`: bearer tokens only on the HTTPS listener and only from the `Authorization` header; its web UI login is an OIDC relying party separate from bearer API access; trusts only `hkm` tokens, never institution IdP tokens; the Ranger plugin in the coordinator enforces and Trino Gateway only routes; a query is authorized when it starts. To confirm: whether it also accepts a token query parameter. (OAuth 2 in Action ch. 7, 8, 10, 13; Learning Digital Identity ch. 12, 13; Identity Attack Vectors ch. 19; Zero Trust Networks ch. 4, 10)
- `nessie`: CORS only if a browser UI calls it from another origin, and only for that origin. To confirm: whether it accepts a token query parameter. (OAuth 2 in Action ch. 7, 8)
- `ranger` (additions): complex entitlements (table, column, masking) and privileged data access stay in Ranger policies written by data owners against `platform_{tenant}_{role}` groups; it is the most sensitive control-plane system, so policy changes need two reviewers and alert on every change; the plugin policy refresh interval (`ranger.plugin.<svc>.policy.pollIntervalMs`, 30 s by default) plus the usersync cycle (60 s, LDAP 1 h by default) bound how fast a revoked permission stops applying; check both defaults against the 2.7/2.8 tags. (Identity Attack Vectors ch. 8, 13; Learning Digital Identity ch. 12; Zero Trust Networks ch. 4, 10; HKM D17, D51; research: component documentation (Superset/Flask-AppBuilder, Airflow Keycloak provider 0.12.0, Grafana, Trino 484, OpenMetadata, Strimzi 1.3.1, Apache Ranger, OpenBao 2.7.0), 2026-11-18)
- `opensearch` (additions): detections for `master` logins, admin role grants, rogue IdP and federation changes, spraying, credential and factor churn, dormant-account returns and impossible travel, with baselines per account class and one view joining Keycloak, Ranger and OpenBao audit; ingest pipelines mask `Authorization` headers and token parameters; read access to `LOGIN` and `LOGIN_ERROR` indices limited to a named security role and logged, tenant dashboards built from aggregates. (Identity Attack Vectors ch. 8, 10, 13, 15, 19; OAuth 2 in Action ch. 10; Learning Digital Identity ch. 8, 11, 21; Zero Trust Networks ch. 4, 10; HKM D19, D20)

Example:

```yaml
# Strimzi Kafka listener validating hkm tokens locally
authentication:
  type: oauth
  validIssuerUri: https://sso.hkm.example/realms/hkm
  jwksEndpointUri: https://sso.hkm.example/realms/hkm/protocol/openid-connect/certs
  checkAudience: true
  clientId: kafka
  userNameClaim: preferred_username
  groupsClaim: "$.groups"
```

### Service-to-service and workload identity

Source: Keycloak docs 26.8 (integration notes, p. 5, p. 8, p. 11, p. 16); Keycloak – IAM for Modern Applications, ch. 6; Cloud Native Data Security with OAuth, ch. 6, 10 (merged 2026-11-17); OAuth 2 in Action (Richer, Sanso, 2017), ch. 7, 10; Zero Trust Networks, 2nd ed., ch. 8 (merged 2026-11-18) (merged 2026-11-18).

1. Run each workload under its own named Kubernetes service account, never the namespace `default`. Why: the service account is the workload's identity and must be unique to it. (Cloud Native OAuth ch. 10; Keycloak docs 26.8, p. 8)
2. Accept a service-account token as client assertion only when `iss` is the cluster issuer, `sub` is the one service account mapped to that client, and `aud` is the realm issuer URL rather than the internal token endpoint URL. Why: the workload identifier differs from the client id, and workloads often call Keycloak through an internal URL that differs from its issuer. (Cloud Native OAuth ch. 10)
3. Let service-account clients fetch a new token at expiry instead of refreshing or checking `sid`. Why: service-account logins use transient sessions. (Keycloak docs 26.8, p. 5)
4. Protect non-HTTP services with the same bearer tokens where the protocol supports it (SASL for Kafka, gRPC metadata, WebSocket handshake). Why: one token model then covers every service type and one set of audience rules applies. (Keycloak IAM ch. 6)
5. Restrict with mesh mTLS or NetworkPolicy which workloads may call each sensitive service, and let Ranger decide what a service account such as `svc-spark-etl` may read; a call arriving over mTLS from a trusted pod grants nothing. Why: infrastructure restriction limits what a compromised workload can reach, and workload identity carries no business permission. (Cloud Native OAuth ch. 10)
6. Keep access tokens of workloads, jobs and resource servers in process memory for as long as they are needed, and never write them to shared volumes, job or task logs or a database. Why: a leaked log, volume or database then yields no usable tokens. (OAuth 2 in Action ch. 10)
7. Authenticate both ends of every connection to a non-public service: the server with a certificate from the private CA, and the caller with mesh mTLS between platform services or with `client-jwt` or `federated-jwt` towards Keycloak, never with mTLS client authentication to Keycloak; keep server-only TLS for endpoints published to the general public, such as `sso.<domain>` for browsers. Why: server-only TLS proves nothing about the caller, and Keycloak clients authenticate by signed JWT on this platform. (Zero Trust Networks ch. 8; HKM D12, D46)

Component notes:

- `spark`: `svc-spark-etl` authenticates with a projected Kubernetes token through Signed JWT - Federated. To confirm: whether this counts as replay-resistant client authentication under ASVS 10.4.16. (Keycloak docs 26.8, p. 8, p. 11; Keycloak IAM ch. 6)
- `airflow`: tasks use their own service account through federated client authentication, never a user token captured at trigger time. (Cloud Native OAuth ch. 6, 10; HKM D16)
- `openbao`: holds database, TLS, SMTP, IdP and bootstrap-admin secrets, client private keys, signing keys and break-glass credentials, synced into the Keycloak namespace at deploy time, and keeps the inventory of keys and secrets with their rotated material; pods authenticate to it with short-lived Kubernetes credentials, and confidential clients have no client secrets. (Keycloak docs 26.8, p. 16; Cloud Native OAuth ch. 11; Solving IdM ch. 17; API Security in Action ch. 11; BG Rehberi 3.1.12.21, 3.2.1.7; HKM D12, D32, D33)
- `kubernetes`: logging-listener levels and LDAP pool settings are server options in the CR; the DNS-rebinding defence is a cluster resolver setting; a ServiceMonitor scrapes `/metrics` on port 9000; workloads authenticate with `federated-jwt` projected tokens (never shared secrets or a custom bootstrap-JWT controller) and request new tokens instead of refreshing; back-channel logout URIs must be reachable from the Keycloak pods; NetworkPolicy isolates components that hold signing keys; CLIs are public clients with a loopback redirect. (Keycloak docs 26.8, p. 3, p. 12, p. 13, p. 16; Solving IdM ch. 9, 13; API Security in Action ch. 7, 11; BG Rehberi 3.1.12.17; HKM D12)
- object storage: presigned URLs follow the share-link rules (claims-contract-and-delegation, Capability links); secure deletion and key handling for tenant data to confirm against the storage design. (Solving IdM ch. 15; API Security in Action ch. 9)
- `openbao` (additions): holds the emergency account credentials, as split material where possible; checkout, two-person release (OpenBao 2.7+ control groups, to be tried on KV v2 paths; approvers are not notified) and rotation audit come from OpenBao, not Keycloak, and its audit log is shipped to OpenSearch; it also holds confidential values kept out of policy text and ADRs. (Identity Attack Vectors ch. 8, 13, 19; Learning Digital Identity ch. 21; Zero Trust Networks ch. 6; HKM D65; research: component documentation (Superset/Flask-AppBuilder, Airflow Keycloak provider 0.12.0, Grafana, Trino 484, OpenMetadata, Strimzi 1.3.1, Apache Ranger, OpenBao 2.7.0), 2026-11-18)
- `kubernetes` (additions): never bake client keys or secrets into images or ConfigMaps; workloads authenticate to Keycloak as clients, never through institution federation; Keycloak runs in its own namespace with restricted admin access and encrypted volumes. (OAuth 2 in Action ch. 7; Learning Digital Identity ch. 13; Identity Attack Vectors ch. 6, 19; Zero Trust Networks ch. 10)

Example:

```yaml
apiVersion: v1
kind: Pod
metadata: { name: spark-etl-driver, namespace: t-acme-jobs }
spec:
  serviceAccountName: svc-spark-etl
  containers:
    - name: driver
      image: registry.hkm.example/hkm/spark:3.5
      volumeMounts: [{ name: kc-token, mountPath: /var/run/secrets/hkm, readOnly: true }]
  volumes:
    - name: kc-token
      projected:
        sources:
          - serviceAccountToken:
              path: token
              audience: https://sso.hkm.example/realms/hkm
              expirationSeconds: 600
# Keycloak client svc-spark-etl: federated JWT client authentication bound to
# system:serviceaccount:t-acme-jobs:svc-spark-etl
```

### Streaming and messaging (Kafka)

Source: Keycloak – IAM for Modern Applications, ch. 6; Cloud Native Data Security with OAuth, ch. 6 (merged 2026-11-17); OAuth 2 in Action, ch. 8, 10; Zero Trust Networks, 2nd ed., ch. 4, 10 (merged 2026-11-18).

1. Authenticate Kafka clients with SASL OAUTHBEARER using client-credentials tokens of their own `svc-*` client, never shared team passwords. Why: per-workload tokens give a named principal for Ranger and can be revoked individually. (Keycloak IAM ch. 6)
2. Never place a user's access token in a message, and carry user identity in a message only as data. Why: messages outlive short tokens, no client is present to retry, and stored tokens can be replayed. (Cloud Native OAuth ch. 6; HKM D16)

Component notes:

- `kafka`: Strimzi OAuth listeners validate locally with an audience check (example under APIs); topics that hold tokens would need credential-grade ACLs; brokers follow the same validation order as other APIs. To confirm: JWKS refresh intervals and Strimzi option names. (Cloud Native OAuth ch. 6; API Security in Action ch. 6)
- `kafka` (additions): OAUTHBEARER listeners only over SASL_SSL; brokers reject expired tokens and check the audience; the Ranger plugin in the brokers enforces by the token principal; client connections can live for days and Kafka never re-authenticates them by default (`connections.max.reauth.ms` = 0), so set re-authentication to the access token lifetime; Strimzi deprecated the `type: oauth` listener in 0.49 in favour of `type: custom`, where the value goes in the listener config in milliseconds. (OAuth 2 in Action ch. 8, 10; Zero Trust Networks ch. 4, 10; research: component documentation (Superset/Flask-AppBuilder, Airflow Keycloak provider 0.12.0, Grafana, Trino 484, OpenMetadata, Strimzi 1.3.1, Apache Ranger, OpenBao 2.7.0), 2026-11-18)

### Proxies, gateways and sidecars

Source: Keycloak – IAM for Modern Applications, ch. 5, 7, 14; Cloud Native Data Security with OAuth, ch. 3, 7, 8, 10 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 8; API Security in Action, ch. 3, 10, 11; OAuth 2 in Action, ch. 8, 10, 11; Zero Trust Networks, 2nd ed., ch. 8, 9 (merged 2026-11-18).

1. Expose services only through the gateway or ingress with no route around it, and let it terminate TLS, rate-limit before any authentication work (answering 429 with `Retry-After`), log every request, reject requests without a valid token, re-encrypt onward to the backends and absorb client differences (cookies, CORS) so services always receive the same JWT. Why: one entry point gives every service the same controls and one validation path, an enforcement point that can be bypassed protects nothing, and authentication is too expensive to run on abusive traffic. (Cloud Native OAuth ch. 3, 8; Solving IdM ch. 8; API Security in Action ch. 3, 10; OAuth 2 in Action ch. 10; HKM D21)
2. Validate the JWT and authorize in every API even behind the gateway and inside the mesh, and treat gateway issuer and audience checks as a prefilter; a sidecar may validate only if it checks issuer, audience, algorithm list and `cnf`, and the service still authorizes. Why: zero trust verifies internal traffic too, and a gateway that accepts any realm token turns every token into a key for every API. (Cloud Native OAuth ch. 3, 8, 10; Keycloak IAM ch. 5, 14; API Security in Action ch. 10; HKM D52)
3. Cache introspection results keyed by a token hash for no longer than the token's `exp`, and never forward the introspection JSON as a credential. Why: the cache protects Keycloak, and unsigned JSON proves nothing downstream. (Cloud Native OAuth ch. 7, 8; OAuth 2 in Action ch. 10, 11)
4. Allow introspection only to the gateway client and to named services for critical operations (Trino, Ranger Admin), each with its own audience scope. Why: introspection reflects logout where it matters, while every extra introspecting client adds load on Keycloak and since 26.6.2 introspection is limited to the `aud` client. (Cloud Native OAuth ch. 7, 8; Keycloak IAM ch. 5, 12; Keycloak docs 26.8, p. 11; HKM D16; decision 2026-11-17)
5. Answer invalid tokens on API routes with 401 and never attach a redirect-to-login step to them. Why: API clients cannot follow a login redirect. (Cloud Native OAuth ch. 8)
6. Use sidecars for TLS, retries and caller restrictions, and keep the original access token available to the service instead of replacing it with forwarded claims. Why: a service that calls upstream APIs needs the token for token exchange. (Cloud Native OAuth ch. 8, 10; Zero Trust Networks ch. 8)
7. Give each authenticating proxy its own confidential client named after the component it protects (for example `proxy-ckan`). Why: a shared proxy client would blur audit and revocation. (Keycloak IAM ch. 7)
8. Make the ingress, the mesh and every authenticating proxy remove client-certificate forwarding headers (such as `ssl-client-verify` or `x-forwarded-client-cert`) and user identity headers from every inbound request and set them only from their own TLS verification or completed login, and let backends trust them only with a successful verification status and only over a mutually authenticated channel from the proxy. Why: a caller that sets these headers itself bypasses certificate and login checks, and identity headers that pass through unchanged can be replayed or spoofed. (API Security in Action ch. 11; Zero Trust Networks ch. 9)
9. Run an authenticating proxy (for example oauth2-proxy in front of a component without OIDC support, with its own `proxy-<component>` client) and any TLS-terminating proxy as a sidecar in the backend's own pod, make the backend reachable only through it, and manage the proxies' certificates and trust anchors centrally. Why: a network hop between proxy and backend stays exposed, and TLS stacks scattered across applications are hard to patch. (Zero Trust Networks ch. 8, 9)

Component notes:

- `wso2`: gateway for published APIs with coarse token checks and CORS; it must enforce each API's audience and scope, and backends still validate; its WAF does not replace API validation. It is the enforcement point for application-level access (audience and scope) and for rate limits where context-aware access needs them, never for data decisions; the phantom-token pattern is not adopted. To confirm: validation mode (whether revoked sessions are noticed, since Keycloak logout does not end its tokens) and DPoP proof checking. (Keycloak docs 26.8, p. 11, p. 14; Keycloak IAM ch. 5, 14; Cloud Native OAuth ch. 8; Solving IdM ch. 8, 13; API Security in Action ch. 8, 11; BG Rehberi 3.2.3.4; HKM D16)
- `kubernetes` ingress: forwards only public Keycloak paths, must not strip Keycloak security headers or inject third-party content; stateless application replicas need a shared session store or revoked-session list for back-channel logout. (RFC 9700 §4.2.4; Keycloak IAM ch. 4, 9; Cloud Native OAuth ch. 11)
- `wso2` (additions): the natural place for CORS, `nosniff`, HSTS, the header-only token rule, rate limiting in front of public endpoints and `Authorization` header masking for the APIs it fronts; its developer portal creates no Keycloak clients, which come from Terraform. To confirm: per-API header policies and whether it accepts a token query parameter. (OAuth 2 in Action ch. 7, 8, 9, 10; Zero Trust Networks ch. 10; HKM D17)
- `oauth2-proxy`: sidecar per legacy UI with its own confidential client, backend reachable only through it, identity headers stripped on entry (rule 9). (Zero Trust Networks ch. 9)

Example:

```yaml
# Intent for the central ingress in front of sso.hkm.example and the platform APIs (product syntax to confirm)
edge:
  tls: terminate-and-reencrypt                                       # HKM D21
  rateLimit: { perClientIp: 50/s, status: 429, retryAfter: true }    # illustrative value
  accessLog: json-to-opensearch                                      # Authorization header masked
  requestHeaders:
    remove: [ssl-client-verify, ssl-client-cert, x-forwarded-client-cert]   # never trusted from callers
```

```yaml
# Intent for a legacy UI without OIDC support (product syntax to confirm)
pod: legacy-report-ui
containers:
  - name: auth-proxy                  # sidecar, same pod as the backend
    client: proxy-legacy-report-ui    # own confidential client in realm hkm, client-jwt
    issuer: https://sso.hkm.example/realms/hkm
    inbound_headers_removed: [x-forwarded-user, x-forwarded-email, authorization]
    upstream: http://127.0.0.1:8080   # backend listens on localhost only
  - name: app
    listen: 127.0.0.1:8080            # not reachable from outside the pod
```
