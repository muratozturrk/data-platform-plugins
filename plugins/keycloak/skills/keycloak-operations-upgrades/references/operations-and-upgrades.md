# Operations and upgrades

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (RFC 9700, CISA/NSA ESF IAM) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action and Türkiye mevzuatı (BG Rehberi), Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Rotate realm signing and encryption keys on a dated schedule by overlap: add the new key with higher priority, keep the old key passive and published until every token it signed has expired and every JWKS cache has refreshed, then delete it. Why: rotation limits what one key protects, and abrupt removal breaks every resource server at once. (plugin v1; Keycloak IAM ch. 14; Cloud Native OAuth ch. 5; Identity Attack Vectors App. A; Learning Digital Identity ch. 9, 14; HKM D33)
2. Rehearse every minor upgrade on a copy of production data, promote the same image through test and staging before production, and plan downtime: use a recreate update for minor upgrades and for migrations that cannot run with mixed versions (26.6 and 26.8.2). Why: these upgrades require a full shutdown and change behaviour (26.8: group claims, admin API scope, IdP mappers), and earlier stages test the release first. (research: Keycloak 26.8 release notes and upgrading guide; Keycloak docs 26.8, p. 16, p. 17; Cloud Native OAuth ch. 11)
3. Alert on administrative changes that do not come from the automation client. Why: console changes outside change control are either break-glass or compromise, and attackers add accounts for persistence. (plugin v1; BG Rehberi 3.1.12.18; CISA/NSA ESF IAM, Identity Governance: Why It Matters; Identity Attack Vectors ch. 10, 12; HKM D17)
4. Restore backups into an isolated environment on a schedule, keep offline backups of the image, providers, themes, keys and configuration, and include Keycloak in DR drills. Why: an untested backup is not a recovery plan, and a site outage hits the authorization server too. (plugin v1; Cloud Native OAuth ch. 11)
5. Track Keycloak security advisories and patch Keycloak and its MFA components within a fixed window. Why: patch releases carry many security fixes. (research: Keycloak 26.8.2 release, 33 CVEs; Keycloak IAM ch. 14; CISA/NSA ESF IAM, Implementing Best Practice; Solving IdM ch. 19)

## Rules by topic

### Monitoring and incident signals

Source: Keycloak 26.8.2 documentation, Server Admin: Managing user sessions, Mitigating security threats; Cloud Native Data Security with OAuth, ch. 3, 11 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 11, 17; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 14; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 10 (merged 2026-11-18).

1. On a leaked token, secret or signing key: sign out the affected sessions, set the not-before revocation to now and push it, rotate realm keys if signing is suspect, disable the compromised client or user, and wait out access token lifetimes for clients that do not receive the push; the security team may end sessions without approval, but disabling an account needs a second person. Why: not-before invalidates every earlier token, the push reaches only adapter clients, and sign-out alone leaves issued access tokens valid. (Keycloak docs 26.8, p. 5, p. 13; Solving IdM ch. 17; Identity Attack Vectors ch. 14; HKM D49)
2. Raise alarms, not restarts, for dependency failures such as the database or an institution IdP, using an exposed error-rate metric. Why: new pods cannot fix a failed dependency. (Cloud Native OAuth ch. 11)
3. Propagate a trace id end to end, show users an error code, time and correlation id, and make every component's logs filterable by that id. Why: support finds the failing component without exposing sensitive data. (Cloud Native OAuth ch. 11)
4. Run Keycloak highly available and include its availability in the SLA of every service that depends on it. Why: single sign-on makes Keycloak a single point of failure, and a token issuance outage stops all services. (Cloud Native OAuth ch. 3; Solving IdM ch. 11; HKM D21)
5. Write down for each engine and client its revocation budget, the accepted time between disabling a user and that user losing access, and meet it: authorize short actions per request, re-authorize long-running ones (Kafka connections, long Trino queries, application web sessions) at an interval inside the budget, and cap every token and session lifetime as the fallback (lifetimes: oauth-oidc-profile starter rule 6). Why: invalidation is only as fast as the longest re-authorization interval in the enforcement path. (Zero Trust Networks ch. 10)

Example (values are placeholders until each component is checked):

```yaml
# docs/revocation-budget.yaml
- { component: trino,    budget: <minutes>, mechanism: Ranger check per query; access token lifetime as cap }
- { component: kafka,    budget: <minutes>, mechanism: connection re-authentication (to confirm); token lifetime as cap }
- { component: superset, budget: <minutes>, mechanism: back-channel logout ends the Flask session; client session max as cap }
```

### Key, certificate and secret rotation

Source: Keycloak – IAM for Modern Applications, ch. 14; CISA/NSA ESF IAM, Implementing MFA, Implementing Best Practice; Cloud Native Data Security with OAuth, ch. 10 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 17; API Security in Action, ch. 6, 11; BG Rehberi 3.1.12.21, 3.2.1.7; Identity Attack Vectors, 2nd ed., ch. 6, 8, 11, 13, 14, 19, App. A; Learning Digital Identity (Windley, 2023), ch. 9, 14; Zero Trust Networks, 2nd ed., ch. 2, 10 (merged 2026-11-18). The rotation procedure is starter rule 1.

1. Protect realm signing keys as the most valuable secret of the platform: restrict who can read or replace them, and, because Keycloak 26.8 has no built-in HSM or KMS key provider, protect them through access control and database encryption and re-evaluate hardware-backed storage when a supported provider exists; keep every other private key (TLS keys, `client-jwt` keys) in OpenBao (rule 7). Why: a stolen signing key forges any identity, and a rule cannot require a key store Keycloak does not support. (CISA/NSA ESF IAM, Implementing MFA, Implementing Best Practice; Zero Trust Networks ch. 8; decision 2026-11-18; research: Keycloak 26.8.2 documentation and source, 2026-11-18)
2. Disable an old key immediately only when compromise is suspected, and expect every session and token it signed to become invalid. Why: a compromised key must stop validating at once, which forces everyone to sign in again. (Keycloak IAM ch. 14; Cloud Native OAuth ch. 5; Learning Digital Identity ch. 14)
3. Keep no secret, API key or password in source code, scripts, CI/CD variables, container images, Git repositories, Terraform variables files, spreadsheets or other flat files; load external credentials (database, SMTP, LDAP bind, IdP client keys) from OpenBao, rendered as files into the directory of Keycloak's file-based vault (Keycloak has no OpenBao or HashiCorp vault provider), not from the realm configuration, and limit access to each private key to its owner. Why: credentials in code and files leak with them and can hand an attacker the whole environment, searching such storage is a standard way to steal credentials, and a database or backup leak then does not reveal them. (Keycloak IAM ch. 14; BG Rehberi 3.1.12.21, 3.2.1.7; Identity Attack Vectors ch. 6, 11, 13, 14, App. A; Learning Digital Identity ch. 9; HKM D12, D33)
4. Keep an inventory of every credential on the request path (client keys, database passwords, cookie keys, internal tokens), and for each key type (realm signing, client `client-jwt`, TLS, institution IdP) record who generates, distributes, stores, rotates and destroys it, with a hardening and rotation plan: choose only credential types that can be rotated, give each an expiry, rotate machine credentials and keys on a consistent schedule, and report any that missed its rotation; user passwords follow the event-based rule (authentication-and-assurance, Brute force rule 5). Why: static secrets are hard to rotate and are often never renewed, every lifecycle step is both a governance decision and a point of compromise, and expiry limits how long a stolen key works. (Cloud Native OAuth ch. 10; Solving IdM ch. 17; Learning Digital Identity ch. 9; Zero Trust Networks ch. 2, 10; HKM D5, D33)
5. Use every key for one purpose and one algorithm: separate realm key providers for signing and for encryption, a separate key for each client's assertions, and, where a component needs extra symmetric keys, derive them with HKDF and distinct context strings instead of reusing one key. Why: a key reused across functions weakens the guarantees of each, and derivation reduces the secrets to distribute. (API Security in Action ch. 6, 11; HKM D33)
6. Deliver workload secrets from OpenBao: let each pod authenticate with its own short-lived Kubernetes service-account token, never put secrets in Git or a pod spec, mount workload secrets as read-only files rather than environment variables (the HKM D24 pepper and the Keycloak CR Secret references stay as decided), and encrypt Kubernetes Secrets in etcd with a KMS provider. Why: a stolen short-lived bootstrap credential is soon useless, environment variables show up in pod descriptions, and etcd stores Secrets unencrypted by default. (API Security in Action ch. 11; HKM D24, D33; decision 2026-11-18)
7. Keep every privileged credential that has to exist (the `master` break-glass passwords, the LDAP bind credential, `client-jwt` private keys, the HKM D24 pepper) only in OpenBao, encrypted at rest and in transit, under a written rotation policy: machine secrets rotate automatically on a schedule, and any credential rotates at once after an emergency use or a suspected exposure; prefer credentials that people and pipelines never see, and ship OpenBao's audit of every retrieval and rotation to OpenSearch. Why: stored privileged secrets are the theft target, scheduled rotation only works when machines enforce the policy, static secrets let one compromise spread, and the retrieval log shows who held a secret and when. (Identity Attack Vectors ch. 8, 13, 19, App. A; HKM D32, D33, D65)
8. Generate realm keys with Keycloak's own key providers, declared in Terraform, and import a key only from a vetted generator (OpenBao or hardware), never from a home-made one. Why: predictable key generation lets an attacker recreate a key without stealing it. (Learning Digital Identity ch. 9; HKM D33)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm = "hkm"
  smtp_server {
    host = "smtp.hkm.example"
    from = "sso@hkm.example"
    auth {
      username = "keycloak"
      password = "$${vault.smtp-password}"   # file-based vault; OpenBao renders the file
    }
  }
}

# OpenBao (Vault-compatible provider): svc-spark-etl reads its secrets with a short-lived pod token
resource "vault_kubernetes_auth_backend_role" "svc_spark_etl" {
  backend                          = "kubernetes"
  role_name                        = "svc-spark-etl"
  bound_service_account_names      = ["svc-spark-etl"]
  bound_service_account_namespaces = ["t-acme-jobs"]
  token_ttl                        = 600
  token_policies                   = ["svc-spark-etl-read"]
}

# OpenBao audit device: every read of a privileged secret reaches OpenSearch
resource "vault_audit" "to_opensearch" {
  type    = "socket"
  options = { address = "log-shipper.observability.svc:9090", socket_type = "tcp", format = "json" }
}

# Next realm signing key, generated by Keycloak (argument names to confirm against the provider docs)
resource "keycloak_realm_keystore_rsa_generated" "hkm_rs256_next" {
  realm_id  = keycloak_realm.hkm.id
  name      = "hkm-rs256-next"
  enabled   = true
  active    = true
  priority  = 200     # above the current key, which stays passive until its tokens expire
  algorithm = "RS256"
  key_size  = 2048
}
```

### Backup, restore and disaster recovery

Source: Keycloak 26.8.2 documentation, Operator guides: rolling updates; Upgrading Guide (merged 2026-11-17); Zero Trust Networks, 2nd ed., ch. 7 (merged 2026-11-18). Scheduled restore tests are starter rule 4.

1. Take a database backup immediately before every upgrade and roll back only by restoring it; never downgrade. Why: Keycloak upgrades migrate the database and offer no downgrade path. (Keycloak docs 26.8, p. 16, p. 17; Zero Trust Networks ch. 7; HKM D42)

### Upgrade strategy and technical debt

Source: Keycloak 26.8.2 documentation, Operator guides: rolling updates; Upgrading Guide 26.5–26.8; Keycloak – IAM for Modern Applications, ch. 13, 14; Cloud Native Data Security with OAuth, ch. 3 (merged 2026-11-17); Identity Attack Vectors, 2nd ed., ch. 16 (merged 2026-11-18). Extension rules are placed here as the closest file.

1. Automate upgrades with a fast regression suite and take every release rather than skipping. Why: community Keycloak has no long-term-support line, so small frequent steps are safer than large jumps. (Keycloak IAM ch. 14)
2. Use the Operator's `Auto` update strategy, and `Explicit` only with a tested `revision` workflow and a pinned Operator version; check the `RecreateUpdateUsed` condition after every update. Why: `Auto` checks compatibility in a Job and rolls only when safe, `Explicit` with automatic Operator upgrades can attempt unsupported rolling updates, and `True` means the Operator chose a recreate with downtime. (Keycloak docs 26.8, p. 16; HKM D21)
3. Before relying on rolling updates, verify graceful shutdown on `TERM` and that the load balancer honours the readiness probe. Why: rolling updates avoid downtime only when nodes drain cleanly and traffic leaves them first. (Keycloak docs 26.8, p. 16)
4. Turn every item of the upgrading guide's migration changes into a Terraform or Operator change before the upgrade; for 26.5–26.8 these include the `kc-admin-api` scope, admin roles via IdP mappers, the introspection audience, Full Scope Allowed, client session timeouts, a realm SMTP test, and resending an IdP's client credential with any change to its token URL. Why: each change breaks clients or logins on the upgraded version if left undone. (Keycloak docs 26.8, p. 17)
5. Meet needs with configuration and built-in providers first, and build unavoidable custom needs (external user stores, claim transformation, unusual IdPs) as Keycloak providers shared by all clients, never as per-application workarounds. Why: most SPIs are internal and break on upgrade, while one reviewed server-side extension is reusable and application patches diverge. (Keycloak IAM ch. 13; Cloud Native OAuth ch. 3)
6. Keep provider factory initialisation free of request state and release resources on close. Why: factories live for the server lifetime while provider instances live for one request. (Keycloak IAM ch. 13)
7. Brand pages with a child theme that inherits a built-in theme and changes only CSS, images and messages; override a FreeMarker template only when CSS cannot achieve the change, and list each overridden template in the upgrade plan. Why: inheritance keeps upstream fixes and translations, and overridden templates must be merged by hand at every release. (Keycloak IAM ch. 13)
8. Run an identity debt audit periodically and before each minor upgrade: list clients, identity providers, LDAP providers, roles and realms without recent use, custom or manual connectors and flat-file imports; compare component login logs with Keycloak `LOGIN` and `CODE_TO_TOKEN` events to find logins that bypass Keycloak; remove what a migration left behind once its last dependency has moved; put remaining deprecated mechanisms (FGAP v1, token exchange V1, adapters, implicit and password grants) into the upgrade plan; and choose open standards (OIDC, SAML, OAuth, SCIM, FIDO2) for every new integration. Why: debt cannot be removed until it is found, leftover components stay attack surface, logins outside Keycloak point to local accounts or shadow IT, and outdated access models block later policy changes. (Identity Attack Vectors ch. 16; HKM D45)

Example:

```yaml
spec:
  update:
    strategy: Auto      # patch releases within 26.8.x; minor upgrades: planned recreate
# after rollout: kubectl get keycloak hkm-sso -o jsonpath='{.status.conditions[?(@.type=="RecreateUpdateUsed")]}'
```

```properties
# themes/hkm/login/theme.properties
parent=keycloak.v2
styles=css/hkm.css
locales=tr,en
```

### Threat model and hardening of the IdP

Source: Keycloak 26.8.2 documentation, Server Admin: Mitigating security threats, Core concepts, realms; Keycloak – IAM for Modern Applications, ch. 14; RFC 9700 §4; CISA/NSA ESF IAM (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 11, 19; API Security in Action, ch. 7, 10; Identity Attack Vectors, 2nd ed., ch. 6, 12, 13, 14; OAuth 2 in Action, ch. 7, 9, 10, 13; Learning Digital Identity, ch. 9; Zero Trust Networks, 2nd ed., ch. 10 (merged 2026-11-18).

1. Treat Keycloak as a prime target and govern it like any critical application: its administrators, service accounts and configuration pass the same review and change control as every other system. Why: forged assertions, persistence accounts and leavers' accounts are the main attack paths, an IAM flaw opens every connected system, attacks on identity infrastructure survive password resets, and the IdP can be the first place a compromise shows. (CISA/NSA ESF IAM, Implementing Best Practice; Solving IdM ch. 11; Identity Attack Vectors ch. 12, 13)
2. Set Require SSL to all requests in every realm, and make every client and resource server validate the Keycloak certificate against a truststore holding only the CA that issues it, over TLS 1.3 or TLS 1.2 with forward-secret cipher suites. Why: tokens on plain HTTP or with disabled trust checks can be stolen in transit, and trust in every token rests on the connection to Keycloak. (Keycloak docs 26.8, p. 1, p. 13; API Security in Action ch. 7; OAuth 2 in Action ch. 9, 10, 13; Learning Digital Identity ch. 9)
3. Keep Keycloak's frame protection (X-Frame-Options and CSP `frame-ancestors`, same origin) on login, consent, device and error pages, and `Referrer-Policy: no-referrer`, and make sure the ingress neither strips these headers nor injects third-party content. Why: framing captures credentials or consent, and Referer headers leak codes. (Keycloak docs 26.8, p. 13; RFC 9700 §4.2.4, §4.16)
4. Host no open redirector on Keycloak or on any host that serves a redirect URI. Why: browsers reattach the token fragment to the next redirect. (RFC 9700 §4.1.2, §4.11.1; OAuth 2 in Action ch. 7, 9)
5. Accept only HTTPS inbound, expose the admin console and admin API only internally, allow outbound traffic only to known targets (back-channel logout URLs, LDAP, institution IdPs, SMTP), and run Keycloak in its own namespace with tighter administrative access while still applying the same token, TLS and policy checks to it as to every other service. Why: it narrows what attackers reach and what a compromised pod can call, and a control plane hidden behind its own trusted zone would become the least protected system. (Keycloak IAM ch. 14; OAuth 2 in Action ch. 9; Zero Trust Networks ch. 10)
6. Apply the Secure Client URIs Pattern executor with an HTTPS allow-list of platform domains to all client URI fields, and configure the cluster DNS resolver to refuse public names that resolve to internal ranges. Why: JWKS and other client URIs make Keycloak call out, which enables SSRF and DNS rebinding. (Keycloak docs 26.8, p. 13; API Security in Action ch. 10)
7. Set `authSessionsLimit` for authentication sub-sessions, limit ingress traffic at the firewall, and plan protection against volumetric denial of service separately, with upstream filtering in front of `sso.<domain>` derived from the known traffic policy and kept on premises. Why: unfinished browser logins consume cache memory and cross-node traffic, authentication and authorization do not stop floods, and hidden endpoints only obscure them. (Keycloak docs 26.8, p. 13; Zero Trust Networks ch. 10; HKM D25)
8. Treat an IPS or WAF as an extra layer, never the only protection. Why: security has to be built into Keycloak's configuration and the applications themselves. (Keycloak IAM ch. 14)
9. Review every human process that touches identity data (help desk, recovery, account restore and closure) for identity checks and fraud limits. Why: several large breaches needed no technical flaw at all. (Solving IdM ch. 19; Identity Attack Vectors ch. 12)
10. Require MFA at every administrative entry point (Keycloak, Kubernetes, OpenSearch, Grafana), checked regularly against an automatically maintained asset inventory. Why: one entry point without MFA is enough for a breach. (Solving IdM ch. 19; Identity Attack Vectors ch. 6; HKM D9)
11. Check OpenSearch, object storage and backup locations periodically and never expose them without authentication. Why: open storage and search clusters have exposed hundreds of millions of records. (Solving IdM ch. 19)
12. Encrypt identity data at rest and in transit, including the Keycloak database, its backups, the event logs and the swap space of the nodes that run Keycloak. Why: unencrypted copies turn an incident into a breach, and encryption stops simpler attacks by other tenants of the same computing platform. (Solving IdM ch. 19; Zero Trust Networks ch. 10)
13. Train staff against phishing and developers on the current OWASP Top 10, with code review and automated scanning of platform code. Why: phishing and injection remain leading causes of identity breaches. (Solving IdM ch. 19; Zero Trust Networks ch. 10)
14. Never paste realm exports, client keys, tokens, events or other personal data into generative AI tools, and review AI-generated Keycloak, Terraform or policy configuration like any other change. Why: prompts can leak, and generated configuration may be insecure. (Identity Attack Vectors ch. 14)
15. Rank control-plane systems by what their compromise allows and protect the decision makers first: change Ranger policies, `hkm` realm configuration (flows, client policies, IdP settings) and signing keys only through pull requests approved by two reviewers, keep such changes rare, alert on every one of them and audit the authentication configuration for misconfiguration; an emergency console change still raises an alert and is written back to Git within 24 h; give decision inputs such as access history restricted access and integrity monitoring. Why: a compromised policy engine can authorize anything while a compromised history store mostly lets an attacker inflate their own trust, and no control-plane change may go unnoticed. (Zero Trust Networks ch. 10; HKM D17, D19)

Example (executor ids beyond those named in the summaries need checking):

```json
{
  "profiles": [{
    "name": "hkm-secure",
    "executors": [
      { "executor": "secure-client-uris-pattern", "configuration": { "allowed-patterns": ["^https://[a-z0-9.-]+\\.hkm\\.example(/.*)?$"] } },
      { "executor": "full-scope-disabled", "configuration": { "auto-configure": "true" } },
      { "executor": "allowed-protocol-mappers" },
      { "executor": "downscope-assertion-grant-enforcer" }
    ]
  }]
}
```

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata: { name: keycloak-egress, namespace: keycloak }
spec:
  podSelector: { matchLabels: { app: keycloak } }
  policyTypes: [Egress]
  egress:
    - to: [{ podSelector: { matchLabels: { cnpg.io/cluster: hkm-keycloak-db } } }]
    - to: [{ ipBlock: { cidr: 10.30.0.0/16 } }]   # application back-channel logout endpoints
      ports: [{ port: 443 }]
```

### Troubleshooting login failures

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 7, 16; Identity Attack Vectors, 2nd ed., ch. 10 (merged 2026-11-18).

1. Reproduce a login problem in a non-production realm built from the same Terraform, with a test user and two isolated private browser windows, one for the user and one for the administrator. Why: tests then cannot affect production, and separate windows keep the two sessions apart. (Solving IdM ch. 16)
2. Rule out the basics first: Keycloak reachable, right environment and credentials, account enabled, correct issuer URL and client id, exact redirect URI, unexpired keys and certificates, and the server and event logs (`LOGIN_ERROR`, `CODE_TO_TOKEN_ERROR`). Why: these causes are common and quick to exclude. (Solving IdM ch. 16)
3. Narrow the scope: how many users, which browsers, which applications, constant or intermittent, and what changed. Why: shared failures point to Keycloak or the application, isolated ones to the user or the device. (Solving IdM ch. 16)
4. For intermittent faults, test each Keycloak pod, application replica, proxy and load balancer instance on its own. Why: one misconfigured instance produces random failures. (Solving IdM ch. 16)
5. In a captured trace, find where the sequence stops (authorization request, login page, callback, code exchange), then check request parameters, HTTP status, token claims (`sub`, `scope`, `aud`, expiry, groups, `preferred_username`) and signature validation; for SAML compare the captured request and response with the agreed metadata. Why: the point of deviation names the component to inspect, and first SAML attempts usually fail on configuration mismatches. (Solving IdM ch. 7, 16)
6. When a login worked before, check recent key rotation, certificate expiry, upgrades, password expiry and server clocks (NTP). Why: these routinely break logins that used to work. (Solving IdM ch. 16)
7. Treat HAR files and traces as secrets: trim them before sharing, reset any captured password or client credential, revoke long-lived tokens they contain, and delete them when the issue closes. Why: traces record secrets in clear text. (Solving IdM ch. 16; Identity Attack Vectors ch. 10)

Example:

```json
{ "query": { "bool": { "filter": [
  { "term":  { "realmId": "hkm" } },
  { "term":  { "clientId": "superset" } },
  { "terms": { "type": ["LOGIN_ERROR", "CODE_TO_TOKEN_ERROR"] } },
  { "range": { "time": { "gte": "now-1h" } } } ] } },
  "aggs": { "by_error": { "terms": { "field": "error" } } } }
```

### Exception runbooks

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 17; Identity Attack Vectors, 2nd ed., ch. 7, 8, 13, App. A; Learning Digital Identity, ch. 21 (merged 2026-11-18). Personal-data breach notification steps wait for the breach runbook, which is still open in the platform decisions.

1. Rank exception scenarios by likelihood and impact, write a runbook for the top ones, and name in each the trigger, how the requester is verified, the Keycloak actions, the logs needed and the records to keep. Why: prepared evidence lets the team act fast and prove the case was handled correctly. (Solving IdM ch. 17)
2. Before restoring or closing an account, verify that the requester owns or administers it, with more scrutiny for late requests or requests about a different account. Why: such requests are a way to steal or destroy data. (Solving IdM ch. 17)
3. For an institution account with several administrators, require a second approver, or a waiting period with notice to all of them, before a restore or closure. Why: one malicious administrator must not act alone. (Solving IdM ch. 17; HKM D49)
4. Hand over an orphaned account only after a written request, validation through an independent channel and a record of every step. Why: an unverified claim is account takeover by process. (Solving IdM ch. 17)
5. Keep no hidden history of earlier email addresses or phone numbers; after a takeover, disable the user, sign out its sessions, identify the real owner through the institution IdP or help-desk re-verification, remove the attacker's credentials, restore the contact data, require a reset with a second factor, then re-enable. Why: the attacker may have changed every visible attribute, and minimisation forbids keeping a shadow copy. (Solving IdM ch. 17; HKM D28, D48, D49)
6. Keep the status page and support contact reachable without SSO, and keep a tested break-glass path to the admin console, monitoring and alerting that works during an SSO outage. Why: the outage must not block the people fixing it. (Solving IdM ch. 17; HKM D23, D65)
7. After a provisioning or Workflow outage, reconcile every in-flight change, account and privilege removals first. Why: a lost removal leaves access in place. (Solving IdM ch. 17)
8. Keep a trained general incident-handling procedure with owner, roles, priorities, timeframes, escalation, containment, evidence and follow-up, place every identity runbook under it, include identity compromise scenarios, and rehearse them in tabletop exercises. Why: an incident is no time to define roles, one umbrella procedure keeps identity incidents from falling between teams, and an untested plan cannot be shown to work. (Solving IdM ch. 17; Identity Attack Vectors ch. 7; Learning Digital Identity ch. 21)
9. Prepare a recovery for mass credential compromise that scales and ignores email or phone values changed after the breach. Why: the thieves may have replaced them. (Solving IdM ch. 17)
10. Write the break-glass use of `master` as a runbook: two people release the emergency credentials, the login raises an alert, a ticket records the reason afterwards, any console change is written back to Terraform by pull request within 24 h, and the password is rotated in OpenBao after the use; keep the personal break-glass accounts needed for recovery enabled and alert on every login, and keep any further emergency account disabled until a recorded break-glass event. Why: emergency access must stay possible, a disabled last-resort account would need a second working administrator to enable it, and every use has to be audited and leave no configuration drift. (Identity Attack Vectors ch. 8, 13, 18, App. A; HKM D17, D32, D65; decision 2026-11-18)

Example:

```yaml
# runbooks/account-takeover.yaml (skeleton)
trigger: OpenSearch alert "UPDATE_EMAIL then UPDATE_CREDENTIAL from a new IP" in realm hkm
verify_requester: institution IdP (t-acme) or help-desk re-verification
actions:
  - sign out user sessions            # security team, no approval needed (HKM D49)
  - disable user                      # a second person approves (HKM D49)
  - remove unknown credentials; restore contact data from the institution IdP
  - send execute-actions email: UPDATE_PASSWORD, CONFIGURE_TOTP or webauthn-register
evidence: [user events, admin events, immudb hashes, ticket id]
```

```yaml
# runbooks/master-break-glass.yaml (skeleton)
trigger: Terraform pipeline or hkm admin console unusable; incident ticket opened
release: two people approve the OpenBao read and the safe holding the hardware passkey (HKM D65)
on_login: OpenSearch alert "master login" to the security team
after:
  - ticket records reason and every admin event id
  - console changes written back to Terraform by pull request within 24 h (HKM D17)
  - password rotated in OpenBao (HKM D32)
evidence: [admin events in OpenSearch, immudb hashes, OpenBao audit, ticket]
```

### Image and provider supply chain

Source: Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 7 (merged 2026-11-18).

1. Build the Keycloak image and every provider JAR in CI only from signed commits with the build definition in Git, verify the signatures before building, and store an SBOM, a hash and a signature with every image and provider; prefer reproducible builds. Why: the build step itself is not cryptographically protected, so its inputs must be, and a compromised build server can inject code into output that is still correctly signed. (Zero Trust Networks ch. 7)
2. Never republish an image tag: deploy by digest, promote a build to production only after a person approves it and it is signed with a key used only for releases, and pull images only from the platform's trusted registry over TLS. Why: mutable versions and untrusted mirrors let an older or tampered build pass as the approved one. (Zero Trust Networks ch. 7)

Example:

```yaml
# releases/keycloak-hkm-26.8.2-1.yaml (release record kept in Git)
image: registry.hkm.example/keycloak-hkm@sha256:<digest>
built_from: hkm/keycloak-image@<signed commit>
providers:
  - { name: hkm-username-tombstone, sha256: <hash>, sbom: sbom/hkm-username-tombstone.spdx.json }   # HKM D24 listener
sbom: sbom/keycloak-hkm.spdx.json
signatures: { build: ci-key, release: release-key }
approved_by: <platform release owner>
```
