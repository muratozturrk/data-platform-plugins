# Tenancy and realm design

Status: merged from Keycloak docs 26.8 and Cloud Native Data Security with OAuth (2026-11-17), Solving Identity Management, Identity Attack Vectors, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Put all institution staff, platform operators and platform service accounts in realm `hkm`, enable Organizations there, model each institution as one organization, and derive the tenant claim from organization membership. Why: one SSO session and one claims contract serve every platform component, organizations give each tenant its own members, domains and IdP links inside one realm, and every service and Ranger policy can then check the tenant. (plugin v1; research: Keycloak Organizations announcement and Server Administration Guide, 2026-11-15; Keycloak docs 26.8, p. 2; Cloud Native OAuth ch. 4; HKM D17)
2. Keep public portal users in the separate realm `hkm-public`. Why: a realm authenticates only its own users, and different assurance, registration, consent and retention rules must not leak into the workforce realm. (plugin v1; Keycloak docs 26.8, p. 1)
3. Create a separate realm for an institution only for a written isolation requirement. Why: every extra realm multiplies clients, issuers and admin cost, realm counts beyond a few hundred slow startup and the admin console, and a separate realm is the high-isolation end of the tenancy spectrum, not the default. (research: skycloak realm-scaling measurement, 2026; Cloud Native OAuth ch. 4; HKM D17)
4. Use `master` only for bootstrap and break-glass administrators, and register no platform clients or ordinary users there. Why: `master` exists to administer other realms, and its admins control every realm. (research: Keycloak bootstrap-admin guide; Keycloak docs 26.8, p. 1)
5. Set each organization's alias to the tenant id, keep the institution's legal name as display name, treat the alias as permanent, and never reissue a retired alias to another institution. Why: the alias is unique and cannot be changed after creation, the tenant id joins Keycloak, Ranger zones, Kafka prefixes and OpenBao paths, and an identifier passed to a new owner inherits the old owner's references. (plugin v1; Keycloak docs 26.8, p. 2; Learning Digital Identity ch. 10)

## Rules by topic

### Identity domains and populations

Source: Keycloak 26.8.2 documentation, Server Admin: Core concepts, realms and the master realm; Operator guide: admin bootstrapping (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 1, 11; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 7, 13, 15, 19; Learning Digital Identity (Windley, 2023), ch. 13 (merged 2026-11-18).

1. Create the first administrator from a Secret you control (bootstrap admin options or `spec.bootstrapAdmin`, synced from OpenBao), replace it with named administrators who sign in with passkeys, then delete the temporary account. Why: bootstrap accounts are temporary by design, and without an explicit Secret the Operator generates `temp-admin` with a password. (Keycloak docs 26.8, p. 1, p. 16; HKM D9)
2. Pass bootstrap secrets only as environment variables, and stop every node before running the `bootstrap-admin` command. Why: the command must run with the server's own options. (Keycloak docs 26.8, p. 1)
3. Regain lost administrator access with a temporary admin service account only as a documented break-glass procedure: two people release the bootstrap secret from OpenBao, the use raises an alert, and the service account is deleted immediately afterwards. Why: the service account skips the passkey that administrators otherwise need, so its use must be rare, witnessed and short. (Keycloak docs 26.8, p. 1; HKM D9, D65; decision 2026-11-17)
4. Classify every user population before choosing a realm or login method: institution staff and operators in `hkm`, open-data portal users in `hkm-public`, workloads as service-account clients such as `svc-spark-etl`, emergency administrators in `master`. Why: staff expect one work account from their institution while the public expects a quick sign-up, and the realm and login method follow from that difference. (Solving IdM ch. 1; Identity Attack Vectors ch. 7)
5. Route every platform application, support and help portals included, through Keycloak single sign-on as the platform's one identity provider, add no parallel user store or synchronisation tool that copies accounts or passwords between systems, and use that one point to disable accounts and record authentication. Why: one login point means fewer passwords and one place for policy, account shut-off and evidence, while every extra identity store or copying tool adds administration work and attack surface. (Solving IdM ch. 1, 11; Identity Attack Vectors ch. 7, 13, 15, 19; Learning Digital Identity ch. 13)

Example:

```yaml
# Keycloak CR excerpt: the first admin is a service account whose secret comes from OpenBao
spec:
  bootstrapAdmin:
    service:
      secret: kc-bootstrap-svc   # ExternalSecret from OpenBao; removed once svc-hkm-terraform exists
```

### Tenancy models: realm, organization, group

Source: Keycloak 26.8.2 documentation, Server Admin: Managing organizations; Upgrading Guide 26.5–26.8; Cloud Native Data Security with OAuth, ch. 4 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 4, 10, 18 (merged 2026-11-18).

1. Fill the tenant value from organization membership and never from a free-form attribute that users or applications can write. Why: a self-editable tenant attribute lets a user move into another institution's data. (Cloud Native OAuth ch. 4)
2. Give each organization its own exact email domains, never register wildcard domains, and derive institution-wide grouping from those verified organization domains, never from raw email values in tokens. Why: a domain belongs to one organization per realm and the most specific match decides the organization and the `domain` claim, a wildcard widens what one tenant can claim, and a grant based on a domain needs a trustworthy source. (Keycloak docs 26.8, p. 2; Solving IdM ch. 10; HKM D31)
3. Configure IdP routing and auto redirect on the organization domain, not on the identity provider. Why: domain routing replaced the IdP-level domain settings, and users who hold a local first-factor credential are asked for it instead of being redirected. (Keycloak docs 26.8, p. 2; Solving IdM ch. 18)
4. Set the membership type explicitly on every IdP link: managed only when the institution IdP is the sole source of truth for the account, unmanaged otherwise and always where a person may keep the account after leaving the institution whose IdP they used. Why: removing a managed member or its organization deletes the account from the realm, new links default to unmanaged, and an account that must survive defederation cannot depend on the old IdP. (Keycloak docs 26.8, p. 2, p. 17; Solving IdM ch. 18)
5. When one IdP serves several organizations, give each organization distinct domains and let at most one link use managed membership. Why: the domain gate keeps users out of other tenants even in IdP-initiated logins, and Keycloak rejects a second managed link on the same IdP. (Keycloak docs 26.8, p. 2)
6. Require a verified email for domain-gated auto membership, and test the realm email server before upgrading to 26.8.2. Why: `email_verified=false` stays unverified even with Trust Email on, and from 26.8.2 domain-based membership waits for email verification. (Keycloak docs 26.8, p. 2, p. 17)
7. Onboard institutions that approve each member through organization invitations with auto membership off: the link is single use, expires, is locked to the invited address, and the user creates the account and sets their own credential from it; institution admins never create accounts. Why: invitations bypass the domain gate and always create unmanaged membership, and a forwarded or stolen link must not let someone else take the account. (Keycloak docs 26.8, p. 2; Solving IdM ch. 4; HKM D61)

Example:

```hcl
resource "keycloak_organization" "t_acme" {
  realm   = keycloak_realm.hkm.id
  name    = "Acme Kurumu"      # legal name as display name
  alias   = "t-acme"           # tenant id, immutable
  enabled = true

  domain {
    name     = "acme.gov.tr"   # exact domain, no wildcard
    verified = true
  }
}

# The tenant claim comes from membership, released through the hkm-claims scope
resource "keycloak_generic_protocol_mapper" "institution" {
  realm_id        = keycloak_realm.hkm.id
  client_scope_id = keycloak_openid_client_scope.hkm_claims.id
  name            = "institution"
  protocol        = "openid-connect"
  protocol_mapper = "oidc-organization-membership-mapper"
  config = {
    "claim.name"         = "institution"
    "access.token.claim" = "true"
    "id.token.claim"     = "false"
  }
}
```

### Isolation guarantees and their limits

Source: Keycloak 26.8.2 documentation, Server Admin: Core concepts, realms; Managing organizations (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 18 (merged 2026-11-18).

1. Treat the realm, not the organization, as the boundary for credentials, sessions, signing keys, SSL mode and themes, set these per realm in Terraform, and give organizations no tenant-specific password, flow or session settings. Why: they are realm settings, each new realm gets its own key pair, the organizations inside `hkm` share them, and one uniform policy keeps every institution reviewable. (Keycloak docs 26.8, p. 1, p. 2; Solving IdM ch. 18; HKM D17)
2. Disable an organization, never delete it, to block its IdP-based logins during an incident. Why: deleting an organization with managed members also deletes their accounts. (Keycloak docs 26.8, p. 2)

### Authentication vs authorization boundary

Source: Cloud Native Data Security with OAuth, ch. 1, 3, 10 (merged 2026-11-17); Identity Attack Vectors, 2nd ed., ch. 5, 7, 15, 19 (merged 2026-11-18).

1. Keep identity management (Keycloak), API management (WSO2, Trino Gateway, ingress) and entitlement management (Ranger for data, Keycloak Authorization Services for Airflow only) in separate components joined only by the access token. Why: each part can then be replaced or extended without rebuilding the others, and a single mechanism that both authenticates and authorizes is lost completely once it is broken. (Cloud Native OAuth ch. 3; Identity Attack Vectors ch. 5, 19; HKM D2)
2. Issue every token from Keycloak and never build token issuance into an application or gateway. Why: a token service needs specialist knowledge of the specifications that application teams do not maintain. (Cloud Native OAuth ch. 3)
3. Require a valid access token on every call, internal calls included, and never accept API keys or forwarded user-id headers as identity. Why: injected user ids and stolen keys go unnoticed once traffic is inside the network, and a caller's network location says nothing about who it is. (Cloud Native OAuth ch. 1; Identity Attack Vectors ch. 19)
4. Treat mesh mTLS and workload identities as an extra layer that proves which workload calls, never as authorization or as a replacement for the access token. Why: a workload identity carries no user, no user authentication and no business permission. (Cloud Native OAuth ch. 1, 10)
5. Record every integration path that crosses the Keycloak boundary (institution IdP brokers, LDAP providers, clients, Ranger usersync, SCIM callers, admin automation) in Git next to its Terraform definition with its owner and trust settings, and cover each path with an automated test. Why: weaknesses hide where separately built components are joined, and one weak integration can cancel the strength of the components on both sides. (Identity Attack Vectors ch. 7, 15; HKM D4)
6. Measure zero-trust progress per workflow against a published framework (the NIST SP 800-207 tenets, the DoD Zero Trust Framework categories) instead of treating it as a product feature. Why: zero trust is a method that has to be designed and measured use case by use case. (Identity Attack Vectors ch. 19)

Example:

```yaml
# integrations/t-acme-oidc.yaml (reviewed with the Terraform change that defines the broker)
path: institution IdP t-acme-oidc -> realm hkm, organization t-acme
owner: platform identity team (contact at t-acme: IT security)
trust: realm signs a client-jwt towards the IdP; issuer and JWKS URL pinned; email trusted only with email_verified
test: tests/integrations/t_acme_oidc.py   # login, mapping to platform_t-acme_analyst, back-channel logout
```

### Control plane and data plane

Source: Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 2 (merged 2026-11-18).

1. Scope the platform threat model up to the trusted insider, operators and administrators included, and close basic gaps such as weak passwords or a login without MFA before designing against rarer attacks. Why: attackers go for the weakest link first. (Zero Trust Networks ch. 2)
2. Treat Keycloak (identity and claims) and Ranger (data decisions) as the control plane and the engines as the data plane: give the data plane only short-lived, revocable trust (access tokens lasting minutes, cached Ranger policy), and make sure no engine credential can change Keycloak or Ranger configuration. Why: the data plane is where attackers enter, so the trust held there must expire and must never lead back into the control plane. (Zero Trust Networks ch. 2; HKM D2)

### Naming and identifiers (immutable subject, username)

Source: Keycloak 26.8.2 documentation, Server Admin: Managing users and the user profile; User federation; Integrating identity providers (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 14, 18; Identity Attack Vectors, 2nd ed., ch. 5, 10; Learning Digital Identity, ch. 10, 14 (merged 2026-11-18).

1. Keep usernames lowercase and non-editable: Edit username and Email as username off in `hkm`, every LDAP provider READ_ONLY, and no case-sensitive usernames or emails. Why: these realm login settings override the user profile for `username` and `email`, LDAP and brokered values are stored lowercase, and only READ_ONLY stops directory-driven username changes. (Keycloak docs 26.8, p. 3, p. 4, p. 8; Solving IdM ch. 14; Learning Digital Identity ch. 10, 14; HKM D1)
2. When an institution merger or rename changes how its people are named, keep `sub` and `preferred_username` unchanged and give a legacy application the old name through a User Attribute mapper in a dedicated client scope assigned only to that named client until it is retired. Why: rewriting identifiers inside application records is complex and error-prone, and the engines must keep their principal. (Solving IdM ch. 18; HKM D1)
3. Give administrator accounts usernames that carry no privilege marker such as an `-admin` suffix, and keep every account, administrators included, traceable to its owner by automation through `sub`, the IdP or federation link and the owner record. Why: usernames are not secret, a privilege marker shows an attacker which account to target, and an alert on an account that cannot be traced to a person is useless. (Identity Attack Vectors ch. 5, 10; HKM D1)

Example:

```hcl
resource "keycloak_realm" "hkm" {
  realm                          = "hkm"
  organizations_enabled          = true
  ssl_required                   = "all"
  edit_username_allowed          = false
  registration_email_as_username = false
}
```

```hcl
# Merger or rename: only the legacy client receives the old identifier
resource "keycloak_openid_client_scope" "legacy_t_acme_uid" {
  realm_id               = keycloak_realm.hkm.id
  name                   = "legacy-t-acme-uid"
  include_in_token_scope = false
}

resource "keycloak_openid_user_attribute_protocol_mapper" "legacy_t_acme_uid" {
  realm_id            = keycloak_realm.hkm.id
  client_scope_id     = keycloak_openid_client_scope.legacy_t_acme_uid.id
  name                = "legacy-uid"
  user_attribute      = "legacy_uid"   # admin-only attribute in the user profile
  claim_name          = "legacy_uid"
  add_to_id_token     = true
  add_to_access_token = false
}
```

### Design questions for every application

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 1; Learning Digital Identity (Windley, 2023), ch. 7, 16 (merged 2026-11-18).

1. Before registering a client, record for the application: its population and realm; the authentication level it needs (`silver` or `gold`), set by a written risk assessment of its context, its business need, the residual risk its owner accepts, the cost of reducing that risk and the actions that depend on the user's `sub`; whether anonymous use is allowed; whether it is a web or native client and which APIs it calls; its session length; and what its logout must end. Why: these answers fix the client type, flow and session settings, these risk factors decide how much evidence of identity is enough, and threats after login (stolen tokens, breached credentials, scripted guessing) must be designed for up front, not found late. (Solving IdM ch. 1; Learning Digital Identity ch. 7, 16)

Example:

```yaml
# tenants/t-acme/apps/superset.yaml (application record, reviewed with the Terraform change)
client: superset
realm: hkm
population: institution staff (organization t-acme)
level: silver            # password + second factor in hkm; dataset administration steps up to gold (passkey)
anonymous: false
type: web (backend for frontend)
calls: [trino]
session: { idle: 30m, max: 10h }
logout: ends the Keycloak SSO session; back-channel logout to the other clients
```

### Trust basis and federation governance

Source: Learning Digital Identity: Design, Deploy, and Manage Identity Architectures (Windley, 2023), ch. 7, 13, 16, 22 (merged 2026-11-18).

1. Protect realm `hkm` (its administrators, signing keys and database) as the primary root of trust of the platform, and list each brokered institution IdP as the root of trust for its organization's members. Why: in an administrative identity architecture every identifier, credential and group binding depends on the administrator that holds it. (Learning Digital Identity ch. 16)
2. Govern the federation between the platform and the institutions with a written trust framework, kept in Git and published to every institution, stating the rules, the roles and responsibilities of the platform and of each institution, the legal agreements behind them and how the framework is maintained. Why: institutions rely on each other's identities through the platform without a direct relationship, and an ad hoc framework cannot carry that trust. (Learning Digital Identity ch. 7, 13, 22; HKM D47)
3. Publish the platform's commitments to institutions (what it releases, keeps and enforces), apply them equally to every institution, and release only the minimal claims of the `hkm-claims` contract. Why: the platform is the strongest party in the ecosystem, and it stays legitimate only by committing publicly and keeping its promises. (Learning Digital Identity ch. 16, 22; HKM D17)

Example (file layout illustrative):

```yaml
# federation/framework.yaml (index of the published trust framework)
roots_of_trust:
  primary: realm hkm (admins with passkeys, realm keys, database)
  per_organization:
    t-acme: t-acme-oidc
commitments:
  claims: hkm-claims contract only
  session: { idle: 30m, max: 10h }        # same for every institution (HKM D10, D17)
  upstream_tokens: not stored, not embedded (HKM D53)
documents: [framework/rules.md, framework/roles.md, agreements/, framework/changelog.md]
```
