# Claims contract and delegated administration

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (OWASP ASVS 5.0 V8) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action and Türkiye mevzuatı (BG Rehberi, KVKK Rehberi), Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections.

## Starter rules

1. Identify users downstream by `iss` + `sub` as the stable technical key, issue the non-editable, lowercase `preferred_username` (never an email address, never reassigned) as the business identifier and engine principal, and keep `sub` in audit records so identity and engine records can be joined. Why: identifiers that can be reassigned let one user inherit another's access, and `sub` survives name changes but is not what engines match users by. (research: OWASP ASVS 5.0, 10.3.3 and 10.5.2; Keycloak IAM ch. 4; Cloud Native OAuth ch. 4; Solving IdM ch. 4, 6, 14, 18; HKM D1, D24) The Keycloak-to-engine hop does not claim FAL2 for the principal, because NIST SP 800-63C-4 §3.4 forbids a plaintext username in a FAL2 federated identifier (Identity Attack Vectors ch. 6; Learning Digital Identity ch. 4, 10, 14; Zero Trust Networks ch. 4; HKM D1).
2. Create platform groups as top-level groups named `platform_{tenant}_{role}` (for example `platform_t-acme_analyst`) with no `/` in the name, and emit them as flat names without path. Why: subgroups inherit parent roles and produce slash paths, Ranger and the engines match names exactly, and Keycloak 26.8 resolves bare group names to top-level groups only. (research: Keycloak 26.8 upgrade notes, CVE-2026-19608; Keycloak docs 26.8, p. 6, p. 17; Keycloak IAM ch. 8, 10; Learning Digital Identity ch. 10; HKM D3, D4)
3. Keep data-access decisions out of Keycloak roles, scopes and claims: Keycloak issues identity claims and group membership, Ranger decides data access, applications decide their own functions, and Keycloak admin permissions decide only who edits identities and memberships. Why: Ranger enforces one audited policy on every engine path, while permissions in tokens freeze until expiry and couple the identity team to every data change. (plugin v1; apache-ranger plugin platform decision; Keycloak docs 26.8, p. 6, p. 9; Keycloak IAM ch. 5, 8; OWASP ASVS 5.0, 8.2.1, 8.2.2, 8.3.1; Cloud Native OAuth ch. 3, 5, 9; Solving IdM ch. 2, 8; API Security in Action ch. 7, 8; BG Rehberi 3.2.3.3; KVKK Rehberi 3.1, p. 20; Identity Attack Vectors ch. 5, 19; Learning Digital Identity ch. 12; HKM D2)
4. Delegate institution administration with FGAP v2 permissions scoped to that institution's groups, never with `view-users`, `manage-users` or similar admin roles. Why: least privilege for institution admins, and any matching admin role skips permission evaluation and grants realm-wide access. (research: Keycloak FGAP v2 blog, 2025-05; Keycloak docs 26.8, p. 9; BG Rehberi 3.2.3.3; KVKK Rehberi 3.1, p. 21; HKM D57)
5. Keep the impersonation role unassigned outside break-glass. Why: impersonation has been an escalation path. (research: Keycloak 26.8.0 security fixes, CVE-2026-17526; Solving IdM ch. 18)

## Rules by topic

### Claims and token content

Source: Keycloak 26.8.2 documentation, Server Admin: Managing organizations, Managing OIDC and SAML clients, Admin console permissions; Keycloak – IAM for Modern Applications, ch. 4, 5, 8, 10; OWASP ASVS 5.0, 8.3.2; Cloud Native Data Security with OAuth, ch. 4, 5, 6, 7, 9 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 5, 6, 8, 14, 18; API Security in Action, ch. 6; OAuth 2 in Action, ch. 10; Learning Digital Identity (Windley, 2023), ch. 4, 8, 9, 12, 13, 14 (merged 2026-11-18).

1. Publish the `hkm-claims` contract in Git beside the Terraform module: for each API the required scopes, the claims each scope adds, the source of each claim and its consumers, including every claim a Ranger policy or service reads. Why: developers and reviewers can rely only on what is written down, and an undocumented claim change can silently change decisions. (Cloud Native OAuth ch. 6, 9)
2. Change the contract only through a reviewed pull request with a version note, and announce claim removals and token size growth before applying them. Why: tokens carry no version, so a removed claim or a much larger token breaks the APIs and clients that consume it. (Cloud Native OAuth ch. 6; HKM D17)
3. Deliver every claim through a protocol mapper in a named shared client scope: `hkm-claims` as a default scope for claims every token needs, optional scopes for claims only some requests need, and a client's dedicated scope only for a documented one-off. Why: claim release then follows the client's granted scopes, and optional scopes keep tokens small. (Keycloak IAM ch. 4, 10; Cloud Native OAuth ch. 4)
4. Issue only claims that an authorization decision uses, that stay stable for the token's lifetime and that more than one API needs; keep address, national id, phone number and email out of access tokens, keep sensitive or large claims out of ID tokens and serve them from UserInfo, and add roles to the ID token only for relying parties that read them. Why: a token is a readable snapshot shared with every recipient, so each extra claim widens exposure, and a stable value such as an email address or phone number lets separate data sets be linked to one person. (Cloud Native OAuth ch. 6, 7, 9; Keycloak IAM ch. 4; Solving IdM ch. 6; API Security in Action ch. 6; OAuth 2 in Action ch. 10; Learning Digital Identity ch. 4, 8, 14)
5. Never issue fine-grained permission claims (`can_*` names, table or topic grants); issue group membership and stable attributes and let Ranger resolve permissions, and never place permissions in a long-lived signed artifact such as a client certificate. Why: volatile permissions in tokens tie every business change to an identity change, and a signed permission can change only by revocation and reissue. (Cloud Native OAuth ch. 5, 6; Learning Digital Identity ch. 9, 12; HKM D2)
6. Put runtime values such as tenant or region in claims, never in scope names, and include the `scope` claim in every access token. Why: scope names that encode values multiply without limit, and APIs and gateways need `scope` for coarse checks. (Cloud Native OAuth ch. 6; Solving IdM ch. 5, 8)
7. Put an API into a token's audience only through an explicit Audience mapper in an `aud-<api>` client scope, and check that role scope mappings to another client's roles do not add unreviewed audiences such as `account`. Why: implicit audience entries widen where the token works without review. (Keycloak IAM ch. 5; HKM D15)
8. Read claim values only from sources Keycloak trusts over secure channels, keep slow or public external lookups off the issuance path, and never take authorization values from request headers or query parameters. Why: APIs trust claims as given, each source's availability becomes token availability, and request inputs are controlled by the caller. (Cloud Native OAuth ch. 6)
9. Give local, brokered and SCIM-provisioned users the same claim shape. Why: services must not see different tokens for different user cohorts. (Cloud Native OAuth ch. 4)
10. Let tenant-aware clients request the organization claim with exactly one scope format, preferably `organization:<alias>` (for example `organization:t-acme`), and never accept a tenant taken only from a URL or a chooser without this membership check. Why: mixing formats is an error, unknown or non-member aliases reject the request, and a tenant selection alone must never grant access. (Keycloak docs 26.8, p. 2; Solving IdM ch. 18)
11. Configure UserInfo output on the mappers available to the client that calls UserInfo, and do not assume it matches the ID token. Why: UserInfo answers per calling client, so two services can see different data for one user. (Keycloak IAM ch. 4)
12. Where decisions rely on group claims, accept that a membership change takes effect at token expiry at the latest, and alert on or revert actions taken in that window when it matters. Why: tokens carry stale groups until they expire (lifetime: oauth-oidc-profile starter rule 6). (OWASP ASVS 5.0, 8.3.2; Solving IdM ch. 14)
13. Keep a decoded sample token per client in the access review (`aud`, `scope`, `groups`, `realm_access`, `resource_access`) and remove claims no consumer uses. Why: every unused claim is disclosure and token size without purpose. (Keycloak IAM ch. 5, 8)
14. Change protocol mappers only through the Terraform principal (`manage-realm`), leave Allowed protocol mapper types for client administrators empty, restrict mapper types with the `allowed-protocol-mappers` executor, and review client scope mappers separately. Why: mappers can inject any claim, client policies skip client scope mappers, and `manage-clients` can attach any scope. (Keycloak docs 26.8, p. 9, p. 11)
15. Leave the consent screen off for first-party platform clients; for third-party clients show plain-language consent per scope, request a small scope set first and more when a feature needs it, request `offline_access` explicitly, let users revoke stored consent, and store consent only for confidential clients. Why: narrow, feature-linked requests are easier to judge, and public clients carry more impersonation risk. (Cloud Native OAuth ch. 4, 6; Keycloak IAM ch. 5; Learning Digital Identity ch. 4, 13; HKM D20)
16. Express a client's attribute need as a predicate the platform already holds, such as membership in `platform_t-acme_analyst`, the `institution` claim or `acr`, instead of the underlying value (job title, birth date, department), and release identifying claims such as the full name or email only to clients whose function needs accountability, such as data access and administration in `hkm` or an accountable action in `hkm-public`. Why: a predicate reveals far less than the value behind it, and systems often ask who a person is when what the person is would give enough confidence. (Learning Digital Identity ch. 4, 8)

Example:

```hcl
# hkm-claims: identity values only, no permission claims
resource "keycloak_openid_client_scope" "hkm_claims" {
  realm_id               = keycloak_realm.hkm.id
  name                   = "hkm-claims"
  include_in_token_scope = true
}

resource "keycloak_openid_group_membership_protocol_mapper" "groups" {
  realm_id            = keycloak_realm.hkm.id
  client_scope_id     = keycloak_openid_client_scope.hkm_claims.id
  name                = "groups"
  claim_name          = "groups"
  full_path           = false   # platform_t-acme_analyst, no path
  add_to_id_token     = false
  add_to_access_token = true
  add_to_userinfo     = false
}

resource "keycloak_openid_client_default_scopes" "superset" {
  realm_id       = keycloak_realm.hkm.id
  client_id      = keycloak_openid_client.superset.id
  default_scopes = ["openid", "hkm-claims"]
}
```

### Groups, roles and entitlements

Source: Keycloak 26.8.2 documentation, Server Admin: Roles and groups; Keycloak – IAM for Modern Applications, ch. 4, 5, 8; Cloud Native Data Security with OAuth, ch. 4, 9 (merged 2026-11-17); API Security in Action, ch. 3, 8; BG Rehberi 3.2.3.1, 3.2.3.4; KVKK Rehberi 3.1, Table 4.1; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 5, 6; Learning Digital Identity, ch. 12; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 4, 5, 9, 11 (merged 2026-11-18).

1. Emit group membership only through a Group Membership mapper in `hkm-claims` with full group path off. Why: groups are not in tokens without a mapper, and with paths on the claim carries a hierarchy that Ranger cannot match. (Keycloak IAM ch. 8; HKM D3)
2. Use groups to organise users and roles to express application functions, and do not use one for the other; grant roles and Ranger policies to groups, never to individual users, keep administrative roles out of broad groups, and handle a job change by moving the person between groups. Why: mixing them blurs the permission model, direct user grants force reviewers to inspect every user as well as every role, group membership is the natural unit of review, and an admin role on a wide group puts every member in the attack path. (Keycloak IAM ch. 8; API Security in Action ch. 8; Identity Attack Vectors ch. 6; Learning Digital Identity ch. 12)
3. Define application roles as client roles of the client that interprets them (for example `airflow` roles), and make a role a realm role only when it means the same to every client. Why: each client gets its own role namespace. (Keycloak docs 26.8, p. 6; Keycloak IAM ch. 8; API Security in Action ch. 8)
4. Do not use roles for fine-grained or data-level permissions, check scope and granularity before creating any new role, and use composite roles sparingly, never chained and never to grant data access. Why: fine-grained roles explode in number, composites expand recursively into tokens, and data permissions belong to Ranger. (Keycloak docs 26.8, p. 6; Keycloak IAM ch. 8; Identity Attack Vectors ch. 6; HKM D2)
5. Keep default roles and default groups free of any tenant, data or administrative group, so that a login by itself grants nothing beyond the baseline and every further entitlement is assigned explicitly. Why: they are assigned to every created, imported or brokered user, and least privilege means as little automatic access as possible. (Keycloak docs 26.8, p. 6; Identity Attack Vectors ch. 5, 6)
6. Give each client a role scope listing only the roles it needs, and put a role scope inside an optional client scope when a role should appear only on request. Why: a token carries the intersection of the user's roles and the client's role scope (Full Scope Allowed off: oauth-oidc-profile, Access token design rule 2). (Keycloak IAM ch. 4, 5, 8; API Security in Action ch. 8; HKM D34)
7. Choose the access model per rule: groups and roles for stable job functions; relationships (ownership, hierarchy) and context (region, time, location, network or IP, usage rate) as Ranger policy conditions or gateway limits, never as token claims; no extra groups or roles for region, dataset or similar dimensions beyond the tenant segment of the group name; write every policy against a group or service identity and a named resource, never with an IP address or subnet as its subject, and use network context only as an extra condition next to a group, never as the sole condition. Why: each model expresses what the others cannot, Keycloak has no relationship graph and its flows have no built-in time or IP condition, role explosion raises misconfiguration risk, workloads move between hosts so address-based rules go stale, and access granted from network location alone contradicts zero trust. (Cloud Native OAuth ch. 9; API Security in Action ch. 8; Learning Digital Identity ch. 12; Zero Trust Networks ch. 4, 5, 9, 11; BG Rehberi 3.2.3.4; HKM D2, D3)
8. Make every policy deny by default with deny overriding permit, and make sure no subject can raise its own privileges or grant more than it holds, for example by joining a group, by editing an attribute a policy reads, or as a delegated administrator assigning rights beyond its own. Why: least privilege at minimum forbids self-elevation, deny-overrides is the safest way to combine rule outcomes, and a delegate that can grant beyond its own rights can mint a stronger account. (Cloud Native OAuth ch. 4, 9; API Security in Action ch. 3, 8; Learning Digital Identity ch. 12)
9. Keep the definition of what a group, role or Ranger policy grants (the platform team, through reviewed Terraform and policy pull requests) separate from the assignment of people to groups (institution administrators for their own `platform_t-acme_*` groups). Why: assigning members is far less error-prone than defining permissions, so splitting the duties limits the damage of either mistake. (API Security in Action ch. 8; HKM D17, D57)
10. Produce, per organization, an authorisation matrix from the `platform_{tenant}_{role}` group memberships and the matching Ranger policies, and update and review it at a defined interval. Why: both guides require a maintained matrix of who may use which application and data. (BG Rehberi 3.2.3.1; KVKK Rehberi 3.1, p. 20, Table 4.1; HKM D4)

Example:

```hcl
resource "keycloak_group" "t_acme_analyst" {
  realm_id = keycloak_realm.hkm.id
  name     = "platform_t-acme_analyst"   # top level, no parent_id
}

resource "keycloak_group_roles" "t_acme_analyst" {
  realm_id = keycloak_realm.hkm.id
  group_id = keycloak_group.t_acme_analyst.id
  role_ids = [keycloak_role.superset_viewer.id]   # client role of superset only
}

resource "keycloak_generic_role_mapper" "superset_scope_viewer" {
  realm_id  = keycloak_realm.hkm.id
  client_id = keycloak_openid_client.superset.id   # role scope of superset
  role_id   = keycloak_role.superset_viewer.id
}
```

### Coarse vs fine-grained authorization (who decides what)

Source: Keycloak 26.8.2 documentation, Server Admin: Managing organizations, Managing OIDC and SAML clients; Keycloak – IAM for Modern Applications, ch. 5, 8; OWASP ASVS 5.0 V8; Cloud Native Data Security with OAuth, ch. 3, 9 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 2, 5, 8, 18; API Security in Action, ch. 7, 8, 11; BG Rehberi 3.1.12.20, 3.2.3.1; Identity Attack Vectors, 2nd ed., ch. 13, 14, 19, 20; OAuth 2 in Action, ch. 14; Learning Digital Identity, ch. 12; Zero Trust Networks, 2nd ed., ch. 4, 9 (merged 2026-11-18).

1. Use tokens as the carrier of authorization input and let the consuming side decide (Ranger for data access, the application for functional rights read from claims), with every subject attribute the decision needs (identity, `groups`, `acr`) gathered at authentication; never look up group membership in Keycloak's store at decision time. Why: Keycloak states who the principal is, constraints live with the resource, and a decision that queries the directory itself breaks once groups arrive from a token or an external directory. (Keycloak IAM ch. 8; Solving IdM ch. 2, 8; API Security in Action ch. 8; HKM D2, D4)
2. On every request, validate the token (signature, issuer, audience, expiry, scope) in the service or gateway before any policy call, and send the decision point only business questions. Why: access must be enforced per request, not only at login, and token failures need standard OAuth errors, which a policy engine would mix up with business denials. (Cloud Native OAuth ch. 3, 9; Solving IdM ch. 8; BG Rehberi 3.2.3.1; Learning Digital Identity ch. 12; Zero Trust Networks ch. 4)
3. Document authorization rules per function, data item and field, with the contextual attributes used, the resulting action (allow, step-up, deny) and who administers each part; keep the mapping from groups and roles to permissions explicit so that who can access what is answerable without reading code; and enforce the rules on the server from explicit permissions, never from what the client hides or shows, checking after token validation that this user may act on this object even when the scope allows the action. Why: undocumented or code-embedded rules cannot be audited, a token does not settle a request on its own, and client-side checks invite IDOR, BOLA and BOPLA. (OWASP ASVS 5.0, 8.1.1–8.1.4, 8.2.1–8.2.3, 8.3.1; Solving IdM ch. 8; API Security in Action ch. 8; Learning Digital Identity ch. 12)
4. Decide on the originating user's permissions (the user in `sub`, principal `preferred_username`), not on those of an intermediate service account, and record the calling client only as the actor. Why: a machine token hides who asked, and a privileged service acting on its own credentials becomes a confused deputy. (OWASP ASVS 5.0, 8.3.3; API Security in Action ch. 11; HKM D1)
5. Confine every operation to the caller's institution (organization `t-acme`, groups `platform_t-acme_*`) in Ranger and in applications. Why: cross-tenant access has the highest impact. (OWASP ASVS 5.0, 8.4.1)
6. Reach administrative interfaces only through a gated admin path from the defined admin network with no back door around it, only from managed devices and never from personal ones, and protect them with continuous identity checks (phishing-resistant step-up); never rely on network location, a source IP address or a VPN connection alone, and name the component outside Keycloak that checks the device, because Keycloak does not evaluate device posture. Why: admin paths have the highest impact, the guide limits admin work to defined addresses, privileged sessions from unmanaged devices are a main entry path, and an address or a VPN tunnel says nothing about who is behind it. (OWASP ASVS 5.0, 8.2.4, 8.4.2; BG Rehberi 3.1.12.20; Identity Attack Vectors ch. 13, 14, 19, 20; HKM D21)
7. Centralize complex, high-value or fast-changing rules in the policy engine (Ranger for data) and leave simple checks where they are; move rules that change faster than a component can be redeployed out of its code. Why: one audited policy serves many services, and a role check in code needs a release for every policy change. (Cloud Native OAuth ch. 3, 9; Keycloak IAM ch. 8; API Security in Action ch. 8)
8. Run the decision point next to the enforcement point, as the Ranger plugin inside each engine does. Why: a remote decision adds latency and an availability dependency. (Cloud Native OAuth ch. 9; HKM D2)
9. Let the enforcement point turn a deny into the protocol's refusal (HTTP 403 for REST) and apply the constraints returned with an allow, such as Ranger row filters and masks. Why: a decision can carry more than allow or deny. (Cloud Native OAuth ch. 9)
10. Look up attributes that change often and must be current from a live source at decision time, not from the token. Why: token claims stay frozen until the token expires. (Cloud Native OAuth ch. 9; Solving IdM ch. 8)
11. Keep policies as code in version control with review, tests and automated rollout, in a form that machines run and data owners can read; restrict who may change production policies, record who changed what and when, and when a policy changes review the whole access path across systems (for example `superset` through Trino to an Iceberg schema), not the changed policy alone; measure decision latency when attribute conditions are added, and let API tests mock only the decision outcome. Why: one tested rule set replaces divergent implementations, policies are a sensitive part of the security system, gaps between per-system policies open unexpected paths to sensitive data, and attribute rules combine unpredictably and can slow every request. (Cloud Native OAuth ch. 9; API Security in Action ch. 8; Learning Digital Identity ch. 12; Zero Trust Networks ch. 4, 9)
12. Turn on Keycloak Authorization Services only for `airflow`, leave User-Managed Access off in `hkm` and `hkm-public`, build its group policies on realm groups (never organization groups), attach a policy to every Airflow resource so that none is reachable without one, and prefer permission-carrying tokens to a remote call per request. Why: Airflow is the platform's only Keycloak-decided application, group policies accept realm groups only, a resource with no policy would be open to anyone who presents no claims, and local token checks avoid a round trip. (Keycloak docs 26.8, p. 2, p. 11; Keycloak IAM ch. 8; Solving IdM ch. 8; OAuth 2 in Action ch. 14; HKM D2)
13. Use OAuth scopes with consent only for third-party clients acting on a user's behalf, and define each such scope as a client scope with no mappers and no roles, keeping the set small; grant internal platform clients access through groups, roles and attributes. Why: scopes protect the user from clients, while internal access is granted by administrators. (Keycloak IAM ch. 5, 8; API Security in Action ch. 7; HKM D20)
14. Keep `aud-<service>` as the naming scheme for audience scopes, and name every other new scope after the business area it grants (for example `reports.read`), never after the technical service that happens to implement it. Why: audience scopes identify the receiving API, while business-area names survive a change of implementing service. (Cloud Native OAuth ch. 6; Keycloak IAM ch. 5; Solving IdM ch. 5, 8; HKM D15; decision 2026-11-17)
15. Assign every access requirement of an application to one of three levels (application access, functional access, data access) and record one enforcement point for it: functional access inside the application from groups or client roles in the token (for example Superset roles from `platform_t-acme_analyst`), with every function denied to a user who holds no platform group or client role for it, and data access in Ranger; place every enforcement point inside or directly in front of the resource with no route around it. Why: one owner per requirement avoids gaps and double logic, role-based control is secure only when every acting user holds a role taken from the authenticated session and each action is checked against it, and an enforcement point that can be bypassed enforces nothing. (Solving IdM ch. 8; Learning Digital Identity ch. 12; Zero Trust Networks ch. 4; HKM D2)
16. Build delegation (acting for another user) into the application or into Ranger per task, with limits, log both the delegating and the acting identity for each delegated action, and do not use Keycloak token-exchange delegation. Why: delegation is task specific and needs a full audit trail, and Keycloak delegation is a preview feature. (Solving IdM ch. 18; HKM D16)
17. Enforce application access (level 1, including keeping an institution's users to the clients meant for them) at WSO2 or the ingress, or in the application, from the `institution` and `groups` claims; inside Keycloak rely only on the built-in checks (organization domain gate, the `organization:<alias>` membership check, Verify essential claim on the IdP) and build no custom per-client deny flows. Why: Keycloak authenticates and issues claims while access decisions stay outside the IdP, and custom flows per client are hard to review and break on upgrades. (Solving IdM ch. 8, 11; Keycloak docs 26.8, p. 2, p. 8; HKM D2; decision 2026-11-18)
18. Name an owner for every protected data resource (for example each `t-acme` schema) in the governance records, defaulting to the institution when no person is named; the owner decides who may access the resource, and the platform team as custodian implements that decision in Ranger. Why: owners carry decision authority, and access control is the enforcement arm of data governance. (Learning Digital Identity ch. 12)
19. Treat enforcement (the Ranger plugins in the engines, gateways), policy decision (Ranger policies; Keycloak Authorization Services for `airflow` only), anomaly detection (OpenSearch) and authoritative data (the Keycloak user inventory, OpenSearch history) as separate responsibilities, each hardened and administered on its own, and record the reason wherever two are combined, such as the Ranger plugin evaluating cached policy inside the engine process or Keycloak acting as both user inventory and Airflow's policy engine. Why: one breach should not hand over the whole authorization system, and an undocumented merge of two parts cannot be reviewed. (Zero Trust Networks ch. 4; HKM D2)
20. Let owning teams propose Ranger policies and changes to their own `platform_{tenant}_*` groups and client scopes through pull requests that the platform team merges, require at least two reviewers with one from security on every such change, and let automated guardrail tests in the pipeline reject any proposal that breaks a platform guardrail, for example granting a tenant schema to all users or to another tenant's groups. Why: well-meaning authors write overly broad rules, and guardrails stop local policy from widening exposure. (Zero Trust Networks ch. 4; HKM D17)

Example:

```yaml
# Trino with Ranger: token checks first, data decision in the Ranger plugin
http-server.authentication.type: JWT
http-server.authentication.jwt.key-file: https://sso.hkm.example/realms/hkm/protocol/openid-connect/certs
http-server.authentication.jwt.required-issuer: https://sso.hkm.example/realms/hkm
http-server.authentication.jwt.required-audience: trino
http-server.authentication.jwt.principal-field: preferred_username
access-control.name: ranger      # the policy for platform_t-acme_analyst lives in Ranger, not in the token
```

```json
{ "service": "hkm_trino", "name": "t-acme-analyst-read",
  "resources": { "catalog": {"values": ["iceberg"]}, "schema": {"values": ["t_acme"]}, "table": {"values": ["*"]} },
  "policyItems": [ { "groups": ["platform_t-acme_analyst"], "accesses": [ {"type": "select", "isAllowed": true} ] } ] }
```

```yaml
# tenants/t-acme/apps/superset.access.yaml: one enforcement point per requirement
- requirement: open dashboards
  level: functional
  enforced_by: superset (role Gamma from group platform_t-acme_analyst)
- requirement: read iceberg schema t_acme
  level: data
  enforced_by: ranger (policy t-acme-analyst-read, group platform_t-acme_analyst)
- requirement: edit dataset definitions
  level: functional
  enforced_by: superset (role Alpha; step-up to gold)
```

### Capability links and resource sharing

Source: API Security in Action (Madden, 2020), ch. 9 (merged 2026-11-18). Share links are issued by data services, not by Keycloak; these rules are what Keycloak-side reviewers check.

1. Use share links only to give access to one resource for a bounded time, and keep group-based access through Ranger for everything else. Why: a link grants least authority for one object but blurs who holds access. (API Security in Action ch. 9; HKM D2)
2. Issue a share link (for example a CKAN download link or an object-storage presigned URL) only after Ranger has allowed the requesting user, and make it unguessable, limited to one resource and action set, and expiring. Why: the link is the whole grant, so it must not be broader or longer-lived than the decision behind it. (API Security in Action ch. 9; HKM D2)
3. Keep share tokens out of logged URL paths where the issuing service allows it, prefer the URL fragment for links shown in browsers, set a strict `Referrer-Policy`, and set an explicit fragment on redirects. Why: paths and query strings reach logs, Referer headers and history, and browsers copy fragments across redirects. (API Security in Action ch. 9)
4. Never let a cookie or a source IP alone authorize a state-changing call. Why: ambient authority invites confused-deputy attacks such as CSRF. (API Security in Action ch. 9)
5. Require the user to sign in to `hkm` or `hkm-public` when a link-based action must be accountable, and bind write or delete links to that authenticated user so that a stolen link fails without the session. Why: a shared link otherwise lets anyone act as anyone, and the audit trail needs the user, not the link. (API Security in Action ch. 9)
6. Let re-sharing only issue a new, narrower link for the recipient, never widen the original. Why: any holder could otherwise turn a narrow grant into a broader one. (API Security in Action ch. 9; HKM D50)

### Delegated administration

Source: Keycloak 26.8.2 documentation, Server Admin: Admin console permissions (FGAP v2, admin API audience), Integrating identity providers, Managing OIDC and SAML clients, Upgrading Guide; Cloud Native Data Security with OAuth, ch. 4 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 18; Identity Attack Vectors, 2nd ed., ch. 8, 13, 15, 19; Zero Trust Networks, 2nd ed., ch. 6, 10 (merged 2026-11-18).

1. Keep the `master` `admin` role for emergency accounts only, and give routine administrators `realm-management` roles inside their own realm, using that realm's dedicated admin console. Why: `master` `admin` is a superuser over every realm. (Keycloak docs 26.8, p. 9; Identity Attack Vectors ch. 8; HKM D65)
2. Assign admin roles only by direct role mapping or group membership, and keep Allow granting admin roles via mappers (`allowAdminRoleMapping`) off on every identity provider. Why: admin roles in protocol mappers are ignored since 26.7.1, and IdP mappers were an escalation path closed by default in 26.8.0. (Keycloak docs 26.8, p. 8, p. 9, p. 17)
3. Give an institution administrator only `query-users` and `query-groups`, plus Groups permissions limited to that institution's groups with `view`, `view-members`, `manage-membership` and `manage-membership-of-members`, and never `manage-members`. Why: membership changes need both the group and the user side, query roles only open console sections, and `manage-members` also allows creating users. (Keycloak docs 26.8, p. 9; HKM D57)
4. Select every needed scope of a permission explicitly. Why: scopes are independent, so `manage` without `view` hides the resource. (Keycloak docs 26.8, p. 9)
5. Exclude the administrators' own group with a resource-specific permission and a negative group policy. Why: a resource-specific permission overrides the all-resources permission, which stops admins from managing each other. (Keycloak docs 26.8, p. 9)
6. Use only User, Group, Role or Aggregated policies in permissions that grant `view`, and keep Fetch Roles disabled on role policies. Why: only these policy types are evaluated in database queries, and with Fetch Roles off a restricted client scope limits what an admin can do. (Keycloak docs 26.8, p. 9)
7. Build permissions per group and per policy, not per user. Why: many permissions referencing one admin slow evaluation and can hit SQL `IN` limits. (Keycloak docs 26.8, p. 9)
8. Define permissions in Terraform and check each one with the Evaluation tab in a staging realm before production. Why: a permission takes effect as soon as it is saved. (Keycloak docs 26.8, p. 9; HKM D17)
9. Assign the `kc-admin-api` client scope to every own client or service account that calls the Admin REST API, keep the audience check on, and keep the hostname stable. Why: the audience is derived from the frontend URL, and from 26.8.2 tokens without it get 401. (Keycloak docs 26.8, p. 9, p. 17)
10. Restrict user-management API clients (SCIM, Admin REST API) to the roles, accounts and fields they need with an exact audience, never give a provisioning client realm admin, and allow password fields to be set but never read. Why: these APIs can change every identity in the realm. (Cloud Native OAuth ch. 4)
11. Give read-only client administrators `view-clients`, never `manage-clients`. Why: only `manage-clients` sees real secrets. (Keycloak docs 26.8, p. 11)
12. Give support staff a separate read-only view (FGAP v2 `view` and `view-members` limited to their institution's groups) instead of impersonation, log and monitor its use, and review often who holds it. Why: impersonation is easily abused, while a read-only view covers troubleshooting. (Solving IdM ch. 18; HKM D29)
13. Keep a register of every privileged identity by tier (tier 0 `master` emergency administrators; tier 1 realm administrators with `realm-management` roles and the per-realm automation clients; tier 2 institution administrators under FGAP v2; and the accounts the identity stack itself uses, such as the LDAP bind account, Ranger usersync and SCIM callers), each with a named owner and last use; regenerate it from Terraform state and the Admin REST API, certify it at the access-review interval (audit starter rule 3), and remove unused holders and clients through a reviewed Terraform change. Why: an ownerless or forgotten privileged account cannot be reconciled, discovering privileged accounts is the first step every later control depends on, and accounts built into the IAM infrastructure are part of the attack surface. (Identity Attack Vectors ch. 8, 13, 15, 19; HKM D17, D57)
14. Require two named people to act together for the platform's highest-risk identity operations, at least releasing either credential of a `master` break-glass account and disabling an account in response to an alert; split the material where the tooling allows and otherwise enforce the rule by a witnessed procedure, alert on every use, and rotate the released credential afterwards. Why: no single person, whether tricked, coerced or malicious, should be able to complete an operation that can authorize anything. (Zero Trust Networks ch. 6, 10; HKM D32, D49, D65)

Example (intent only; Terraform provider support for FGAP v2 is unconfirmed):

```yaml
# FGAP v2 intent for t-acme institution admins in realm hkm
policies:
  t-acme-admins:     { type: group, groups: [platform_t-acme_admin] }
  not-t-acme-admins: { type: group, groups: [platform_t-acme_admin], logic: NEGATIVE }
permissions:
  - resourceType: Groups
    resources: [platform_t-acme_analyst, platform_t-acme_engineer]
    scopes: [view, view-members, manage-membership, manage-membership-of-members]
    policies: [t-acme-admins]
  - resourceType: Groups
    resources: [platform_t-acme_admin]
    scopes: [manage-membership]
    policies: [not-t-acme-admins]
adminRoles: [query-users, query-groups]
```

```yaml
# access-review/privileged-register.yaml (generated; reviewed each access-review cycle)
- { tier: 0, realm: master, account: <named emergency administrator>, owner: platform operations lead, last_use: <date> }
- { tier: 1, realm: hkm, account: svc-hkm-terraform, rights: realm-management, owner: platform identity team }
- { tier: 2, realm: hkm, account: <t-acme institution administrator>, rights: FGAP v2 membership scopes on platform_t-acme_*, owner: t-acme IT }
- { tier: machine, realm: hkm, account: svc-spark-etl, owner: t-acme-data-engineering }
```

### Attributes for attribute-based access

Source: Keycloak 26.8.2 documentation, Server Admin: Mitigating security threats, Workflows, Managing users and the user profile; Keycloak – IAM for Modern Applications, ch. 8, 10; Cloud Native Data Security with OAuth, ch. 4, 9 (merged 2026-11-17).

1. Store in Keycloak only stable attributes shared by all products (business identifier, tenant, country, broad roles) and leave volatile, product-specific attributes to the owning service. Why: remodelling identity data for every product slows the identity team and breaks claims. (Cloud Native OAuth ch. 4)
2. Put every attribute that drives tenant or authorization decisions (for example `institution`) on the server-level read-only attribute lists for users, and for administrators where needed, and set its Who can edit to Admin. Why: users must not change access-driving metadata, and workflows or mappers that read it would otherwise trust user input. (Keycloak docs 26.8, p. 4, p. 10, p. 13; Solving IdM ch. 14)
3. Map a user attribute into tokens, UserInfo or introspection only through an explicit mapper, only when an application needs it, and only from the documented contract set. Why: attributes stay private until mapped, and ABAC on free-form attributes is hard to review. (Keycloak IAM ch. 8, 10)
4. Define the meaning of every attribute a policy uses (for example primary versus current department) before a policy relies on it. Why: a meaning that differs between issuer and policy author becomes a security bug. (Cloud Native OAuth ch. 9)

Example (server option name to confirm against the Keycloak docs):

```yaml
# Keycloak CR additionalOptions
- name: spi-user-profile--declarative-user-profile--read-only-attributes
  value: "institution,tenant*"
```
