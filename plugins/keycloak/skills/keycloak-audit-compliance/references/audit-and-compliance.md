# Audit and compliance

Status: merged from Keycloak docs 26.8, Keycloak – IAM for Modern Applications, Identity standards (CISA/NSA ESF IAM) and Cloud Native Data Security with OAuth (2026-11-17); Solving Identity Management, API Security in Action and Türkiye mevzuatı (BG Rehberi 1.1, KVKK Rehberi), Identity Attack Vectors, OAuth 2 in Action, Learning Digital Identity and Zero Trust Networks (2026-11-18); no pending sections. Retention periods are still open in the platform decisions; rules marked "retention open" require a defined period without setting one.

## Starter rules

1. Turn on Save events for user events and for admin events (with Include representation) in `master`, `hkm` and `hkm-public`, ship them, together with Ranger decision logs, to a restricted log store, and review them regularly. Why: neither stream is stored by default, only error events reach the log, the database event store is for troubleshooting, not evidence, and administrator, operator and user activity must all be traceable and reviewed. (plugin v1; Keycloak docs 26.8, p. 12; Cloud Native OAuth ch. 9, 11; BG Rehberi 3.1.8.1, 3.1.8.2, 3.1.8.6; KVKK Rehberi 3.2, p. 22-23; Identity Attack Vectors ch. 7, 8, 9, App. A; Learning Digital Identity ch. 7; HKM D20)
2. Protect the integrity of authentication, administration and data-access logs: keep the full event in OpenSearch and send only event id, time, actor `sub` and a hash of each event to immudb, so that no record can be deleted or changed unnoticed. Why: log records must be tamper-evident, and Keycloak has no built-in immutable store. (Keycloak docs 26.8, p. 9, p. 12; BG Rehberi 3.1.8.4, 3.2.3.2; KVKK Rehberi 3.2, p. 22-23; Learning Digital Identity ch. 16; HKM D19)
3. Review platform group memberships and administrative rights at least quarterly, including every holder of `admin` and `realm-admin`, review every authorisation at least once a year, revoke what is no longer needed, and require sign-off for new privileges. Why: access drifts as people move, the guide sets a yearly minimum for all authorisations, and FGAP never restricts `admin` and `realm-admin`. (research: quarterly interval; Keycloak docs 26.8, p. 9; Cloud Native OAuth ch. 9; Solving IdM ch. 15; BG Rehberi 3.1.12.8, 3.1.12.20; KVKK Rehberi 3.1, p. 20-21; Identity Attack Vectors ch. 5, 21)
4. Treat event IP addresses, cookies, correlation identifiers, public keys bound to a user, user attributes, lists of user ids and account inventories, and other operational data (sessions, tokens, logs) as personal data kept only as long as a retention schedule allows. Why: KVKK applies to identity data and logs, these values count as personal data even when they do not always identify a person, a list of identifiers reveals identities and personas, and data is kept only as long as its purpose needs it. (Cloud Native OAuth ch. 4; Solving IdM ch. 11; KVKK Rehberi 2.4, p. 16-17; Identity Attack Vectors ch. 6; Learning Digital Identity ch. 8) Retention open.
5. Keep an inventory of every identity system (realms, institution IdPs brokered by organizations, LDAP federations) and of every client integrated with them. Why: the guide requires an inventory of identity providers and the applications that rely on them. (BG Rehberi 3.1.12.13)

## Rules by topic

### What to log and how

Source: Keycloak 26.8.2 documentation, Server Admin: Configuring auditing to track events; Cloud Native Data Security with OAuth, ch. 7, 9 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 11, 17, 19; API Security in Action, ch. 3; BG Rehberi 3.1.8, 3.1.12, 3.2.1–3.2.3; KVKK Rehberi 2.4; Identity Attack Vectors, 2nd ed. (Haber, Rolls, 2024), ch. 20; OAuth 2 in Action, ch. 7, 10; Learning Digital Identity (Windley, 2023), ch. 8, 11, 12; Zero Trust Networks, 2nd ed. (Rais, Morillo, Gilman, Barth, 2024), ch. 6 (merged 2026-11-18).

1. Set an explicit user event expiration in every realm and record it in the Terraform module, keep the evidence in OpenSearch for a defined period, and delete it securely when that period ends. Why: the documentation gives no default, and the guide requires records to be kept for a set time and then disposed of. (Keycloak docs 26.8, p. 12; BG Rehberi 3.1.8.1; KVKK Rehberi 2.4, p. 16-17) The value waits for the retention decision.
2. Save every event type the detection rules need: login, logout, code to token, refresh token, client login of service accounts, both brute force lockout events, federated identity link override events, OAuth2 extension grant events and credential and email change events, each with its error variant. Why: only saved types are kept, the error variant carries the failure signal, and every successful and failed authentication attempt of users and systems must be recorded. (Keycloak docs 26.8, p. 12; BG Rehberi 3.1.12.3, 3.2.1.3; OAuth 2 in Action ch. 10)
3. Use `UPDATE_CREDENTIAL` and `REMOVE_CREDENTIAL` in saved types and detection rules instead of the password- and TOTP-specific types. Why: Update Password, Update TOTP and Remove TOTP are deprecated, and the credential events cover both. (Keycloak docs 26.8, p. 12)
4. Make every event carry time, realm, user or client, source IP, operation and target (the resource path for admin events, the details for user events), cap admin event representations with the JPA event store max field length option only after a trial shows it still exists (the 26.8 Server Admin guide lists it, but the upgrading guide removed it in 23.0 and the 26.8.2 code lacks it), verify that no secrets reach representations, and limit OpenSearch access to authorised staff. Why: the guide sets these minimum fields and restricts review to authorised people, login events reveal a user's activity across applications, full representations can put large amounts of data in the database, and Keycloak offers no masking. (Keycloak docs 26.8, p. 12; Solving IdM ch. 11, 19; BG Rehberi 3.1.8.4, 3.1.8.6; Identity Attack Vectors ch. 20; HKM D20; research: Keycloak 26.8.2 documentation and source, 2026-11-18)
5. Keep the logging event listener enabled, raise its success level so successful events reach the log, and ship the `org.keycloak.events` category as JSON to OpenSearch, setting the log options in the Keycloak CR. Why: by default the listener writes error events only, and these are server options, not realm settings. (Keycloak docs 26.8, p. 12)
6. Never write bearer tokens, session identifiers, passwords or client secrets to any log or error message, mask the Authorization header in component, gateway and ingress logs, and scan logs regularly for leaked secrets. Why: tokens, identifiers and cleartext passwords leak through logs, allow session takeover and turn incidents into breaches. (Cloud Native OAuth ch. 7; Solving IdM ch. 19; BG Rehberi 3.2.2.2; OAuth 2 in Action ch. 7, 10; Zero Trust Networks ch. 6)
7. Log every data-access decision, denied requests included, with a decision id and request context, audit both policy changes and enforcement, protect these records from deletion and change, and make each decision traceable to the user's Keycloak login by `sub` and time. Why: access to critical data must be traceable, denied attempts are attack evidence, and a decision id links one response to its full evaluation record. (Cloud Native OAuth ch. 9; API Security in Action ch. 3; BG Rehberi 3.2.3.2; Learning Digital Identity ch. 12)
8. Enable the email event listener only in realms whose users have verified addresses, and add the lockout events to it explicitly if users should be told. Why: it sends nothing to unverified addresses, and lockout events are off unless included. (Keycloak docs 26.8, p. 12; Solving IdM ch. 17)
9. Never use Clear user events or Clear admin events in production; let the expiration setting delete events. Why: clearing removes all stored evidence at once. (Keycloak docs 26.8, p. 12)
10. Synchronise Keycloak nodes, the log shipper and OpenSearch to the same redundant time source. Why: events can only be correlated when their timestamps agree. (BG Rehberi 3.1.8.3)
11. Alert when the event store or the OpenSearch indices approach their capacity, and when Keycloak events stop arriving in OpenSearch for a realm. Why: a full disk or a broken shipper loses evidence silently. (BG Rehberi 3.1.8.5, 3.1.8.6)
12. Give read access to events that identify users (user, source IP, time) only to a named security role, open them to others only under a written trigger such as an incident investigation or a legal request, log and review every access to the event store, and build capacity and tenant dashboards from aggregates without user identifiers or IP addresses. Why: identity in audit data that is revealed only when needed keeps privacy while preserving accountability, and access patterns in raw login events enable traffic analysis and spear phishing. (Learning Digital Identity ch. 8, 11; HKM D20)

Example (expiration is a placeholder until the retention decision):

```hcl
resource "keycloak_realm_events" "hkm" {
  realm_id                     = keycloak_realm.hkm.id
  events_enabled               = true
  events_expiration            = var.user_event_retention_seconds
  admin_events_enabled         = true
  admin_events_details_enabled = true
  events_listeners             = ["jboss-logging"]
  enabled_event_types = [
    "LOGIN", "LOGIN_ERROR", "LOGOUT", "LOGOUT_ERROR",
    "CODE_TO_TOKEN", "CODE_TO_TOKEN_ERROR", "REFRESH_TOKEN", "REFRESH_TOKEN_ERROR",
    "CLIENT_LOGIN", "CLIENT_LOGIN_ERROR",
    "USER_DISABLED_BY_TEMPORARY_LOCKOUT", "USER_DISABLED_BY_PERMANENT_LOCKOUT",
    "UPDATE_CREDENTIAL", "UPDATE_CREDENTIAL_ERROR", "REMOVE_CREDENTIAL", "REMOVE_CREDENTIAL_ERROR",
    "UPDATE_EMAIL", "UPDATE_EMAIL_ERROR",
  ]
}
```

```yaml
# Keycloak CR additionalOptions (server options)
- { name: spi-events-listener--jboss-logging--success-level, value: info }
- { name: log-console-output, value: json }
```

```json
{ "name": "hkm-keycloak-events-missing",
  "schedule": { "period": { "interval": 5, "unit": "MINUTES" } },
  "inputs": [ { "search": { "indices": ["keycloak-events-*"],
    "query": { "size": 0, "query": { "bool": { "filter": [
      { "term":  { "realmId": "hkm" } },
      { "range": { "time": { "gte": "now-<window>" } } } ] } } } } } ],
  "triggers": [ { "name": "no-events", "severity": "2",
    "condition": { "script": { "source": "ctx.results[0].hits.total.value == 0" } } } ] }
```

### Detection and response for identity attacks

Source: Keycloak 26.8.2 documentation, Server Admin: Configuring auditing, Mitigating security threats, Workflows; CISA/NSA ESF IAM Recommended Best Practices for Administrators; Cloud Native Data Security with OAuth, ch. 3, 11 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 17; BG Rehberi 3.1.8.7-3.1.8.8, 3.1.12.11, 3.1.12.18; Identity Attack Vectors, 2nd ed., ch. 5, 6, 9, 10, 12, 13, 15, 21, App. A; Zero Trust Networks, 2nd ed., ch. 4, 5, 6, 10 (merged 2026-11-18).

1. Feed `LOGIN_ERROR` events with their client IP to OpenSearch alerting or an IPS, pair them with the lockout events, alert on a paced failure-rate threshold per user and per realm rather than on single failures, alert on failures spread across many accounts from one source with few attempts each (password spraying) and on a successful `LOGIN` that follows such a pattern, and never rely on brute force lockout alone. Why: lockout can be abused to deny service, spraying stays below every per-account lockout and shows only across accounts, the documented bot defence is blocking the source IP, and responders need early warning without floods. (Keycloak docs 26.8, p. 12, p. 13; Cloud Native OAuth ch. 3, 11; Solving IdM ch. 17; Identity Attack Vectors ch. 10; Zero Trust Networks ch. 10; HKM D30)
2. Alert on every creation or deletion of an administrator account, on every login attempt by an administrator account, on every admin role grant, and on any account creation, group membership change, role grant, or change to a client, identity provider, user federation provider or organization IdP link that has no matching Terraform change or approved request. Why: administrator accounts are the highest-value target, attackers add accounts and privileges for persistence, and a rogue identity provider lets an attacker sign in as any user even after that user's password is reset. (CISA/NSA ESF IAM, Identity Governance: Why It Matters; BG Rehberi 3.1.12.11; Identity Attack Vectors ch. 10, 12, 21; HKM D17)
3. Run two analyses: at rest, reconcile Keycloak group and role mappings, FGAP v2 permissions, clients and identity providers with the Terraform state and the last access review; at runtime, baseline normal login behaviour and role or group changes of administrators and users from Keycloak events, combine these baselines with fixed rules instead of relying on either alone, weigh location lightly and plan for legitimate deviations such as travel, and alert on deviations, which also catches misuse of correctly granted access. Confirm a suspicious admin action with a person before responding; the security team may end sessions without approval, while disabling an account needs a second person. Why: the first analysis finds wrong entitlements and the second finds misuse of correct ones, fixed rules miss unexpected attacks while learned baselines misjudge new but valid behaviour, location data is often wrong, and unusual admin activity may also be a real emergency. (CISA/NSA ESF IAM, IAM Auditing and Monitoring: Actions to Take Now; BG Rehberi 3.1.12.18; Identity Attack Vectors ch. 5, 9, 10; Zero Trust Networks ch. 4, 5, 6; HKM D49)
4. Alert on `WorkflowStepFailedEvent` and on `org.keycloak.models.workflow` errors. Why: failed workflow steps retry on every run without limit. (Keycloak docs 26.8, p. 10)
5. Run detection as a defined process: correlation rules in OpenSearch for at least repeated failed logins, administrator role grants, login attempts on disabled accounts, the first login of an account after long inactivity, pairs of logins whose locations cannot both be true in the time between them, and sudden rises in a user's login or token requests, each with an owner and a written response step, over the user and admin events of `master`, `hkm` and `hkm-public` correlated across realms and accounts and with group and client state from Terraform, Ranger audit and OpenBao audit in one view per institution and application; review and tune the rules on a fixed schedule. Why: the guide requires SIEM-style analysis of events, isolated events are dismissed while an intruder keeps moving, reactivated dormant accounts and impossible travel are common signs of fraudulent access, no single product sees the whole identity infrastructure, and untuned rules either miss attacks or flood responders. (BG Rehberi 3.1.8.7-3.1.8.8; Identity Attack Vectors ch. 9, 13, 15; Zero Trust Networks ch. 4, 5, 6)
6. Run scheduled posture checks next to the detection rules and review their output: an inventory of every account and service-account client in `master`, `hkm` and `hkm-public`, tagged human or machine, with age, last login and second-factor status; local `hkm` accounts (federation › Federation vs brokering rule 17); human accounts without a second factor; dormant accounts and any login on one; disabled users that still hold sessions or offline tokens; accounts whose email domain matches no registered domain of their organization; privileged holders without an owner or recent use; and any confidential client that still uses a client secret. Why: prevention removes attack paths before an attacker uses them, an account nobody knows about cannot be monitored or removed, and these hygiene gaps are what attackers exploit first. (Identity Attack Vectors ch. 9, 13, App. A; HKM D12, D22, D31)
7. Classify every account as super administrator (`master`), realm administrator, institution administrator, automation client, service account, application client, third party, break-glass or daily user, give each class its own expected behaviour and detection rules, and alert when a service account or automation client acts outside its task. Why: each class is attacked differently and carries a different impact, and a machine identity has no reason to behave like a person. (Identity Attack Vectors ch. 6, 10)
8. Alert on credential and contact changes that point to account takeover: `UPDATE_EMAIL`, `UPDATE_CREDENTIAL` or `REMOVE_CREDENTIAL` followed by a login from a new source, and, on administrator accounts, a second factor added and then removed within 6 months, more authenticators than the two expected passkeys, or all factors reset from an unusual address. Why: these are the signals of account takeover and of help-desk MFA-reset attacks, and they show before the attacker uses the account. (Identity Attack Vectors ch. 10, 12; HKM D26, D48)
9. Alert on administrative actions in sessions that did not reach `gold`, that arrive through an anonymising proxy or from an address outside the admin network, or that follow right after a second-factor prompt; until `acr` is confirmed in admin event details, correlate the admin event with the user's `LOGIN` event. Why: misuse of a stolen administrator session showed exactly these signals. (Identity Attack Vectors ch. 12; HKM D26)
10. Record every identity risk finding in one structure (name, description, occurrences, risk level from critical to informational, affected identity, identity provider, detection source and remediation) and build it into the OpenSearch rule template. Why: a common structure lets findings from different rules be compared and prioritised. (Identity Attack Vectors ch. 21)

Example (OpenSearch alert query fragment):

```json
{ "query": { "bool": {
  "filter": [
    { "term":  { "realmId": "hkm" } },
    { "terms": { "operationType": ["CREATE"] } },
    { "terms": { "resourceType": ["USER", "GROUP_MEMBERSHIP", "REALM_ROLE_MAPPING"] } } ],
  "must_not": [ { "term": { "authDetails.clientId": "svc-hkm-terraform" } } ] } } }
```

```json
{ "name": "hkm-password-spraying",
  "severity": "high",
  "identity_provider": "keycloak realm hkm",
  "detection": "LOGIN_ERROR from one ipAddress on many distinct userId values",
  "remediation": "rate-limit the source at the ingress; check for a later LOGIN from that address",
  "query": { "size": 0, "query": { "bool": { "filter": [
      { "term":  { "realmId": "hkm" } },
      { "term":  { "type": "LOGIN_ERROR" } },
      { "range": { "time": { "gte": "now-<window>" } } } ] } },
    "aggs": { "by_ip": { "terms": { "field": "ipAddress" },
      "aggs": { "users": { "cardinality": { "field": "userId" } } } } } } }
```

### Access reviews and identity governance

Source: CISA/NSA ESF IAM, Identity Governance; Cloud Native Data Security with OAuth, ch. 9 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 4, 15, 19; API Security in Action, ch. 3, 8; BG Rehberi 3.1.12.1, 3.1.12.8, 3.1.12.9, 3.1.12.17; KVKK Rehberi 3.1; Identity Attack Vectors, 2nd ed., ch. 5, 6, 7, 8, 11, 13, 14, 18, 19, 21, App. A; Learning Digital Identity, ch. 7, 12, 16; Zero Trust Networks, 2nd ed., ch. 4, 6, 11 (merged 2026-11-18). The review interval is starter rule 3.

1. Automate joiner, mover and leaver handling for every population: remove old group memberships when a person moves, and disable a leaver's account at its source and revoke their sessions without delay, also for contractors whose duties change or end and for unplanned absences (long illness, extended leave, death) that the institution reports. Why: privileges pile up, prompt termination after departure is a stated requirement, and the accounts of leavers and absent people are prime targets. (CISA/NSA ESF IAM, Identity Governance: What it Does; Cloud Native OAuth ch. 9; Solving IdM ch. 4, 15; BG Rehberi 3.1.12.8; KVKK Rehberi 3.1, p. 21; Identity Attack Vectors ch. 6, 7, 11, 21; HKM D22)
2. Grant privileged access just in time, with no standing administrator rights for people or API accounts: keep privileged admin groups and roles empty by default, add a person only for an approved task through a Terraform change that names the requester, the ticket and the end date, remove the grant when the task or its window ends, or earlier on suspicious behaviour (the primary session ends, sensitive data or unexpected accounts are touched, lateral movement is attempted), and certify after each window that no other privilege changed. Enforce segregation of duties, at minimum keeping audit-log readers (view-only event roles such as `view-events`) apart from realm administrators and treating platform administrator and audit-log manager as mutually exclusive in every access review; segregate and review partner accounts, have owners review access periodically, and alert on unusually high-volume reads. Why: admin rights are limited to need, Keycloak has no built-in time-bound grant, fewer standing privileges mean less damage and less lateral movement, temporary access must stay traceable, one person holding both roles can hide their own actions, and partner access and insiders cause major breaches. (CISA/NSA ESF IAM, Identity Governance: Why It Matters; Solving IdM ch. 19; API Security in Action ch. 3, 8; KVKK Rehberi 3.1, p. 21; Identity Attack Vectors ch. 5, 7, 8, 11, 13, 18, 19, App. A; Learning Digital Identity ch. 7, 12; HKM D17)
3. Publish a written access control policy, review it periodically, and handle every account opening, change and closure and every access request through a tracked process: a reviewed Terraform pull request, or a ticket where the change is not in code. Why: account operations must follow a defined process that leaves a record. (BG Rehberi 3.1.12.1; KVKK Rehberi 3.1, p. 20; Identity Attack Vectors ch. 5, 8; Learning Digital Identity ch. 16; HKM D17)
4. Give every workload its own client service account with the least privilege it needs, a named owner and a review date recorded on the client, monitor its activity (`CLIENT_LOGIN` events) continuously, review service accounts periodically, and never run a job with a personal or administrator account. Why: shared or over-privileged service identities hide who is accountable, and non-person entities often cannot use MFA, so their use must be watched instead. (BG Rehberi 3.1.12.17, 3.2.1.2; Identity Attack Vectors ch. 6, 8, 14; Zero Trust Networks ch. 6, 11; HKM D12)
5. Give external parties (vendors, contractors) access only after approval by platform staff, for a limited period ending with the account being disabled, with MFA, over TLS only, limited to the clients and groups they need, and with their events logged and alerted. Why: the guide sets these conditions for third-party remote access. (BG Rehberi 3.1.12.9; Identity Attack Vectors ch. 11; HKM D9)
6. Produce two reports per organization for auditors, who can access (holders of admin roles, FGAP v2 permissions and `platform_{tenant}_{role}` groups, from Keycloak and Terraform state) and who did access and what they did (admin and user events of privileged accounts), and track privileged access over time with live and historical indicators. Why: auditors usually ask for both views, and indicators show whether privileges are actually being managed. (Identity Attack Vectors ch. 8)
7. Record for every access policy who (person, group or service identity), what (application or API), when (time window, if any), where (resource), why (justification) and how (traffic handling), and approve no policy whose justification is empty. Why: the justification is the evidence that compliance and access reviews ask for. (Zero Trust Networks ch. 4)

Example (`hkm.owner` and `hkm.review-due` are platform attribute names, not provider fields):

```hcl
resource "keycloak_openid_client" "svc_spark_etl" {
  realm_id                  = keycloak_realm.hkm.id
  client_id                 = "svc-spark-etl"
  access_type               = "CONFIDENTIAL"
  client_authenticator_type = "federated-jwt"
  service_accounts_enabled  = true
  standard_flow_enabled     = false
  extra_config = {
    "hkm.owner"      = "t-acme-data-engineering"   # accountable team
    "hkm.review-due" = "2027-05-31"                # next service-account review
  }
}
```

```yaml
# policies/t-acme/analyst-read.record.yaml (kept next to the Ranger policy in Git)
who:   group platform_t-acme_analyst (realm hkm, organization t-acme)
what:  trino via superset
when:  no time limit; reviewed yearly
where: iceberg.t_acme.*
why:   t-acme analysts build monthly service reports (request <ticket id>)
how:   select only; column masks from Ranger
```

### Privacy and personal data in the IdP

Source: Keycloak 26.8.2 documentation, Server Admin: Managing users and the user profile, Workflows; Keycloak – IAM for Modern Applications, ch. 10, 14 (merged 2026-11-17); Solving Identity Management in Modern Applications, ch. 1, 4, 15, 17, 20; KVKK Rehberi 2.4, 3.1; Identity Attack Vectors, 2nd ed., ch. 20; Learning Digital Identity, ch. 4, 8, 16 (merged 2026-11-18). KVKK legal basis, consent records, retention periods and data subject rights are still open (platform decisions, "Still open"); no rule sets them.

1. Collect, store and release in tokens only accurate personal data that a documented purpose needs, record which law applies to each attribute, and require no account at all where a task needs none, for example reading public CKAN datasets without a `hkm-public` account. Why: every stored or released attribute is exposure if Keycloak or a token leaks, an account that is never created cannot be stolen, and the guide limits data to what the purpose requires. (Keycloak IAM ch. 10, 14; Solving IdM ch. 1, 4, 20; KVKK Rehberi 2.4, p. 16-17; Identity Attack Vectors ch. 20; Learning Digital Identity ch. 4, 8)
2. Delete a user account once access has ended and the retention period for its data is over, under the retention and destruction policy (saklama ve imha politikası), and give the `delete-account` role only where self-deletion is intended. Why: deletion removes the profile and its data, while disabling keeps the audit trail until then. (Keycloak docs 26.8, p. 4, p. 10; Solving IdM ch. 15, 17; KVKK Rehberi 2.4, p. 16-17; Learning Digital Identity ch. 8; HKM D22) Retention open.
3. In `hkm-public`, once the destruction policy sets a grace length, replace immediate self-deletion with "disable now, delete later": disable the account at the user's request, email the user that deletion starts, and let a scheduled Workflow `delete-user` step remove it when the grace period ends; until then keep Keycloak's Delete Account with its confirmation. Why: a grace period lets a user undo a mistaken or forced deletion, and Keycloak's Delete Account removes the account at once. (Solving IdM ch. 15; Keycloak docs 26.8, p. 4, p. 10; decision 2026-11-18)
4. Remove personal identifiers (username, `sub`, email, IP address) from metrics, traces, support bundles and analytics exports before they leave the platform, for example in a vendor support case. Why: privacy law can forbid sending data that carries granular identity details, and identity data stays on premises. (Identity Attack Vectors ch. 20; HKM D25)
5. Release identity data to a party outside the platform (a third-party client in `hkm-public` or an external service) only through a flow the user sees, the login redirect and, for third-party clients, the consent screen, only under a written agreement that binds the recipient to the purpose and confidentiality the platform promised, and never through an undisclosed server-to-server lookup. Why: hidden transfers remove the transparency that user control needs, and shared data must keep the promise under which it was collected. (Learning Digital Identity ch. 4, 8; HKM D20) Whether consent is a legal basis stays open.
6. Build privacy into configuration at design time: state the privacy requirement (what is released, to whom and why) in the pull request of every new client, claim or attribute, make the most private setting the default (optional rather than default scopes, Unmanaged Attributes Disabled, consent on for third-party clients), and enforce each decision in Terraform, client policies and user profile permissions, not only in written policy. Why: designers, not users, must judge the privacy effect of a setting, and privacy that relies on policy alone erodes through user and administrator error. (Learning Digital Identity ch. 8, 16)

### Tenant exit and deletion evidence

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 15 (merged 2026-11-18). Retention periods are still open in the platform decisions; no rule sets one.

1. Keep, for every closed account, a minimal closure record (username, `sub`, date and time, reason) together with the admin events of each leaver step, encrypted and readable only by authorised staff, for the period the destruction policy sets. Why: fraud can surface months after closure, while privacy law limits retention to what is needed. (Solving IdM ch. 15; HKM D19)
2. Require reauthentication or step-up before any self-service export of a user's data. Why: an unattended session must not leak the data. (Solving IdM ch. 15)
3. For a departing institution's bulk export, require approval by two people of that institution and deliver the export directly to it without platform staff handling the data. Why: a single actor could extract sensitive data, and manual dumps add transfer risk. (Solving IdM ch. 15)
4. Be ready to issue a certificate of deletion (of secure deletion for sensitive data) when an institution leaves, and where secure deletion is required sanitize media following NIST SP 800-88 rather than relying on a database delete. Why: institutions use the certificate to show due diligence, and a delete may only remove pointers. (Solving IdM ch. 15)

### Regulatory mapping

Source: Solving Identity Management in Modern Applications, 2nd ed. (Wilson, Hingnikar, 2022), ch. 1, 20; BG Rehberi 1.1 (Siber Güvenlik Başkanlığı, 01.03.2026), 3.1.8, 3.1.12, 3.2.1, 3.2.2, 3.2.3; KVKK Kişisel Veri Güvenliği Rehberi (KVKK Yayınları No: 72), 2.4, 3.1, 3.2; Identity Attack Vectors, 2nd ed., ch. 21; Learning Digital Identity, ch. 8; Zero Trust Networks, 2nd ed., ch. 10, 11 (merged 2026-11-18).

1. Classify each obligation as legislated (KVKK, BG Rehberi), industry or elective (for example ISO 27001), and confirm early which apply, including requirements for serving public institutions. Why: mandatory obligations shape the design, elective ones are a choice. (Solving IdM ch. 20)
2. Record for each identity data flow whether the platform acts as data controller (veri sorumlusu) or processor (veri işleyen). Why: the role sets the privacy obligations. (Solving IdM ch. 20)
3. Keep a data map of every identity store and flow (Keycloak user profile and database, events, backups, OpenSearch, immudb, application user tables); for each personal data element record its purpose, how it was collected, its owner and custodian, who reads it, where it is stored, its retention, any transfer to third parties, the backups that hold it, whether access to it is logged, and the obligations of its data set; review the map with every new client or attribute and in the access review, and collect only the minimum. Why: the inventory is the first compliance step and the basis for every later answer to an auditor, and transparency and independent verification need recorded answers. (Solving IdM ch. 1, 20; Learning Digital Identity ch. 8) Retention open.
4. Add the security duties contained in privacy law to the platform's security roadmap. Why: privacy laws also impose security requirements. (Solving IdM ch. 20)
5. Keep the mapping below current: each BG Rehberi or KVKK requirement names the Keycloak control that meets it, the reference rule that defines the control and the evidence an auditor checks (Terraform code in Git, event queries, membership exports); name an owner per evidence category, check periodically that the evidence is being produced, and update the row in the same pull request that changes the rule. Why: the guide's measures are audited one by one, evidence must come from configuration and records rather than statements, and some certifications need a year of past evidence. (Solving IdM ch. 20; BG Rehberi 3.1.12; Identity Attack Vectors ch. 21; Learning Digital Identity ch. 8)
6. Record the guide's level (1 basic, 2 extended, 3 advanced) for each requirement, and implement all three levels on the platform unless the owner records a lower target. Why: the summaries apply every level to Keycloak, and an undocumented lower level cannot be defended in an audit. (BG Rehberi 3.1.8, 3.1.12, 3.2.1–3.2.3)
7. Mark requirements that Keycloak cannot meet alone (time synchronisation, script-tool restriction on endpoints, recording of host admin sessions) with the platform component that owns them. Why: an auditor must see who owns every requirement, also those outside the IdP. (BG Rehberi 3.1.8.3, 3.1.12.12, 3.1.12.19)
8. Self-assess before an external audit, map controls across frameworks so one control serves several, and pursue one certification at a time. Why: overlapping requirements let one control satisfy several frameworks. (Solving IdM ch. 20; Zero Trust Networks ch. 11)
9. Cite NIST SP 800-207 as the architecture reference in the platform's compliance documents and map its components: Keycloak (claims) and Ranger (data decisions) together form the policy decision point, engines and gateways are policy enforcement points, and Keycloak is the identity management system; cite UK NCSC principles, EU Directive 2022/2555, CSA and The Open Group only as supporting references. Why: SP 800-207 is vendor neutral and defines the components, while the others give principles rather than a reference architecture. (Zero Trust Networks ch. 11; HKM D2)
10. Record which trust algorithm the platform uses: criteria based (every flow condition, the required level and the Ranger policy must all hold), not score based, and whether a decision is singular or also draws on stored history. Why: score-based and contextual algorithms need tuning and stored history, and an auditor must know which kind applies. (Zero Trust Networks ch. 11; HKM D2)
11. Record the platform's target level for the identity pillar of the CISA Zero Trust Maturity Model (traditional, initial, advanced, optimal) with the Keycloak controls behind it: automated identity lifecycle (Workflows, SCIM, first broker login) for advanced, just-in-time access for optimal. Why: each level corresponds to identity capabilities that can be checked, so a recorded target becomes measurable. (Zero Trust Networks ch. 11; HKM D22)
12. Keep audit and configuration evidence in a form a cyber insurer can check, and treat insurance as risk transfer, never as a control. Why: insurers usually require a security assessment and often exclude incidents caused by poor configuration management. (Zero Trust Networks ch. 10)

Mapping ("→" names the rule that defines the control):

| Requirement | Level | Keycloak control | Evidence |
| --- | --- | --- | --- |
| BG 3.1.8.1–3.1.8.2 logs in every system, kept and disposed of; activity of admins, operators and users | 1 | user and admin events in `master`, `hkm`, `hkm-public` → audit starter 1; What to log 1 | `keycloak_realm_events` in Terraform; OpenSearch retention policy |
| BG 3.1.8.3 time synchronisation | 1 | outside Keycloak: cluster time source → What to log 10 | node time-sync status |
| BG 3.1.8.4 minimum fields, integrity | 1 | event fields; immudb hashes → audit starter 2; What to log 4 | sample event; immudb verification of a hash |
| BG 3.1.8.5–3.1.8.6 storage monitoring, central collection, delivery alert, restricted review | 1–2 | OpenSearch alerts; restricted index access → What to log 4, 11 | alert definitions; OpenSearch role list |
| BG 3.1.8.7-3.1.8.8 correlation and tuning | 2 | OpenSearch correlation rules → Detection 1, 5 | rule list with last tuning date |
| BG 3.1.12.1 access policy, tracked account process | 1 | Terraform pull requests → Access reviews 3 | written policy; Git history |
| BG 3.1.12.2, 3.2.1.2 individual accounts, no shared accounts | 1 | one user per person, one client per workload → authentication › Authentication flows 16 | user and client export |
| BG 3.1.12.3, 3.2.1.1, 3.2.1.9 failed logins, locks, enumeration | 1 | brute force detection, ingress rate limit, administrator disable, uniform answers → authentication › Brute force 1, 13, 17 | realm settings in Terraform; `LOGIN_ERROR` and lockout events |
| BG 3.1.12.4, 3.2.1.6 default accounts | 1 | bootstrap admin removed; break-glass password rotated after use → authentication › Brute force 18; tenancy › Identity domains 1 | `master` user list; OpenBao rotation log |
| BG 3.1.12.5, 3.1.12.11 separate admin identities, MFA, alerts | 1–2 | passkey-only admin accounts; admin alerts → authentication starter 1; Detection 2 | credential report of admins; alert definitions |
| BG 3.1.12.6, 3.2.2.3 idle and maximum session | 1 | SSO Session Idle / Max → authentication starter 3 | realm settings in Terraform |
| BG 3.1.12.8, 3.1.12.20 rights review, yearly review | 1, 3 | membership export and review → audit starter 3; Access reviews 1 | signed review record; membership export per organization |
| BG 3.1.12.9 external parties | 1 | time-limited accounts with MFA → Access reviews 5 | approval ticket; disable date |
| BG 3.1.12.10, 3.2.1.12 dormant accounts | 2 | Workflows notify, then disable → federation starter 4 | workflow definition in Git; disabled-account report |
| BG 3.1.12.13 inventory | 2 | inventory of realms, IdPs, LDAP federations, clients → audit starter 5 | inventory file generated from Terraform |
| BG 3.1.12.14, 3.2.1.13 central authentication, e-Devlet | 2, 3 | every application an OIDC client; e-Devlet only in `hkm-public` → authentication › Authentication flows 1; federation › Federation vs brokering 16 | client list from Terraform; application reviews without local password tables |
| BG 3.1.12.15 MFA for external access | 2 | MFA for every `hkm` login (stricter) → authentication starter 2 | bound browser flow in Terraform |
| BG 3.1.12.16, 3.2.1.4 credential storage and transport | 1–2 | default Argon2 hashing, TLS only → authentication › Brute force 8 | realm hashing policy; TLS configuration |
| BG 3.1.12.17 service accounts | 2 | client service accounts with owner → Access reviews 4 | client attributes `hkm.owner`, `hkm.review-due` |
| BG 3.1.12.18 behaviour and privilege-change monitoring | 3 | OpenSearch baselines and alerts → Detection 2, 3 | alert definitions; alert history |
| BG 3.1.12.20 admin from defined addresses | 3 | admin console on internal hostname only → claims › Coarse vs fine-grained 6; sizing-and-ha starter 5 | ingress configuration |
| BG 3.1.12.21, 3.2.1.7 vault, no secrets in code | 1, 3 | OpenBao; `client-jwt` / `federated-jwt` → operations › Key, certificate and secret rotation 3 | repository secret scan; OpenBao paths |
| BG 3.2.1.3 authentication logs | 1 | `LOGIN`, `LOGIN_ERROR`, `CODE_TO_TOKEN`, `CLIENT_LOGIN` saved → authentication starter 6; What to log 2 | event query per realm |
| BG 3.2.1.5 initial password | 1 | execute-actions email (HKM D8) → authentication › Brute force 10 | admin events show no password set by an administrator |
| BG 3.2.1.8 password policy, maximum age | 1 | password policy; long maximum age (HKM D5) → authentication › Brute force 4, 5 | `password_policy` in Terraform |
| BG 3.2.1.10–3.2.1.11 step-up, recovery | 2 | ACR step-up; recovery via institution IdP or second factor → authentication › Step-up 2; Brute force 11 | flow configuration; client minimum ACR |
| BG 3.2.2.1–3.2.2.2 session identifiers | 1 | new session per login; code flow, no tokens in URLs → authentication › Sessions 7 | client grant settings |
| BG 3.2.2.3 logout, revocation on change | 1 | back-channel logout; session revocation on password or group change → authentication › Sessions 10, 12 | client logout settings; admin events |
| BG 3.2.2.4 cookie flags | 1 | application cookie flags → authentication › Sessions 8 | application review |
| BG 3.2.2.5 user-managed sessions | 3 | account console device activity → authentication › Sessions 5 | account console enabled in both realms |
| BG 3.2.3.1 authorisation matrix, per-request check | 1 | group membership export plus Ranger policies; audience check per API → claims › Groups 10; Coarse vs fine-grained 2 | matrix per organization; API audience settings |
| BG 3.2.3.2 audit of critical data access | 1 | Ranger audit plus Keycloak events, integrity in immudb → What to log 7; audit starter 2 | Ranger audit query; immudb verification |
| BG 3.2.3.3 least privilege | 1 | minimal groups and `aud-*` scopes; no data permissions in roles → claims starter 3, 4 | sample decoded token per client |
| BG 3.2.3.4 context-aware access, rate limits | 3 | Ranger conditions, gateway limits → claims › Groups 7 | Ranger policy export; WSO2 throttling policy |
| KVKK Rehberi 3.1, p. 20-21 need-to-know, matrix, individual authentication, admin when needed, leavers | — | groups, matrix, password policy, brute force, JIT admin, leaver disable → claims › Groups 10; Access reviews 1, 2 | matrix; Git history; disabled-account report |
| KVKK Rehberi 3.2, p. 22-23 user action logs, evidence | — | events to OpenSearch, hashes to immudb → audit starters 1, 2 | event queries; immudb verification |
| KVKK Rehberi 2.4, p. 16-17 minimisation, retention and destruction | — | declarative user profile, event expiration → Privacy 1, 2 | user profile in Git; retention settings (retention open) |
| BG 3.1.12.12 script tools; 3.1.12.19 host admin session recording | 2, 3 | not a Keycloak control (owner: endpoint and host management) | — |

Example (role and obligations are recorded per flow; values illustrative):

```yaml
# compliance/data-map.yaml (excerpt)
- store: keycloak realm hkm, user profile
  element: department
  purpose: assign platform_t-acme_* groups
  role: <controller | processor>      # decided per flow, see rule 2
  obligations: [KVKK, BG Rehberi 3.2.3]
- store: opensearch keycloak-events-hkm
  element: ipAddress
  purpose: brute-force detection, audit evidence
  role: <controller | processor>
  obligations: [KVKK, BG Rehberi 3.1.8]
  owner: platform IAM owner
  custodian: keycloak operations
  backups: keycloak-db nightly
  access_logged: true
```

```yaml
# compliance/zero-trust-map.yaml (excerpt)
reference: NIST SP 800-207
components:
  policy_decision_point: [keycloak realm hkm (claims, acr), ranger admin (data policy)]
  policy_enforcement_points: [ranger plugins in trino and kafka, wso2, kubernetes ingress]
  identity_management: keycloak (realms hkm, hkm-public; organization t-acme)
trust_algorithm: { type: criteria-based, history: <singular | contextual> }   # HKM D2: no score
cisa_ztmm_identity_target: <traditional | initial | advanced | optimal>
```

### Identity policies and decision records

Source: Learning Digital Identity: Design, Deploy, and Manage Identity Architectures (Windley, 2023), ch. 21, 22 (merged 2026-11-18).

1. Keep identity policies (naming and identifiers, authentication, access control, directories, federation, privacy, provisioning, encryption) as their own suite, separate from security policies, and make security policies refer to them; derive each policy top-down from an activity the platform performs (institution onboarding, data access, administrator operations), write no policy that no activity needs, and record for each policy the activities it governs and the BG Rehberi and KVKK items it answers. Why: identity is broader than security, identity rules scattered through security policies contradict each other, and a top-down stack keeps effort on policies that matter. (Learning Digital Identity ch. 21)
2. Record each significant technical choice, including each enabled Keycloak feature and allowed protocol, as an architecture decision record (ADR) in Git with identifier, title, status, context, decision and consequences; cite ADRs from policies by number, keep product names, standards and procedures out of policy text, and enforce each decision through Terraform. Why: ADRs keep the reasoning available when a choice must be justified, changed or retired, these choices set the boundary of what the platform permits, and a policy that states only what is required stays valid as products change. (Learning Digital Identity ch. 21, 22; HKM D17)
3. Base every user term, institution agreement and onboarding decision on a written policy, publish no policy statement that the platform cannot or will not enforce, and map each statement to an enforcement point (a Keycloak setting in Terraform, a client policy, a Ranger policy, a CI check) or to an audit procedure. Why: policy makes allowed behaviour consistent and clear to every participant, and ignored policies weaken the whole policy process. (Learning Digital Identity ch. 21, 22)
4. Write every policy from one template with nine sections (purpose, scope, status, definitions, references, policy, enforcement, contacts, revision history), with numbered paragraphs, short sentences, active voice and contacts named by role; keep the suite in short modular documents written with subject-matter experts, and have HR and legal review the enforcement sections. Why: a common, readable format can be referenced and survives staff changes, modular documents last longer, and without HR and legal review the platform cannot act on violations. (Learning Digital Identity ch. 21)
5. Answer the information governance questions in the identity policy (where identity data is stored, who has access, how long it is kept and how it is destroyed) before teams write policy-based access rules in Ranger. Why: small, automatically enforced access policies must comply with a clear and complete general policy. (Learning Digital Identity ch. 21) Retention open.
6. Prepare policies or ADRs for key areas (encryption, signing key rotation, incident response) before they are needed, keep an expedited procedure for urgent single questions, and scale deliberation to reversibility: hard-to-reverse changes, such as removing passwords for all users, are one-way decisions, quick monitored changes are two-way decisions. Why: business or breach deadlines can outrun the normal policy process and push teams into ad hoc decisions, and the cost of a wrong decision depends on whether it can be undone. (Learning Digital Identity ch. 21)
7. Maintain a policy review framework: a schedule per policy (identifier, title, responsible group, status, approval date, review cycle) and a yearly calendar renewed in the last month of the year, and assess every policy at creation and at each review for completeness, effectiveness against a stated goal, cost of compliance and non-compliance, and risk, including the incident reports that touch it. Why: policies must grow with unforeseen cases and prove that they meet their goal. (Learning Digital Identity ch. 21)
8. Assign enforcement and audit of identity policies to a party other than the Keycloak operations team, run approved, unannounced audits with standard forms that cover IT, executive and `master` emergency administrator accounts, and generate review inputs and segregation-of-duties checks automatically from Keycloak data (membership exports per organization, admin events). Why: one group cannot both serve and police, exempt groups weaken audits, and rule-based checks make audits simpler and less error-prone. (Learning Digital Identity ch. 21, 22)
9. Provide a documented exception path for exigent circumstances, and aim enforcement at reaching compliance through roadmaps, milestone reports and signed compliance statements. Why: policies so strict that work stops will be circumvented. (Learning Digital Identity ch. 21; HKM D17)

Example:

```markdown
# ADR-012: Confidential client authentication
Status: accepted
Context: Identity policy IP-04, paragraph 3 requires asymmetric credentials for confidential clients.
Decision: `client-jwt` for platform clients such as `superset`; `federated-jwt` for workloads such as `svc-spark-etl`.
Consequences: no client secrets for these clients; client policy `hkm-secure` rejects other authenticators.
```
