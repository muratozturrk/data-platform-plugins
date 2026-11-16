# Identity, principals and attributes

Status: starter rules; custom security attributes and group membership filled from the Azure RBAC and ABAC documentation (2026-11-14) and NIST SP 800-162 (2026-11-15); principals section partly filled from NIST; attribute administration separated from policy administration and acyclic group nesting from Attribute-Based Access Control (merged 2026-11-15); identity life cycle, principals and context from Authorization in Action (merged 2026-11-15); impersonation and service accounts written with the decision on conflicts #23 (2026-11-16); usersync from Keycloak, group-removal path and version rules from the Apache Ranger wiki and the verification lab (merged 2026-11-15); name mapping, provisioning and group delivery confirmed by Hadoop Security (merged 2026-11-15); its token and impersonation points written with the decision on conflicts #23 (2026-11-16); Kafka principal mapping, name reuse and reauthentication from Kafka: The Definitive Guide, 2nd ed. (merged 2026-11-15); its Kafka Connect and client identity points written with the decision on conflicts #23 (2026-11-16); Trino authentication and principal-to-user mapping from Trino: The Definitive Guide, 2nd ed. (merged 2026-11-16); its service-client points written with the decision on conflicts #23 (2026-11-16); two-factor sign-in, HR-fed directory and informed membership decisions from Data Governance: The Definitive Guide (merged 2026-11-16); its service-account points written with the decision on conflicts #23 (2026-11-16); single identity provider across institutions from Data Management at Scale, 2nd ed. (merged 2026-11-16); data processors and external maintenance staff from the KVKK Personal Data Security Guide (merged 2026-11-16); their non-human clients written with the decision on conflicts #23 (2026-11-16); impersonation and delegation rules and service accounts and ingest identities from the decision on conflicts #23 (2026-11-16); no section pending.

## Starter rules

1. Normalise principal and group names once to one lowercase canonical form on Keycloak, usersync and every engine; usersync takes each user name from a unique LDAP attribute, never a non-unique one such as `cn`; where a name has to be rewritten (a brokered identity provider, an engine's principal mapping), apply ordered first-match mapping rules with the default rule last and accept names only from trusted issuers; drop only technical realm suffixes, and when institutions use separate realms or brokered identity providers include the institution ID at a fixed position in human user names, with a separator chosen by a lab trial of the characters Trino, Kafka and Ranger accept. Why: policies match on names, names must not collide across institutions, a non-unique source attribute creates duplicate users, and in an ordered mapping the rule order decides the result. (plugin v1; Authorization in Action ch. 13.3.1–13.3.2; Apache Ranger wiki p. 11; Hadoop Security ch. 5; Kafka DG ch. 11; decision 2026-11-15, conflicts #33.)
2. Service principals get their own least-privilege assignments and explicit impersonation rules. Why: they are the widest paths.
3. Revoke at the identity provider first, then reconcile Ranger. Why: the token is the first gate.

### Identity life cycle: joiner, mover, leaver

Source: Authorization in Action, ch. 1, 3, 12, 13 (merged 2026-11-15); Hadoop Security, ch. 5 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §3.1 (merged 2026-11-16).

1. Create human accounts only from an authoritative source: the institution's identity provider, fed from its HR records, federated into Keycloak, or the institution's users hosted in Keycloak, authenticated through OIDC; keep Keycloak the single identity provider in front of every engine and institution, so that a token and the user attributes looked up for it resolve the same way wherever they are used. Why: enrollment attributes become the context later decisions rely on, institutions own their users' life cycle, an HR-fed directory carries job changes into group membership without per-dataset edits, and context lookups across institutions need one identity. (Authorization in Action ch. 3.3.1, 13.3.2; Data Governance DG ch. 7; DMaS ch. 8.)
2. Propagate every create, update and disable from Keycloak to usersync and the engines' group sources, consistently on every enforcing component and as events rather than batches where possible, within the intervals of Group membership rule 3 and by the update path of conflicts #17; provisioning a user for data access never grants shell or host access. Why: an update that misses the authorizer leaves stale permissions, pushed events shorten exposure, and shell access is a separate privilege. (Authorization in Action ch. 3.3.2, 12.4.3; Hadoop Security ch. 5; conflicts #3, #17, #19.)
3. On a mover event, remove the old group memberships in the same change that adds the new ones. Why: accumulated memberships make "who has access to what" unanswerable. (Authorization in Action ch. 1.2.5, 1.2.6, 3.3.4; Data Governance DG ch. 7.)
4. On a leaver event, disable the account and remove all its group memberships automatically on the leave date, keep the identity record as conflicts #11 requires, never reissue its name to another principal, and clean up dormant user accounts on a schedule; when a service client is retired, remove its direct assignments in the same change. Why: orphaned accounts and lingering permissions are where many breaches start, and a reused name inherits every grant and audit trail of the old one. (Authorization in Action ch. 3.3.5; Kafka DG ch. 11; DMaS ch. 8; KVKK guide §3.1; decision 2026-11-14, conflicts #11.)
5. Protect account recovery in Keycloak at least as strongly as the primary sign-in factor. Why: a weaker recovery path becomes the real way in. (Authorization in Action ch. 3.3.4.)
6. Carry a known departure as the Keycloak attribute `leaveDate`, fed from the HR source, and start the leaver steps automatically on that date. Why: a departure known in advance should end access on time without waiting for a ticket. (decision 2026-11-16)

### Usersync from Keycloak: source, cycle and group removal

Source: Apache Ranger wiki, p. 11 Usersync (2016/2022/2024), p. 13 Hardening (2026), p. 14 Release notes 2.9.0 (merged 2026-11-15); lab V4, V6.

1. Sync users, groups and memberships from the LDAP directory that Keycloak writes, never from a file source or the Unix source. Why: a file is a manual snapshot unlinked from Keycloak, and the Unix group builder allows command injection before 2.9.0 (CVE-2026-28672). (Apache Ranger wiki p. 11, p. 13.)
2. Run the LDAP sync cycle at 60 s with the force flag (`ranger.usersync.ldap.force.sleeptimeinmillisbetweensynccycle.enabled=true`). Why: unixusersync 2.8.0 raises any shorter LDAP cycle to 1 h without the flag, which breaks the 5-minute group-removal budget, unlike the wiki's 5-minute example. (Apache Ranger wiki p. 11; trial: verification lab V6, 2026-11-14; conflicts #3, #19.)
3. Change group memberships in Ranger only by updating the user record (the path usersync and the Admin UI use), never through the xusers membership DELETE endpoint, and let a CI or monitoring check compare Keycloak groups with the user store that plugins download. Why: the DELETE endpoint leaves the user-store version unchanged, so plugins kept the removed membership (still allowed after 150 s), on 2.8.0 and 2.9.0 alike; the 5-minute budget is the usersync interval plus the plugin user-store refresh (60 s by default). (trial: verification lab V4, V6, 2026-11-14; decision 2026-11-14, conflicts #17.)
4. Treat group removal through usersync as revocation according to the Ranger version: on 2.9.0 or later it counts within the 5-minute budget, with plugins downloading policies, roles and the user store under Kerberos or JWT credentials and both unauthenticated-download flags false; on 2.8.0 it does not, so the revocation of record is Keycloak disable plus token lifetime, the per-user incident deny of conflicts #5 covers access that must end sooner, and a control-plane job re-saves the user record of every user whose groups changed after each usersync cycle and alerts if a removed membership is still in the plugin user store 2 minutes later. Why: 2.8.0 usersync did not change the user-store version, so Trino kept allowing a removed user for 300 s; 2.9.0 fixed this (RANGER-5463: user store after 38 s, Trino denied after 57 s); the re-save job revoked in about 111 s on 2.8.0. Lab V6 is rerun after every upgrade (security-operations.md › Ranger versions, rule 6). (Apache Ranger wiki p. 11, p. 14; trial: verification lab V6, 2026-11-14/15; decision 2026-11-14, conflicts #19.)

Example (rewritten, monitoring check manifest):
```yaml
kind: SyncCheck
metadata: { id: usersync-group-drift }
spec:
  compare:
    source: keycloak-groups            # e.g. platform_t-acme_analyst members
    target: ranger-plugin-user-store   # what the trino plugin downloaded
  after: PT2M                          # alert window after each usersync cycle (conflicts #19, 2.8.0)
  forbidden: { membershipDeleteEndpoint: true }   # conflicts #17
```

### Engine authentication and the user passed to authorization (Trino)

Source: Trino: The Definitive Guide, 2nd ed., ch. 10 (merged 2026-11-16). Service clients and pipelines: Service accounts and ingest identities (decision 2026-11-16, conflicts #23).

1. Enable authentication and HTTPS on every Trino coordinator, and give all nodes of a cluster the same long random shared secret, stored in OpenBao. Why: without authentication anyone who reaches the coordinator can query under any user name, and the nodes authenticate each other with the shared secret. (Trino DG ch. 10.)
2. Authenticate people on Trino only through Keycloak OAuth2/JWT, never through password files or LDAP binds, and admit only members of the groups allowed on that cluster. Why: the book names OAuth 2.0 and JWT as the modern authentication types and shows that otherwise valid users should be filtered by group. (Trino DG ch. 10.)
3. Map each authenticated principal to exactly one Trino user, its canonical name (conflicts #33), which is the user Ranger evaluates; a principal never runs a query under another user name except through an impersonation rule (trino-security.md › Paths beside the Trino plugin, rule 2). Why: Trino separates the authenticated principal from the query user, and by default any principal can run queries as anyone. (Trino DG ch. 10; conflicts #33.)

Example (rewritten, platform manifest):
```yaml
kind: EngineAuthentication
metadata: { id: trino-shared-auth }
spec:
  engine: trino
  https: true
  authentication: { type: oauth2-jwt, issuer: keycloak }
  sharedSecretRef: "openbao:kv/trino/shared-secret"     # same on every node of the cluster
  allowedGroups: [platform_t-acme_analyst, platform_t-acme_auditor]
  userMapping: canonical-name                           # one principal -> one Trino user (conflicts #33)
```

### Data processors (veri işleyen) and external maintenance staff

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §1.3, §2.5, §3.5, §4.2 (merged 2026-11-16). Non-human processor and vendor clients: Service accounts and ingest identities, rule 7 (decision 2026-11-16, conflicts #23).

1. Give each data processor that works for an institution its own Keycloak group in that institution for the processor's staff (for example `platform_t-acme_proc-payroll`), never membership in the institution's internal groups; its staff authenticate individually under Principals, groups and name mapping, rule 1. Why: a processor is jointly responsible for security, so its actions must be attributable to it and separable from the institution's own. (KVKK guide §2.5.) Note: the processor's non-human clients follow conflicts #23.
2. Assign a processor group only the data roles its contract covers: the assignment description cites the contract (ID, purpose, data categories), the scope covers only the contracted datasets and columns, and the group gets no PII Reader assignment unless the contract names clear personal data; data categories outside the contract stay outside its scope. Why: a processor may act only on the controller's instructions, within the contracted purpose, scope and data categories. (KVKK guide §2.5, §4.2.)
3. End every processor assignment through `validitySchedules` at the contract end date (for an eligible assignment, its eligibility end). Why: access must not outlive the relationship. (KVKK guide §2.5.)
4. Include processor groups in every access review and in the alert scope, and keep their audit records available, through the Auditor's read-only access (audit-design.md › Retention and protection of audit data, rule 1), for the institution's inspection of the processor. Why: the controller audits the processor or has it audited, which needs the processor's actions on record. (KVKK guide §2.5; conflicts #30.)
5. Give external maintenance staff of a component a dedicated eligible group per vendor with management access to that component only and no data role, so that default deny keeps personal data closed to them; activate it for a `validitySchedules` window through an approved request, review every action in the audit review, and never add a vendor-specific deny item. Why: outside maintenance staff must not be able to copy personal data out, and deny stays a platform guardrail. (KVKK guide §3.5; conflicts #1, #8.)
6. Declare each processor's contract number, end date and purpose as mandatory fields in its manifest entry, and let CI reject an entry, or an assignment that refers to it, when a field is missing or the end date has passed. Why: the contract is what justifies the access, and a field that CI checks cannot be forgotten. (KVKK guide §2.5; decision 2026-11-16.)

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0181 }
spec:
  principal: { type: group, id: kc-grp-6100, name: platform_t-acme_proc-payroll }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales/datasets/orders
  description: "Processor contract DPA-0042: invoice reconciliation; categories: order data; PII columns stay masked"
  validity: { end: "2027-03-31" }      # contract end -> validitySchedules on the compiled item
```

### Principals, groups and name mapping

Source: NIST SP 800-162, §3.2.2.2–3.2.2.3 (merged 2026-11-15); Authorization in Action, ch. 6, 10 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7, Appendix A (merged 2026-11-16); KVKK Personal Data Security Guide, §3.1 (merged 2026-11-16).

1. Authenticate every request to one unique principal through Keycloak, at an assurance level that fits the request and with two-factor authentication for every interactive user, before any decision; reject anonymous requests, decide on the authenticated identity rather than on the network a request comes from, and never let several people share one account. Why: a decision and its audit record must be attributable to one individual, and in a shared environment an internal network is no trust signal. (NIST SP 800-162 §3.2.2.2–3.2.2.3; Kafka DG ch. 11; Data Governance DG ch. 7; KVKK guide §3.1.)
2. Treat the right to change membership of any group that holds an assignment, or a decision-driving principal attribute, as an access-administration right: limit it to named administrators of that institution or the authoritative sync, log every change through Keycloak admin events, let operators trace the path from a principal through its groups to the assignment that grants access, and show whoever approves adding a member every assignment the group holds. Why: anyone who can edit the data a policy relies on is effectively a policy administrator, and whether a person may join a group cannot be judged without seeing what the group grants. (Authorization in Action ch. 6.1.2, 6.1.3, 10 p. 190; Data Governance DG Appendix A.)
3. Require no assurance level per data class beyond MFA for every interactive user and the step-up MFA at activation of PII Reader or `RESTRICTED` access (azure-style-rbac-abac.md › Privileged access (eligible, time-bound) and access reviews), and offer no per-session activation of principal attributes: elevation happens only through PIM activation with `validitySchedules`. Why: one assurance model is easier to operate and audit, and session-level attribute switching would add a second, unreviewed path to elevation. (decision 2026-11-16)

### Impersonation and delegated access rules

Source: Authorization in Action, ch. 4.3.1, 11.1.1, 18.6 (merged 2026-11-15); Hadoop Security, ch. 5, 8, 12 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 10 (merged 2026-11-16); decision 2026-11-16, conflicts #23.

1. Allow impersonation, a service querying as an end user, only for interactive clients that act on a present user's request (Superset toward Trino, trino-security.md › Enforcement points on every path, rule 2): the rule names the service client and the groups it may act for, never a wildcard, and its source network where the engine supports it; pipelines, scheduled jobs and other workloads never impersonate a user. Why: a decision taken on the service instead of the user makes the service a confused deputy and cannot be attributed to a person, while a job that runs without a present user has no user to act for. (Authorization in Action ch. 4.3.1, 11.1.1; Hadoop Security ch. 5, 12; Trino DG ch. 10; decision 2026-11-16, conflicts #23.)
2. Record the impersonating service and the end user as separate audit fields, and list every impersonation rule in the access review. Why: an audit record that shows only one of them cannot answer who acted through whom. (Hadoop Security ch. 8; conflicts #14.)
3. Treat a pipeline as the actor, not as a delegate of the user who triggered it: the pipeline's workload identity is the audited principal, the triggering user (for example from Airflow) is written as metadata of the run, and the pipeline's grants are bounded by what the job needs, not by its creator's rights. Why: the rule that a delegate never exceeds the delegator fits interactive use, but a pipeline capped by a person's rights breaks when that person moves or leaves, and long-lived jobs must survive departures. (Authorization in Action ch. 18.6.5; Kafka DG ch. 11; Hadoop Security ch. 8, 10; decision 2026-11-16, conflicts #23.)
4. Use no delegation credentials: grant nobody rights on the Kafka `delegationtoken` resource, issue no Kafka delegation tokens, and do not enable Keycloak's token exchange delegation; a job obtains short-lived, audience-scoped tokens for its own identity only. Why: Kafka delegation tokens are a broker-issued secret beside Keycloak with their own master key and restart-bound rotation, an explicit delegation model (delegator, delegate, scope, expiry) is not needed while no workload acts for a user, and Keycloak lists token exchange delegation as experimental. (Hadoop Security ch. 5; Kafka DG ch. 11; Authorization in Action ch. 18.6; decision 2026-11-16, conflicts #23; (research: Keycloak release notes 26.2, 26.6 and Server Administration Guide › Kubernetes identity providers, https://www.keycloak.org/docs/latest/, 2026-11-16).)

Example (rewritten, platform manifest):
```yaml
kind: ImpersonationRule
metadata: { id: imp-superset-t-acme }
spec:
  service: { type: servicePrincipal, name: svc-superset }   # interactive client only
  engine: trino
  actFor: [platform_t-acme_analyst]                          # listed groups, never "*"
  sourceNetwork: superset-namespace
# pipelines get no ImpersonationRule; they run as their own WorkloadIdentity
```

### Service accounts and ingest identities

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), page 01 (merged 2026-11-14); Attribute-Based Access Control, ch. 2.2, 8.2.2 (merged 2026-11-15); Apache Ranger wiki, p. 3, p. 11, p. 14 (merged 2026-11-15); Hadoop Security, ch. 5, 6, 10 (merged 2026-11-15); Trino: The Definitive Guide, 2nd ed., ch. 6, 10 (merged 2026-11-16); Data Governance: The Definitive Guide, Appendix A (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16); KVKK Personal Data Security Guide, §2.5, §3.1 (merged 2026-11-16); Data Mesh, ch. 12 (merged 2026-11-16); decisions 2026-11-15 and 2026-11-16, conflicts #23. Kafka clients: kafka-security.md › Principals and principal mapping, rules 4 and 5.

1. Use three principal types and no others: a human (Keycloak user, access only through groups), a workload identity (a job or service inside the platform's Kubernetes clusters, with no stored secret; Azure managed identity), and an external service principal (a client outside the clusters, such as a processor, vendor or source system, with Keycloak client credentials kept in OpenBao, an owner and an expiry at contract end; Azure service principal). Why: Azure separates platform-managed credentials from owner-managed secrets, a secret that never exists cannot leak, and a shared or ownerless service login can be neither audited nor revoked. (Azure docs 01; Attribute-Based Access Control ch. 2.2; KVKK guide §2.5, §3.1; decision 2026-11-15, conflicts #23.)
2. Authenticate each workload identity to Keycloak with its Kubernetes ServiceAccount token through federated client authentication, using one dedicated ServiceAccount per Keycloak client and never a namespace's default account, short-lived projected tokens with Keycloak as audience, and standard token exchange only to obtain a token for another audience of the same identity; an exchanged token carries the workload identity, never a user. Why: the cluster already proves the pod's identity, so no client secret has to be stored; Keycloak supports federated client authentication since 26.6 and standard token exchange since 26.2, and warns that a default ServiceAccount is shared by every pod in its namespace. (Hadoop Security ch. 5; decision 2026-11-15, conflicts #23; (research: Keycloak release notes 26.2, 26.6 and Server Administration Guide › Kubernetes identity providers, https://www.keycloak.org/docs/latest/, 2026-11-16).) Note: Airflow tasks get their own identity only when each task runs in a pod with its own ServiceAccount (for example through the Kubernetes executor); tasks run inside the scheduler or a shared worker share that identity.
3. Give each pipeline its own workload identity by default, and a separate one per environment; a shared identity per institution domain needs a written reason in the manifest, and engine runtime accounts hold no standing data access. Why: a shared identity cannot be audited or revoked per pipeline, and each identity limits the damage of one compromised job. (Attribute-Based Access Control ch. 2.2, 8.2.2; Hadoop Security ch. 6, 10; DMaS ch. 8; Data Mesh ch. 12; decision 2026-11-15, conflicts #23.)
4. Create workload identities and external service principals from the Git manifest through the control plane, in Keycloak and in the Ranger user store alike, never through usersync, and assign them directly with an owner and a review date, without group membership. Why: usersync is built for directory users and groups, a non-human principal defined in Git is reviewable and removable in one change, and a service in a shared group inherits rights nobody reviewed for it. (Apache Ranger wiki p. 11; Hadoop Security ch. 5; decision 2026-11-16, conflicts #12, #23.)
5. Identify a workload only by its authenticated principal: route pipelines to the ETL cluster, select resource groups and match policies by the principal from the validated token, never by client-set values such as Trino `--user` or `--source` or a header. Why: client-set values can be chosen by any caller, and header-based authentication trusts an identity the caller asserts. (Apache Ranger wiki p. 14; Trino DG ch. 10; decision 2026-11-16, conflicts #23.)
6. Give no workload identity a mask or row-filter exemption by default (the one exception is the owning pipeline's row-filter item of trino-security.md › Row filters, masks and views on the SQL path, rule 10, conflicts #67); a pipeline that must read personal data in clear holds PII Reader through a separate, documented assignment reviewed every quarter (audit-design.md › Access review process and cadence, rule 11). Why: a named exemption is invisible in ordinary role reviews, and processing that needs clear values should be visible as such. (Apache Ranger wiki p. 3; Data Governance DG Appendix A; decision 2026-11-16, conflicts #23.)
7. Give each external service principal one Keycloak client per processor, vendor or source system, its secret only in OpenBao with rotation, and an expiry at the end of the contract; never share a login across them, and keep the catalog service users at the data sources (trino-security.md › Data source credentials and cluster separation, rule 1) as credentials of this type. Why: KVKK ties every access to an individual credential and ends a processor's access with its contract, and anyone who reads a stored credential opens the source. (KVKK guide §2.5, §3.1; Trino DG ch. 6; decision 2026-11-15, conflicts #23.)

Example (rewritten, platform manifest):
```yaml
kind: WorkloadIdentity
metadata: { id: wi-t-acme-orders-ingest }
spec:
  institution: t-acme
  environment: prod
  kubernetes: { namespace: t-acme-pipelines, serviceAccount: orders-ingest }   # federated client authentication
  keycloakClient: wi-t-acme-orders-ingest
  owner: t-acme data platform lead
  reviewBy: 2027-02-15
---
kind: RoleAssignment
metadata: { id: ra-0207 }
spec:
  principal: { type: workloadIdentity, name: wi-t-acme-orders-ingest }   # direct, no group
  roleDefinition: data-contributor
  scope: /institutions/t-acme/domains/sales                             # published layer: pipeline-only writes
  description: "Orders ingest pipeline; ADR-031"
```

### Custom security attributes: who may assign them

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 11, 13 (merged 2026-11-14); NIST SP 800-162, §2.4.2, §3.1.3.3–3.1.3.4, §3.2.1.6, §3.4.1 (merged 2026-11-15); Authorization in Action, ch. 7, 12, 13 (merged 2026-11-15); Apache Ranger wiki, p. 11 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.2, 7.4 (merged 2026-11-15).

1. Store authorization attributes apart from general profile data, with restricted read and write; only the designated authority (for example the HR sync or an attribute-assignment role) sets a sensitive attribute, and no principal ever edits its own. Why: Azure separates protected security attributes from freely readable extensions, and a self-modifiable attribute lets a subject grant itself access. (Azure docs 13; NIST SP 800-162 §3.1.3.3–3.1.3.4; Attribute-Based Access Control ch. 7.4.1.)
2. Split attribute rights into "define" (schema) and "assign" (values), held by different roles and delegable per attribute set. Why: Azure has separate definition, assignment and log-reader roles scoped to attribute sets. (Azure docs 13.)
3. Do not give attribute rights to Keycloak realm administrators or to policy administrators (platform-folder code owners, the deployment pipeline) by default: attribute administration (each institution's Keycloak attribute administrators, the OpenMetadata stewards for tags) is an administrative path of its own, separate from policy administration. Why: an Azure Global Administrator cannot read or assign security attributes unless granted, and attribute values drive every decision, so whoever sets them needs separate control. (Azure docs 13; Attribute-Based Access Control ch. 3.2, 7.4.)
4. Give every attribute set an owner and every principal attribute exactly one named authority, and prefer predefined values that match the dataset tag values they are compared with. Why: sets are the delegation unit, an authority is the root of trust for its attribute, and predefined values align with resource tags. (Azure docs 13; NIST SP 800-162 §2.4.2; Attribute-Based Access Control ch. 7.4.1.)
5. Fix each attribute's name, data type and single- or multi-valued choice before first use. Why: Azure cannot change them later. (Azure docs 13.)
6. Limit attribute values to characters valid in the tags they are compared with. Why: other characters never match. (Azure docs 13.)
7. Use a principal attribute in a condition only after it is defined and an assignment role for it exists. Why: Azure requires both before principal attributes can be used. (Azure docs 11.)
8. Let only trained, authorized people change attribute definitions and assignments, and audit every change (Keycloak admin events shipped to OpenSearch and immudb). Why: Azure provides a dedicated log-reader role for attribute changes, and NIST notes that decision quality equals attribute quality. (Azure docs 13; NIST SP 800-162 §3.4.1.)
9. Feed principal attributes and group memberships to Ranger through usersync from Keycloak, configured to load the extra user attributes that policies use (for example institution, region, clearance) and to declare every attribute that is not a string, not through token claims; keep access tokens short-lived and limited to stable identity and institution claims. Why: Azure custom security attributes are not emitted in tokens, usersync loads extra LDAP attributes as Ranger user attributes since RANGER-2697 (string by default), and claims embedded in tokens go stale and can be replayed if stolen. (Azure docs 13; Authorization in Action ch. 7.3.1, 12.3.2, 13.3.2; Apache Ranger wiki p. 11.)
10. Plan attribute counts within Azure's documented ceilings as a design guide, although Keycloak has no such quota: at most 500 active attribute definitions, 100 predefined values per attribute, 64 characters per value, 32-character names without special characters, and 50 values per object across all attributes. Why: these limits keep attributes few, short and comparable with tags. (research: Microsoft Learn, Overview of custom security attributes, updated 2026-06-15, accessed 2026-11-15.)
11. Share principal attributes with another institution or an external party only under a written agreement that states the purpose of use and the liability. Why: principal attributes are often personal data. (NIST SP 800-162 §3.2.1.6.)
12. Sync to Ranger and to each engine only the principal attributes its policies use, decided centrally, never full user profiles. Why: per-integration choices over-collected and over-retained attributes. (Authorization in Action ch. 12.5.3, 12 Do/Don't.)

### Group membership in tokens and its limits

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 04 (merged 2026-11-14); NIST SP 800-162, §3.3.1 (merged 2026-11-15); Authorization in Action, ch. 4, 12 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.5, 6.3, 7.7 (merged 2026-11-15); Hadoop Security, ch. 5 (merged 2026-11-15); Kafka: The Definitive Guide, 2nd ed., ch. 11 (merged 2026-11-15).

1. Resolve nested group membership transitively in Keycloak or usersync and give Ranger and each engine the flattened groups; keep group nesting acyclic and let the sync check reject a cycle before import. Why: Azure counts nested membership through a token holding transitive groups, Ranger does not expand nesting, a member's effective groups are its direct plus inherited ones, and a cycle in group inheritance is a policy fault. (Azure docs 01; Attribute-Based Access Control ch. 3.5, 6.3.)
2. Deliver the same transitive groups on every path, from one source: usersync for Ranger and the engine's group source (for example Trino's group provider) for the request. Why: a group missing on one path changes the decision for the same user, and a group exists for authorization only if the deciding plugin sees it. (Azure docs 01; Hadoop Security ch. 5.)
3. Treat every refresh interval on the path (Keycloak token lifetime, usersync cycle, plugin user-store, tag and policy refresh) as the maximum revocation delay of its layer, approve and document each interval by risk, short enough for revocation but not so short that it overloads Keycloak or the directory, and apply the revocation targets of conflicts #3 (with #19 for the Ranger version in use) and the emergency cutoff of #5; expect a removed or deleted principal to keep access until its token expires and, on Kafka, until its connection reauthenticates (kafka-security.md › Principals and principal mapping, rule 3). Why: Azure documents that deleted principals keep working until token expiry, NIST notes that cached attributes keep old privileges until refreshed, group changes stay invisible until the cache expires, an open Kafka connection keeps the principal it authenticated with, and the freshness of cached users, groups and tags in each plugin, not only of policies, bounds revocation, so policy removal alone is not instant revocation. (Azure docs 04; NIST SP 800-162 §3.3.1; Authorization in Action ch. 4.5.3, 12.4.2, 12 engine notes; Attribute-Based Access Control ch. 7.7.1; Hadoop Security ch. 5; Kafka DG ch. 11; conflicts #3, #5, #19.)

