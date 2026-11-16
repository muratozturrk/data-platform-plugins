# Security operations for the authorization plane

Status: starter rules; attribute and policy maintenance and the introduction of attribute-based policy filled from NIST SP 800-162 (2026-11-15); control-plane protection, support and platform access and bypass paths from Authorization in Action (merged 2026-11-15); hardening, versions and upgrade checks from the Apache Ranger wiki (merged 2026-11-15); risk-based operations, network segmentation and case studies from Hadoop Security (merged 2026-11-15); operator access transparency, exfiltration controls, pre-collection review and incident accountability from Data Governance: The Definitive Guide (merged 2026-11-16); risk assessment per personal dataset, exports off the platform and breach handling from the KVKK Personal Data Security Guide (merged 2026-11-16).

## Starter rules

1. Prove revocation by plugin policy version, do not assume it. Why: cached policies keep working.
2. Treat a wrongly allowed access as an incident with an exposure window. Why: audit must answer who saw what.

### Introducing attribute-based policy: planning, governance and denials

Source: NIST SP 800-162, §3.1.2.2, §3.1.2.5, §3.1.3, §3.1.3.6, §3.2.1.5 (merged 2026-11-15).

1. Plan attribute-based policy in this order: resources to protect, access rules, attributes with their authorities, policy authoring and validation, distribution of decision and enforcement points. Why: each step depends on the previous one. (NIST SP 800-162 §3.1.3.)
2. Keep one governance body for the platform (platform team: tags, guardrails, role definitions) and one per institution (its Institution Owner and Access Administrator, conflicts #13), and document a trust model that names the authority of each attribute, the authors of each policy folder and the owner of each dataset, with their liability and a periodic check of each; write it in the platform's template as `trust-model.yaml` in each institution folder and in the platform folder, and let CI reject a policy that uses an attribute or tag with no named authority there. Why: without governance teams build stovepipes, and ABAC trust has many roots. A machine-readable trust model makes the authority rule checkable and gives Auditors one place to answer who may set which attribute. (NIST SP 800-162 §3.1.2.5; conflicts #13; decision 2026-11-15.)
3. Give every denied user an explanation, the missing attribute or group, a link to an access request (Flowable) and a help contact, and track and review denials. Why: automated denials are harder to understand than ACL entries, and some denials reveal policies that do not match business needs. (NIST SP 800-162 §3.1.2.2, §3.1.3.6, §3.2.1.5.)

### Protecting the control plane

Source: Authorization in Action, ch. 17, 18 (merged 2026-11-15).

1. Keep every non-human principal out of the control plane: service clients, pipelines, automation and AI agents (including `ranger-policy-author` and `ranger-config-reviewer`) hold no Ranger Admin write, zone, user or policy rights and no `delegateAdmin`; they propose policy changes only as Git pull requests that a named person approves; the only principals that write to Ranger Admin are the deployment pipeline and the sealed lockout credential of azure-style-rbac-abac.md › Privileged access, rule 10; widening an agent's or service's limits never touches these protections. Why: a principal able to change its own safeguards can weaken them, so the rules that govern it must stay out of its reach. (Authorization in Action ch. 17.2.4, 18.5, 18 p. 363; conflicts #6; decision 2026-11-15, conflicts #22.)

### Support and cross-institution platform access

Source: Authorization in Action, ch. 13.6 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 7 (merged 2026-11-16).

1. Give platform support access to one institution only through the break-glass pattern of conflicts #8 with azure-style-rbac-abac.md › Privileged access, rules 1–3: an eligible support group per institution (for example `platform_support_t-acme`) holding only a small on-call group, a request naming the institution and the reason, approval in Flowable by a different person, a `validitySchedules` window of at most 4 hours, audit of request, approval and every access, and review within 2 working days; report every activation to the institution's Owner and Auditor when it starts, and mark its accesses in the audit as operator access so that they can be reported apart from the institution's own; guardrails and institution policies still apply during the window. Why: support needs time-boxed access to one tenant, standing support access across zones breaks isolation, and operator access to tenant data must be transparent to, and verifiable by, the institution. (Authorization in Action ch. 13.6.2, 13 Do/Don't; Data Governance DG ch. 7; decision 2026-11-14, conflicts #8, #9.)
2. Run cross-institution platform jobs (backup, monitoring) only as declared platform service principals in the platform folder: read-only, metadata-only where possible, never exempt from `PII` and `RESTRICTED` masks, logged through the same audit pipeline, reported in the Auditor's review, and never as a Ranger admin. Why: such access is unavoidable for platform jobs but must be declared and visible. (Authorization in Action ch. 13.6.4, 13 Do/Don't; Apache Ranger wiki p. 4; decision 2026-11-14, conflicts #9.)
3. End a support activation automatically when its ticket closes, let the on-call engineer end it earlier in Flowable, and switch to the incident path of conflicts #5 when an incident is involved. Why: access tied to a ticket must not outlive it. (decision 2026-11-16)
4. Notify the affected Institution Owner automatically through Flowable at the moment an operator activation on that institution's zone starts. Why: an institution must know when platform staff can reach its data. (decision 2026-11-16)
5. Keep the on-call engineers who may activate support access in a named group of two to four people, reviewed every quarter. Why: a small, named group keeps privileged access attributable and reviewable. (decision 2026-11-16)
6. Give the legal review team metadata-only access across institutions, through an eligible, time-bound and approved assignment following the pattern of conflicts #8. Why: a legal review needs to see what exists and who holds it, not the data itself. (Authorization in Action ch. 13.6.1; decision 2026-11-16.)

Example (manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0110 }
spec:
  principal: { type: group, id: kc-grp-5800, name: platform_support_t-acme }
  roleDefinition: data-reader
  scope: /institutions/t-acme
  description: "Platform support for t-acme, incident-bound, ticket SUP-14"
  assignmentType: eligible
  eligibility: { end: "2027-06-30" }
  activation: { maxDuration: PT4H, requireMfa: true, requireJustification: true, requireApproval: true }
```

### Ranger versions, hardening and upgrades

Source: Apache Ranger wiki, p. 13 Lock down for production (2018), Fixed in 2.8.0 and 2.9.0 (2026), p. 14 Release notes 2.5–2.9 (2024–2026), p. 1 Backward compatibility (2016) (merged 2026-11-15).

1. Change the default passwords of `admin`, `keyadmin`, `rangerusersync` and `rangertagsync` before the first start and keep them in OpenBao; `admin` becomes the sealed lockout credential of azure-style-rbac-abac.md › Privileged access, rule 10. Why: these accounts ship with default passwords. (Apache Ranger wiki p. 13; decision 2026-11-15, conflicts #22.)
2. While a version before 2.9.0 runs, allow network access to Ranger Admin only from the control plane and the plugins, keep Ranger logs out of broadly readable indexes, and use neither UnixAuth login nor the Unix usersync source. Why: before 2.9.0 the download APIs expose plugin data without authentication outside Kerberos (CVE-2026-55814), logs hold replayable JWT bearer tokens (CVE-2026-65945), UnixAuth has no brute-force protection (CVE-2026-65948) and the Unix group builder allows command injection (CVE-2026-28672). (Apache Ranger wiki p. 13.)
3. Keep header-based authentication (RANGER-5499, 2.9) switched off. Why: it trusts an identity asserted by the caller (trino-security.md › Enforcement points, rule 3). (Apache Ranger wiki p. 14.)
4. Upgrade the plugins together with Ranger Admin, and before publishing a deny or exception item confirm that every plugin version enforces it. Why: older plugins ignore deny and exception items silently and apply only the allows. (Apache Ranger wiki p. 1.)
5. Check before rollout that every plugin package contains the audit destination library the platform uses. Why: from 2.7 plugins ship only the Solr and HDFS destinations. (Apache Ranger wiki p. 14.)
6. After every Ranger upgrade, rerun the lab cases that pin version-dependent behaviour (V1 `override` priority against tag deny, V6 group removal through usersync) before relying on the old results. Why: 2.7 changed `override` precedence (RANGER-5225) and 2.9.0 changed user-store propagation (RANGER-5463). (Apache Ranger wiki p. 14; trial: verification lab V1, V6, 2026-11-14; conflicts #6, #19.)
7. Run at least Ranger 2.8.0 and plan the move to 2.9.0; do not run 2.7 or earlier. Why: 2.8.0 fixes the Nashorn script-condition remote code execution and 2.9.0 fixes ten further CVEs and the usersync propagation of conflicts #19. (Apache Ranger wiki p. 13, p. 14; decision 2026-11-15, conflicts #41.)

### Risk-based operations and threat model

Source: Hadoop Security, ch. 2, 10 (merged 2026-11-15); KVKK Personal Data Security Guide, §2.1, §4.2 (merged 2026-11-16).

1. Start security design from three threat categories: masquerade (stolen or guessed credentials), insiders who hold valid credentials and grants, and threats to the data itself; counter insiders with least privilege, the periodic audit review and a written incident procedure (starter rule 2), and detect masquerade with audit-design.md › Security monitoring, rules 5 and 6. Why: insiders pass every check by definition, and a stolen credential differs from its owner only in how it is used. (Hadoop Security ch. 2.)
2. Assess users by business function before granting access, including indirect users such as operators and auditors, who get control-plane or monitoring roles without data access types (access-control-models.md › Service-level vs object-level, rule 3). Why: access patterns follow function, and indirect users need the platform, not the data. (Hadoop Security ch. 2.)
3. Raise the risk rating of a component or institution for Internet exposure and for third-party or cloud hosting, and never treat a low rating as zero risk. Why: hosting widens the insider scope to the provider. (Hadoop Security ch. 2.)
4. Layer network segmentation, Ranger data policy, host and pod access, and audit so that no single control carries the protection, and never rely on the perimeter alone (access-control-models.md › Definitions, rule 8). Why: any single control can fail. (Hadoop Security ch. 2.)
5. Decide each ingest flow's protection when its pipeline is designed, from its data classes, threat model and regulation (KVKK): encrypt sensitive classes in transit even inside the platform, keep human operators of brokers and staging areas from reading data they are not authorized for, and match the platform's controls to the source system's in both directions. Why: data is exposed on the wire and on intermediate servers, and locking down one side is pointless while the other is open. (Hadoop Security ch. 10.)
6. Before choosing controls for a dataset that holds personal data, record a risk assessment in the risk register linked from its Git folder: rate each risk by likelihood and by the harm a breach would cause the data subjects, using the factors of attribute-and-tag-design.md › Classification to policy chain, rule 4, prioritise the risks, and weigh the candidate controls by cost, feasibility and benefit. Why: KVKK sets this order (inventory, rating, prioritisation, then measures), and a dataset that is not rated gets controls by habit rather than by need. (KVKK guide §2.1, §4.2.)

### Exfiltration by authorized users

1. Treat exfiltration by authorized users as a threat that grants alone do not stop, and layer controls that hold for legitimate access: narrowly scoped group assignments (azure-style-rbac-abac.md › The four building blocks, rule 3), tamper-evident audit (audit-design.md › Retention and protection of audit data, rule 3), no download or export in Superset for roles that only need to view, with the data served logged (trino-security.md › Paths beside the Trino plugin, rule 4), scheduled scans for sensitive data in unexpected places (attribute-and-tag-design.md › Classification to policy chain, rule 6), and a lower alert threshold for leavers (audit-design.md › Security monitoring indicators and alerts, rule 6); treat every export or portable copy that leaves the platform (downloads, extracts, files on personal devices) as personal data that keeps the protection level it had on the platform. Why: exfiltration uses legitimate access, so it is countered by limits and visibility rather than by refusing grants, and KVKK asks for the same measures off premises and on personal devices. (Data Governance DG ch. 7; KVKK guide §3.3.)
2. Use only tokenized, masked or simulated data in development and test environments, never a copy of production personal data. Why: a production copy in a test environment is an exfiltration path through access that is wider there by design. (Data Governance DG ch. 7.) (Data Governance DG ch. 7; KVKK guide §3.3, §3.5.)

Example (rewritten, Superset role mapping):
```yaml
supersetRoles:
  - { name: t-acme-viewer,  fromGroup: platform_t-acme_analyst, features: [view_dashboards],             export: false }
  - { name: t-acme-export,  fromGroup: platform_t-acme_export,  features: [view_dashboards, csv_export], export: true, logServedData: true }
```

### Network zoning and closing bypass paths

Source: Authorization in Action, ch. 15 (merged 2026-11-15); Hadoop Security, ch. 3, 10 (merged 2026-11-15); KVKK Personal Data Security Guide, §3.3 (merged 2026-11-16).

1. Count and retire the implicit-trust paths outside policy (VPN-only access, IP allow-lists, bypass flags, special-case grants), using the path inventory in trino-security.md › Enforcement points on every path, rule 1. Why: each is trust granted outside governance. (Authorization in Action ch. 15.5.4, 15.5.5.)
2. Run compute and storage in their own network segment (Kubernetes namespaces with network policies) and expose them only through purpose-specific entry points: the SQL gateway (Trino Gateway) for analysts, ingest endpoints (Kafka Connect, Debezium, Airflow) for ingest principals only, and the user portal (Superset); every entry point opens only its service ports and gives no shell. Why: a segment is easier to guard at its boundary, entry points are not equivalent, and analysts must not reach ingest endpoints. (Hadoop Security ch. 3, 10.) (Hadoop Security ch. 3, 10; KVKK guide §3.3.)
3. Keep management services (Ranger Admin and its database, usersync, tag sync, configuration, monitoring and alerting backends, repositories) unreachable from user networks: only the platform team, the plugins and, for Ranger Admin's read-only views, the Auditor groups reach them, and developers get read-only monitoring dashboards and nothing more. Why: management is an administrative function, and read-only dashboards are the one exception the book accepts. (Hadoop Security ch. 3; role-engineering.md › Administrative roles, rules 2, 4.)

Example (rewritten, entry-point inventory):
```yaml
entryPoints:
  - { name: trino-gateway, audience: [platform_t-acme_analyst], tls: true, shell: false }
  - { name: kafka-connect, audience: [t-acme ingest principals], shell: false }   # closed to analysts
  - { name: superset,      audience: [platform_t-acme_analyst], tls: true, shell: false }
managementPlane:
  reachableFrom: [platform-team, ranger-plugins, platform_t-acme_auditor]          # auditors: read-only views
```

### Attribute and policy maintenance

Source: NIST SP 800-162, §3.1.2, §3.1.2.5, §3.2.2.4–3.2.2.5, §3.4.1 (merged 2026-11-15); Apache Ranger wiki, p. 2, p. 8, p. 11, p. 13 (merged 2026-11-15).

1. Run attribute, policy and decision services with redundancy and failover (redundant Ranger Admin; exactly one active usersync, restarted by Kubernetes on failure, because two running at once conflict; plugins keep deciding on cached policies), and put availability, response time and data quality into the agreement with every outsourced service or attribute provider. Why: access depends on these services, and decision quality equals attribute quality. (NIST SP 800-162 §3.4.1; trial: ranger-policy-compile, 2026-11-16; decision 2026-11-16, conflicts #73.)
2. Before widening attribute-based policy to an engine, check whether it enforces natively or through a Ranger plugin and measure the latency of each remote call (policy download, attribute and tag lookups). Why: every interaction between authorization components has a performance cost. (NIST SP 800-162 §3.1.2.)
3. Mutually authenticate every authorization component with integrity and origin checks (plugin to Ranger Admin, usersync to Keycloak, tag sync to OpenMetadata), run TLS on Ranger Admin and on every link to it, and bind usersync to LDAP with its own credentials over TLS; connect authorization to audit, configuration management, intrusion detection, DLP and monitoring, and verify that distributed plugins still deliver complete audit. Why: these components exchange sensitive data, anonymous LDAP bind is not supported, authorization alone is not enough, and distribution can weaken auditing. (NIST SP 800-162 §3.1.2.5, §3.2.2.4–3.2.2.5; Apache Ranger wiki p. 11, p. 13; conflicts #19.)
4. Keep plugins deciding on their cached policies, users and tags during a Ranger Admin, usersync or tag-sync outage, give every plugin a local policy cache directory so that a restarted plugin starts from its last policies and tags while Ranger Admin is unreachable, and keep a missing attribute narrowing access; alert when any plugin's policy, user-store or tag version lags beyond the revocation targets of conflicts #3, and treat a lag beyond 15 minutes as an incident, never as an automatic deny-all. Why: availability matters during outages, but staleness must stay bounded and visible, and without a cache directory nothing is cached locally; 15 minutes is a platform proposal, not a source value. (Authorization in Action ch. 4.5.2, 15 p. 297; Apache Ranger wiki p. 2, p. 8; decision 2026-11-15, conflicts #34.)

### End-to-end case studies

Source: Hadoop Security, ch. 10, 13 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 3, Appendix A (merged 2026-11-16); KVKK Personal Data Security Guide, §3.5 (merged 2026-11-16).

1. Give each institution its own workspace: one zone (`t-acme`) with its catalog, schemas and topic prefix, analyst access through Data Reader assignments on its domains, and institution administration through Institution Owner and Access Administrator assignments limited to that zone and made through Git, never `delegateAdmin`. Why: the institution then controls its own sandbox without platform rights. (Hadoop Security ch. 13; conflicts #6, #20.)
2. Keep curated datasets writable only by the institution's non-interactive pipeline principals (workload identities, identity-and-principals.md › Service accounts and ingest identities) and by reviewed changes, never interactively by analysts. Why: the curated layer is managed, not edited by hand. (Hadoop Security ch. 10, 13.)
3. Before building an application or onboarding a data source on the platform, write down its security requirements (private and public content, which metadata is open, authentication, impersonation, enforcement in the engine) and derive every configuration step from that list; before a new personal dataset is collected, a reviewer outside the developing team checks its justification, consent, who can see it, encryption and audit logging; admit a tool to regulated data (`PII`, `RESTRICTED`) only when it meets every requirement, never a beta or partly compliant tool. Why: the case derives each step from the list, unstated requirements become ad hoc settings, preventing a problem before collection is cheaper than repairing it, and one non-compliant tool breaks compliance for all the data it touches. (Hadoop Security ch. 13; Data Governance DG ch. 3, Appendix A.) (Hadoop Security ch. 13; Data Governance DG ch. 3, Appendix A; KVKK guide §3.5.)

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0150 }
spec: { principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }, roleDefinition: data-reader, scope: /institutions/t-acme/domains/sales }
---
kind: RoleAssignment
metadata: { id: ra-0151 }
spec: { principal: { type: group, id: kc-grp-5600, name: platform_t-acme_access-admin }, roleDefinition: access-administrator, scope: /institutions/t-acme }
# no zone admin and no delegateAdmin: changes reach Ranger only through the deployment pipeline (conflicts #6)
```


### Incident accountability and transparency

1. Name in the incident runbook the accountable party for each governance aspect, taken from the RACI of role-engineering.md › Governance roles, rule 6: enforcement of access controls (the platform team for guardrails and the deployment pipeline, the institution's Access Administrator for its assignments), access policy (the Institution Owner, with the compliance reviewer for regulated data) and classification (the tag authority); close every misuse incident with that party named. Write in the same runbook, in advance and per personal-data class, the breach-handling steps: a breach reported by a data processor enters the incident process at once, staff reports of vulnerabilities and threats arrive through a published reporting procedure, and the runbook names who prepares the controller's notification to the Board (Kişisel Verileri Koruma Kurulu) and the data subjects. Why: accountability needs an end of the line, and when everyone is responsible no one is; KVKK expects breach steps to be defined beforehand to take pressure off staff, and requires immediate notification. (Data Governance DG ch. 9; KVKK guide §2.3, §2.5, §3.2.)
2. Keep gathering legal and regulatory requirements (legal, with the regulatory-change owner of role-engineering.md › Governance roles, rule 6), deciding which of them apply (the governance body of Introducing attribute-based policy, rule 2) and enforcing them (the platform team and the institutions' Access Administrators through Git) in different hands. Why: separating the three functions keeps compliance robust. (Data Governance DG ch. 8, 9.)
3. Publish internally what data the platform collects, how it is used and protected, and how incidents were handled, and give data users a forum for questions. Why: transparency builds trust in the platform's handling of data. (Data Governance DG ch. 9.)
