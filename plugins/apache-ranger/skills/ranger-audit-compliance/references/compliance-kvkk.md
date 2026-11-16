# KVKK technical measures mapped to Ranger evidence

Status: starter rules and all sections filled from the KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025 (merged 2026-11-16); measures already ruled elsewhere are mapped to those rules as evidence; no section pending.

## Starter rules

1. Keep the authorization and control matrix (erişim yetki ve kontrol matrisi) as the Git manifest of role assignments, compile Ranger policies only from it, and produce the matrix document from it as the effective-access report of audit-design.md › Access review process and cadence, rule 9, with the drift check as proof that Ranger matches; never keep the matrix as a separate spreadsheet, and keep the written access policy and procedure that the assignments implement in Git, referenced by ID in each assignment (azure-style-rbac-abac.md › Role assignments, rule 5). Why: KVKK asks for the matrix together with a separate, enforced access policy and procedure, and a matrix that is not the compiler's input drifts from what Ranger enforces. (plugin v1; KVKK guide §3.1, §4.1, §4.2; conflicts #6.)
2. Bind access to personal data to a purpose and, where the relationship is temporary, to a time: every assignment names its purpose, and processor, vendor, exception and privileged access ends through `validitySchedules`. Why: purpose limitation, and access must not outlive the duty or contract that justified it. (plugin v1; KVKK guide §2.4, §2.5, §3.1.)
3. Keep access logs of allowed and denied requests, protect them from change and review them on a schedule; evidence: audit-design.md › Starter rule 1, › Retention and protection of audit data, rules 1, 3, 4, and › Access review process and cadence. Why: accountability, and KVKK requires records of all users' actions and their regular review. (plugin v1; KVKK guide §3.2.)

### Authorization matrix and least privilege

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §2.4, §3.1, §4.2 (merged 2026-11-16). The matrix itself: Starter rules, rule 1.

1. Give each person access to personal data only as far as their duties require, as narrow group assignments; evidence: azure-style-rbac-abac.md › The four building blocks, rule 3 (narrowest scope), and attribute-and-tag-design.md › Differentiated access, masking and tokenization principles, rules 1–3 (column scope, masks by default, clear values only through PII Reader). Why: KVKK makes need-based access to personal-data systems a requirement, and exposure of fields that serve no purpose for a reader is what minimisation forbids. (KVKK guide §2.4, §3.1.)
2. Authenticate every person individually through Keycloak before any access to a personal-data system, never through a shared account or a shared engine login; evidence: identity-and-principals.md › Principals, groups and name mapping, rule 1. Why: KVKK ties access to an individual credential. (KVKK guide §3.1.)
3. On every Keycloak realm that holds passwords, enable brute-force detection that locks the account after 5 consecutive failed sign-ins, and a password policy that requires mixed case, digits and symbols, rejects personal or guessable values, and forces a periodic password change at the interval recorded in the measure map; force an immediate change on suspected compromise and after an incident, and keep MFA for every interactive user (identity-and-principals.md › Principals, groups and name mapping, rule 1) in addition. Why: guessable secrets and brute-force attacks defeat every access limit, and KVKK requires an attempt limit, a password policy and regular password changes. (KVKK guide §3.1; decision 2026-11-16, conflicts #59.) Note: the attempt limit of 5 is a platform value (KVKK gives none) and sits well below the about-20-failure alert of audit-design.md › Security monitoring indicators and alerts, rule 6; the change interval is 180 days for now, a provisional platform value set in the Keycloak realm password policy and recorded in the measure map so that it can be changed without touching any other rule (decision 2026-11-16, open for revision).
4. Open administrative rights only when needed; evidence: azure-style-rbac-abac.md › Privileged access (eligible, time-bound) and access reviews, rules 1–3 (eligible Institution Owner and Access Administrator, MFA, approval, `validitySchedules`), › Best practices and limits, rule 7 (platform administrators through elevation), and security-operations.md › Protecting the control plane, rule 1 (no human `delegateAdmin` or zone admin). Why: KVKK limits the use of admin accounts and rights to cases of need. (KVKK guide §3.1; conflicts #6, #22.)
5. Approve an access request for any role on a scope that holds personal data only when the requester's signed confidentiality undertaking (gizlilik taahhütnamesi) is recorded in the Flowable request. Why: KVKK lists confidentiality undertakings among the administrative measures, and an undertaking signed after access is granted protects nothing. (KVKK guide §4.2.)

Example (rewritten, manifest; one row of the matrix):
```yaml
kind: RoleAssignment
metadata: { id: ra-0180 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales            # compiles to t_acme_lake.gold_sales.*
  description: "Monthly churn report; ticket ACC-310; access policy WP-ACME-KVKK-01; undertaking CU-2207 on file"
# the matrix document is generated from all RoleAssignments; the drift check proves Ranger matches
```

### Access logging and log protection

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §1.3, §3.2, §4.1 (merged 2026-11-16). Logging, protection and review: Starter rules, rule 3.

1. Collect the access logs of paths without a Ranger plugin (ClickHouse, Superset direct connections) from the engine itself into the same OpenSearch and immudb pipeline (audit-design.md › What to audit per engine and why, rule 5), keep these paths in the path inventory (trino-security.md › Enforcement points on every path, rule 1), and record in the measure map that Ranger evidence does not cover them. Why: for these paths the KVKK "authorization control" and "access logs" evidence can only come from the engine. (KVKK guide §3.2, §4.1.)
2. Deliver each institution's scheduled access review report, together with a summary of its alerts and the action taken on each, to the institution's data controller contact (its Institution Owner) as soon as the report is produced. Why: KVKK requires the system administrator to consolidate the automatic reports and present them to the data controller in the shortest possible time. (KVKK guide §3.2.)
3. Take the retention period of the audit store from the OPA retention policy (audit-design.md › Retention and protection of audit data, rules 1 and 2) and record in the measure map that KVKK sets no period for log keeping, naming the OPA value (1 year for OpenSearch, 2 years for immudb; audit-design.md › Retention and protection of audit data, rule 6) and its approver. Why: the guide asks for regular log keeping without a period, and the retention and destruction policy (saklama ve imha politikası) is the platform's OPA policy. (KVKK guide §1.3, §3.2; conflicts #30, #50; decision 2026-11-16.)

### Periodic review and removal of access

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §2.3, §2.5, §3.1, §3.2, §3.5, §4.2 (merged 2026-11-16). Review process: audit-design.md › Access review process and cadence.

1. Run the access review of audit-design.md › Access review process and cadence every quarter for every zone that holds personal data, the authorization matrix included, on the same quarterly cycle as privileged roles (rule 11 there); keep each run's dated `/ranger-audit-review` report as the KVKK control record, and close every finding with a merged Git change that the next run checks again. Why: KVKK requires regular, documented controls in a closed loop of control, record, fix and control again. (KVKK guide §2.3, §3.2, §4.2.)
2. Run at least one unscheduled (random) access review of a zone, group or tag of each institution between two scheduled reviews, with the same report format. Why: KVKK lists periodic and/or random internal audits. (KVKK guide §4.2; decision 2026-11-16.)
3. End access on departure or role change through the identity life cycle, never by waiting for the next review; evidence: identity-and-principals.md › Identity life cycle: joiner, mover, leaver, rules 3 (mover) and 4 (leaver disabled and memberships removed on the leave date, identity record kept), within the revocation targets of conflicts #3. Why: KVKK requires a leaver's access to end without delay. (KVKK guide §3.1; conflicts #3, #11.)
4. End processor and vendor access with the contract or the maintenance window; evidence: identity-and-principals.md › Data processors (veri işleyen) and external maintenance staff, rules 3 and 5. Why: access must not outlive the relationship that justified it. (KVKK guide §2.5, §3.5.)

### Masked data outside production

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §3.3, §3.5, §4.1 (merged 2026-11-16). The guide names neither environments nor masked test data; both rules below are platform derivations from its separation and maintenance measures.

1. Keep development, test and production as separate environments with separate manifests, groups, service identities and tag services, and no assignment shared between them; evidence: azure-style-rbac-abac.md › Best practices and limits, rule 5, and attribute-and-tag-design.md › Tag services and tag policies, rule 1. Why: KVKK asks for separated, restricted components around personal data, and the platform applies that separation to environments. (KVKK guide §3.3.)
2. Use only masked, tokenized or anonymised personal data in development and test (security-operations.md › Exfiltration by authorized users, rule 2); a developer who needs clear values for a case activates an eligible PII Reader assignment on the production scope (azure-style-rbac-abac.md › Privileged access, rule 1), and clear values are never copied into development or test. Why: development work does not need clear personal data, and outside staff and wider development access must not become a way to copy it out. (KVKK guide §3.5; Data Governance DG ch. 7.)
3. Deploy the `PII` tag mask policy with a `public` default item to the development tag service as well, so that personal data that reaches a non-production environment by mistake is still masked for everyone. Why: masking is a listed KVKK technical measure, and a default mask in development turns a copying mistake into masked data instead of exposed data. (KVKK guide §3.5, §4.1.)

Example (rewritten, Ranger tag mask policy JSON, development tag service):
```json
{
  "service": "platform_tag_dev",
  "name": "tag-mask-pii-dev",
  "policyType": 1,
  "description": "Default mask for PII in development; no unmasked item outside production",
  "resources": { "tag": { "values": ["PII"] } },
  "dataMaskPolicyItems": [
    { "groups": ["public"], "accesses": [ { "type": "trino:select", "isAllowed": true } ],
      "dataMaskInfo": { "dataMaskType": "MASK_HASH" } }
  ]
}
```

### Measure-by-measure checklist with Ranger evidence

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §2.1, §3.1, §3.2, §3.5, §4.1, §4.2 (merged 2026-11-16). Checklist items: synthesis/checklist-pool.md, area "KVKK".

1. Keep one measure map in the platform folder with an entry for each of the 17 technical measures of Table 4.1 and the 10 administrative measures of Table 4.2, each naming the aim it serves (preventing unlawful processing, preventing unlawful access, or keeping data lawfully), its owning system or team, its evidence artefact and its review frequency; list training, employment terms, crisis communication and VERBİS as owned outside the platform. Why: a measure without evidence cannot be answered in a review, and KVKK states these three aims as the purpose of the tables. (KVKK guide §4.1, §4.2.)
2. Choose the set and strength of measures for each dataset by the nature of its data and the medium that holds it, starting from its protection level (attribute-and-tag-design.md › Classification to policy chain, rule 4) and its risk assessment (security-operations.md › Risk-based operations and threat model, rule 6). Why: KVKK makes the nature of the data and its storage medium the basis for choosing measures. (KVKK guide §2.1, §4.1.)
3. Treat data masking and access logging as mandatory for personal data on the platform, although the tables list measures a controller may take; masking means the Ranger tag mask policies of attribute-and-tag-design.md › Differentiated access, masking and tokenization principles, rule 3. Why: the platform's base rules make both mandatory, and Table 4.1 lists masking without describing it, so the platform defines it. (KVKK guide §4.1.)
4. Point every measure that Ranger cannot prove (data loss prevention, antivirus, firewalls, backup, network segmentation, TLS on collection endpoints, removal of unused services and patching, service inventory, staff reporting of vulnerabilities, vulnerability scans and penetration tests, removal of storage before third-party repair) to its owning team and that team's evidence, never to the Ranger audit. Why: claiming Ranger evidence for a measure it does not see fails the first review. (KVKK guide §3.1, §3.2, §3.5, §4.1.)
5. Check the measure map, entry by entry, in every `/ranger-audit-review` cycle. Why: the map records proof, and proof that is never checked goes stale. (KVKK guide §4.1.)

Example (rewritten, platform measure map; aim and review frequency per entry are set by the compliance reviewer):
```yaml
kind: KvkkMeasureMap
metadata: { id: kvkk-measures, checkedIn: ranger-audit-review }
technical:                                   # Table 4.1
  - { measure: authorization-matrix,    owner: platform-team,       evidence: [git-manifest, effective-access-report, drift-check] }
  - { measure: authorization-control,   owner: platform-team,       evidence: [ranger-policies, audit-result-field, engine-logs-without-plugin] }
  - { measure: access-logs,             owner: platform-team,       evidence: [ranger-audit-opensearch, clickhouse-superset-engine-logs] }
  - { measure: user-account-management, owner: institution-idp,     evidence: [keycloak-admin-events, keycloak-password-and-lockout-policy, usersync-log, validity-schedules] }
  - { measure: network-security,        owner: platform-operations, evidence: [k8s-network-policies, zone-resources] }
  - { measure: application-security,    owner: platform-team,       evidence: [pr-security-criteria, ci-checks] }
  - { measure: encryption,              owner: platform-operations, evidence: [gateway-tls-config, openbao-keys] }
  - { measure: penetration-testing,     owner: security-operations, evidence: [test-reports, remediation-tickets] }
  - { measure: intrusion-detection,     owner: security-operations, evidence: [opensearch-alert-rules, alert-tickets] }
  - { measure: log-records,             owner: platform-team,       evidence: [ranger-admin-audit, immudb-verification] }
  - { measure: data-masking,            owner: platform-team,       evidence: [tag-mask-policies-from-git] }
  - { measure: dlp-software,            owner: security-operations, evidence: [own] }
  - { measure: backup,                  owner: platform-operations, evidence: [backup-access-policy-openbao] }
  - { measure: firewalls,               owner: network-operations,  evidence: [own] }
  - { measure: antivirus,               owner: security-operations, evidence: [own] }
  - { measure: deletion-destruction-anonymisation, owner: platform-team, evidence: [opa-retention-policy, opa-run-log] }
  - { measure: key-management,          owner: platform-operations, evidence: [openbao-policy, openbao-audit] }
administrative:                              # Table 4.2, access-related entries
  - { measure: processing-inventory,     owner: institution,         evidence: [openmetadata-inventory-links, ranger-tags] }
  - { measure: institutional-policies,   owner: institution,         evidence: [written-policy-in-git, manifest-references] }
  - { measure: processor-contracts,      owner: institution,         evidence: [assignment-descriptions, validity-schedules] }
  - { measure: confidentiality-undertakings, owner: institution,     evidence: [flowable-records] }
  - { measure: periodic-and-random-audits, owner: platform-team,     evidence: [ranger-audit-review-reports] }
  - { measure: risk-analyses,            owner: institution,         evidence: [risk-register] }
outsidePlatform: [training, employment-terms, crisis-communication, verbis]
```

### Privacy and regulatory use of roles

Source: KVKK Personal Data Security Guide (Kişisel Veri Güvenliği Rehberi), KVKK Publications No. 72, April 2025, §1.3, §2.3, §2.4, §2.5 (merged 2026-11-16).

1. Map the KVKK roles onto the platform as follows: data controller (veri sorumlusu) → the owning institution, accountable for its zone through its Institution Owner (accountability, not data access; conflicts #20); data processor (veri işleyen) → a dedicated processor group (identity-and-principals.md › Data processors (veri işleyen) and external maintenance staff, rule 1); data recording system (veri kayıt sistemi) → a catalog, schema or topic prefix registered with its owning institution (attribute-and-tag-design.md › Classification to policy chain, rules 1 and 9); data subject (ilgili kişi) → only the person the data describes, never a platform principal, group or role name. Why: KVKK's measures attach to these terms, and mixing data subjects with principals breaks audit reporting. (KVKK guide §1.3, §2.5.)
2. Resolve every legal term that §1.3 of the guide leaves undefined (data controller, data processor, special-category data) against Law No. 6698 before naming a tag, data class or rule after it. Why: the guide defers undefined terms to the Law. (KVKK guide §1.3.)
3. Route the destruction (imha) of personal data, deletion and anonymisation included, only to the OPA retention policy, never to a Ranger policy; Ranger only removes access. Why: the guide defines anonymisation as part of destruction, which is a life-cycle act and not an access decision. (KVKK guide §1.3, §2.4; conflicts #50.)
4. Handle personal-data breaches through the incident runbook of security-operations.md › Incident accountability and transparency, rule 1, which holds the per-class breach steps, the immediate intake of processor breach reports and the notification of the Board and the data subjects. Why: KVKK expects breach steps to be defined in advance and the controller to notify at once. (KVKK guide §2.3, §2.5.)
5. Answer a data subject's request to see their own data through the institution's KVKK application process, not through a Ranger view or role. Why: identifying one person's records across datasets is a controlled process with its own legal deadlines, not an access right. (decision 2026-11-16)

