# Role engineering and separation of duties

Status: starter rules; role hierarchies, separation of duties and governance roles filled from Authorization in Action (merged 2026-11-15); administrative roles from the Apache Ranger wiki (merged 2026-11-15); separation of duties through nesting, conflict classes and dynamic roles from Attribute-Based Access Control (merged 2026-11-15); starter rules confirmed by Hadoop Security (merged 2026-11-15); governance committee, compliance reviewer, RACI accountability and governance hats mapped to built-in roles from Data Governance: The Definitive Guide (merged 2026-11-16); domain duties in the RACI, application owner and multiple owners from Data Management at Scale, 2nd ed. (merged 2026-11-16); federated governance committee, data product owner life cycle, platform product owner and custodian mapping from Data Mesh (merged 2026-11-16); role engineering method and migration from ad hoc grants as a platform method (decision 2026-11-16); no section pending.

## Starter rules

1. Assign roles to groups, not users, so that every role is reachable only through group membership, and grant only through a role definition, never a raw access type on a group; keep groups (sets of people) apart from roles (named permission sets): a group name never implies a permission, only the group's role assignments do, and one role may be assigned to many groups and one group may hold many roles; service clients are the direct-assignment exception of conflicts #12. Why: membership changes must not require policy edits, group entries make review and revocation one edit, per-user grants give no global view, mixing groups with roles hides what membership grants, and a strict permission-role-group chain keeps every grant traceable. (plugin v1; Authorization in Action ch. 4.3.2, 5.1.3, 5.1.5; Attribute-Based Access Control ch. 2.5, 3.12; Hadoop Security ch. 1, 6, 7; Data Governance DG ch. 7; conflicts #12.)
2. Keep the built-in role set small, tied to stable job functions and named after them, never after an institution or scope (Institution Owner, Data Contributor, Data Reader, Access Administrator, PII Reader, Auditor), and add custom roles only for a documented need. Why: fewer definitions, fewer mistakes, the role is the unit of grant, and a small job-function vocabulary lets auditors read who can do what and why. (plugin v1; Authorization in Action ch. 5.2, 5.2.4; Hadoop Security ch. 7.)
3. Separate who can use data from who can grant access, at every scope level: holding a data role never implies the right to assign it to anyone else. Why: prevents self-escalation and "I shared it because I could open it", and the right to grant is a permission of its own at every level. (plugin v1; Authorization in Action ch. 9.5.1; Hadoop Security ch. 6.)

### Role engineering method (top-down, bottom-up, scenarios)

Source: platform method; no summarized source covers it (decision 2026-11-16). Role set: Starter rules, rule 2; role definitions: azure-style-rbac-abac.md › Role definitions: actions, data actions and subtraction.

1. Start top-down: list an institution's job functions and the tasks each performs, and map every task to a built-in role on a scope before considering anything else. Why: the six built-in roles are meant to cover stable job functions, and a role derived from one team's current grants copies its accidents. (decision 2026-11-16.)
2. Check bottom-up against evidence: compare the top-down mapping with 30 days of Ranger audit and the institution's existing grants, and treat every used access the mapping does not cover as a question for the Institution Owner, not as a new role. Why: audit shows what people really need, while grants alone show only what was once given (audit-design.md › Access review process and cadence, rule 4). (decision 2026-11-16.)
3. Write each recurring access need as a scenario (principal group, task, dataset, expected decision) and keep the scenarios as CI decision-table cases (policy-testing-and-verification.md › Test methods for policies). Why: a scenario fixes the intended outcome before the policy exists and fails the build when a later change breaks it. (decision 2026-11-16.)
4. Propose a custom role only when a task cannot be covered by a built-in role on a narrower scope, by subtraction in a role definition or by a condition, and record the reason, the owner and a review date in the role definition. Why: every custom role adds a definition to review, and most apparent gaps are scope or condition questions (Starter rules, rule 2). (decision 2026-11-16.)

Example (platform manifest, one scenario):
```yaml
kind: AccessScenario
metadata: { id: sc-t-acme-0012 }
spec:
  principal: platform_t-acme_analyst
  task: monthly churn report
  resource: t_acme_lake.gold_sales.orders
  expected: { roleDefinition: data-reader, decision: allow, masked: [customer_email] }
```

### Role hierarchies versus scope inheritance

Source: Authorization in Action, ch. 5 (merged 2026-11-15).

1. Keep roles flat: no role inherits from or copies the access types of another; layered responsibility is two assignments of two flat roles, and inheritance happens only through scope, by generated wildcard resources. Why: Azure has no role hierarchy, copied access types drift apart, and hidden inherited permissions make effective access hard to predict. (Authorization in Action ch. 5.2.1, 5 Do/Don't; decision 2026-11-13, conflicts #2.)
2. Compile assignments to Ranger group and user entries only; do not use Ranger role objects in compiled policies. Why: role definitions live in the manifest, and a second role layer inside Ranger would hide who holds access and reintroduce role nesting (conflicts #2). (decision 2026-11-15.)

Example (manifest): a steward who reads and manages tags holds two assignments, not a "Data Steward" role inheriting Data Reader:
```yaml
kind: RoleAssignment
metadata: { id: ra-0101 }
spec: { principal: { type: group, name: platform_t-acme_steward }, roleDefinition: data-reader, scope: /institutions/t-acme/domains/sales }
---
kind: RoleAssignment
metadata: { id: ra-0102 }
spec: { principal: { type: group, name: platform_t-acme_steward }, roleDefinition: access-administrator, scope: /institutions/t-acme/domains/sales }
```

### Dynamic roles: attribute-driven eligibility

Source: Attribute-Based Access Control, ch. 3.11, 3.12 (merged 2026-11-15).

1. Let attributes decide role membership (dynamic roles) only where the context changes quickly but the permission bundle stays stable, such as incident response, on-call duty or workflow-driven eligibility. Implement it as an eligible assignment of a built-in role that the access-request workflow activates with `validitySchedules` (azure-style-rbac-abac.md › Privileged access, rules 1–3). Review the eligibility and activation rules and the attribute sources they read, not only the members. Why: dynamic roles sit on top of RBAC and avoid role explosion, and their review has to cover what decides membership. (Attribute-Based Access Control ch. 3.11.)
2. Combine attribute-driven membership with a condition on the same assignment only when both stages are needed, and record the reason in the assignment's description. Why: the review cost of the combined form is the sum of both stages. (Attribute-Based Access Control ch. 3.12.)

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0131 }
spec:
  principal: { type: group, id: kc-grp-5720, name: platform_t-acme_incident-eligible }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/ops
  assignmentType: eligible
  description: "Activated only for an open incident assigned to the requester, ticket OPS-90"
  eligibility: { end: "2027-06-30" }
  activation: { maxDuration: PT4H, requireJustification: true }
```

### Static and dynamic separation of duties

Source: Authorization in Action, ch. 5, 10, 14 (merged 2026-11-15); Attribute-Based Access Control, ch. 2.6, 6.4 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1, 3 (merged 2026-11-16).

1. Define static separation-of-duties pairs (never both sides) and let CI block any manifest change that gives one principal both sides on the same scope. CI evaluates this after nested groups are flattened, so no group can inherit both sides through nesting; roles cannot inherit from each other (conflicts #2). The pairs are Access Administrator and PII Reader on the same scope, and Auditor and any data role in the audited zone. Why: SoD forces two people into a sensitive transaction and limits fraud in regulated work, inheritance violates SoD without any direct assignment, and reviewers who have to spot conflicting assignments themselves miss them. (Authorization in Action ch. 5.2.1, 5 Do/Don't; Attribute-Based Access Control ch. 2.6.3, 6.4; Data Governance DG ch. 1, 3; decision 2026-11-14, conflicts #13.)
2. Enforce in the Git host that the approver of every policy pull request differs from its author, and keep the pull-request discussion and test results as audit evidence. Why: SoD applies to the change process too, and auditors ask who decided a grant and when. (Authorization in Action ch. 10.2.2, 14.3.2.)
3. Where competing institutions exist, declare them as conflict classes in the platform folder. CI blocks any manifest change that gives one principal, after group flattening, data-role assignments (read or write) in two institutions of the same class. The declared platform principals of conflicts #9 are the only exception, and they appear in the Auditor's review. Why: a conflict-of-interest barrier keeps a person or a process from carrying one institution's data into a competitor's, and Ranger keeps no access history, so the platform enforces the barrier statically on assignments instead of on what was read first. (Attribute-Based Access Control ch. 2.6.2; conflicts #9.)
4. Allow a `dynamic` pair type in the SoD file, enforced only at activation: the access-request workflow refuses to activate an eligible assignment while the same principal has an active assignment of the paired role on the same scope; define no dynamic pair until an institution documents the need, and test dynamic pairs with stateful scenarios. Why: Ranger has no sessions, so the only point where two roles become active together is the activation workflow. (Attribute-Based Access Control ch. 2.6.3, 6.2; decision 2026-11-15, conflicts #43.)
5. Add two static pairs to the SoD file: Auditor and Institution Owner, and Auditor and Access Administrator, in the audited zone; give the steward's Access Administrator role and the tag authority for `PII` and `RESTRICTED` to different groups, or record an exception. Why: an auditor who manages access or owns the zone audits their own decisions, and one person who both grants access and sets the tags that guard it can open data alone. (Data Governance DG ch. 3; decision 2026-11-16, conflicts #53; conflicts #13.)

Example (rewritten, platform manifest):
```yaml
kind: SeparationOfDuties
metadata: { id: sod-platform }
spec:
  staticPairs:
    - { roles: [access-administrator, pii-reader], scope: same }      # conflicts #13
    - { roles: [auditor, data-reader], scope: audited-zone }
    - { roles: [auditor, data-contributor], scope: audited-zone }
  conflictClasses:
    - { id: cc-insurers, institutions: [t-acme, t-beta] }            # no principal holds data roles in both
```

### Administrative roles and delegation of assignment

Source: Apache Ranger wiki, p. 12 Read-only Ranger Admin and external role mapping (2018/2019), p. 4 Security zones (2022) (merged 2026-11-15). Azure-style delegation: azure-style-rbac-abac.md › Delegating assignment management with conditions.

1. Give human Ranger Admin accounts only the User or the Auditor role; the Admin role belongs only to the deployment pipeline's service account and the sealed lockout credential (azure-style-rbac-abac.md › Privileged access, rule 10). Why: Admin can change every policy, and no human holds write rights in Ranger Admin. (Apache Ranger wiki p. 12; conflicts #6, #22.)
2. Give the platform's review staff the Ranger Auditor role, and treat its policy exports (CSV, Excel) as confidential. Why: the Auditor role sees every policy but cannot create, update, delete or import, and stays read-only even where a policy marks it delegate admin, while an export reveals every institution's access model. (Apache Ranger wiki p. 12; conflicts #28.)
3. Map no human LDAP group to an administrator role through usersync's group-to-role rules; map only the platform auditor group to the Auditor role, with the rule kept in Git. Why: a group-to-role rule makes anyone who joins the LDAP group an administrator at the next sync, without approval. (Apache Ranger wiki p. 12.)
4. Set each zone's administrators to the deployment service account only and its auditors to the institution's Auditor group (`platform_t-<institution>_auditor`). Why: zone admins can change their zone's policies by hand, while zone auditors see only their own zone's policies and audits. (Apache Ranger wiki p. 4; conflicts #6; decision 2026-11-15, conflicts #38: zone roles not adopted.)
5. Keep service-admin rights (which services and resources belong to a zone, which tag services are linked to it) with the platform team, changed only through CI. Why: these settings decide which policies govern a resource. (Apache Ranger wiki p. 4.)

### Governance roles (owner, steward) mapped to built-in roles

Source: Authorization in Action, ch. 14 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 1, 3, 4, 8, 9 (merged 2026-11-16); Data Management at Scale, 2nd ed., ch. 2, 4, 8 (merged 2026-11-16); Data Mesh, ch. 5, 16 (merged 2026-11-16)

1. Name an owner, author, reviewer and approver for every policy and map them to platform roles: owner = the Institution Owner for its institution folder and the platform team for the platform folder; approver = the folder's code owner (may be the owner); author = whoever opens the change (an Access Administrator for assignments; the steward of conflicts #13); reviewer = a second person, never the author; ownership continues after deployment. Why: unowned policies become orphaned, and named roles make approval an endorsement of intent. (Authorization in Action ch. 14.3.2, 14.6; conflicts #7, #13, #20.)
2. Add an access-control coordinator (platform team) to changes that affect several institutions, and a compliance reviewer (the book's privacy tsar: holds no data role) before approval only to platform-layer changes (guardrails, tag policies, role definitions, platform templates) and to the eligibility definitions of PII Reader and `RESTRICTED` access; keep governance staff out of the routine access path: assignments inside a zone are approved by the institution's steward (Access Administrator, conflicts #13) with CI checks only, and cross-institution shares by the owning Institution Owner (conflicts #46); for data tagged `PII` or `RESTRICTED` the compliance reviewer is not the Institution Owner of the same zone, unless the exception is recorded. Why: cross-team impact and regulatory fit need their own check where the rules themselves change, a governance approval on every routine grant becomes the bottleneck federated governance avoids, and when the reviewer, steward and owner hats fall to one person, classification is the first thing dropped. (Authorization in Action ch. 14.2.2, 14.3.2; Data Governance DG ch. 3; Data Mesh ch. 5, 16; decision 2026-11-15: coordinator = platform team; decision 2026-11-16, conflicts #60.)
3. Let the Auditor check governance itself, read-only: reviews done on time, policy metadata complete, incidents followed up. Why: the auditor role reviews the process, not the data. (Authorization in Action ch. 14.3.2.)
4. Run governance federated, with no central data council: a written governance policy approved by a committee of executives and data owners sets protection by data value and risk (policy-testing-and-verification.md › Policy life cycle, rule 8); the committee's members include the Institution Owners, the platform product owner and architect and subject-matter experts (legal, security, compliance), and its output is the few platform-wide policies and the sensitivity levels every dataset must support (access-control-models.md › Central vs domain policy ownership (federated governance), rule 2); the platform team owns the standards (naming, testing, review, documentation, retirement), the templates and the automation that enforces those policies, institutions own their zone policies and each Institution Owner is accountable for using the platform's enforcement correctly, and the governance bodies and trust model are those of security-operations.md › Introducing attribute-based policy, rule 2. Why: central standards with local ownership keep autonomy without divergence, a committee that weighs value and risk gives the written policy its authority, the committee decides while the platform automates, and governance is not accountable for securing each dataset. (Authorization in Action ch. 14.3.1; NIST SP 800-162 §3.1.2.5; Data Governance DG ch. 4; Data Mesh ch. 5, 16; conflicts #13.)
5. Hold a monthly policy review board coordinated by the platform team: new and changed policies, template changes, ADRs, access incidents and overdue reviews, with outcomes linked to policy IDs and tickets. Why: a fixed forum catches drift early and leaves evidence. (Authorization in Action ch. 14 p. 280; decision 2026-11-15: coordinated by the platform team.)
6. Keep a RACI matrix in the platform folder and in each institution folder that names one accountable group per governance task (classification and tagging; supplying the metadata of each new dataset at onboarding: column classification, row-filter and mask columns, schema, glossary links and lineage; access approval and the agreements for sharing its data; policy authoring; review and audit, including answering the access review of its own data; tracking regulatory change; incident handling) and an owner for every critical domain; one named owner tracks regulatory change, works with legal and reports to the governance body; every role description states its data-handling responsibility and the consequence of failing it. Why: one person often wears several hats, overlapping hats blur responsibility, when everyone is responsible no one is, fine-grained policies need metadata only the providing domain has, and the owner judges who may see its data. (Data Governance DG ch. 3, 4, 8, 9; DMaS ch. 2, 8.)
7. Map each governance hat to platform roles and give it nothing more: data owner = Institution Owner (control plane) plus an eligible Data Contributor assignment when it needs data, accountable for the life cycle of its datasets (change, revision, retirement); application owner (the technical custodian of a system, separate from the data owner) = the named owner of that system's service clients and pipelines (azure-style-rbac-abac.md › Role assignments, rule 3), with no data role by that title, and custodian work (running the pipelines and stores) = the institution's developers under that application owner; platform product owner = the platform team member who owns how policy automation, templates and onboarding fit the users' journeys, with no data role by that title; data steward = Access Administrator at institution scope, approving requests; data analyst or scientist = Data Reader, PII Reader only by request; business analyst = Data Reader on the published layers; customer support = an eligible PII Reader assignment at dataset scope with a row filter, without ad hoc query (SQL Lab) or export feature permissions; external auditor = Auditor on the audited zone with an end date (`validitySchedules`) and no data role; privacy tsar = compliance reviewer (rule 2); legal and executives = no platform role. Why: hats have different tasks and access needs, business accountability for data differs from technical custody of the system that holds it, ownership of a dataset's life cycle sits in its domain, the platform product owner prioritises platform services rather than using data, customer support views records rather than analysing them, and auditors must prove who has access independently of it. (Data Governance DG ch. 1, 3; DMaS ch. 8; Data Mesh ch. 16; conflicts #13, #20, #21.)
8. When several parties own one data product, keep one owning institution whose Institution Owner approves access (conflicts #27) and record the others as co-owners whose agreed restrictions are written once in that zone; publish one dataset per owner where ownership splits cleanly along tables, and where owners are many and mixed within rows, carry the owner in a reserved column registered in the catalog (attribute-and-tag-design.md › Classification to policy chain, rule 1) and drive a row filter from it (trino-security.md › Row filters, masks and views, rule 2). Why: rows need an owner for row-level access, many owners cannot each approve every request, and coarse ownership is simplest as separate products. (DMaS ch. 4, 8; conflicts #27.)
9. Give an external auditor the Auditor role through an assignment whose `validitySchedules` end with the audit contract, and make the compliance reviewer's approval a mandatory step of that Flowable request; map subject-matter experts to the compliance reviewer as advisers and never to the Auditor role. Why: outside reviewers must lose access with their contract, and advisers need to judge rules, not to read every institution's policies. (decision 2026-11-16)

Example (rewritten, manifest):
```yaml
kind: RoleAssignment
metadata: { id: ra-0241 }
spec:
  principal: { type: group, id: kc-grp-6010, name: platform_t-acme_ext-auditor }
  roleDefinition: auditor
  scope: /institutions/t-acme
  description: "External KVKK audit 2027, engagement AUD-7; no data role"
  validity: { start: "2027-02-01", end: "2027-03-15" }
```

Example (rewritten, platform manifest):
```yaml
kind: Dataset
metadata: { id: ds-t-acme-customers-shared }
spec:
  resource: t_acme_lake.gold_sales.customers
  owningInstitution: t-acme                    # its Institution Owner approves access (conflicts #27)
  coOwners: [t-acme sales, t-acme service]     # agreed restrictions written once in zone t-acme
  ownerColumn: owner_domain                    # reserved column, registered in OpenMetadata
  rowFilter: ra-0260-rowfilter                 # filter expression reads owner_domain
```

Example (rewritten, RACI excerpt in the platform folder):
```yaml
kind: Raci
metadata: { id: raci-platform }
spec:
  - { task: "PII mask and RESTRICTED guardrail automation", accountable: platform-product-owner, responsible: platform-team }
  - { task: "Retire dataset t_acme_lake.gold_sales.orders_v1", accountable: t-acme-institution-owner, responsible: t-acme-developers }
  - { task: "Approve data-reader request on /institutions/t-acme/domains/sales", accountable: platform_t-acme_access-admin }
```

### Migration from ad hoc grants to the role model

Source: platform method; no summarized source covers it (decision 2026-11-16). Effective-access report: audit-design.md › Access review process and cadence, rule 9.

1. Migrate one institution at a time: export its existing Ranger policies and engine-local grants, and map every grant item to a group, a built-in role and a scope; user-level grants become group assignments, and an item that maps to nothing is either dropped or becomes a documented custom-role candidate (Role engineering method, rule 4). Why: a per-institution cut keeps the change reviewable and follows the one-zone-per-institution model. (decision 2026-11-16.)
2. Drop any grant not used in the last 30 days of Ranger audit unless the Institution Owner confirms it in writing, and give confirmed exceptions a `validitySchedules` end date. Why: migration is the cheapest moment to remove stale access, and an exception without an end date becomes permanent. (decision 2026-11-16.)
3. Before cut-over, compare the effective-access report of the new manifest with the access the old grants give, and have the Institution Owner sign off every difference; then replace the old policies with the compiled ones in one change and keep the old export in Git history. Why: the comparison shows exactly who gains or loses access, and a single change can be rolled back as a whole. (decision 2026-11-16.)
4. After cut-over, run the drift check and an access review of the institution within the first quarter, and accept no new ad hoc grant from then on. Why: the first review catches what the mapping missed, and a single exception reopens the old model. (decision 2026-11-16.)

Example (platform manifest, migration record):
```yaml
kind: GrantMigration
metadata: { id: mig-t-acme-0004 }
spec:
  legacy: { rangerPolicyId: 812, item: "user ayse.k: select on t_acme_lake.gold_sales.*" }
  mappedTo: { group: platform_t-acme_analyst, roleDefinition: data-reader, scope: /institutions/t-acme/domains/sales }
  usedLast30Days: true
  ownerSignOff: FLW-2291
```

