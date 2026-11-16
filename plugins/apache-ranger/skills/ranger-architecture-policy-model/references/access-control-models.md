# Access control models and terms

Status: starter rules; definitions filled from NIST SP 800-162 (2026-11-15); policy combining and control plane vs data plane partly filled from the Azure RBAC and ABAC documentation (merged 2026-11-14); models compared, policy combining and language selection filled from Authorization in Action (merged 2026-11-15); Ranger evaluation order, security zones and context enrichers from the Apache Ranger wiki (merged 2026-11-15); DAC and MAC, the role-centric hybrid default, XACML decision outcomes and deny-overrides combining, layer separation and enforcement inside the platform from Attribute-Based Access Control (merged 2026-11-15); CIA and AAA terms, service-level vs object-level authorization and one central policy for every path from Hadoop Security (merged 2026-11-15); Trino file-rule ordering compared with Ranger and Azure evaluation from Trino: The Definitive Guide, 2nd ed. (merged 2026-11-16); central vs domain policy ownership (federated governance) from Data Management at Scale, 2nd ed. (merged 2026-11-16); federated global and local policy, embedded enforcement and platform-wide erasure as a control-plane function from Data Mesh (merged 2026-11-16); no section pending.

## Starter rules

1. Use the Azure names with their NIST equivalents: principal (NIST subject: a person or a non-person entity such as a service client), resource (NIST object), action (NIST operation) and environment (conditions independent of principal and resource, such as time, location or threat level); name the functions PAP, PDP, PEP and PIP as NIST does; and break every policy and every request into the same four parts, principal, action, resource and conditions (PARC), before compiling it to Ranger. Why: one term per concept keeps manifests aligned with Azure `@Principal`, `@Resource`, actions and `@Environment` while staying readable against the books, and one shared decomposition makes policies analysable and testable. (plugin v1; NIST SP 800-162 §1.4, §2.2; Authorization in Action ch. 4.3.1, 7.2.)
2. Treat RBAC as the base and ABAC as conditions on top (hybrid, role-centric), as Azure does: a role assignment sets the maximum permission set, and conditions, tag policies and row filters only narrow it, so effective access is the role's permissions intersected with what the attributes permit. Why: roles stay readable and bound what a review must cover (group membership per role, then role permissions), while attributes remove role explosion and keep least privilege. (plugin v1; Attribute-Based Access Control ch. 3.7, 3.12; decision 2026-11-15, conflicts #24.)
3. Evaluation order differs by system (Azure: collect role and deny assignments, block on deny, then check that a role at the scope contains the action after subtraction, then conditions; Ranger: first find the security zone the resource belongs to, then evaluate tag policies before resource policies, where a deny on any tag of the resource wins over every allow, an allow on any tag decides next, and resource policies decide only when no tag policy gave a result; within each policy set, deny before allow; Trino's own file rules, which Ranger replaces on the platform: ordered, the first matching rule decides, no match is denied, and the `system` catalog is open by default); design rules so the outcome is the same under Azure and Ranger. Why: the model must not depend on engine quirks, in Ranger a tag allow can grant access before any resource policy is read, and an ordered first-match model lets an earlier rule hide a later allow. (plugin v1; Azure docs 01, 05; Apache Ranger wiki p. 2, p. 4; Trino DG ch. 10.)

### Policy language and engine selection

Source: Authorization in Action, ch. 8 (merged 2026-11-15).

1. Make every access decision in one engine, Ranger, and keep OPA/Rego only for non-access policy such as retention. Why: Rego has no built-in principal, action and resource structure or types, so access policies written there are hard to analyse and maintain. (Authorization in Action ch. 8.2.4, 8.5.2.)
2. Keep the authored policy format, the manifest, readable by the people who maintain it, and treat generated Ranger policy JSON as build output that nobody edits by hand. Why: XACML policies became too fragile and obscure to update safely. (Authorization in Action ch. 8.1.3; conflicts #6.)
3. When a new engine or policy framework is proposed, record the choice against five questions: primary model, enforcement location, portability, maintainer skills and tolerance for complexity. Why: these narrow the choice better than feature lists. (Authorization in Action ch. 8.5.6.)

### Security zones: resource placement and evaluation

Source: Apache Ranger wiki, p. 4 Introduction of Security Zones (last edited 2022) (merged 2026-11-15).

1. Expect every request to be decided inside one zone: the plugin first finds the zone whose resource list matches the resource and then evaluates only that zone's resource policies; a resource that matches no zone is decided by the unzoned policies. Why: zone policies replace unzoned policies for a zoned resource, so a platform-wide rule reaches institution data only through a tag service linked to the zone or through items inside the zone itself, never through an unzoned resource policy (guardrails as linked tag policies, shared platform policies as tag policies or generated zone items; decision 2026-11-15, conflicts #36). (Apache Ranger wiki p. 4.)
2. Keep zone resource patterns free of overlap, checked in CI across all zones. Why: the plugin places each resource in one zone, and the wiki defines no result for overlapping patterns. (Apache Ranger wiki p. 4.)
3. Treat a policy's zone as fixed: to move a policy, delete it in the old zone and create it in the new one in the same Git change. Why: Ranger cannot move a policy between zones. (Apache Ranger wiki p. 4.)
4. Link the platform tag service to every institution zone and let CI fail a zone without the link. Why: tag services are associated per zone, and a zone without the link silently loses the guardrail tag policies (azure-style-rbac-abac.md › Policy layers, rule 3). (Apache Ranger wiki p. 4.)

Example (rewritten, platform manifest):
```yaml
kind: Zone
metadata: { id: t-acme }
spec:
  resources:
    trino: [ { catalog: t_acme_lake, schema: "*", table: "*" } ]
    kafka: [ { topic: "t.acme.*" } ]           # must not overlap any other zone (CI)
  tagServices: [platform-tags]                  # guardrails reach the zone only through this link
  admins:   { users: [svc-policy-deployer] }   # deployment pipeline only
  auditors: { groups: [platform_t-acme_auditor] }
```

### Distributed processing: coordinator, workers and compute trust

Source: Attribute-Based Access Control, ch. 8.2.2–8.2.3 (merged 2026-11-15).

1. Keep Ranger as the only data-access decision: execution-environment limits (Kubernetes namespace, node pool, the service account of a Spark or Flink job) may only narrow access, never grant it. Why: a local rule that wins over central policy would be a second, unreviewed decision point. (Attribute-Based Access Control ch. 8.2.2; decision 2026-11-15, conflicts #45.)
2. Map each classification (`PII`, `RESTRICTED`) to the namespaces qualified to process it, keep the map in the platform folder, check every job specification against it in CI, and requalify the namespaces on a schedule. Why: data of a class should run only on compute trusted for that class, and qualification decays. (Attribute-Based Access Control ch. 8.2.2, 8.2.3; decision 2026-11-15, conflicts #45.)
3. Reschedule or fail a job that a qualified environment refuses; never bypass the refusal. The job runs as its own workload identity (identity-and-principals.md › Service accounts and ingest identities). Why: a bypass turns the limit into a suggestion. (Attribute-Based Access Control ch. 8.2.3; decision 2026-11-15, conflicts #45.)

### Definitions aligned to NIST SP 800-162

Source: NIST SP 800-162, Guide to ABAC Definition and Considerations, §1.4, §2–§2.4.3, §3.2.2.13 (merged 2026-11-15); Authorization in Action ch. 4, 6, 11, 13 (merged 2026-11-15); Apache Ranger wiki p. 6 (merged 2026-11-15); Attribute-Based Access Control, ch. 3.2, 8.2 (merged 2026-11-15); Hadoop Security, ch. 1 (merged 2026-11-15); Data Management at Scale, 2nd ed., ch. 8 (merged 2026-11-16); Data Mesh, ch. 3, 5, 9 (merged 2026-11-16)

1. Define ABAC as deciding each request by evaluating principal, resource and environment attributes against policies written in those attributes; treat ACLs (identity as the attribute) and RBAC (role as the attribute) as special cases, and apply it on the platform in the hybrid form of starter rule 2 (role assignments narrowed by conditions). Why: one model in which roles, groups and tags are all attributes of the same decision makes the books and Azure comparable; pure ABAC without roles is not adopted (decision 2026-11-15, conflicts #24). (NIST SP 800-162 §2, §2.1, §2.2.)
2. Treat authorization and access control as the same thing and keep the four stages identity, authentication, authorization and accounting (AAA) apart: Keycloak OIDC verifies who is calling, Ranger decides permit or deny on the identity carried by that validated token without a second challenge, and the Ranger audit records the outcome; name what the controls protect with the CIA terms (confidentiality, integrity, availability). Why: verifying an identifier is not a permit or deny decision, and mixing the stages hides which component failed. (NIST SP 800-162 §1.4; Hadoop Security ch. 1.)
3. Express each rule in one of two forms: a Boolean combination of attribute tests for one action (a Ranger policy condition) or a relation between attribute values and the actions they allow (a Ranger tag policy). Why: these are the two rule forms NIST describes, and both compile to existing Ranger mechanisms. (NIST SP 800-162 §2.3.)
4. Give a new or external principal access by giving it the required attributes or group membership, never by editing a policy for that principal; keep rules stable and let day-to-day access changes alter data (groups, attributes, tags), not rules; keep the membership source (Keycloak, read by usersync) and the rule source (the Git manifest) in separate systems with their own owners. Why: in ABAC a new subject gains access by receiving attributes, without a policy change, stable rules over changing inputs avoid policy churn, and membership and rules change at different rates under different owners. (NIST SP 800-162 §2.3; Authorization in Action ch. 6.1.1, 6.2.1; Hadoop Security ch. 1.)
5. Keep PAP, PDP, PEP and PIP as distinct functions even where they run in one process (the four layers of enforcement, decision, access-control data and administration), and map them as follows: PAP = Git manifest plus Ranger Admin, and policy is authored only there, each change committed, peer-reviewed, scenario-tested and deployed by the pipeline; PDP and PEP = the Ranger plugin inside each engine (Trino, Kafka, Spark), which builds the request, evaluates cached policies in-process, enforces the result and then audits it; PIP = usersync from Keycloak for principal attributes and the Ranger tag store fed from OpenMetadata for resource attributes, plus, inside each plugin, context enrichers that add attributes to the request before evaluation, while condition evaluators decide whether a policy item matches; attribute administration (Keycloak attributes, OpenMetadata tags) is an administrative path of its own beside the PAP (identity-and-principals.md › Custom security attributes, rule 3). Why: separate functions can be tested, secured, scaled and audited on their own and change independently, NIST requires coordinated distribution of them, an authorization change is a business change that needs the same discipline as code, and Ranger's hooks place the local information point inside the plugin. (NIST SP 800-162 §2.4.3; Authorization in Action ch. 4.4.2, 4.5.5; Apache Ranger wiki p. 6; Attribute-Based Access Control ch. 3.2; DMaS ch. 8; conflicts #6, #13.)
6. Distribute decisions as local PDPs that read one central policy store: every engine plugin downloads its policies, users, groups and tags from Ranger Admin and decides locally, never through a synchronous call to Ranger Admin per request; do not add a second decision service per application; version the distribution and monitor that every plugin's downloaded versions match Ranger Admin. Why: NIST describes local decision points reading a central digital-policy store as one valid distribution, an externalized decision service serves many applications from one policy source, and distributed decision points give inconsistent answers when their caches are stale. (NIST SP 800-162 §2.4.3, §3.2.2.13; Authorization in Action ch. 4.5.3, 4 Do/Don't; Data Mesh ch. 3, 5, 9.)
7. Keep actions a fixed, enumerated set per engine (the Ranger access types, for example `select`, `insert`, `publish`, `consume`) and never model an action as an attribute; request context such as columns or payload values is a separate `@Request` family. Why: NIST defines operations as fixed values, and naming the two apart keeps the Azure `@Request` family without a second meaning for actions. (NIST SP 800-162 §2.3; decision 2026-11-15, conflicts #26.)
8. Place the enforcement point where the context for the decision exists, and enforce and audit inside the platform, not only at its edge: the Ranger plugin in each engine decides on data for every path into it, including jobs and service-to-service calls (trino-security.md › Enforcement points on every path, rule 1); a gateway (Trino Gateway), a network or mTLS rule or an OAuth scope only pre-filters at the edge and never replaces the plugin decision. Why: each placement trades context for uniformity, the edge lacks resource context, an insider who is past the perimeter otherwise has unrestricted access, and on the platform only Ranger decides access. (Authorization in Action ch. 4.5.1, Table 4.1, 11.1.5, 11.2.2, 13.5.1; Attribute-Based Access Control ch. 8.2.3, 8.2.4; Data Mesh ch. 3, 9.)

### DAC, MAC, RBAC, ABAC compared; role explosion

Source: Authorization in Action, ch. 1, 5, 6 (merged 2026-11-15); Attribute-Based Access Control, ch. 2, 3.9 (merged 2026-11-15); Hadoop Security, ch. 11, Afterword (merged 2026-11-15).

1. Do not use access-control lists (principals listed on each resource) for human access: they fit only stable resources, few principals, clear ownership and simple policy, which on the platform is the direct service-client assignment of conflicts #12; people get access through role assignments to groups. Why: per-resource lists give no global view, record no reason, and every join or leave touches many lists. (Authorization in Action ch. 5.1.3–5.1.5; conflicts #12.)
2. Use role assignments for stable job functions and add conditions only for what roles cannot express (azure-style-rbac-abac.md › Conditions, rule 2). Why: roles are fast, predictable and readable by auditors but blind to time, relationship and other runtime context. (Authorization in Action ch. 5.2, 5.3, 6.3.3; decision 2026-11-15, conflicts #24.)
3. Treat as role explosion, and its attribute-based twin, a role catalogue many times larger than the number of people, dozens to hundreds of variants of one role, or dozens to hundreds of overlapping near-duplicate policies; remove the varying part into attributes, tags, scope or time-bound conditions (attribute-and-tag-design.md › When to decide by role, by scope, or by attribute). Why: each signal shows context encoded in names instead of in attributes, and n permissions allow up to 2^n distinct roles, so a role catalogue has no natural bound. (Authorization in Action ch. 5.2.4, 5.2.5, 6.2.5; Attribute-Based Access Control ch. 3.9.)
4. Treat Ranger as the policy-based runtime: roles, attributes and tags are inputs to externalized policies evaluated at request time, and access logic never lives in engine or application code, engine rule files or views built as access controls; enforce in the shared engine through its Ranger plugin instead of separately in each application, and keep the policies of every engine in one place that administrators and Auditors can both read. Why: embedded logic cannot be audited or changed consistently, static lists fail on scale, context and audit, every application would otherwise re-implement authorization, and controls spread across components make mistakes too easy. (Authorization in Action ch. 1.2, 4 Do/Don't, 6.3; Hadoop Security ch. 11, Afterword; conflicts #10.)
5. Do not adopt relationship-based access control: express ownership and sharing as owner groups per dataset or as "principal attribute equals resource tag" conditions, and never take policy from an end-user sharing interface. Why: Ranger and Azure have no relationship graph, and a sharing UI would be a second policy source beside Git. (Authorization in Action ch. 6.1, 9.5; decision 2026-11-15, conflicts #32.)
6. Administer access centrally, as mandatory access control does, and never at a data holder's discretion: no dataset owner, steward or reader grants access to others by any path outside the reviewed Git manifest (Ranger UI, engine-native grants, sharing interfaces); an Institution Owner approves changes to its folder but does not grant directly. Why: discretionary owner grants cannot stop data from being copied and re-shared, and one central path keeps every authorization reviewable. (Attribute-Based Access Control ch. 2.3, 2.5; conflicts #6, #32.)

### Policy combining and evaluation order (XACML, Azure, Ranger)

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 02, 05 (merged 2026-11-14); Authorization in Action, ch. 7, 8, 13, 15 (merged 2026-11-15); Apache Ranger wiki, p. 1 Deny conditions and excludes (2015/2016), p. 2 Tag-based policies, p. 8 Custom plugin, p. 14 Release notes 2.5–2.9 (merged 2026-11-15); Attribute-Based Access Control, ch. 4, 6 (merged 2026-11-15).

1. Combine allow decisions by union: a principal may perform an action if any assignment reaching it allows it, and no allow item restricts another. Why: Azure RBAC is additive. (Azure docs 01.)
2. Let a deny guardrail win over every allow, so that the platform runs one combining algorithm, deny-overrides, at every level: Ranger evaluates the deny items of all policies that match a resource before any allow item, and platform guardrails and institution zone policies are evaluated as one set in one decision whose outcome and deciding policy are logged; the metapolicy states this algorithm (attribute-and-tag-design.md › Written policy, digital policy and metapolicy, rule 3), and no permit-overriding policy exists except the break-glass deny exception of conflicts #8, which still needs a matching allow. Why: Azure deny assignments block actions even when a role assignment grants them; forbid-overrides-permit across layered policy stores gives one result no matter which team wrote which policy; deny-overrides and permit-overrides give different results for the same rules; and Ranger's fixed deny-first order is what makes a deny predictable. (Azure docs 05; Authorization in Action ch. 7.3.2–7.3.3, 8.4.1, 13.4.1, 13.4.3, 15 p. 297; Apache Ranger wiki p. 1; Attribute-Based Access Control ch. 4.4.3; conflicts #8.)
3. Do not treat subtraction inside a role as part of combining: excluded actions are removed from that role only, and another role may still grant them. Why: Azure `NotActions` are not deny. (Azure docs 02.)
4. Give every compiled policy an explicit effect (allow; deny only in guardrails), a scope (principals, access types, resources) and conditions kept apart from the scope, and review each part on its own. Why: scope selects the relevant policies quickly, conditions add context without bloating scope, and a three-part contract can be reviewed part by part. (Authorization in Action ch. 7.2.1–7.2.3.)
5. Treat as denied in every engine both a request that matches no allow item and a request whose evaluation fails. Ranger returns "undetermined" when nothing matches (the XACML "not applicable"). An evaluation error such as a type mismatch or a missing attribute (the XACML "indeterminate") must also end in deny. So configure every plugin to deny undetermined requests, check per plugin that no other access control (for example Trino file-based rules or an engine's native permissions) answers when no Ranger policy matches, make every custom authorizer treat a missing result or an error as deny, and keep the outcome in the audit record (policy ID -1 when no policy decided; audit-design.md › Starter rules, rule 1). Why: deny by default makes an explicit allow the only way in, a missing or failed evaluation must never grant, and the wiki notes that some components fall back to their own permissions on undetermined, which reopens access outside Ranger. (Authorization in Action ch. 7.3.2, 7.3.3, 7 engine notes; Apache Ranger wiki p. 1, p. 8; Attribute-Based Access Control ch. 4.3.2, 4.6, 6.2.1.)
6. Keep the combined result independent of policy order on Ranger: deny items and `override` priority exist only in platform guardrail policies, and CI rejects both elsewhere. Why: the book's combining model is order-independent, while Ranger precedence also depends on policy priority and exception items; since Ranger 2.7 an `override`-priority resource allow beats a normal-priority tag deny (RANGER-5225), which lab V1 confirmed on 2.8.0 (effective after about 29 s), while a normal-priority allow still loses to the tag deny. (Authorization in Action ch. 7.3.3; Apache Ranger wiki p. 1, p. 2, p. 14; trial: verification lab V1, 2026-11-14; conflicts #1, #6.)
7. Read every Ranger access policy as four ordered lists, deny items, deny exceptions, allow items and allow exceptions: a match on a deny exception only skips that deny item and evaluation continues with the next deny item and then the allow phase; a match on an allow exception only skips that allow item; a request that no allow item grants is undetermined (rule 5). An exception therefore never grants access and never cancels a matching deny item of another policy. Why: the engine checks all deny items of all matching policies first and an exception acts only on its own item. (Apache Ranger wiki p. 1.)

Example (Ranger policy JSON; effect = `policyItems` (allow), scope = groups, access types and resources, condition = `validitySchedules`):
```json
{
  "service": "trino-interactive",
  "zoneName": "t-acme",
  "name": "ra-0081",
  "description": "ra-0081 [ADR-012]: Quarterly reporting window, ticket ACC-210",
  "resources": {
    "catalog": { "values": ["t_acme_lake"] },
    "schema":  { "values": ["gold_sales"] },
    "table":   { "values": ["*"] },
    "column":  { "values": ["*"] }
  },
  "policyItems": [
    { "groups": ["platform_t-acme_analyst"], "accesses": [ { "type": "select", "isAllowed": true } ] }
  ],
  "validitySchedules": [
    { "startTime": "2027/01/01 00:00:00", "endTime": "2027/03/31 23:59:59", "timeZone": "Europe/Istanbul" }
  ]
}
```

Example (rewritten; CI decision-table cases for the four-list order):
```yaml
# platform guardrail guardrail-restricted-read: deny group public, deny exception platform_breakglass
# assignment ra-0043: allow platform_t-acme_analyst select on t_acme_lake.gold_sales.*
- id: restricted-analyst
  request: { principal: { user: alex, groups: [platform_t-acme_analyst] }, access: "trino:select",
             resource: t_acme_lake.gold_sales.orders }
  entities: { tags: { t_acme_lake.gold_sales.orders: [RESTRICTED] } }
  expect: deny          # deny item for public matches; no exception applies
- id: restricted-breakglass-without-allow
  request: { principal: { user: kim, groups: [platform_breakglass] }, access: "trino:select",
             resource: t_acme_lake.gold_sales.orders }
  entities: { tags: { t_acme_lake.gold_sales.orders: [RESTRICTED] } }
  expect: deny          # exception skips the deny, but no allow item matches: undetermined = deny
- id: restricted-breakglass-with-allow
  request: { principal: { user: kim, groups: [platform_breakglass, platform_t-acme_analyst] },
             access: "trino:select", resource: t_acme_lake.gold_sales.orders }
  entities: { tags: { t_acme_lake.gold_sales.orders: [RESTRICTED] } }
  expect: allow         # exception skips the deny, ra-0043 allows
```

Example (rewritten; CI decision-table case for a missing attribute):
```yaml
# ra-0051 grants data-reader on /institutions/t-acme with condition "exists(@Principal.projects) and ..."
- id: ra-0051-missing-attribute
  request: { principal: { user: alex, groups: [platform_t-acme_analyst], attributes: {} },
             access: "trino:select", resource: t_acme_lake.gold_sales.orders }
  entities: { tags: { t_acme_lake.gold_sales.orders: [project=apollo] } }
  expect: deny        # attribute absent: the condition cannot match, no other allow item applies
```

### Service-level vs object-level authorization; control plane vs data plane

Source: Azure RBAC and Azure ABAC documentation (Microsoft Learn), pages 01, 02, 17 (merged 2026-11-14); Attribute-Based Access Control, ch. 2.2 (merged 2026-11-15); Hadoop Security, ch. 6 (merged 2026-11-15); Data Mesh, ch. 5, 9, 12 (merged 2026-11-16)

1. Keep control-plane rights (manage zones, policies, assignments, datasets) and data-plane rights (read and write data) in separate permission lists of every role, and never bundle administrative operations into a data role. Why: Azure grants data access through data actions kept apart from management actions, administrators who manage resources do not necessarily need the data, and administrative rights change who can access data, so they need their own assignment and review. (Azure docs 01, 02, 17; Attribute-Based Access Control ch. 2.2; Data Mesh ch. 9.)
2. Grant data access only through data-plane access types and administrative capability only through control-plane rights, so that a wildcard on one plane never reaches the other. Why: Azure keeps management actions out of data-action lists so a broad management role does not read data. (Azure docs 02.)
3. Give owner and administrator roles no data access types; an owner who needs data holds a separate, eligible data-role assignment. Why: a management role that implies data access merges the two planes. (Azure docs 02, 06; decision 2026-11-15, conflicts #20.)
4. Authorize every request in two steps: a service-level gate first decides whether the principal may use the interface at all (Trino Gateway routing and Trino's system-level access types; the Kafka cluster resource; the Ranger Admin API), then the object-level policy decides on the data (catalog, schema, table, column; topic, consumer group); treat the gate as a control-plane check and never let it replace an object-level data policy. Why: the protocol check stops even a highly privileged account before any data is touched, and object permissions are a separate layer with their own review. (Hadoop Security ch. 6.)
5. Separate client interfaces from administrative interfaces in every engine and in Ranger, open the administrative ones only to named platform admin groups or, for Ranger Admin writes, only to the deployment service account, and check every admin group list in each access review. Why: open defaults let ordinary users run administrative operations. (Hadoop Security ch. 6; conflicts #6.)
6. Replace every open default on a service-level gate (all users, `*`, `public`) with named groups; leave a gate open to all authenticated principals only where callers cannot be listed in advance and the object-level policy or a per-job token still protects every call, and record each such exception with its reason. Why: service-level defaults ship open, and an open gate is safe only while another check still decides. (Hadoop Security ch. 6; conflicts #16.)
7. Give the engines' own runtime accounts and the BI tool's service client no Ranger data grants of their own, and give no group standing superuser rights: administrative bypass exists only as the per-engine service principals of conflicts #4, the eligible break-glass group of conflicts #8 and the sealed lockout credential of conflicts #22. Why: a service account with standing data access hides who acted, and a superuser group reads and deletes any data outside every guardrail. (Hadoop Security ch. 6; conflicts #4, #8, #22.)
8. Run a data-changing operation that applies to every dataset in the same way, such as erasing a data subject's records for a right-to-be-forgotten request, only as one platform function with one defined trigger and one execution path for all datasets: the OPA retention policy decides (compliance-kvkk.md › Privacy and regulatory use of roles, rule 3), a platform job executes, and no consumer, steward or institution receives update or delete for it; where erasure works by destroying keys, keep those keys in OpenBao, outside the dataset. Why: such an operation is rare, sensitive and identical for every dataset, so it belongs to the control plane and not to any data grant, and erasure by key destruction needs the keys to live outside the data. (Data Mesh ch. 5, 9, 12; conflicts #50.) The job runs as its own workload identity without a mask exemption unless it holds a documented PII Reader assignment (identity-and-principals.md › Service accounts and ingest identities, rule 6).

Example (rewritten; CI decision-table cases for gate then object check):
```yaml
# ra-0140 (hypothetical): data-reader for platform_t-acme_analyst on /institutions/t-acme/domains/sales
- id: gate-kafka-cluster-admin
  request: { principal: { user: alex, groups: [platform_t-acme_analyst] },
             access: "kafka:alter_configs", resource: { cluster: kafka-prod } }
  expect: deny        # management operation: not in any data role (service-level gate)
- id: object-consume
  request: { principal: { user: alex, groups: [platform_t-acme_analyst] },
             access: "kafka:consume", resource: { topic: t.acme.sales.orders } }
  expect: allow       # authenticated listener passed, ra-0140 allows on the topic prefix
```

Example (rewritten, platform manifest):
```yaml
kind: PlatformFunction
metadata: { id: pf-subject-erasure }
spec:
  trigger: "opa:retention/erasure-request"          # one trigger for every dataset
  executor: platform-erasure-job                     # workload identity (conflicts #23)
  appliesTo: /institutions/*                         # same path for t-acme and every other zone
  keyStore: "openbao:transit/t-acme-subject-keys"    # only where erasure is by key destruction
  dataGrants: none                                   # no update/delete for platform_t-acme_analyst or any group
```

### Central vs domain policy ownership (federated governance)

Source: Data Management at Scale, 2nd ed., ch. 2, 3, 4, 8, 11 (merged 2026-11-16). Layer split: azure-style-rbac-abac.md › Policy layers; governance bodies: role-engineering.md › Governance roles, rule 4; Data Mesh, ch. 5 (merged 2026-11-16)

1. Run the governance services once for all institutions, in the platform layer and owned by the platform team (Ranger Admin, the Git policy repository, OpenMetadata with its lineage, Keycloak); institutions use them and run no governance service of their own. Add a separate set only when an institution's metadata must stay hidden from the other institutions and the per-institution catalog visibility of attribute-and-tag-design.md › Classification to policy chain, rule 7 cannot achieve it, and record the reason in the institution's directory record (azure-style-rbac-abac.md › Best practices and limits, rule 4). Why: one domain-agnostic management layer oversees what every domain creates and uses, and every further management layer adds overhead. (DMaS ch. 3; conflicts #7, #52.)
2. Keep ownership with the institutions under every topology: each institution owns the datasets, policies and access decisions of its own zone, and enterprise-wide data owned by a central team (for example shared reference data) is onboarded like any institution, with its own zone, owner and folder, never placed at platform scope or outside zones. Make a decision platform-wide only when it cuts across every dataset (the access-control model, identity, the sensitivity levels and how each is handled, regulatory actions that apply to all data, interoperability standards, and the rule that every dataset has one owner), keep those platform-wide policies few and narrow, and implement each one as platform automation (a guardrail or tag policy, a template, a CI check), never as a manual review; leave to the owning institution what only its domain knows (data quality, modelling, timeliness, and which of its columns carry which class, attribute-and-tag-design.md › Classification to policy chain, rule 5). Why: shared data still needs an accountable owner, unzoned policies are reserved for platform resources, a decision belongs at the most local level that has the knowledge and the accountability, and platform-wide policies are hard to change and take effect only through automation. (DMaS ch. 3; Data Mesh ch. 5; conflicts #13, #27, #36.)
3. Let ownership follow the origin of the data: register each golden source (the system where a fact is first recorded) in OpenMetadata with its owning institution and data owner before anything is shared; change original data only there, with write access types only for the owning institution's pipelines and changes approved by the owner; and let nobody publish, copy or grant another owner's data onward without that owner's recorded approval (to another institution only through azure-style-rbac-abac.md › Role assignments, rule 12). Why: accountability depends on a known origin, edits away from the source break trust and lineage, and the owner stays accountable for where its data goes. (DMaS ch. 2, 8, 11; conflicts #27, #46.)
4. Treat data that a consuming domain creates by business logic (new facts, not filters, unions or renames) as new data: the creating domain owns it and publishes it as a new data product with its own classification, policies and lineage, and onboarding, classification and approval start again for it; data that is only integrated for one use case is not offered to other domains until it is published this way. Why: new facts mean new ownership, consuming raw use-case data creates coupling, and integrated data is tied to one use case. (DMaS ch. 2, 4, 8, 11; conflicts #10.) A derived dataset that adds no new facts keeps its own owner but records its `sourceOwners`, who approve its creation and any further sharing (decision 2026-11-16, conflicts #56).
5. Share data between domains and institutions only through registered, published data products (for tables, the published layers of azure-style-rbac-abac.md › Scope design and inheritance, rule 7), never through an intermediate integration layer shared between domains. Why: extra layers hide the source and break lineage. (DMaS ch. 2; conflicts #48.)
6. Centralise control, not data, where compliance outweighs autonomy: the platform decides once how access is granted, revoked and verified, while every grant is encoded per dataset and evaluated by the engine's Ranger plugin at access time (Definitions aligned to NIST SP 800-162, rule 6); every grant across an institution boundary passes the one governed point of azure-style-rbac-abac.md › Role assignments, rules 12 and 13 (Git review, the owning Institution Owner's approval, Ranger in the owning zone), while grants inside a zone stay with the institution; never route data through a central hub copy to control its distribution. Why: one governed point can block distribution without taking ownership away from the domains, and a global decision executed locally at each access avoids a central bottleneck. (DMaS ch. 3, 8; Data Mesh ch. 5; conflicts #10, #46.)
7. Record the reason why a policy is cross-cutting, and therefore platform-wide, in an ADR approved by the governance committee. Why: the line between global and local policy has no test, so each decision needs a written reason. (Data Mesh ch. 5; decision 2026-11-16.)

Example (rewritten, platform manifest):
```yaml
kind: Dataset
metadata: { id: ds-t-acme-sales-orders }
spec:
  institution: t-acme
  resource: t_acme_lake.gold_sales.orders
  goldenSource: t-acme order service          # where each order is first recorded (rule 3)
  dataOwner: t-acme sales lead
  applicationOwner: t-acme order platform team
  writers: [pipeline:t-acme-orders-ingest]     # workload identity (conflicts #23)
---
kind: Dataset
metadata: { id: ds-t-acme-churn-scores }
spec:
  institution: t-acme
  resource: t_acme_lake.gold_marketing.churn_scores
  kind: product                                # new facts, new owner (rule 4)
  dataOwner: t-acme marketing lead
  derivedFrom: [ds-t-acme-sales-orders]        # lineage; inherited tags re-evaluated by the tag authority
```

