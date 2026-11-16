# Policy verification and testing

Status: starter rules; verification properties and policies as code filled from NIST SP 800-162 (2026-11-15) and Authorization in Action (2026-11-15); test methods, policy life cycle and AI-assisted policy work filled from Authorization in Action (2026-11-15); fault classes, completeness and consistency across tag and resource policies, safety requirements, mutation and oracle testing from Attribute-Based Access Control (merged 2026-11-15); Ranger REST objects, import/export and labels from the Apache Ranger wiki (merged 2026-11-15); whole-set parse, per-principal privilege listing and engine query tests confirmed by Hadoop Security (merged 2026-11-15); lineage impact analysis, review triggers and the written governance policy from Data Governance: The Definitive Guide (merged 2026-11-16); one standard per policy type, build/deploy/access checks, runtime conformance monitoring, governance phases and fitness functions from Data Mesh (merged 2026-11-16).

## Starter rules

1. Ship every policy change with a decision table that includes negative cases: for every case "group G may do X", at least one case shows that everyone else is denied X (for example the same request from another institution). Pass the table before the change is pushed to Ranger. Why: the table is both the review artefact and the test, policies must be tested before enforcement, policies fail silently at their boundaries, and wildcard grants leak permissions that positive tests miss. (plugin v1; NIST SP 800-162 §2.4.3; Authorization in Action ch. 10.3; Attribute-Based Access Control ch. 6.4.3.)
2. Test through the engine, not only the policy store: run allowed and denied queries per role against a test Ranger before deploy. Why: enforcement is what matters. (plugin v1; Apache Ranger wiki p. 6; Hadoop Security ch. 7.)

### AI-assisted policy work and its limits

Source: Authorization in Action, ch. 17 (merged 2026-11-15).

1. Never let a language model decide allow or deny, and never hold access rules in a prompt; Ranger decides. Why: a model gave both permit and deny for identical requests. (Authorization in Action ch. 17.1, Fig. 17.1.)
2. Let AI assistants, including the plugin's `ranger-policy-author` and `ranger-config-reviewer`, only draft, explain and analyse: they change policy only by opening a Git pull request, and a named person accepts or rejects every proposal. Why: choosing among valid options needs accountable judgement. (Authorization in Action ch. 17.2.4.)
3. Check every AI explanation of a decision against the Ranger audit record and the policies it names; the engine's record is the only authoritative explanation. Why: a fluent AI explanation did not match the real evaluation. (Authorization in Action ch. 17 p. 334.)

### Verification properties: consistency, completeness, conflicts

Source: NIST SP 800-162, §2.3, §3.1.3.6, §3.2.1.2, §3.2.1.7 (merged 2026-11-15); Authorization in Action, ch. 7, 9, 10, 13, 15 (merged 2026-11-15); Attribute-Based Access Control, ch. 4.5–4.6, 6.3–6.4 (merged 2026-11-15); Hadoop Security, ch. 7 (merged 2026-11-15).

1. Check completeness and consistency in CI over tag and resource policies combined. Report every in-scope dataset that no assignment covers with allowed principals, actions and conditions (incomplete: no rule decides), and every request that one policy allows and another denies (inconsistent) unless the deny is a declared guardrail. Why: an object without a policy has no defined access rule, policy is spread over several Ranger policy types, and Ranger resolves overlaps silently by its fixed order (deny before allow, tag before resource), so CI is the only place an unintended overlap shows up. (NIST SP 800-162 §2.3; Attribute-Based Access Control ch. 6.4.)
2. Before deployment, check in CI that no institution change weakens, broadens or contradicts a platform policy (guardrail, role definition, mandatory tag), that rules from different authors do not contradict each other, and that combining the layers keeps security (what a layer denies stays denied) and autonomy (what an institution permits stays permitted unless a declared guardrail removes it); the Ranger deny guardrails hold at runtime. Why: many stakeholders write rules, a local relaxation opens access that the platform forbids, lower policy layers may never override upper ones, and cross-domain inheritance can grant what one domain denies. (NIST SP 800-162 §3.1.3.6, §3.2.1.2, §3.2.1.7; Authorization in Action ch. 13.5.9, 15.3.1; Attribute-Based Access Control ch. 6.3; conflicts #6, #7.)
3. Validate every manifest against the platform schema before merge, and parse the whole change set so that any parse or schema error fails all of it before anything is deployed: principal types, resource types and access types per engine, attribute names with their types, and context keys. Bind each access type to the narrowest principal and resource types, never "any". CI blocks unknown names, type mismatches (a string compared with a number) and "any" principals, and checks that the attribute sources (the usersync attribute list, tag sync) deliver every attribute that a condition uses. Change the schema first, then the policies, then the tests; institutions cannot redefine the schema. Why: a schema gives one vocabulary and type safety, a misspelt action name broke combined policies in the book's case, one syntax error once invalidated a whole policy file and denied everyone, loose typing silently widens access, and a type mismatch or an attribute nobody delivers makes legitimate requests fail silently. (Authorization in Action ch. 7.3.2, 7.4.1, 9.2, 9.2.3, 10.2.1, 13.5.7; Attribute-Based Access Control ch. 4.5.1, 4.6; Hadoop Security ch. 7.)
4. Fail CI on the four policy faults: privilege leakage (a principal reaches access that no requirement grants, including through scope wildcards or nested groups), privilege blocking (a required access is denied), cyclic inheritance (a cycle in group nesting, identity-and-principals.md › Group membership in tokens, rule 1) and unintended conflicting decisions (rule 1). Why: each fault breaks safety, either by over-granting or by blocking legitimate work. (Attribute-Based Access Control ch. 6.3.)

Example (CI decision-table case):
```yaml
# an institution change in folder t-acme must not open RESTRICTED data
- { expect: deny, principal: platform_t-acme_analyst, access: "trino:select",
    resource: t_acme_lake.gold_finance.ledger, tags: [RESTRICTED] }
```

### Test methods for policies

Source: Authorization in Action, ch. 6, 7, 10 (merged 2026-11-15); Attribute-Based Access Control, ch. 6 (merged 2026-11-15); Hadoop Security, ch. 7 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 6 (merged 2026-11-16); Data Mesh, ch. 5 (merged 2026-11-16)

1. Build every test request from complete inputs: principal (user and groups as the validated token and usersync deliver them), access type, resource path, context, and entity data (group memberships, tags) as fixtures. Why: a decision depends on all of these, and a test missing one tests something else. (Authorization in Action ch. 7.3.1, 10.3.)
2. Run four kinds of tests in CI: unit tests per rule, scenario tests, a regression test for every fixed bug, and coverage with at least one test per major rule; merge only when validation, analysis and tests all pass. Why: example tests alone miss boundaries, and one gate keeps every check mandatory. (Authorization in Action ch. 10.2.4, 10.3; Data Mesh ch. 5.)
3. Analyse the whole compiled policy set before deploy, beyond example tests: over-grant, unreachable or redundant items, near-duplicate policies, gaps that fall to default deny, a listing of effective privileges per principal, and an effective-access diff before and after the change, which a refactor must leave unchanged; for a change to a class policy or a tag, or the deletion of a table, add from OpenMetadata lineage the downstream datasets and dashboards whose access changes, and show the principals and end products that lose or gain access in the pull request before it is applied. Why: example tests cover only the listed scenarios, a per-principal listing shows what one person can actually do, Ranger has no built-in set-level analysis, so the CI harness must approximate it, and an impact list lets administrators decide knowingly. (Authorization in Action ch. 6.2.5, 7.4.6, 10 p. 202–203; Hadoop Security ch. 7; Data Governance DG ch. 6.)
4. Keep each institution's safety requirements in Git beside its manifests as machine-checkable expectations (must allow, must deny, SoD pairs, conflict classes). Classify each requirement as static, dynamic (counts, dynamic separation of duties) or historical (conflict of interest), and test dynamic and historical ones with stateful scenarios (sequences of activations, group changes and requests) instead of single request-decision rows. Why: verification is only as good as the stated requirements, and a request-to-decision table cannot capture state. (Attribute-Based Access Control ch. 6.2, 6.4, 6.4.1.)
5. Run a second line of defence after the requirement checks: mutate each compiled policy item (flip allow to deny, or remove the item) and require at least one test to fail, and keep the surviving mutants and the results of every release as evidence. Why: requirement checks still pass when the requirements are incomplete, and a rule that no requirement detects is unverified. (Attribute-Based Access Control ch. 6.4.1, 6.4.2, 6.5.)
6. Before release, generate a decision oracle from all combinations of the requirement variables: principals, access types, resources and expected decisions, plus row-filter and mask results on Trino and topic prefixes and consumer groups as separate variables on Kafka. Replay the oracle against a test Ranger through the engines (starter rule 2) and compare the live decisions. When requirements reach the hundreds, cut the combinations with a combinatorial reduction method, never with ad hoc omissions. Why: implementation typos show up only under exhaustive comparison, and the full product grows too large to run. (Attribute-Based Access Control ch. 6.4.4, 6.5, 6 engine notes.)
7. Cover 100% of guardrail, PII and `RESTRICTED` policies with CI decision-table cases and accept zero surviving mutants on them. Why: these are the policies whose failure exposes data. (Authorization in Action ch. 10; decision 2026-11-16.)

Example (CI test cases):
```yaml
- id: ra-0043-allow
  request: { principal: { user: alex, groups: [platform_t-acme_analyst] },
             access: "trino:select", resource: t_acme_lake.gold_sales.orders }
  entities: { tags: { t_acme_lake.gold_sales.orders: [] } }
  expect: allow
- id: ra-0043-deny-other-institution
  request: { principal: { user: sam, groups: [platform_t-beta_analyst] },
             access: "trino:select", resource: t_acme_lake.gold_sales.orders }
  expect: deny
- id: ra-0043-deny-pii-column
  request: { principal: { user: alex, groups: [platform_t-acme_analyst] },
             access: "trino:select", resource: t_acme_lake.gold_sales.customers.email }
  entities: { tags: { t_acme_lake.gold_sales.customers.email: [PII] } }
  expect: masked
```

Example (rewritten, requirement file and mutation result):
```yaml
kind: SafetyRequirements
metadata: { id: req-t-acme, zone: t-acme }
spec:
  static:
    - { id: R1, expect: allow, principal: { group: platform_t-acme_analyst },
        access: "trino:select", resource: "t_acme_lake.gold_sales.*" }
    - { id: R2, expect: deny,  principal: { notGroup: platform_t-acme_analyst },
        access: "trino:select", resource: "t_acme_lake.gold_sales.*" }
    - { id: R3, expect: masked, principal: { group: platform_t-acme_analyst },
        access: "trino:select", resource: t_acme_lake.gold_sales.customers.email, tags: [PII] }
# CI mutation report, release 2027.01: ra-0043 allow->deny killed by R1; ra-0043 removed killed by R1; surviving mutants: 0
```

### Policy life cycle: plan, design, operate, retire

Source: Authorization in Action, ch. 4, 10, 13, 14 (merged 2026-11-15); Data Governance: The Definitive Guide, ch. 4 (merged 2026-11-16).

1. Run every policy through six stages: create, review and approve, deploy and use, monitor, update, retire. Why: deployment discipline alone lets policies drift, become orphaned or outlive their purpose. (Authorization in Action ch. 14.2.)
2. Create in this order: intent, purpose and scope, an ADR with the rationale, the entity and attribute model, then the policy logic. Why: logic written before modelling encodes accidental intent. (Authorization in Action ch. 14.2.1, 14 p. 273.)
3. Carry policy metadata in the manifest: owner, ADR ID, review-by date and compliance tags; start with quarterly review dates, review earlier when the organization or a regulation changes, measure adherence through CI results and the drift check, and record each remedy. Why: rules stay traceable to their reason and owner, review dates stop policies outliving their purpose, and a policy is a living document that has to follow organizational and regulatory change. (Authorization in Action ch. 14.2.1, 14.5, 14.7; Data Governance DG ch. 4.)
4. Approve a change only after the automated checks (schema, analysis, tests) pass and a second person has reviewed its intent (role-engineering.md › Static and dynamic separation of duties, rule 2), with the coordinator and compliance reviewer of role-engineering.md › Governance roles, rule 2 where they apply. Why: approval endorses intent, not syntax. (Authorization in Action ch. 4.5.5, 14.2.2, 14.3.2.)
5. Change a deployed policy only through a new change proposal with the same review, whatever the trigger; a temporary exception is a time-bound assignment (azure-style-rbac-abac.md › Role assignments, rule 10). Why: in the book's case an override made outside review stayed active for nine months. (Authorization in Action ch. 14.2.5, 14 p. 271.)
6. Keep every compiled Ranger policy traceable to a stable ID and version, and keep the previous version deployable for a one-step rollback. Why: traceability explains decisions and rollback limits the damage of a bad change. (Authorization in Action ch. 10.2.3, 13.5.8.)
7. Retire deliberately: check dependants (assignments, templates, tests), remove the policy through Git in one coordinated change, and keep it archived in Git history with a link to its successor. Why: in the book's case a policy outlived the retention period it referred to. (Authorization in Action ch. 14.2.6, 14 Do/Don't.)
8. Keep one written governance policy above the manifests, approved by the governance committee (role-engineering.md › Governance roles, rule 4), with a fixed structure: the sources it covers, each with its classification and required protection; principles for access, usage, integration and integrity, each naming the mechanism that enforces it (manifest, CI check, OPA retention policy, monitoring); the roles as a RACI matrix (role-engineering.md › Governance roles, rule 6); its review date; and how adherence is measured and remedied. Why: rules derive from classes, a principle without an enforcing procedure cannot be audited, and the written policy is the human layer above policy as code. (Data Governance DG ch. 4.)
9. Target at most 5 minutes from merge to enforcement in every engine and at most 15 minutes to roll back a faulty policy change through a Git revert; measure decision latency only for monitoring, without a target, because Ranger plugins decide locally. Why: a known propagation and rollback time tells an institution when a change takes effect and how fast a mistake can be undone. (Authorization in Action ch. 10, 14, 15; decision 2026-11-16.)

Example (manifest metadata):
```yaml
kind: RoleAssignment
metadata:
  id: ra-0043
  owner: t-acme-institution-owner
  adr: ADR-012
  reviewBy: "2027-03-31"
  compliance: ["kvkk:personal-data"]
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales
```

### Policies as code in a federated model

Source: NIST SP 800-162, §3.2.1.7, §3.2.2.1, §3.3.3 (merged 2026-11-15); Authorization in Action, ch. 1, 6, 10 (merged 2026-11-15); Data Mesh, ch. 3, 5, 14, 15 (merged 2026-11-16)

1. Protect the policy repository like the data it governs: only code owners who know the written policy may change an institution folder, and only the platform team the platform folder. Why: a wrong author is a direct path to wrongful access. (NIST SP 800-162 §3.2.1.7; conflicts #7.)
2. Publish the specification of every authorization interface (manifest schema, usersync and tag-sync contracts, the Ranger REST calls the pipeline uses), use only a narrow profile of any standard, and notify consumers before a change. Why: optional standard parts break interoperability, and relying parties must plan for changes. (NIST SP 800-162 §3.2.2.1, §3.3.3.)
3. Keep Git as the policy source and Ranger Admin as the runtime store that accepts only what the deployment pipeline writes after review and CI; a direct change in the Ranger UI is drift and is overwritten; limit read access to the policy repository to the platform team, institution code owners and Auditors; hide restricted attribute values from principals, not the policy logic. Why: policy-as-code must stay reviewable while rule details and value lists need not be public, and in the book's case a direct store hotfix was untraceable and later overwritten. (NIST SP 800-162 §3.1.3; Authorization in Action ch. 10 p. 197; conflicts #6; decision 2026-11-15, conflicts #28.)
4. Separate the two change paths: policy as code (role definitions, assignments, guardrails in the manifest) goes through pull request, second-person review and CI; policy as data (Keycloak groups and attributes, OpenMetadata tags) changes only in its authoritative system, by its owner, through a logged and approved workflow, takes effect without a policy release, and is never edited in the manifest. Why: the two forms have different authors and review paths, and data changes shift decisions immediately and need their own audit trail. (Authorization in Action ch. 1.4.1–1.4.3, 6.3.1, 6.3.2, 10 p. 190.)
5. Never let an event, obligation or access history change access automatically: approximate history-based rules statically (conflict classes, role-engineering.md › Static and dynamic separation of duties, rule 3), and when an event must change access, let the workflow open a Git pull request that a named person approves. Why: automatic changes would bypass review and the deployment pipeline, and Ranger keeps no access history to base them on. (Attribute-Based Access Control ch. 2.6.2, 5.3.3; decision 2026-11-15, conflicts #44; conflicts #6, #7.)
6. Keep every policy type that all datasets must implement as code, each with one standard way to express, configure and enforce it on every engine, versioned with its history and observed after deployment: access as manifests compiled to Ranger, audit settings and filters as service configuration from Git (audit-design.md › Audit filters and exclusions, rule 1), retention as the OPA retention policy, consent and purpose as purpose-group assignments (attribute-and-tag-design.md › When to decide by role, by scope, or by attribute, rule 3), and locality and applicable regulations as declared dataset fields (attribute-and-tag-design.md › Classification to policy chain, rule 1); check each at build time (CI), at deploy time (the pipeline refuses a dataset or zone that lacks a mandatory platform policy, such as an owner, a classification, the tag-service link or the guardrails) and at access time (the Ranger plugin). Why: every dataset must implement these policies, a different approach per engine adds complexity and friction, policies change over time like data and schema, and a policy embedded in the data product can be validated at several points of its life cycle. (Data Mesh ch. 3, 5, 14.)
7. Monitor conformance continuously after deployment, not only in CI: the scheduled Ranger-to-Git diff (Ranger REST objects, import and export, rule 4), the zone-to-blueprint comparison (azure-style-rbac-abac.md › Best practices and limits, rule 6), and datasets that miss a mandatory platform policy or lag behind its current version; route each finding first to the owning institution, and when it is not fixed within the tolerance the platform folder sets for that finding type, mark the dataset non-compliant in OpenMetadata and hold its publication (conflicts #13); a difference between Ranger and Git has no tolerance and is overwritten (rule 3); the governance committee defines the recovery step for each finding type and the platform applies it. Why: maintaining policies needs runtime observation as well as build-time checks, escalation starts with notifying the owner, and governance's focus moves to recovery when errors occur. (Data Mesh ch. 5, 15; conflicts #13.)
8. Allow no tolerance for a conformance finding on a guardrail or on PII handling: the dataset is flagged non-compliant at once; give every other finding 7 days to be fixed before the flag is set. Why: a guardrail or PII gap exposes data immediately, while other findings can be fixed in the normal cycle. (decision 2026-11-16)
9. Write to Ranger Admin only through the CI service identity, require signed commits on the policy repository, and run the drift check every hour; do not sign policy bundles, because Ranger cannot verify them. Why: these three controls together show that what Ranger enforces came from a reviewed commit. (Authorization in Action ch. 10; decision 2026-11-16.)

Example (rewritten, platform manifest):
```yaml
kind: ConformanceRule
metadata: { id: conf-pii-mask-coverage }
spec:
  check: "every column tagged PII in zone t-acme is covered by tag-mask-pii"
  notifyFirst: platform_t-acme_owner                    # owning institution
  tolerance: "<platform-folder value>"                  # no source value (Unclear)
  onTolerance: { openmetadata: non-compliant, publication: hold }   # conflicts #13
  recovery: "re-link platform-tags to zone t-acme from Git"          # defined by the governance committee
```


### Governance maturity and fitness functions

Source: Data Mesh, ch. 15 (merged 2026-11-16). Board reporting: audit-design.md › Governance monitoring metrics, rule 1.

1. Introduce platform policies in three phases: bootstrap (automate the security and privacy policies first, the `PII` tag mask and the `RESTRICTED` guardrail), scale (automate most policies, make institution onboarding frictionless and run basic policy monitoring), and sustain (every dataset carries automated policies and non-conformance is detected automatically and routed to its owner, Policies as code in a federated model, rule 7). Why: the first phase covers only the essentials, and each later phase widens automation and detection. (Data Mesh ch. 15; azure-style-rbac-abac.md › Conditions, rule 15.)
2. Fold policy implementations that an institution built for itself early on into platform templates (azure-style-rbac-abac.md › Role definitions, rule 21), so that every institution uses the same implementation. Why: the platform harvests early local work instead of keeping one variant per institution. (Data Mesh ch. 15; conflicts #7.)
3. Track five governance fitness functions per zone and report them with the governance metrics: the share of datasets on the latest policy version (the role-definition version every compiled policy records), policy-as-code coverage, lead time for a new platform policy to be adopted, lead time from an access request to an active policy (Flowable request to Ranger), and time to detect and to recover from a breach; treat them as experiments and set no target until measured values exist. Why: they give objective measures of governance, and the source offers them as unproven examples. (Data Mesh ch. 15.)

Example (rewritten, platform metrics manifest):
```yaml
kind: FitnessFunctions
metadata: { id: ff-t-acme, zone: t-acme }
spec:
  latestPolicyVersionShare: { source: "compiled policy descriptions (data-reader vN)" }
  policyAsCodeCoverage:     { source: "Ranger export vs Git" }
  policyAdoptionLeadTime:   { source: "platform folder merge -> last zone compiled" }
  requestToActiveLeadTime:  { source: "Flowable request -> Ranger policy version" }
  breachDetectRecoverTime:  { source: "incident tickets" }
  targets: none            # experiments until measured
```

### Ranger REST objects, import and export

Source: Apache Ranger wiki, p. 7 REST APIs and import-export (2016/2017), p. 4 Security zones (2022), p. 6 Context enrichers (2015), p. 8 Custom plugin (2022), p. 12 Policy labels (2018/2019) (merged 2026-11-15).

1. Keep in Git only what defines a policy (service, zone, name, policy type access, mask or row filter, resources, items, conditions, labels, enabled flag), let Ranger assign `id`, `guid` and `version`, and address every policy by service and name, never by numeric ID. Why: IDs are set on create and differ per environment, while names are stable keys. (Apache Ranger wiki p. 7.)
2. Deploy only by creating, updating and deleting whole policies; never use the API's "apply" operation. Why: apply merges items into a matching policy, so Ranger would no longer equal Git. (Apache Ranger wiki p. 7.)
3. Never import with the override option into a live service, and move policies between environments only in a reviewed promotion with an explicit service mapping (for example `trino_dev` → `trino`); a zone-to-zone import needs the same service types on both sides. Why: override deletes every policy of the destination services before importing, and the mapping rewrites service names. (Apache Ranger wiki p. 4, p. 7.)
4. Export the Ranger policies on a schedule and diff the export against Git; every difference is drift under Policies as code, rule 3. Why: export exists for backup and test-to-production moves and gives the drift check its input. (Apache Ranger wiki p. 7; conflicts #6.)
5. Keep every service definition in use (resources, access types, configs, condition evaluators, context enrichers) in Git and register it through the Admin API. Why: Ranger Admin then supports the engine's policies without code changes, and the definition wires in the hooks. (Apache Ranger wiki p. 6, p. 8.)
6. Label every compiled policy with its institution, principal group and assignment ID. Why: labels are global and drive search, reports and selective export; the assignment ID also stays in the description (azure-style-rbac-abac.md › Role assignments, rule 4). (Apache Ranger wiki p. 12.)

Example (rewritten, manifest and compiled labels):
```yaml
kind: RoleAssignment
metadata: { id: ra-0042 }
spec:
  principal: { type: group, id: kc-grp-5512, name: platform_t-acme_analyst }
  roleDefinition: data-reader
  scope: /institutions/t-acme/domains/sales/datasets/orders
# compiled Ranger policy: service trino, zone t-acme, name ra-0042 (looked up by name, never by id)
#   labels: [t-acme, platform_t-acme_analyst, ra-0042]
```
