---
name: stlc-and-test-case-lifecycle
description: The software testing lifecycle (requirement analysis, planning, test case design, environment setup, execution, closure) and the test case lifecycle (requirements analysis, environment and tool setup when needed, test case, mapping, review, execution with automation of unit/integration/API and UI tests, regression, inspection) applied to the data platform. Use when organizing testing work, defining phases and gates, or explaining where a testing activity belongs.
---

# STLC and Test Case Lifecycle

## Software Testing Lifecycle (STLC)
| Phase | Purpose | Platform artefacts | Exit gate |
|---|---|---|---|
| 1. Requirement analysis | decide which requirements must be tested; identify testable/non-testable items | requirement ids from manifests/ADRs/contracts; quality attributes per requirement | requirement list with test need, risk, owner |
| 2. Test planning | priorities, goals, strategy, resources | test plan, test strategy, environment/tool needs, schedule | plan approved |
| 3. Test case design | design cases/scripts before testing | test cases (YAML/Gherkin), automation skeletons, test data design | cases reviewed and mapped |
| 4. Environment setup | environment simulating real use | ephemeral namespaces, seeded tenants, synthetic/anonymised data, engine versions pinned | smoke suite green on the environment |
| 5. Execution | run the plan; log errors/defects/failures | CI runs, manual sessions, defect tickets linked to case ids | all planned cases executed or waived |
| 6. Closure | close, report, learn | test report, RTM update, inspection of cases, quality-debt ledger | report accepted; cycle restarts |

## Test case lifecycle (from the platform diagram)
```
Requirements Analysis
   ├─ new environment/tool needed ──> Environment ──> Tool ──┐
   └─ testable in existing environment/tool ─────────────────┴─> Test Case
Test Case ─> Mapping (to requirements) ─> Review ─> Execution ─> Regression Test ─> Test Cases Inspection ─> (back to Requirements Analysis)
Review ── if needed ──> Automation of Integration, Unit or API tests ──> Execution / Regression
Execution ── if needed ──> Automation of UI tests ──> Regression
```
Rules: no execution without mapping and review; automation decision is taken at review (API/unit/integration) or after first execution (UI); regression suite is the destination of every automated case; inspection retires obsolete cases and feeds the next requirements analysis.

## Platform-specific phase notes
- Requirement analysis sources: `Pipeline`/`Policy`/`Tenant` manifests, data contracts, SLAs, ADRs, security policies, upgrade notes of OSS engines.
- Environment: Kubernetes namespace per test run with the platform Helm umbrella, tenant fixtures, Keycloak test realm, Vault dev mount, object-storage bucket with lifecycle delete; versions identical to prod.
- Tooling decisions are explicit tickets ("new tool needed: k6 for Trino load") so the lifecycle's Environment/Tool branch is visible.
- Closure includes engine-upgrade regression evidence (monthly) and data-quality SLA attainment.

## Anti-patterns
- Designing cases after execution starts
- Cases without requirement mapping ("orphans")
- Regression suite that only grows; no inspection/retirement
- Environment differing from prod in engine versions or auth mode
