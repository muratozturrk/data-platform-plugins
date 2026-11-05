---
name: defect-management
description: Defect lifecycle and process for the platform: definitions (defect, error, failure, change request), priority and severity scales and matrix, discovery → triage → resolution → verification → closure with reopen rules, triage meetings, quality debt, tools (Jira, Azure DevOps, Bugzilla, Rally, Teamwork) and defect metrics. Use when setting up or running defect handling.
---

# Defect Management

## Definitions
Defect (bug/fault): code does not produce the expected result against specification. Error: developer mistake, including misunderstood requirements. Failure: application not functioning as developers/clients intended. Change request: documented request to modify the system; not a bug; created separately by BA/PO.

## Scales
- **Priority** (order of fixing): Low, Medium, High, Critical.
- **Severity** (damage to system or user): Trivial, Minor, Major, Critical, Blocker.
Matrix guidance: Blocker/Critical severity ⇒ at least High priority unless the path is unreachable; Minor/Trivial can be Critical priority when release-visible or compliance-related (e.g. KVKK notice text).

## Lifecycle
`New/Open` (discovery; ticket submitted) → `Triaged` (assessed, prioritized, resources assigned; still open) → `In Progress` (developer assigned, resolution) → `Resolved` (fix implemented and checked by developer) → `Verified` (QA re-tests with originating case + regression subset) → `Closed`. `Reopened` when a resolved defect is still reproducible; `Deferred` when accepted into quality debt; `Rejected` (not a defect / duplicate / CR).

## Process
1. **Discovery**: test engineers (and monitoring/canaries) open defects with: environment, version, tenant, run/query ids, steps, expected vs actual, evidence, linked test case id.
2. **Triage** (defect triage meetings, cadence per release phase): review and assess; prioritize/reprioritize by severity, frequency, risk, business impact; assign resources; identify change requests and route them separately.
3. **Resolution**: fix with a test that reproduces the defect (regression protection); PR links the defect id.
4. **Verification and closure**: QA verifies; closed only after verification in the target environment.
5. **Quality debt**: effort needed to fix defects existing in released code; tracked as a ledger with owner and target; reported in every test report.

## Platform conventions
- Defect fields: component (from the suite taxonomy areas), tenant impact (none/one/many), data impact (none/stale/incorrect/exposed), rollback available (y/n).
- Data-exposure defects (cross-tenant, masking) are Blocker and trigger the security incident runbook regardless of priority discussions.
- Every Verified defect adds or updates a regression case.
- Tools: Jira (default) with Zephyr/Xray links; Azure DevOps when the organisation standardises on it; GitHub Issues for OSS-facing components.

## Metrics
Open by severity/priority, age, mean time to triage/resolve, reopen rate, defect density per component, escape rate (found in prod / total), fixed defects %, quality-debt size and trend.

## Anti-patterns
- Severity used as priority
- Change requests tracked as bugs
- Closing without QA verification
- Quality debt invisible in reports
