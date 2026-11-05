---
name: coverage-and-traceability
description: Tracking what is tested and what is not: requirement test coverage reports, the Requirement Traceability Matrix (requirement → test case → execution → defect), test case management (library, suites, runs, versioning), coverage metrics for code and data, and tool choices (Jira/Zephyr/Xray, TestRail, Azure DevOps, repo-based YAML). Use when building or auditing traceability.
---

# Coverage and Traceability

## Concepts
- **Test coverage** (requirements view): how many requirements have tests designed, which are missing tests; status of passed/failed cases per requirement; which template version covers which requirement; extent of code tested.
- **Requirement Traceability Matrix (RTM)**: single document delivered at the end of the lifecycle capturing all requirements mapped to test cases, validating that no functionality is unchecked; also the source for the coverage report.
- **Test case management**: planning tests, writing cases, managing schedule, tracking progress/results via test plans (scope, resources, timeline; contain suites/cases), suites (collections for specific functionality or test type; cases shared across suites), cases (steps verifying one piece of functionality).
- **Test library**: catalog of cases with versions, labels, suite assignment. **Test run**: selection of cases/suites for a phase, sprint or release; registers executions and controls tests.

## RTM shape for the platform
| req_id | source (manifest/ADR/contract/SLA) | quality attribute | risk | test_case_ids | suites | last_run | result | open_defects |
Requirements come from: manifest schemas (each `kind`), data contracts (each dataset), SLAs, security policies, ADRs, user stories; ids stable (`REQ-{area}-{nnn}`).

## Coverage views
- Requirement coverage %: requirements with ≥ 1 active, mapped case / total (target 100% for Critical/High).
- Execution coverage: mapped cases executed in the run / mapped cases.
- Code coverage: line/branch (ruff/pytest-cov; ≥ 85% core/sdk) — necessary, not sufficient.
- Data-quality coverage: gold tables with contract + freshness + uniqueness + business-rule tests / total gold tables.
- Policy coverage: OPA rules with tests / rules.
- Upgrade coverage: engines with a passing compatibility run on the current version.

## Tooling options
- Jira + Zephyr Scale/Xray, TestRail, Azure DevOps Test Plans, Jama, Rally: templates, execution recording, automatic link failed test → bug tracker, requirement–case–execution linkage, library with version history.
- Repo-based (platform default): cases as YAML in Git, RTM generated in CI from case `requirement_ids` + results (JUnit XML), published as a page; defects in the issue tracker linked by case id. Keeps traceability as code, reviewed in PRs.
Expected properties of any tool: templates for fast case creation; easy execution + result recording; failed tests auto-linked to bugs with assignment/notifications; requirements–cases–executions interlinked for coverage; library with reuse, search, version history.

## Anti-patterns
- RTM built once at the end by hand
- Requirements without ids (nothing to trace)
- Coverage reported as code coverage only
- Cases living in a tool nobody can diff
