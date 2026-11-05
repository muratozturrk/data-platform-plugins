---
name: qa-metrics-and-reporting
description: QA metrics (result vs predictive), test results summary, test reports per iteration/release, dashboards and release decisions for the platform. Use when defining metrics, writing reports, or deciding on a release.
---

# QA Metrics and Reporting

## Metric categories
- **Result metrics**: absolute measures of completed work; gauge thoroughness and efficiency; rarely actionable alone. Examples: total cases, passed/failed, defects found, critical defects, executed cases, requirements covered.
- **Predictive metrics**: combine absolute metrics to surface early warning signs for speed or quality. Examples: passed cases %, fixed defects %, failed-case trend, defect discovery rate vs fix rate, reopen rate, flaky-test rate, requirement coverage %, defect escape rate, test runtime trend, data-quality SLA attainment.

## Platform metric set (reported per iteration and per release)
| Metric | Formula | Signal |
|---|---|---|
| Pass % | passed / executed | quality of build |
| Fail % | failed / executed | |
| Requirement coverage % | reqs with active mapped cases / reqs (by priority) | gaps |
| Execution coverage % | executed mapped cases / mapped cases | plan completion |
| Fixed defects % | closed / (closed + open) in scope | fix velocity |
| Reopen rate | reopened / resolved | fix quality |
| Defect escape rate | prod defects / total defects | test effectiveness |
| Flaky rate | tests with pass/fail flips / tests | suite health |
| Regression runtime | minutes per tier | feedback loop |
| DQ SLA attainment | gold tables meeting freshness/accuracy / gold tables | data quality |
| Isolation test status | pass/fail per component | security gate |
| Perf budget status | budgets met / budgets | performance gate |

## Test report
Written by the responsible test engineer/test lead at the end of each iteration; stakeholders use it to evaluate quality and decide the release. Contents: scope and environment; **test results summary** (executed, passed, failed, pass %, fail %, tester comments); coverage vs RTM; defects by severity/priority with open blockers; non-functional results vs budgets; predictive metrics and trends; quality debt; risks; recommendation (go / conditional go / no-go) with conditions.

## Release decision rule (default)
Go when: all smoke and regression tiers green; zero open Blocker/Critical defects; requirement coverage 100% for Critical/High; isolation and security gates green; performance budgets met or formally waived; engine-upgrade compatibility green for the versions shipped.

## Anti-patterns
- Reporting only result metrics
- Pass % without coverage (100% of 10 cases)
- Reports written days after the run
- No trend vs previous iteration
