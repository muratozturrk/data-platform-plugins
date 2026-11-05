# Test Strategy — <platform | component>

Owner: · Version: · Last review:

## 1. Targets of test and quality attributes
| Target | Functionality | Availability | Performance | Testability | Security | Usability | Reliability |
|---|---|---|---|---|---|---|---|

## 2. Product risks → test types
| Risk | Attribute | Test types | Suite(s) | Automation |
|---|---|---|---|---|

## 3. Organization
Developers: unit/contract/component, PR gates. QA: integration/system/e2e/non-functional, design review, RTM, reports. Ops: infra/DR. Data owners: DQ rules.

## 4. Approach per test type
| Type | Method (white/grey/black) | Design approach | Execution (manual/automated + tool) | Environment |
|---|---|---|---|---|

## 5. Test pyramids
Code: unit → contract → integration → system/e2e → acceptance. Data: schema → row rules → reconciliation → cross-system → business acceptance.

## 6. Automation policy
Always: … Critical journeys only: … Never: …

## 7. Environments and tools
| Environment | Purpose | Contents | Data |
| Tool | Layer | Owner |

## 8. Coverage targets and metrics
## 9. Regression and engine-upgrade policy
## 10. Governance (review, inspection cadence, deprecation)
