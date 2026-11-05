package platform.retention_test

import rego.v1
import data.platform.retention

rules := [
  {"id": "r1", "classification": "pii", "max_age_days": 365, "action": "anonymize"},
  {"id": "r2", "classification": "pii", "max_age_days": 730, "action": "purge"},
]

test_most_restrictive_wins if {
  retention.action == "purge" with input as {"dataset": {"id": "d", "classification": "pii", "age_days": 800, "legal_hold": false}} with data.rules as rules
}
test_legal_hold_forces_retain if {
  retention.action == "retain" with input as {"dataset": {"id": "d", "classification": "pii", "age_days": 800, "legal_hold": true}} with data.rules as rules
}
test_young_data_retained if {
  retention.action == "retain" with input as {"dataset": {"id": "d", "classification": "pii", "age_days": 10, "legal_hold": false}} with data.rules as rules
}
