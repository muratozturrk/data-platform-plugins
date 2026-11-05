# OPA retention/lifecycle policy engine (not access control — that is Ranger).
# Evaluates what must happen to a dataset given its classification, legal basis and age;
# "most restrictive wins" when several rules match. Bundle is Git-versioned and hot-reloaded.
package platform.retention

import rego.v1

default action := "retain"

# candidate actions from all matching rules
candidates contains a if {
    some r in data.rules
    r.classification == input.dataset.classification
    input.dataset.age_days >= r.max_age_days
    a := r.action                         # one of: retain | archive | anonymize | purge
}
candidates contains "retain" if { input.dataset.legal_hold == true }

rank := {"retain": 0, "archive": 1, "anonymize": 2, "purge": 3}

# most restrictive = the action that limits data the most... except legal hold, which forces retain
action := "retain" if { input.dataset.legal_hold == true }
action := a if {
    not input.dataset.legal_hold
    some a in candidates
    every b in candidates { rank[a] >= rank[b] }
}

evidence := {"dataset": input.dataset.id, "action": action, "matched": [r.id | some r in data.rules; r.classification == input.dataset.classification; input.dataset.age_days >= r.max_age_days]}
