# Admission policy for topic creation (used by the control plane or a Kafka proxy).
package platform.naming.kafka

import rego.v1

default allow := false

pattern := `^t-[a-z0-9]{2,16}\.[a-z0-9-]+\.[a-z0-9-]+\.(?:[a-z-]+|cdc|snapshot|cmd|log)(?:\.dlq)?\.v[0-9]+$`

allow if {
    regex.match(pattern, input.topic)
    count(input.topic) <= 249
    startswith(input.topic, sprintf("%s.", [input.tenant]))
}

deny[msg] if {
    not regex.match(pattern, input.topic)
    msg := sprintf("topic %q must match {tenant}.{domain}.{entity}.{event}.v{n}", [input.topic])
}
deny[msg] if {
    not startswith(input.topic, sprintf("%s.", [input.tenant]))
    msg := sprintf("topic %q is outside tenant prefix %q", [input.topic, input.tenant])
}
