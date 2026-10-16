# register_mirrormaker2.py
#
# Registers MirrorMaker 2 connectors via the Kafka Connect REST API.
# MirrorMaker 2 runs as three Kafka Connect connectors:
#   - MirrorSourceConnector   (copies messages)
#   - MirrorCheckpointConnector (syncs consumer group offsets)
#   - MirrorHeartbeatConnector  (monitors replication lag)
#
# Usage: python register_mirrormaker2.py
# Requirements: pip install requests

import requests
import json

CONNECT_URL = "http://localhost:8083/connectors"

connectors = [
    {
        "name": "passthrough-mirror-source",
        "config": {
            "connector.class":
                "org.apache.kafka.connect.mirror.MirrorSourceConnector",
            "source.cluster.alias":  "source",
            "target.cluster.alias":  "target",
            "source.cluster.bootstrap.servers": "source-kafka:9092",
            "target.cluster.bootstrap.servers": "target-kafka:9092",

            # Topics to replicate (regex supported)
            "topics": "orders_events,customers,product_catalog",

            # Keep original topic names — no source. prefix
            "replication.policy.class":
                "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",

            # Passthrough: no transforms
            "transforms": "",

            "tasks.max":           "4",
            "replication.factor":  "1",

            # Sync topic configurations (retention, compaction) to target
            "sync.topic.configs.enabled": "true",
            "sync.topic.acls.enabled":    "false",
        }
    },
    {
        "name": "passthrough-mirror-checkpoint",
        "config": {
            "connector.class":
                "org.apache.kafka.connect.mirror.MirrorCheckpointConnector",
            "source.cluster.alias":  "source",
            "target.cluster.alias":  "target",
            "source.cluster.bootstrap.servers": "source-kafka:9092",
            "target.cluster.bootstrap.servers": "target-kafka:9092",
            "replication.policy.class":
                "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",

            # Sync consumer group offsets every 10 seconds
            # Enables consumer failover to target cluster
            "sync.group.offsets.enabled":           "true",
            "sync.group.offsets.interval.seconds":  "10",
            "emit.checkpoints.interval.seconds":    "10",
            "tasks.max": "1",
        }
    },
    {
        "name": "passthrough-mirror-heartbeat",
        "config": {
            "connector.class":
                "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
            "source.cluster.alias":  "source",
            "target.cluster.alias":  "target",
            "source.cluster.bootstrap.servers": "source-kafka:9092",
            "target.cluster.bootstrap.servers": "target-kafka:9092",
            "replication.policy.class":
                "org.apache.kafka.connect.mirror.IdentityReplicationPolicy",

            # Heartbeat interval — used to measure replication lag
            "emit.heartbeats.interval.seconds": "10",
            "tasks.max": "1",
        }
    }
]

for connector in connectors:
    response = requests.post(
        CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector),
    )
    name = connector["name"]
    if response.status_code in (200, 201):
        print(f"Registered: {name}")
    elif response.status_code == 409:
        print(f"Already exists: {name} (skipped)")
    else:
        print(f"Failed {name}: {response.status_code} — {response.text}")


def check_all_statuses():
    for connector in connectors:
        name = connector["name"]
        r = requests.get(f"{CONNECT_URL}/{name}/status")
        state = r.json().get("connector", {}).get("state", "UNKNOWN")
        print(f"  {name}: {state}")

print("\nConnector statuses:")
check_all_statuses()