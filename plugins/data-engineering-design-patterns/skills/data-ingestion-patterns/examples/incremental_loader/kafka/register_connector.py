# register_connector.py
# Run this ONCE to tell Kafka Connect to start pulling from Postgres.
# Usage: python register_connector.py

import os
import requests
import json

CONNECT_URL = "http://localhost:8083/connectors"

connector_config = {
    "name": "orders-incremental-loader",
    "config": {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": "jdbc:postgresql://postgres:5432/mydb",
        "connection.user": "user",
        "connection.password": os.environ["DB_PASSWORD"],
        "table.whitelist": "orders",

        # Incremental mode: use BOTH timestamp and incrementing column.
        # This handles records that share the same updated_at timestamp
        # by additionally tracking the id — so no record is ever missed.
        "mode": "timestamp+incrementing",
        "timestamp.column.name": "updated_at",
        "incrementing.column.name": "id",

        # Poll every 60 seconds
        "poll.interval.ms": "60000",

        # Look-back buffer: re-read records up to 2 hours old on every poll.
        # This catches late-arriving rows that were committed after the last poll.
        "timestamp.delay.interval.ms": "7200000",

        # All rows go to topic: incremental_orders
        "topic.prefix": "incremental_",

        # Add a kafka_ingested_at field to every message
        "transforms": "addMetadata",
        "transforms.addMetadata.type":
            "org.apache.kafka.connect.transforms.InsertField$Value",
        "transforms.addMetadata.timestamp.field": "kafka_ingested_at"
    }
}

response = requests.post(
    CONNECT_URL,
    headers={"Content-Type": "application/json"},
    data=json.dumps(connector_config)
)

if response.status_code in (200, 201):
    print(f"Connector registered successfully: {response.json()['name']}")
else:
    print(f"Failed to register connector: {response.status_code}")
    print(response.text)