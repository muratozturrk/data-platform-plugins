# register_debezium_connector.py
# Posts the Debezium connector config to Kafka Connect REST API.
# Run once to start CDC — Debezium handles everything after that.
#
# Usage: python register_debezium_connector.py
# Requirements: pip install requests

import json
import requests

CONNECT_URL = "http://localhost:8083/connectors"

with open("debezium_postgres_connector.json") as f:
    config = json.load(f)

response = requests.post(
    CONNECT_URL,
    headers={"Content-Type": "application/json"},
    data=json.dumps(config),
)

if response.status_code in (200, 201):
    print(f"Connector registered: {response.json()['name']}")
    print(f"Topic produced: dbserver1.public.orders")
else:
    print(f"Error {response.status_code}: {response.text}")


def check_status(name: str):
    r = requests.get(f"{CONNECT_URL}/{name}/status")
    print(json.dumps(r.json(), indent=2))

check_status("orders-cdc-connector")