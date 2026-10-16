import os
from kafka import KafkaProducer
import psycopg2
import json

# Connect to source DB
conn = psycopg2.connect(
    host="source-db", dbname="mydb", user="user", password=os.environ["DB_PASSWORD"]
)
cur = conn.cursor()

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["kafka-broker:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",          # wait for all replicas to confirm
    retries=5
)

# Full extract from source
cur.execute("SELECT id, name, description, updated_at FROM product_categories")
rows = cur.fetchall()
columns = [desc[0] for desc in cur.description]

# Publish every record to Kafka topic
for row in rows:
    record = dict(zip(columns, row))
    # Use primary key as Kafka message key (ensures ordering per entity)
    producer.send(
        topic="product_categories_full",
        key=record["id"],
        value=record
    )

producer.flush()
print(f"✅ Published {len(rows)} records to Kafka topic")
cur.close()
conn.close()