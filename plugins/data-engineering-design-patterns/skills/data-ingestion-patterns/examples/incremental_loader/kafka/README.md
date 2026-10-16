# Incremental Loader with Kafka Connect JDBC

You need three things running: Kafka + Zookeeper, Kafka Connect, and a Postgres source.
Docker Compose starts all of them.

- `docker-compose.yml` — Kafka, Zookeeper, Kafka Connect (JDBC plugin), Postgres.
- `init.sql` — creates the source `orders` table with seed data.
- `register_connector.py` — POSTs the JDBC source connector config to the Kafka Connect REST API
  (one-time). From then on Kafka Connect runs the polling loop itself.
- `incremental_consumer.py` — reads the `incremental_orders` topic Kafka Connect writes to. It never
  touches the connector config; it only talks to the Kafka broker.

## Running it locally

```bash
export POSTGRES_PASSWORD=...   # used by docker-compose
export DB_PASSWORD=$POSTGRES_PASSWORD   # used by the Python scripts

# 1. Start everything (wait ~60s for Kafka Connect to install the JDBC plugin)
docker-compose up -d

# 2. Register the connector (one-time)
pip install requests kafka-python psycopg2-binary
python register_connector.py

# 3. Check the connector is running
curl http://localhost:8083/connectors/orders-incremental-loader/status

# 4. Start the consumer
python incremental_consumer.py

# 5. In another terminal, insert a new order and watch it flow through
docker exec -it <postgres_container> psql -U user -d mydb -c \
  "INSERT INTO orders (customer_id, amount, status) VALUES (99, 250.00, 'new');"
```
