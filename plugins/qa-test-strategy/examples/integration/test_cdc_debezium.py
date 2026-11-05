"""T-cdc-001: insert/update/delete/tombstone propagate in order with before/after images."""
import json, time, pytest, httpx, psycopg
from kafka import KafkaConsumer

@pytest.mark.integration
def test_cdc_events_in_order(postgres, kafka, debezium, unique):
    table = f"orders_{unique}"
    with psycopg.connect(postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql"), autocommit=True) as c:
        c.execute(f"CREATE TABLE public.{table}(id int primary key, amount numeric)")
    httpx.put(f"{debezium}/connectors/pg-{unique}/config", json={
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector", "plugin.name": "pgoutput",
        "database.hostname": "postgres", "database.port": "5432", "database.user": "hkm", "database.password": "hkm", "database.dbname": "erp",
        "topic.prefix": f"t.e2e.{unique}", "table.include.list": f"public.{table}", "slot.name": f"slot_{unique}",
        "tombstones.on.delete": "true"}).raise_for_status()
    with psycopg.connect(postgres.get_connection_url().replace("postgresql+psycopg2", "postgresql"), autocommit=True) as c:
        c.execute(f"INSERT INTO public.{table} VALUES (1, 10)"); c.execute(f"UPDATE public.{table} SET amount=12 WHERE id=1"); c.execute(f"DELETE FROM public.{table} WHERE id=1")
    consumer = KafkaConsumer(f"t.e2e.{unique}.public.{table}", bootstrap_servers=kafka.get_bootstrap_server(), auto_offset_reset="earliest",
                             consumer_timeout_ms=30000, value_deserializer=lambda v: json.loads(v) if v else None)
    ops = []
    for m in consumer:
        ops.append(None if m.value is None else m.value["payload"]["op"])
        if ops[-1] is None: break
    assert ops == ["c", "u", "d", None]      # create, update, delete, tombstone
