"""Federation joins the lakehouse with operational data in place."""
from common.federation import pg_execute, pg_table_reads, run, wait_for_pg_stats
from federated_queries.federated import (
    customer_lookup, revenue_by_region, revenue_by_region_without_federation)


def test_federated_join_matches_point_to_point_integration(record):
    federated = revenue_by_region()
    manual = revenue_by_region_without_federation()
    assert federated == manual
    assert len(federated) == 5
    record(regions=len(federated), orders=sum(r[1] for r in federated))


def test_operational_change_is_visible_without_a_pipeline(record):
    """Fraud desk suspends an account in the OLTP app; the next analytical query already sees it."""
    cid = 7
    before = customer_lookup(cid)
    assert before["status"] == "active" and before["orders"] > 0
    try:
        pg_execute("UPDATE customers SET status = 'suspended', updated_at = now() WHERE customer_id = %s", (cid,))
        after = customer_lookup(cid)
        assert after["status"] == "suspended"
        assert after["lifetime_value"] == before["lifetime_value"]   # history comes from Iceberg
        active_orders_now = sum(r[1] for r in revenue_by_region())
        record(customer_orders=before["orders"], status_before=before["status"], status_after=after["status"],
               active_orders_after_suspension=active_orders_now)
    finally:
        pg_execute("UPDATE customers SET status = 'active' WHERE customer_id = %s", (cid,))


def test_new_source_table_is_queryable_immediately_for_prototyping(record):
    """Exploratory analysis: a table that appeared in the source minutes ago, no ETL built."""
    pg_execute("DROP TABLE IF EXISTS support_tickets")
    pg_execute("""CREATE TABLE support_tickets AS
                  SELECT g AS ticket_id, (g * 13) % 1500 + 1 AS customer_id,
                         CASE WHEN g % 4 = 0 THEN 'chargeback' ELSE 'question' END AS topic
                  FROM generate_series(1, 400) g""")
    try:
        res = run("""
            SELECT t.topic, count(DISTINCT t.customer_id) AS customers, sum(o.amount) AS revenue_at_risk
            FROM pg.public.support_tickets t
            JOIN lake.sales.orders o ON o.customer_id = t.customer_id
            GROUP BY t.topic ORDER BY t.topic""")
        topics = {r[0]: r for r in res.rows}
        assert set(topics) == {"chargeback", "question"}
        assert topics["chargeback"][2] > 0
        record(chargeback_customers=topics["chargeback"][1], question_customers=topics["question"][1])
    finally:
        pg_execute("DROP TABLE support_tickets")


def test_every_federated_query_puts_load_on_the_source(record):
    """Federated queries hit the operational system every time."""
    before = pg_table_reads("customers")
    runs = 5
    for _ in range(runs):
        revenue_by_region()
    after = wait_for_pg_stats("customers", before)
    scans, tuples = after[0] - before[0], after[1] - before[1]
    assert scans >= runs
    assert tuples >= runs * 1500          # no index on status: PostgreSQL reads all 1,500 rows each time
    record(queries=runs, pg_scans=scans, pg_tuples_read=tuples)
