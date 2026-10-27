"""Pushdown keeps filters, projections, aggregations close to the source."""
from common.federation import pg_query
from federation_requirements.pushdown import analyze_pushdown

TOTAL_CUSTOMERS = 1500


def test_simple_filter_is_pushed_and_only_matching_rows_travel(record):
    rep = analyze_pushdown("SELECT name, email FROM pg.public.customers WHERE region = 'EUROPE'")
    expected = pg_query("SELECT count(*) FROM customers WHERE region = 'EUROPE'")[0][0]
    assert {"filter", "projection"} <= rep.pushed
    assert "filter" not in rep.in_engine
    assert rep.rows_from_source == expected == 300
    record(rows_from_source=rep.rows_from_source, table_rows=TOTAL_CUSTOMERS)


def test_function_predicate_is_not_pushed_and_pulls_the_whole_table(record):
    rep = analyze_pushdown("SELECT name FROM pg.public.customers WHERE lower(region) = 'europe'")
    assert "filter" in rep.in_engine and "filter" not in rep.pushed
    assert rep.rows_from_source == TOTAL_CUSTOMERS
    record(rows_from_source=rep.rows_from_source)


def test_top_n_is_pushed(record):
    rep = analyze_pushdown("SELECT name, credit_limit FROM pg.public.customers ORDER BY credit_limit DESC LIMIT 5")
    assert "topn" in rep.pushed
    assert rep.rows_from_source == 5
    record(rows_from_source=rep.rows_from_source)


def test_aggregation_pushdown_depends_on_the_grouping_type(record):
    numeric = analyze_pushdown(
        "SELECT loyalty_tier, count(*), sum(credit_limit) FROM pg.public.customers GROUP BY loyalty_tier")
    text = analyze_pushdown(
        "SELECT segment, count(*), sum(credit_limit) FROM pg.public.customers GROUP BY segment")
    assert "aggregation" in numeric.pushed and "GROUP BY" in numeric.remote_sql
    assert numeric.rows_from_source == 4                 # one row per loyalty tier
    # Grouping by a text column is not pushed (collation semantics differ): all rows travel.
    assert "aggregation" not in text.pushed and "aggregation" in text.in_engine
    assert text.rows_from_source == TOTAL_CUSTOMERS
    record(numeric_group_rows=numeric.rows_from_source, text_group_rows=text.rows_from_source,
           remote_sql=numeric.remote_sql)


def test_join_pushdown_is_cost_based(record):
    selective = analyze_pushdown(
        "SELECT c.name, r.returned_at FROM pg.public.customers c "
        "JOIN pg.public.order_returns r ON r.order_id = c.customer_id WHERE c.region = 'EUROPE'")
    unfiltered = analyze_pushdown(
        "SELECT c.name, r.returned_at FROM pg.public.customers c "
        "JOIN pg.public.order_returns r ON r.order_id = c.customer_id")
    assert "join" in selective.pushed and "join" not in selective.in_engine
    assert "join" in unfiltered.in_engine
    record(selective_rows_from_source=selective.rows_from_source,
           unfiltered_rows_from_source=unfiltered.rows_from_source)
