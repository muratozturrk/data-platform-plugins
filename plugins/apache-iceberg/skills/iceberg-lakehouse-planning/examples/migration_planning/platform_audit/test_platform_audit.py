import json

from platform_audit.audit import DOMAINS, run_audit


def ids(finding):
    return set(finding.datasets)


def test_postgres_inventory_matches_the_warehouse(audit_report):
    trades = audit_report.dataset("pg:trading.trades")
    clients = audit_report.dataset("pg:crm.clients")
    positions = audit_report.dataset("pg:reporting.daily_positions")
    view = audit_report.dataset("pg:reporting.v_desk_exposure")
    assert (trades.rows, trades.has_primary_key) == (50_000, True)
    assert (clients.rows, clients.has_primary_key) == (5_000, False)
    assert positions.rows == 310                       # 31 days x 5 symbols x 2 regions
    assert view.kind == "view" and view.rows is None
    assert trades.bytes > 1_000_000                     # pg_total_relation_size incl. the PK index
    assert clients.pii_columns == ["full_name", "email", "iban"]
    assert audit_report.dataset("pg:staging.vendor_feed_raw").semi_structured


def test_object_store_inventory_detects_formats_partitions_and_schemas(audit_report, lake_rows):
    trades = audit_report.dataset("s3:legacy-lake/trades_history")
    assert (trades.main_format, trades.files, trades.rows) == ("parquet", 20, lake_rows["trades_history"])
    assert trades.partition_keys == ["trade_date"]
    assert trades.columns == ["trade_id", "symbol", "qty", "price", "desk_region", "trade_date"]
    feeds = audit_report.dataset("s3:legacy-lake/vendor_feeds")
    assert (feeds.main_format, feeds.files, feeds.rows) == ("csv", 5, lake_rows["vendor_feeds"])
    ticks = audit_report.dataset("s3:legacy-lake/market_ticks")
    assert (ticks.main_format, ticks.rows, ticks.semi_structured) == ("json", lake_rows["market_ticks"], True)
    assert audit_report.dataset("s3:legacy-lake/compliance_records").pii_columns == ["client_email"]


def test_workload_is_attributed_from_pg_stat_statements(audit_report):
    reads = {d.id: d.reads for d in audit_report.datasets if d.system == "postgresql"}
    assert reads == {"pg:trading.trades": 10, "pg:crm.clients": 3, "pg:reporting.daily_positions": 30,
                     "pg:reporting.v_desk_exposure": 20, "pg:staging.vendor_feed_raw": 0}
    assert audit_report.dataset("pg:staging.vendor_feed_raw").writes == 6     # 5 INSERT + 1 DELETE
    top = audit_report.workload[0]
    assert top["calls"] == 30 and "$1" in top["query"]                        # constants normalized away


def test_auditor_traffic_is_excluded_from_the_workload(pg, audit_report):
    again = run_audit(pg)                               # its count(*) queries ran twice by now
    assert {d.id: d.reads for d in again.datasets} == {d.id: d.reads for d in audit_report.datasets}


def test_findings_encode_the_audit_rules(audit_report):
    f = audit_report.finding
    assert ids(f("F-ROW-FORMATS")) == {"s3:legacy-lake/vendor_feeds", "s3:legacy-lake/market_ticks",
                                       "s3:legacy-lake/compliance_records"}
    assert ids(f("F-PARQUET-IN-PLACE")) == {"s3:legacy-lake/trades_history"}
    assert ids(f("F-NO-PRIMARY-KEY")) == {"pg:crm.clients"}
    assert ids(f("F-PII")) == {"pg:crm.clients", "s3:legacy-lake/compliance_records"}
    assert ids(f("F-WAREHOUSE-VIEWS")) == {"pg:reporting.v_desk_exposure"}
    assert ids(f("F-HOT-DATASETS")) == {"pg:reporting.daily_positions", "pg:reporting.v_desk_exposure"}
    assert ids(f("F-SMALL-FILES")) == {"s3:legacy-lake/trades_history", "s3:legacy-lake/vendor_feeds"}
    assert {x.domain for x in audit_report.findings} == set(DOMAINS)


def test_report_serializes_for_the_next_phase(audit_report):
    doc = json.loads(audit_report.to_json())
    assert len(doc["datasets"]) == 9 and len(doc["stakeholder_findings"]) == 5
    md = audit_report.to_markdown()
    assert "| governance | F-PII:" in md and "| s3:legacy-lake/trades_history | parquet | 10000 | 20 |" in md
