"""T-bi-015 / T-trino-002: identical masked and filtered output across chart-data JSON, CSV export and embedded guest token."""
import csv, io, pytest

@pytest.mark.integration
def test_mask_and_rls_consistent_across_outputs(superset_client, seeded, guest_token):
    chart = seeded["chart_customers_table"]                     # dim_customers with iban/email, dataset has RLS on institution_id
    qc = superset_client.query_context(chart)
    json_rows = superset_client.chart_data(qc, as_user="t-acme-analyst").json()["result"][0]["data"]
    qc_csv = dict(qc, result_format="csv")
    csv_rows = list(csv.DictReader(io.StringIO(superset_client.chart_data(qc_csv, as_user="t-acme-analyst", raw=True).text)))
    embedded_rows = superset_client.chart_data(qc, guest_token=guest_token("t-acme", seeded["dashboard_shared_uuid"])).json()["result"][0]["data"]

    def norm(rows): return sorted((r["customer_id"], r["iban"], r["email"], r["institution_id"]) for r in rows)
    assert norm(json_rows) == norm(csv_rows) == norm(embedded_rows), "policy diverges across outputs"
    assert all(r["institution_id"] == "t-acme" for r in json_rows)
    assert all(str(r["iban"]).startswith("XXXX") for r in json_rows)
    assert all(len(str(r["email"])) == 64 for r in json_rows)   # sha256 hex mask

@pytest.mark.integration
def test_pii_reader_sees_unmasked(superset_client, seeded):
    qc = superset_client.query_context(seeded["chart_customers_table"])
    rows = superset_client.chart_data(qc, as_user="t-acme-pii-reader").json()["result"][0]["data"]
    assert any(str(r["iban"]).startswith("TR") for r in rows)
