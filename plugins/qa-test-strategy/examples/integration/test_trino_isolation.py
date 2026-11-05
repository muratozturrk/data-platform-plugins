"""T-trino-001 (fast band, file access control mirrors the Ranger decision table): institution A cannot read B's catalog."""
import pytest, trino

def q(url, user, sql):
    host, port = url.replace("http://", "").split(":")
    cur = trino.dbapi.connect(host=host, port=int(port), user=user).cursor(); cur.execute(sql); return cur.fetchall()

@pytest.mark.integration
def test_cross_institution_denied(trino):
    assert q(trino, "t-acme-analyst", "SELECT 1 FROM t_acme_lake.gold_sales.fct_orders LIMIT 1") is not None
    with pytest.raises(trino.exceptions.TrinoUserError) as e:
        q(trino, "t-acme-analyst", "SELECT 1 FROM t_globex_lake.gold_sales.fct_orders LIMIT 1")
    assert "Access Denied" in str(e.value)
