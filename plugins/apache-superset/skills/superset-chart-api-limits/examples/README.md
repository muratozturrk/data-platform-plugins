# Examples (copied unchanged from a Superset 4.1.2 lab base)

| File | Purpose |
|---|---|
| `superset_client.py` | REST client: login + CSRF, database, dataset, chart, chart data, dashboard (`attach_charts`, `row_layout`), export, `frontend_bootstrap()` |
| `viz_registry.py` | `registered_viz_keys(base_url)`: reads the served JS bundle and returns every registered `viz_type` (63 on 4.1.2) |
| `warehouse.py` | Load pandas DataFrames into a Postgres warehouse and run SQL checks (needs `psycopg`, `pandas`) |

Run against a lab (needs `requests`; any reachable Superset with a Postgres warehouse):

```python
import os
os.environ.update(SUPERSET_URL="http://localhost:8088", SUPERSET_USER="admin", SUPERSET_PASSWORD="admin",
                  WAREHOUSE_SQLALCHEMY_URI="postgresql+psycopg2://analyst:analyst@warehouse:5432/warehouse")
from superset_client import SupersetClient
from viz_registry import registered_viz_keys
c = SupersetClient().login()
print(sorted(registered_viz_keys(c.base_url)))
```

Defaults point at docker-compose service names (`http://superset:8088`, `warehouse:5432`); override with the env vars above. Run from this folder so the imports resolve.
