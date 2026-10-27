"""Curated Dremio views (the semantic layer) and the 'Sales Analytics' dashboard.

The dashboard is usually built by clicking in Superset. Here the same pieces are code:
- VIEWS: virtual datasets in Dremio for trends by region, customer segment and campaign
- CHARTS: the two Superset charts (line: sales by day, pie: sales by category), each a query a BI tool would send
- run_dashboard(): executes every chart over Arrow Flight (the `dremio+flight://` transport Superset uses)
- render(): a static PNG of the dashboard
"""
from dataclasses import dataclass

import pyarrow as pa

from common.dremio import Dremio, flight_query

VIEWS = {
    "sales_by_region": """
        SELECT store_location AS region, count(*) AS orders, sum(sales_amount) AS revenue,
               round(avg(sales_amount), 2) AS avg_order_value
        FROM {src} GROUP BY store_location""",
    "sales_by_segment": """
        SELECT customer_age_group AS segment, category, count(*) AS orders, sum(sales_amount) AS revenue
        FROM {src} GROUP BY customer_age_group, category""",
    "campaign_performance": """
        SELECT campaign_name, date_trunc('MONTH', sales_date) AS sales_month,
               count(*) AS orders, sum(sales_amount) AS revenue
        FROM {src} GROUP BY campaign_name, date_trunc('MONTH', sales_date)""",
}


@dataclass(frozen=True)
class Chart:
    """What Superset stores for a chart: dataset, viz type, x/dimension, metric."""
    name: str
    viz: str            # "line" | "pie"
    dimension: str
    metric: str         # SQL aggregate, as typed into Superset's metric box

    def sql(self, dataset: str) -> str:
        return (f"SELECT {self.dimension}, {self.metric} AS metric FROM {dataset} "
                f"GROUP BY {self.dimension} ORDER BY {self.dimension}")


CHARTS = [
    Chart("Sales by Day", "line", "sales_date", "SUM(sales_amount)"),
    Chart("Sales by Category", "pie", "category", "SUM(sales_amount)"),
]


def create_views(dremio: Dremio, source_table: str, schema: str) -> list[str]:
    """CREATE VIEW in the Nessie source: Dremio stores them in the catalog next to the table."""
    created = []
    for name, body in VIEWS.items():
        view = f"{schema}.{name}"
        dremio.sql(f"CREATE OR REPLACE VIEW {view} AS {body.format(src=source_table)}")
        created.append(view)
    return created


def run_dashboard(dataset: str, charts: list[Chart] = CHARTS) -> dict[str, pa.Table]:
    return {c.name: flight_query(c.sql(dataset)) for c in charts}


def render(results: dict[str, pa.Table], path: str, title: str = "Sales Analytics") -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, len(CHARTS), figsize=(13, 4.5))
    for ax, chart in zip(axes, CHARTS):
        t = results[chart.name]
        x, y = t.column(chart.dimension).to_pylist(), [float(v) for v in t.column("metric").to_pylist()]
        if chart.viz == "line":
            ax.plot(x, y, linewidth=1.2)
            ax.set_ylabel(chart.metric)
            ax.tick_params(axis="x", labelrotation=30)
        else:
            ax.pie(y, labels=x, autopct="%1.1f%%", startangle=90)
        ax.set_title(chart.name)
    fig.suptitle(title)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path
