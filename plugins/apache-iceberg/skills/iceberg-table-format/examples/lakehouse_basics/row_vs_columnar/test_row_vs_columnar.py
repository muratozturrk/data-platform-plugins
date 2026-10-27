"""Why OLTP row stores struggle with analytics, and why columnar engines took over."""
import os

import pytest

from common.orders import orders
from row_vs_columnar.layout_io import analytic_query, point_lookup, write_layouts

N = 200_000


@pytest.fixture(scope="module")
def layouts(tmp_path_factory):
    return write_layouts(orders(N), str(tmp_path_factory.mktemp("layouts")))


def test_both_layouts_give_the_same_answer(layouts):
    csv_result, _ = analytic_query(layouts, "csv")
    pq_result, _ = analytic_query(layouts, "parquet")
    assert csv_result == pytest.approx(pq_result)
    assert len(pq_result) == 10


def test_analytic_query_reads_whole_rows_in_row_layout(layouts):
    _, csv_bytes = analytic_query(layouts, "csv")
    _, pq_bytes = analytic_query(layouts, "parquet")
    csv_size, pq_size = os.path.getsize(layouts.csv_path), os.path.getsize(layouts.parquet_path)
    print(f"\nCSV: file {csv_size:,} B, read {csv_bytes:,} B | "
          f"Parquet: file {pq_size:,} B, read {pq_bytes:,} B | ratio {csv_bytes / pq_bytes:.1f}x")
    assert csv_bytes == csv_size                 # row layout: every byte of every row is read
    assert pq_bytes < 0.25 * pq_size             # column layout: only 2 of 9 column chunks
    assert csv_bytes > 20 * pq_bytes


def test_point_lookup_favors_row_layout(layouts):
    """The flip side (OLTP): fetching one full record is cheap in a row store, costly in columns."""
    rec_csv, csv_bytes = point_lookup(layouts, "csv", 123_456)
    rec_pq, pq_bytes = point_lookup(layouts, "parquet", 123_456)
    print(f"\npoint lookup: row layout {csv_bytes:,} B, Parquet {pq_bytes:,} B")
    assert rec_csv["order_id"] == rec_pq["order_id"] == 123_456
    assert rec_csv["_columns"] == rec_pq["_columns"] == 9
    assert csv_bytes <= 512                      # index -> one or two small blocks
    assert pq_bytes > 100 * csv_bytes            # whole row group of every column is decoded
