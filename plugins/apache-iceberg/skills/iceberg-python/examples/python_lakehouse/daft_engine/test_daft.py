from datetime import date

import daft
import polars as pl
import pyarrow.compute as pc
import pytest
from daft import col

from common.lakehouse import S3_ENDPOINT, AWS_KEY, AWS_SECRET, AWS_REGION
from common.trips import create_trips_table
from daft_engine.daft_ops import mean_distance_of_expensive_trips, read, scan_tasks, write


@pytest.fixture(scope="module")
def trips(catalog, ns):
    return create_trips_table(catalog, f"{ns}.trips")


def test_read_iceberg_uses_the_table_fileio(trips):
    df = read(trips)
    assert df.count_rows() == 5000
    assert df.column_names == [f.name for f in trips.schema().fields]


def test_dict_aggregation_fails_expression_form_works(trips):
    df = read(trips)
    with pytest.raises(ValueError, match="only accepts expression type"):
        df.filter(df["total_amount"] > 100).groupby("passenger_count").agg({"trip_distance": ["mean"]})
    got = mean_distance_of_expensive_trips(df)
    raw = trips.scan().to_arrow()
    rich = raw.filter(pc.greater(raw["total_amount"], 100))
    for k, v in zip(got["passenger_count"], got["trip_distance"]):
        exp = pc.mean(rich.filter(pc.equal(rich["passenger_count"], k))["trip_distance"]).as_py()
        assert abs(v - exp) < 1e-9


def test_partition_and_column_filters_prune_scan_tasks(trips):
    df = read(trips)
    assert scan_tasks(df) == 5
    assert scan_tasks(df.where(col("pickup_date") == date(2023, 1, 2))) == 1
    assert scan_tasks(df.where(col("total_amount") > 10_000)) == 0      # min/max column stats


def test_legacy_arrow_conversion_names(trips):
    df = read(trips)
    with pytest.raises(AttributeError, match="to_arrow_table"):
        df.to_arrow_table()
    polars_df = pl.from_arrow(df.to_arrow())          # fix: to_arrow()
    assert polars_df.height == 5000
    with pytest.raises(AttributeError, match="to_arrow"):
        df.schema().to_arrow()
    assert df.schema().to_pyarrow_schema().names == polars_df.columns


def test_daft_writes_iceberg_directly(catalog, ns, trips):
    target = create_trips_table(catalog, f"{ns}.cleaned_trips", rows=0)
    log = write(read(trips).where(col("total_amount") > 50), target)
    assert set(log["operation"]) == {"ADD"} and len(log["file_name"]) == 5   # one file per day
    target.refresh()
    expected = trips.scan(row_filter="total_amount > 50").to_arrow().num_rows
    assert target.scan().to_arrow().num_rows == expected
    # Partition-scoped overwrite, like PyIceberg's overwrite_filter.
    day1 = read(trips).where(col("pickup_date") == date(2023, 1, 1)).limit(3)
    write(day1, target, mode="overwrite", overwrite_filter=col("pickup_date") == date(2023, 1, 1))
    target.refresh()
    after = target.scan().to_arrow()
    assert after.filter(pc.equal(after["pickup_date"], date(2023, 1, 1))).num_rows == 3


def test_parquet_then_create_table_registers_nothing(catalog, ns, trips):
    io_config = daft.io.IOConfig(s3=daft.io.S3Config(
        endpoint_url=S3_ENDPOINT, key_id=AWS_KEY, access_key=AWS_SECRET, region_name=AWS_REGION,
        force_virtual_addressing=False))
    location = f"s3://warehouse/{ns}/cleaned_parquet"
    written = read(trips).write_parquet(location, io_config=io_config).to_pydict()["path"]
    # Daft returns bucket/key paths without the scheme; PyIceberg needs full URIs.
    files = [p if p.startswith("s3://") else f"s3://{p}" for p in written]
    # Older docs: create_table(schema=..., location=...) over the exported files.
    t = catalog.create_table(f"{ns}.cleaned_by_location", schema=read(trips).schema().to_pyarrow_schema(),
                             location=location)
    assert t.scan().to_arrow().num_rows == 0            # a location is not a file registration
    # Registering them is refused too: Daft kept the source table's Iceberg field IDs in the
    # Parquet schema, and add_files only accepts files without field IDs.
    with pytest.raises(NotImplementedError, match="field IDs"):
        t.add_files(file_paths=files)
    write(read(trips), t)                               # fix: let Daft commit the snapshot
    assert t.scan().to_arrow().num_rows == 5000
