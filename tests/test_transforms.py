"""Unit tests for transformation logic using a local Spark session."""

import sys
import pytest
from pyspark.sql import functions as F

sys.path.append(".")
from src.config import spark_session


@pytest.fixture(scope="session")
def spark():
    s = spark_session("tests")
    yield s
    s.stop()


def test_revenue_calculation(spark):
    df = spark.createDataFrame(
        [(1, 2, 10.0), (2, 3, 5.0)],
        ["order_id", "quantity", "unit_price"],
    ).withColumn("revenue", F.col("quantity") * F.col("unit_price"))

    rows = {r["order_id"]: r["revenue"] for r in df.collect()}
    assert rows[1] == 20.0
    assert rows[2] == 15.0


def test_dedupe_keeps_latest(spark):
    from pyspark.sql import Window
    data = [
        (1, "2026-01-01"),
        (1, "2026-02-01"),  # latest
        (2, "2026-01-15"),
    ]
    df = spark.createDataFrame(data, ["order_id", "ingested"])
    w = Window.partitionBy("order_id").orderBy(F.col("ingested").desc())
    deduped = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    assert deduped.count() == 2
    latest = {r["order_id"]: r["ingested"] for r in deduped.collect()}
    assert latest[1] == "2026-02-01"
