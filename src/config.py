"""Central configuration for the Retail Lakehouse Pipeline."""

from pathlib import Path

# Root directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
LAKEHOUSE = BASE_DIR / "lakehouse"

# Medallion layer paths (Delta)
BRONZE = LAKEHOUSE / "bronze"
SILVER = LAKEHOUSE / "silver"
GOLD = LAKEHOUSE / "gold"

# Source datasets
SOURCES = {
    "orders": DATA_RAW / "orders.csv",
    "customers": DATA_RAW / "customers.csv",
    "products": DATA_RAW / "products.csv",
}

# Data quality thresholds
DQ_DELTA_THRESHOLD_PCT = 20.0   # acceptable row variance between runs
DQ_MAX_NULL_PCT = 5.0           # max null % allowed in critical columns


def spark_session(app_name: str = "retail-lakehouse"):
    """Create a Delta-enabled Spark session."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
