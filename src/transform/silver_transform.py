"""Silver layer — clean, deduplicate, type-cast, and conform Bronze data."""

from __future__ import annotations
import sys
from pyspark.sql import functions as F, Window

sys.path.append(".")
from src.config import spark_session, BRONZE, SILVER, DQ_MAX_NULL_PCT
from src.quality import data_quality as dq


def _latest_per_key(df, keys, order_col):
    """Keep the most recent record per key (idempotent dedupe)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_orders(spark):
    print("\n🥈 SILVER transform: orders")
    bronze = spark.read.format("delta").load(str(BRONZE / "orders"))

    silver = (
        bronze
        .withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .filter(F.col("order_id").isNotNull())
    )
    silver = _latest_per_key(silver, ["order_id"], "_ingested_at")

    dq.check_no_nulls(silver, ["order_id", "customer_id", "revenue"], "silver.orders", DQ_MAX_NULL_PCT)
    dq.check_unique(silver, ["order_id"], "silver.orders")

    silver.write.format("delta").mode("overwrite").save(str(SILVER / "orders"))
    print(f"  → written to {SILVER / 'orders'}")


def build_customers(spark):
    print("\n🥈 SILVER transform: customers")
    bronze = spark.read.format("delta").load(str(BRONZE / "customers"))
    silver = (
        bronze
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("signup_date", F.to_date("signup_date"))
        .filter(F.col("customer_id").isNotNull())
    )
    silver = _latest_per_key(silver, ["customer_id"], "_ingested_at")
    dq.check_unique(silver, ["customer_id"], "silver.customers")
    silver.write.format("delta").mode("overwrite").save(str(SILVER / "customers"))
    print(f"  → written to {SILVER / 'customers'}")


def main():
    spark = spark_session("silver-transform")
    build_orders(spark)
    build_customers(spark)
    print("\n✅ Silver transformation complete")
    spark.stop()


if __name__ == "__main__":
    main()
