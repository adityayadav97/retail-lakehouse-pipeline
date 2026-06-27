"""Gold layer — business-level aggregates and KPIs from Silver."""

from __future__ import annotations
import sys
from pyspark.sql import functions as F

sys.path.append(".")
from src.config import spark_session, SILVER, GOLD
from src.quality import data_quality as dq


def daily_sales(spark):
    print("\n🥇 GOLD aggregate: daily_sales")
    orders = spark.read.format("delta").load(str(SILVER / "orders"))

    gold = (
        orders.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("revenue").alias("total_revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("order_date")
    )
    dq.check_not_empty(gold, "gold.daily_sales")
    gold.write.format("delta").mode("overwrite").save(str(GOLD / "daily_sales"))
    print(f"  → written to {GOLD / 'daily_sales'}")


def customer_ltv(spark):
    print("\n🥇 GOLD aggregate: customer_ltv")
    orders = spark.read.format("delta").load(str(SILVER / "orders"))
    customers = spark.read.format("delta").load(str(SILVER / "customers"))

    ltv = (
        orders.groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("lifetime_value"),
            F.countDistinct("order_id").alias("total_orders"),
            F.max("order_date").alias("last_order_date"),
        )
    )
    gold = customers.join(ltv, "customer_id", "left").na.fill(
        {"lifetime_value": 0.0, "total_orders": 0}
    )
    gold.write.format("delta").mode("overwrite").save(str(GOLD / "customer_ltv"))
    print(f"  → written to {GOLD / 'customer_ltv'}")


def main():
    spark = spark_session("gold-aggregate")
    daily_sales(spark)
    customer_ltv(spark)
    print("\n✅ Gold aggregation complete")
    spark.stop()


if __name__ == "__main__":
    main()
