"""Bronze layer — ingest raw source files into Delta as-is (append-only history)."""

from __future__ import annotations
import sys
from pyspark.sql import functions as F

sys.path.append(".")
from src.config import spark_session, SOURCES, BRONZE
from src.quality import data_quality as dq


def ingest_table(spark, name: str, source_path) -> None:
    print(f"\n🥉 BRONZE ingest: {name}")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(source_path))
        # lineage / audit columns
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path.name))
    )

    dq.check_not_empty(df, f"bronze.{name}")

    target = str(BRONZE / name)
    df.write.format("delta").mode("append").save(target)
    print(f"  → written to {target}")


def main() -> None:
    spark = spark_session("bronze-ingest")
    for name, path in SOURCES.items():
        ingest_table(spark, name, path)
    print("\n✅ Bronze ingestion complete")
    spark.stop()


if __name__ == "__main__":
    main()
