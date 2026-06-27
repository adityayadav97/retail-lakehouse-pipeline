"""Reusable data quality framework.

Each check raises DataQualityError on failure so the pipeline stops before
bad data is promoted to the next layer.
"""

from __future__ import annotations
from pyspark.sql import DataFrame, functions as F


class DataQualityError(Exception):
    """Raised when a data quality gate fails."""


def check_not_empty(df: DataFrame, name: str) -> None:
    count = df.count()
    if count == 0:
        raise DataQualityError(f"[{name}] dataset is empty")
    print(f"  ✅ [{name}] row count = {count:,}")


def check_no_nulls(df: DataFrame, columns: list[str], name: str, max_null_pct: float = 0.0) -> None:
    total = df.count()
    if total == 0:
        raise DataQualityError(f"[{name}] cannot run null check on empty dataset")
    for col in columns:
        nulls = df.filter(F.col(col).isNull()).count()
        pct = (nulls / total) * 100
        if pct > max_null_pct:
            raise DataQualityError(
                f"[{name}] column '{col}' has {pct:.2f}% nulls (max allowed {max_null_pct}%)"
            )
        print(f"  ✅ [{name}] '{col}' null% = {pct:.2f}")


def check_unique(df: DataFrame, keys: list[str], name: str) -> None:
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    if total != distinct:
        raise DataQualityError(
            f"[{name}] duplicate keys on {keys}: {total - distinct} duplicates found"
        )
    print(f"  ✅ [{name}] unique on {keys}")


def check_schema(df: DataFrame, expected_cols: list[str], name: str) -> None:
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise DataQualityError(f"[{name}] missing expected columns: {missing}")
    print(f"  ✅ [{name}] schema contains all expected columns")


def check_delta_threshold(source_count: int, target_count: int, name: str, threshold_pct: float) -> None:
    """Row-count reconciliation between source and target within a threshold."""
    if source_count == 0:
        raise DataQualityError(f"[{name}] source count is zero")
    variance = abs(source_count - target_count) / source_count * 100
    if variance > threshold_pct:
        raise DataQualityError(
            f"[{name}] row variance {variance:.2f}% exceeds threshold {threshold_pct}% "
            f"(source={source_count:,}, target={target_count:,})"
        )
    print(f"  ✅ [{name}] reconciliation variance = {variance:.2f}% (within {threshold_pct}%)")
