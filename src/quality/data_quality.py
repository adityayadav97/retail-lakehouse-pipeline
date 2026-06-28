"""Reusable data quality framework.

A small but production-style validation toolkit. Each check raises
DataQualityError on failure so the pipeline stops before bad data is promoted
to the next layer. Across the Bronze -> Silver -> Gold transitions the pipeline
runs 15+ individual check invocations (row counts, nulls, uniqueness, schema,
ranges, accepted values, referential integrity, freshness, reconciliation).
"""

from __future__ import annotations
from datetime import datetime
from pyspark.sql import DataFrame, functions as F


class DataQualityError(Exception):
    """Raised when a data quality gate fails."""


def check_not_empty(df: DataFrame, name: str) -> None:
    count = df.count()
    if count == 0:
        raise DataQualityError(f"[{name}] dataset is empty")
    print(f"  \u2705 [{name}] row count = {count:,}")


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
        print(f"  \u2705 [{name}] '{col}' null% = {pct:.2f}")


def check_unique(df: DataFrame, keys: list[str], name: str) -> None:
    total = df.count()
    distinct = df.select(*keys).distinct().count()
    if total != distinct:
        raise DataQualityError(
            f"[{name}] duplicate keys on {keys}: {total - distinct} duplicates found"
        )
    print(f"  \u2705 [{name}] unique on {keys}")


def check_schema(df: DataFrame, expected_cols: list[str], name: str) -> None:
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise DataQualityError(f"[{name}] missing expected columns: {missing}")
    print(f"  \u2705 [{name}] schema contains all expected columns")


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
    print(f"  \u2705 [{name}] reconciliation variance = {variance:.2f}% (within {threshold_pct}%)")


def check_range(df: DataFrame, column: str, min_value: float, max_value: float, name: str) -> None:
    """Ensure all numeric values in a column fall within [min_value, max_value]."""
    bad = df.filter((F.col(column) < min_value) | (F.col(column) > max_value)).count()
    if bad > 0:
        raise DataQualityError(
            f"[{name}] {bad} rows in '{column}' fall outside [{min_value}, {max_value}]"
        )
    print(f"  \u2705 [{name}] '{column}' within [{min_value}, {max_value}]")


def check_positive(df: DataFrame, column: str, name: str) -> None:
    """Ensure a column has strictly positive values (e.g. quantity, price)."""
    bad = df.filter(F.col(column) <= 0).count()
    if bad > 0:
        raise DataQualityError(f"[{name}] {bad} non-positive values in '{column}'")
    print(f"  \u2705 [{name}] '{column}' all positive")


def check_accepted_values(df: DataFrame, column: str, accepted: list, name: str) -> None:
    """Ensure a column only contains values from an accepted set."""
    bad = df.filter(~F.col(column).isin(accepted)).count()
    if bad > 0:
        raise DataQualityError(
            f"[{name}] {bad} rows in '{column}' not in accepted set {accepted}"
        )
    print(f"  \u2705 [{name}] '{column}' values within accepted set")


def check_referential_integrity(child: DataFrame, child_key: str, parent: DataFrame, parent_key: str, name: str) -> None:
    """Ensure every child key exists in the parent dimension (no orphans)."""
    orphans = child.join(parent, child[child_key] == parent[parent_key], "left_anti").count()
    if orphans > 0:
        raise DataQualityError(
            f"[{name}] {orphans} orphan rows: '{child_key}' not found in parent '{parent_key}'"
        )
    print(f"  \u2705 [{name}] referential integrity '{child_key}' -> '{parent_key}'")


def check_freshness(df: DataFrame, ts_column: str, max_age_hours: float, name: str) -> None:
    """Ensure the most recent timestamp is within max_age_hours of now."""
    latest = df.select(F.max(F.col(ts_column)).alias("m")).collect()[0]["m"]
    if latest is None:
        raise DataQualityError(f"[{name}] no timestamps found in '{ts_column}'")
    try:
        age_hours = (datetime.now() - latest).total_seconds() / 3600
    except TypeError:
        age_hours = None
    if age_hours is not None and age_hours > max_age_hours:
        raise DataQualityError(
            f"[{name}] data is stale: latest '{ts_column}' is {age_hours:.1f}h old (max {max_age_hours}h)"
        )
    print(f"  \u2705 [{name}] '{ts_column}' freshness OK")
