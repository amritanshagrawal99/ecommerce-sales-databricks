"""Small runtime helpers that differ between classic Spark and Databricks serverless."""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame

log = logging.getLogger("ecommerce_sales.spark_utils")


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def is_serverless() -> bool:
    """Serverless compute reports runtime versions like 'client.2.5'."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client.")


def try_cache(df: DataFrame) -> DataFrame:
    """cache() where supported. Serverless compute rejects PERSIST; there the
    DataFrame is returned unchanged (correct, just recomputed when re-used)."""
    if is_serverless():
        return df
    try:
        return df.cache()
    except Exception as exc:  # pragma: no cover - environment specific
        log.info("cache not supported here (%s); continuing without it", exc)
        return df


def try_unpersist(df: DataFrame) -> None:
    if is_serverless():
        return
    try:
        df.unpersist()
    except Exception:  # pragma: no cover
        pass
