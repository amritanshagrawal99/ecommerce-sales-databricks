"""Raw layer: read source files into Spark DataFrames.

The raw layer is deliberately thin - it keeps the data as delivered, only
  * standardises column names to snake_case (Delta-friendly, no spaces),
  * applies an explicit schema,
  * adds ingestion metadata (_source_file, _ingested_at),
so the enriched layer can always be rebuilt from it.
"""

from __future__ import annotations

import os
import re
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from .schemas import (
    CUSTOMER_RAW_SCHEMA,
    ORDER_RAW_SCHEMA,
    PRODUCT_RAW_SCHEMA,
)


class IngestionError(Exception):
    """A source file is missing, unreadable or does not have the expected shape."""


CORRUPT_RECORD_COL = "_corrupt_record"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def to_snake_case(name: str) -> str:
    """'Customer ID' -> 'customer_id', 'Sub-Category' -> 'sub_category'."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())
    return name.strip("_").lower()


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Rename every column to snake_case. Leading underscores (metadata) are kept."""
    renamed = []
    for c in df.columns:
        new = c if c.startswith("_") else to_snake_case(c)
        renamed.append(F.col(f"`{c}`").alias(new))
    return df.select(*renamed)


def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    return df.withColumn("_source_file", F.lit(source_file)).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def to_local_path(path: str) -> str:
    """pandas cannot read Spark-style URIs.
    dbfs:/x -> /dbfs/x (FUSE mount), file:///x -> local OS path,
    /Volumes/... and plain local paths are returned unchanged."""
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/"):].lstrip("/")
    if path.startswith("file:"):
        from urllib.parse import unquote, urlparse
        from urllib.request import url2pathname

        return url2pathname(unquote(urlparse(path).path))
    return path


def _check_columns(actual: Iterable[str], expected: Iterable[str], source: str) -> None:
    actual_set = {c.strip() for c in actual}
    missing = [c for c in expected if c not in actual_set]
    if missing:
        raise IngestionError(
            f"{source}: missing expected column(s) {missing}; found {sorted(actual_set)}"
        )


def _with_corrupt_col(schema: StructType) -> StructType:
    from pyspark.sql.types import StringType, StructField

    return StructType(schema.fields + [StructField(CORRUPT_RECORD_COL, StringType(), True)])


# --------------------------------------------------------------------------- #
# readers
# --------------------------------------------------------------------------- #
def read_customers_raw(spark: SparkSession, path: str) -> DataFrame:
    """Customer.xlsx -> DataFrame (all columns string).

    Spark has no built-in Excel reader. The file is small (dimension data), so it
    is read with pandas/openpyxl on the driver and converted with an explicit
    schema. For very large workbooks swap this for the com.crealytics:spark-excel
    library (see README).
    """
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover
        raise IngestionError("pandas + openpyxl are required to read Excel files") from exc

    local_path = to_local_path(path)
    if not os.path.exists(local_path):
        raise IngestionError(f"Customer source file not found: {path}")
    try:
        pdf = pd.read_excel(local_path, dtype=str, engine="openpyxl")
    except Exception as exc:  # corrupt / not an xlsx
        raise IngestionError(f"Unable to read Excel file {path}: {exc}") from exc

    pdf.columns = [str(c).strip() for c in pdf.columns]
    expected = CUSTOMER_RAW_SCHEMA.fieldNames()
    _check_columns(pdf.columns, expected, path)
    pdf = pdf[expected]
    # NaN -> None so Spark receives proper nulls
    records = [
        tuple(None if pd.isna(v) else str(v) for v in row)
        for row in pdf.itertuples(index=False, name=None)
    ]
    df = spark.createDataFrame(records, schema=CUSTOMER_RAW_SCHEMA)
    return add_ingestion_metadata(standardize_column_names(df), path)


def read_products_raw(spark: SparkSession, path: str) -> DataFrame:
    """Products.csv -> DataFrame. Product names contain quoted commas and
    non-breaking spaces (UTF-8), hence quote/escape/encoding options."""
    try:
        header_cols = (
            spark.read.option("header", True).option("encoding", "UTF-8").csv(path).columns
        )
    except AnalysisException as exc:
        raise IngestionError(f"Product source file not found/unreadable: {path}") from exc
    _check_columns(header_cols, PRODUCT_RAW_SCHEMA.fieldNames(), path)

    df = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
        .schema(_with_corrupt_col(PRODUCT_RAW_SCHEMA))
        .csv(path)
    )
    return add_ingestion_metadata(standardize_column_names(df), path)


def read_orders_raw(spark: SparkSession, path: str) -> DataFrame:
    """Orders.json (a single JSON array, pretty printed) -> DataFrame."""
    try:
        df = (
            spark.read.option("multiLine", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COL)
            .schema(_with_corrupt_col(ORDER_RAW_SCHEMA))
            .json(path)
        )
        df.columns  # force analysis: a missing path fails here, also on Spark Connect
    except AnalysisException as exc:
        raise IngestionError(f"Order source file not found/unreadable: {path}") from exc
    return add_ingestion_metadata(standardize_column_names(df), path)
