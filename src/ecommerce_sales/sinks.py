"""Table sinks. The pipeline only talks to a sink, so the same orchestration
code writes Delta tables on Databricks and in-memory temp views locally/in tests."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from .spark_utils import try_cache

log = logging.getLogger("ecommerce_sales.sinks")


class DeltaSink:
    """Managed Delta tables (Unity Catalog or hive_metastore)."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ensure_schemas(self, schemas: List[str]) -> None:
        for s in schemas:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

    def write(
        self,
        df: DataFrame,
        table: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        comment: Optional[str] = None,
    ) -> DataFrame:
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(table)
        if comment:
            safe = comment.replace("'", "\\'")
            self.spark.sql(f"COMMENT ON TABLE {table} IS '{safe}'")
        log.info("wrote %s", table)
        return self.spark.table(table)

    def read(self, table: str) -> DataFrame:
        return self.spark.table(table)


class MemorySink:
    """Keeps DataFrames in memory and registers temp views (dots -> underscores),
    so SQL can be run against them locally exactly as on Databricks."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.tables: Dict[str, DataFrame] = {}

    @staticmethod
    def view_name(table: str) -> str:
        return table.replace(".", "__")

    def ensure_schemas(self, schemas: List[str]) -> None:  # nothing to create
        pass

    def write(self, df: DataFrame, table: str, **_: object) -> DataFrame:
        df = try_cache(df)
        self.tables[table] = df
        df.createOrReplaceTempView(self.view_name(table))
        return df

    def read(self, table: str) -> DataFrame:
        return self.tables[table]
