# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuration
# MAGIC Shared setup, pulled into every stage notebook with `%run ./00_config`.
# MAGIC
# MAGIC * **source_path**: folder holding `Customer.xlsx`, `Products.csv` and `Orders.json`
# MAGIC   (a Unity Catalog volume is recommended, e.g. `/Volumes/workspace/default/ecommerce_landing`).
# MAGIC * **catalog**: Unity Catalog catalog for the output schemas. Leave it blank to use the current catalog.
# MAGIC
# MAGIC The transformation code is in `../src/ecommerce_sales` (a plain Python package). The notebooks only
# MAGIC orchestrate it, so the exact code that is unit tested is the code that runs here.

# COMMAND ----------

import logging
import os
import sys

dbutils.widgets.text("source_path", "/Volumes/workspace/default/ecommerce_landing", "Source files directory")
dbutils.widgets.text("catalog", "", "Catalog (blank = current)")

# make ../src importable (Git folder / workspace files)
_src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if _src not in sys.path:
    sys.path.insert(0, _src)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s", force=True)

from ecommerce_sales import pipeline, sql_queries  # noqa: E402
from ecommerce_sales import transformations as T  # noqa: E402
from ecommerce_sales.config import PipelineConfig  # noqa: E402
from ecommerce_sales.sinks import DeltaSink  # noqa: E402

cfg = PipelineConfig(
    source_path=dbutils.widgets.get("source_path"),
    catalog=dbutils.widgets.get("catalog").strip() or None,
)
sink = DeltaSink(spark)

print(f"source_path      : {cfg.source_path}")
print(f"raw tables       : {cfg.raw_customers}, {cfg.raw_products}, {cfg.raw_orders}")
print(f"enriched tables  : {cfg.enriched_customers}, {cfg.enriched_products}, {cfg.enriched_orders}")
print(f"aggregate table  : {cfg.profit_aggregate}")
