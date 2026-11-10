# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Run the whole pipeline
# MAGIC Runs the stage notebooks in order with the same parameters. In production the same chain runs as a
# MAGIC multi-task Databricks Job (see `databricks.yml`), which gives per-stage retries and lineage.

# COMMAND ----------

dbutils.widgets.text("source_path", "/Volumes/workspace/default/ecommerce_landing", "Source files directory")
dbutils.widgets.text("catalog", "", "Catalog (blank = current)")
params = {"source_path": dbutils.widgets.get("source_path"), "catalog": dbutils.widgets.get("catalog")}

for nb in ["01_raw_ingestion", "02_enriched_customers_products", "03_enriched_orders",
           "04_aggregate_profit", "05_sql_aggregates"]:
    print(f"running {nb} ...")
    dbutils.notebook.run(f"./{nb}", timeout_seconds=3600, arguments=params)
print("pipeline finished")
