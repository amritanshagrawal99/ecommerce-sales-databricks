# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Raw tables (Task 1)
# MAGIC One raw Delta table per source file:
# MAGIC
# MAGIC | Source | Format | Raw table |
# MAGIC |---|---|---|
# MAGIC | Customer.xlsx | Excel | `ecommerce_raw.customers` |
# MAGIC | Products.csv | CSV (quoted, UTF-8) | `ecommerce_raw.products` |
# MAGIC | Orders.json | JSON array (multi-line) | `ecommerce_raw.orders` |
# MAGIC
# MAGIC The raw layer keeps data as delivered: explicit schema, snake_case column names, plus
# MAGIC `_source_file` / `_ingested_at` lineage columns. Rows Spark cannot parse land in `_corrupt_record`
# MAGIC and are not dropped. All cleaning happens in the enriched layer, so it can always be rebuilt from raw.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# Fail early with a clear message if the source files have not been uploaded
for f in (cfg.files.customers, cfg.files.products, cfg.files.orders):
    try:
        dbutils.fs.ls(cfg.source(f))
    except Exception as exc:
        raise FileNotFoundError(f"Missing source file {cfg.source(f)} - upload it first (see README)") from exc

# COMMAND ----------

raw = pipeline.run_raw(spark, cfg, sink)
for table, df in raw.items():
    print(f"{table:<40} {df.count():>6} rows")

# COMMAND ----------

display(spark.table(cfg.raw_customers).limit(10))

# COMMAND ----------

display(spark.table(cfg.raw_products).limit(10))

# COMMAND ----------

display(spark.table(cfg.raw_orders).limit(10))
