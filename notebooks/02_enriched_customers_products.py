# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Enriched customers and products (Task 2)
# MAGIC
# MAGIC **Customers** → `ecommerce_enriched.customers` (one row per `customer_id`)
# MAGIC * Removes noise injected into names (`Gary567 Hansen` → `Gary Hansen`, `Tam&^*ara Willing___)ham` →
# MAGIC   `Tamara Willingham`) and keeps real apostrophes/hyphens (`O'Rourke`, `Corey-Lock`). The original value
# MAGIC   is kept in `customer_name_raw`.
# MAGIC * Missing names (8 customers) become `Unknown`.
# MAGIC * Phone numbers are normalised to 10 digits (extensions and the `001` prefix are removed; invalid numbers become null).
# MAGIC * ZIP codes get their leading zeros back (Excel stored `2920` for `02920`), and address line breaks are removed.
# MAGIC
# MAGIC **Products** → `ecommerce_enriched.products` (one row per `product_id`)
# MAGIC * The source repeats 33 product ids (different state, name or price). Category and sub-category are identical
# MAGIC   across those duplicates (a DQ check verifies this), so one record is kept per id. The pick is deterministic and
# MAGIC   `source_record_count` records how many rows were merged. Without this step the orders join would fan out and
# MAGIC   double count profit.
# MAGIC * Non-breaking spaces in product names are replaced.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dims = pipeline.run_enriched_dimensions(spark, cfg, sink)

# COMMAND ----------

display(
    spark.table(cfg.enriched_customers)
    .filter("customer_name <> customer_name_raw OR customer_name_raw IS NULL")
    .select("customer_id", "customer_name_raw", "customer_name", "phone", "postal_code", "country")
)

# COMMAND ----------

display(spark.table(cfg.enriched_products).filter("source_record_count > 1"))
