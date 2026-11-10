# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Enriched order master table (Task 3)
# MAGIC `ecommerce_enriched.orders` has one row per order line, with:
# MAGIC 1. **Order information**: ids, order and ship dates (parsed from `d/M/yyyy`), `order_year`, ship mode,
# MAGIC    quantity, price and discount, plus **profit rounded to 2 decimal places** (half-up, stored as `DECIMAL(18,2)`).
# MAGIC 2. **Customer name and country** from `ecommerce_enriched.customers`.
# MAGIC 3. **Product category and sub-category** from `ecommerce_enriched.products`.
# MAGIC
# MAGIC Design notes
# MAGIC * **Left joins**: an order is never dropped because a dimension row is missing, so profit reconciles with the source.
# MAGIC * **Broadcast joins**: the dimensions are small, so the fact table is not shuffled.
# MAGIC * 44 product ids (204 order lines) are **not in the product master**. Their category and sub-category are inferred
# MAGIC   from the product-id code (`OFF-EN-…` → Office Supplies / Envelopes). Only codes that map to exactly one pair in
# MAGIC   the master are used. `product_match_status` shows which path was taken.
# MAGIC * **Validation**: rows with a missing key, an unparseable date, a missing profit, a quantity ≤ 0, a discount outside
# MAGIC   [0, 1] or a duplicate `row_id` go to `ecommerce_enriched.orders_rejected` with a `reject_reason`. They are
# MAGIC   quarantined, not silently dropped.
# MAGIC * **DQ gates** (the run stops if one fails): no join fan-out (row count preserved), unique `row_id`, no nulls in
# MAGIC   enriched columns, and the profit total reconciles with the validated input.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

out = pipeline.run_enriched_orders(spark, cfg, sink)

# COMMAND ----------

display(spark.table(cfg.enriched_orders).limit(20))

# COMMAND ----------

display(spark.table(cfg.enriched_orders).groupBy("product_match_status", "customer_match_status").count())

# COMMAND ----------

display(spark.table(cfg.rejected_orders))
