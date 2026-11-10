# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Aggregate table (Task 4)
# MAGIC `ecommerce_aggregate.profit_by_year_category_customer` holds profit by **Year × Product Category ×
# MAGIC Product Sub-Category × Customer**.
# MAGIC
# MAGIC * The grain is `order_year, category, sub_category, customer_id, customer_name`. `customer_id` is included
# MAGIC   because names are not unique (for example, several `Unknown` customers).
# MAGIC * Measures: `total_profit` (DECIMAL(18,2)), `order_count` (distinct orders) and `total_quantity`.
# MAGIC * The coarser views in Task 5 are roll-ups of this table, so every report shares one source of truth.
# MAGIC * DQ gates: the grain is unique and the total profit equals the order master.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

agg = pipeline.run_aggregate(spark, cfg, sink)

# COMMAND ----------

display(spark.table(cfg.profit_aggregate).orderBy("order_year", "category", "sub_category", "customer_id"))
