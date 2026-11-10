-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 · SQL aggregates (Task 5)
-- MAGIC Each output is a roll-up of the aggregate table `ecommerce_aggregate.profit_by_year_category_customer`
-- MAGIC built in notebook 04.
-- MAGIC
-- MAGIC If you set a `catalog` in `00_config`, run `USE CATALOG <catalog>` first.
-- MAGIC The same queries are in `../sql/*.sql` and are unit tested in `tests/test_sql_queries.py`.

-- COMMAND ----------

-- MAGIC %md ## 5.1 Profit by Year

-- COMMAND ----------

SELECT
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ecommerce_aggregate.profit_by_year_category_customer
GROUP BY order_year
ORDER BY order_year;

-- COMMAND ----------

-- MAGIC %md ## 5.2 Profit by Year + Product Category

-- COMMAND ----------

SELECT
    order_year,
    category,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ecommerce_aggregate.profit_by_year_category_customer
GROUP BY order_year, category
ORDER BY order_year, category;

-- COMMAND ----------

-- MAGIC %md ## 5.3 Profit by Customer

-- COMMAND ----------

SELECT
    customer_id,
    customer_name,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ecommerce_aggregate.profit_by_year_category_customer
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC, customer_id;

-- COMMAND ----------

-- MAGIC %md ## 5.4 Profit by Customer + Year

-- COMMAND ----------

SELECT
    customer_id,
    customer_name,
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ecommerce_aggregate.profit_by_year_category_customer
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year;

-- COMMAND ----------

-- MAGIC %md ### Reconciliation: every output sums to the source total profit

-- COMMAND ----------

SELECT
    (SELECT CAST(SUM(profit) AS DECIMAL(18,2)) FROM ecommerce_enriched.orders)                                 AS order_master_profit,
    (SELECT CAST(SUM(total_profit) AS DECIMAL(18,2)) FROM ecommerce_aggregate.profit_by_year_category_customer) AS aggregate_profit;
