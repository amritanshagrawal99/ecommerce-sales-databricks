-- 5.3 Profit by Customer
-- Source: aggregate table (default ecommerce_aggregate.profit_by_year_category_customer)
SELECT
    customer_id,
    customer_name,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ${aggregate_table}
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC, customer_id;
