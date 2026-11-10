-- 5.4 Profit by Customer + Year
-- Source: aggregate table (default ecommerce_aggregate.profit_by_year_category_customer)
SELECT
    customer_id,
    customer_name,
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ${aggregate_table}
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year;
