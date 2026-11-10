-- 5.1 Profit by Year
-- Source: aggregate table (default ecommerce_aggregate.profit_by_year_category_customer)
SELECT
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ${aggregate_table}
GROUP BY order_year
ORDER BY order_year;
