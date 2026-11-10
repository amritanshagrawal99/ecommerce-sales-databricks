-- 5.2 Profit by Year + Product Category
-- Source: aggregate table (default ecommerce_aggregate.profit_by_year_category_customer)
SELECT
    order_year,
    category,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM ${aggregate_table}
GROUP BY order_year, category
ORDER BY order_year, category;
