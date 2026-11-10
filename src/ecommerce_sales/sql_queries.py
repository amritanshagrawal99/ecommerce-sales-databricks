"""Task 5 - SQL outputs, all rolled up from the aggregate table.

Kept as plain strings (also exported to sql/*.sql) so the exact same SQL runs
in the Databricks %sql notebook and in the unit tests.
"""

from __future__ import annotations

PROFIT_BY_YEAR = """
SELECT
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM {table}
GROUP BY order_year
ORDER BY order_year
"""

PROFIT_BY_YEAR_CATEGORY = """
SELECT
    order_year,
    category,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM {table}
GROUP BY order_year, category
ORDER BY order_year, category
"""

PROFIT_BY_CUSTOMER = """
SELECT
    customer_id,
    customer_name,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM {table}
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC, customer_id
"""

PROFIT_BY_CUSTOMER_YEAR = """
SELECT
    customer_id,
    customer_name,
    order_year,
    CAST(SUM(total_profit) AS DECIMAL(18,2)) AS total_profit
FROM {table}
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year
"""

QUERIES = {
    "profit_by_year": PROFIT_BY_YEAR,
    "profit_by_year_category": PROFIT_BY_YEAR_CATEGORY,
    "profit_by_customer": PROFIT_BY_CUSTOMER,
    "profit_by_customer_year": PROFIT_BY_CUSTOMER_YEAR,
}


def render(query_name: str, table: str) -> str:
    return QUERIES[query_name].format(table=table).strip()
