"""Task 4 - aggregate table: profit by year / category / sub-category / customer."""

from datetime import date
from decimal import Decimal

import pytest

from ecommerce_sales import transformations as T

ENRICHED_SCHEMA = (
    "row_id long, order_id string, order_date date, order_year int, customer_id string, customer_name string, "
    "category string, sub_category string, quantity long, profit decimal(18,2)"
)


@pytest.fixture
def enriched_orders(spark):
    D = Decimal
    rows = [
        # id, order, date, year, cust, name, cat, subcat, qty, profit
        (1, "O-1", date(2016, 1, 1), 2016, "C-1", "Gary Hansen", "Furniture", "Chairs", 2, D("10.00")),
        (2, "O-1", date(2016, 1, 1), 2016, "C-1", "Gary Hansen", "Furniture", "Chairs", 1, D("5.55")),
        (3, "O-2", date(2016, 5, 1), 2016, "C-1", "Gary Hansen", "Furniture", "Tables", 1, D("-20.00")),
        (4, "O-3", date(2017, 1, 1), 2017, "C-1", "Gary Hansen", "Furniture", "Chairs", 3, D("1.10")),
        (5, "O-4", date(2016, 2, 1), 2016, "C-2", "Unknown", "Technology", "Phones", 1, D("100.00")),
        (6, "O-5", date(2016, 2, 1), 2016, "C-3", "Unknown", "Technology", "Phones", 1, D("50.00")),
    ]
    return spark.createDataFrame(rows, ENRICHED_SCHEMA)


def test_aggregate_grain_and_values(spark, enriched_orders):
    agg = T.build_profit_aggregate(enriched_orders)
    got = {
        (r.order_year, r.category, r.sub_category, r.customer_id): (r.total_profit, r.order_count, r.total_quantity)
        for r in agg.collect()
    }
    assert got == {
        (2016, "Furniture", "Chairs", "C-1"): (Decimal("15.55"), 1, 3),
        (2016, "Furniture", "Tables", "C-1"): (Decimal("-20.00"), 1, 1),
        (2017, "Furniture", "Chairs", "C-1"): (Decimal("1.10"), 1, 3),
        (2016, "Technology", "Phones", "C-2"): (Decimal("100.00"), 1, 1),
        (2016, "Technology", "Phones", "C-3"): (Decimal("50.00"), 1, 1),
    }


def test_customers_with_same_name_are_not_merged(spark, enriched_orders):
    agg = T.build_profit_aggregate(enriched_orders).filter("customer_name = 'Unknown'")
    assert agg.count() == 2


def test_aggregate_preserves_total_profit(spark, enriched_orders):
    agg = T.build_profit_aggregate(enriched_orders)
    total = agg.agg({"total_profit": "sum"}).first()[0]
    assert total == Decimal("146.65")


def test_aggregate_schema(spark, enriched_orders):
    agg = T.build_profit_aggregate(enriched_orders)
    assert agg.columns[:6] == T.AGGREGATE_KEYS + ["total_profit"]
    assert dict(agg.dtypes)["total_profit"] == "decimal(18,2)"


def test_aggregate_unique_on_grain(spark, enriched_orders):
    agg = T.build_profit_aggregate(enriched_orders)
    assert agg.groupBy(*T.AGGREGATE_KEYS).count().filter("count > 1").count() == 0


def test_aggregate_of_empty_input_is_empty(spark):
    empty = spark.createDataFrame([], ENRICHED_SCHEMA)
    assert T.build_profit_aggregate(empty).count() == 0
