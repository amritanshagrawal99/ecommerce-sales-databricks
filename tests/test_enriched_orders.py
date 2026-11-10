"""Task 3 - enriched order master: validation, date parsing, profit rounding, joins."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import make_raw_orders
from ecommerce_sales import transformations as T


@pytest.fixture
def dims(raw_customers, raw_products):
    return T.enrich_customers(raw_customers), T.enrich_products(raw_products)


def _enrich(spark, rows, dims):
    valid, rejected = T.validate_orders(make_raw_orders(spark, rows))
    return T.enrich_orders(valid, *dims), rejected


# --------------------------------------------------------------------------- #
# profit rounding
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw, expected",
    [
        (63.69, Decimal("63.69")),
        (2.345, Decimal("2.35")),       # half-up
        (2.675, Decimal("2.68")),       # 2.675 is 2.67499.. in binary; decimal path rounds correctly
        (-14.925, Decimal("-14.93")),   # negative half-up (away from zero)
        (-16438.0, Decimal("-16438.00")),
        (0.004, Decimal("0.00")),
        (1.0, Decimal("1.00")),
    ],
)
def test_round_money(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "v double")
    out = df.select(T.round_money(F.col("v")).alias("v")).first().v
    assert out == expected


def test_profit_is_decimal_2dp(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": 10.126}], dims)
    assert dict(out.dtypes)["profit"] == "decimal(18,2)"
    assert out.first().profit == Decimal("10.13")


# --------------------------------------------------------------------------- #
# dates
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "raw, expected",
    [
        ("21/8/2016", date(2016, 8, 21)),
        ("6/10/2016", date(2016, 10, 6)),
        ("01/02/2017", date(2017, 2, 1)),
        ("31/12/2014", date(2014, 12, 31)),
        ("2016-08-21", None),
        ("31/2/2016", None),
        ("garbage", None),
        (None, None),
    ],
)
def test_parse_date(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "v string")
    assert df.select(T.parse_date(F.col("v")).alias("d")).first().d == expected


def test_order_year_from_order_date(spark, dims):
    # order id says 2016 but the order date is 2017 -> year comes from the date
    out, _ = _enrich(
        spark,
        [{"row_id": 1, "order_id": "CA-2016-1", "order_date": "2/1/2017",
          "customer_id": "C-1", "product_id": "FUR-CH-1"}],
        dims,
    )
    row = out.first()
    assert row.order_date == date(2017, 1, 2) and row.order_year == 2017


# --------------------------------------------------------------------------- #
# validation / quarantine
# --------------------------------------------------------------------------- #
def test_validate_orders_rejects_bad_rows(spark):
    rows = [
        {"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1"},                         # good
        {"row_id": 2, "customer_id": "C-1", "product_id": "FUR-CH-1", "order_date": "xx"},     # bad date
        {"row_id": 3, "customer_id": None, "product_id": "FUR-CH-1"},                          # no customer
        {"row_id": 4, "customer_id": "C-1", "product_id": "  "},                               # blank product
        {"row_id": 5, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": None},         # no profit
        {"row_id": 6, "customer_id": "C-1", "product_id": "FUR-CH-1", "quantity": 0},          # qty 0
        {"row_id": 7, "customer_id": "C-1", "product_id": "FUR-CH-1", "discount": 1.5},        # discount > 100%
        {"row_id": None, "customer_id": "C-1", "product_id": "FUR-CH-1"},                      # no row id
        {"row_id": 9, "order_id": None, "customer_id": "C-1", "product_id": "FUR-CH-1"},       # no order id
        {"row_id": 10, "_corrupt_record": "{bad json"},                                        # corrupt
    ]
    valid, rejected = T.validate_orders(make_raw_orders(spark, rows))
    assert [r.row_id for r in valid.collect()] == [1]
    reasons = {r.row_id: r.reject_reason for r in rejected.collect()}
    assert reasons[2] == "invalid_order_date"
    assert reasons[3] == "missing_customer_id"
    assert reasons[4] == "missing_product_id"
    assert reasons[5] == "missing_profit"
    assert reasons[6] == "invalid_quantity"
    assert reasons[7] == "invalid_discount"
    assert reasons[None] == "missing_row_id"
    assert reasons[9] == "missing_order_id"
    assert "corrupt_record" in reasons[10]
    assert valid.count() + rejected.count() == len(rows)    # nothing silently lost


def test_validate_orders_multiple_reasons_are_listed(spark):
    _, rejected = T.validate_orders(make_raw_orders(spark, [{"row_id": 1, "customer_id": None, "product_id": None}]))
    assert rejected.first().reject_reason == "missing_customer_id,missing_product_id"


def test_validate_orders_rejects_duplicate_row_ids(spark):
    rows = [
        {"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": 1.0,
         "_ingested_at": datetime(2026, 1, 1)},
        {"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": 2.0,
         "_ingested_at": datetime(2026, 1, 2)},   # later load wins
        {"row_id": 2, "customer_id": "C-1", "product_id": "FUR-CH-1"},
    ]
    valid, rejected = T.validate_orders(make_raw_orders(spark, rows))
    assert sorted((r.row_id, r.profit) for r in valid.collect()) == [(1, 2.0), (2, 1.0)]
    assert [(r.row_id, r.reject_reason) for r in rejected.collect()] == [(1, "duplicate_row_id")]


def test_invalid_duplicate_does_not_displace_valid_row(spark):
    rows = [
        {"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "_ingested_at": datetime(2026, 1, 1)},
        {"row_id": 1, "customer_id": None, "product_id": "FUR-CH-1", "_ingested_at": datetime(2026, 1, 2)},
    ]
    valid, rejected = T.validate_orders(make_raw_orders(spark, rows))
    assert valid.count() == 1
    assert rejected.first().reject_reason == "missing_customer_id"


def test_ids_are_normalised_before_join(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": " c-1 ", "product_id": "fur-ch-1"}], dims)
    row = out.first()
    assert (row.customer_name, row.category) == ("Gary Hansen", "Furniture")


# --------------------------------------------------------------------------- #
# enrichment / joins
# --------------------------------------------------------------------------- #
def test_enrich_orders_adds_customer_and_product_attributes(spark, dims):
    out, _ = _enrich(
        spark,
        [
            {"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": 63.69},
            {"row_id": 2, "customer_id": "C-2", "product_id": "OFF-PA-1", "profit": -14.92},
        ],
        dims,
    )
    rows = {r.row_id: r for r in out.collect()}
    assert (rows[1].customer_name, rows[1].country, rows[1].category, rows[1].sub_category) == (
        "Gary Hansen", "United States", "Furniture", "Chairs")
    assert (rows[2].customer_name, rows[2].country, rows[2].category, rows[2].sub_category) == (
        "Mary O'Rourke", "Canada", "Office Supplies", "Paper")
    assert rows[1].product_match_status == T.PRODUCT_MATCHED
    assert rows[1].customer_match_status == T.CUSTOMER_MATCHED


def test_enrich_orders_output_columns(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1"}], dims)
    required = {"row_id", "order_id", "order_date", "ship_date", "order_year", "ship_mode", "customer_id",
                "customer_name", "country", "product_id", "category", "sub_category", "quantity", "price",
                "discount", "profit"}
    assert required.issubset(out.columns)


def test_duplicate_product_in_master_does_not_fan_out(spark, dims):
    # FUR-CH-1 is listed twice in the raw product file
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-1", "product_id": "FUR-CH-1", "profit": 5.0}], dims)
    assert out.count() == 1
    assert out.agg(F.sum("profit")).first()[0] == Decimal("5.00")


def test_missing_product_category_is_inferred_from_product_code(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-1", "product_id": "OFF-PA-99999"}], dims)
    row = out.first()
    assert (row.category, row.sub_category) == ("Office Supplies", "Paper")
    assert row.product_match_status == T.PRODUCT_INFERRED


def test_unknown_product_kept_with_unknown_category(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-1", "product_id": "ZZZ-QQ-1", "profit": 7.5}], dims)
    row = out.first()
    assert (row.category, row.sub_category, row.product_match_status) == (T.UNKNOWN, T.UNKNOWN, T.PRODUCT_UNMATCHED)
    assert row.profit == Decimal("7.50")    # order (and its profit) is not dropped


def test_unknown_customer_kept_with_unknown_name(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "NOPE-1", "product_id": "FUR-CH-1"}], dims)
    row = out.first()
    assert (row.customer_name, row.country, row.customer_match_status) == (T.UNKNOWN, T.UNKNOWN, T.CUSTOMER_UNMATCHED)


def test_customer_with_null_name_gets_unknown(spark, dims):
    out, _ = _enrich(spark, [{"row_id": 1, "customer_id": "C-3", "product_id": "FUR-CH-1"}], dims)
    row = out.first()
    assert row.customer_name == T.UNKNOWN and row.customer_match_status == T.CUSTOMER_MATCHED


def test_row_count_and_profit_preserved(spark, dims):
    rows = [
        {"row_id": i, "customer_id": c, "product_id": p, "profit": pr}
        for i, (c, p, pr) in enumerate(
            [("C-1", "FUR-CH-1", 1.11), ("C-2", "OFF-PA-1", -2.22), ("NOPE", "ZZZ-QQ-1", 3.33),
             ("C-3", "TEC-PH-1", 4.44), ("C-1", "OFF-PA-7", 5.55)], start=1)
    ]
    out, rejected = _enrich(spark, rows, dims)
    assert out.count() == len(rows) and rejected.count() == 0
    assert out.agg(F.sum("profit")).first()[0] == Decimal("12.21")
