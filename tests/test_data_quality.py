"""Data-quality checks and error handling."""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecommerce_sales import data_quality as dq


@pytest.fixture
def df(spark):
    return spark.createDataFrame(
        [(1, "a", Decimal("1.00")), (2, None, Decimal("2.50")), (2, "c", Decimal("3.00"))],
        "id int, name string, amount decimal(18,2)",
    )


def test_check_not_null(df):
    assert dq.check_not_null(df, ["id"], "t").passed
    r = dq.check_not_null(df, ["id", "name"], "t")
    assert not r.passed and "name" in r.details


def test_check_unique(df):
    assert dq.check_unique(df, ["name"], "t").passed
    assert not dq.check_unique(df, ["id"], "t").passed
    assert dq.check_unique(df, ["id", "name"], "t").passed


def test_check_not_empty(spark, df):
    assert dq.check_not_empty(df, "t").passed
    assert not dq.check_not_empty(df.filter("1 = 0"), "t").passed


def test_check_row_count_equal(df):
    assert dq.check_row_count_equal(3, df, "t").passed
    assert not dq.check_row_count_equal(2, df, "t").passed


def test_check_sum_equal(df):
    assert dq.check_sum_equal(df, "amount", df, "amount", "t").passed
    other = df.withColumn("amount", F.col("amount") + F.lit(Decimal("0.01")))
    r = dq.check_sum_equal(df, "amount", other, "amount", "t")
    assert not r.passed and "0.03" in r.details


def test_check_max_ratio(df):
    assert dq.check_max_ratio(df, F.col("name").isNull(), 3, 0.5, "t").passed
    assert not dq.check_max_ratio(df, F.col("name").isNull(), 3, 0.1, "t").passed


def test_enforce_raises_only_on_errors():
    ok = dq.CheckResult("a", True, "")
    warn = dq.CheckResult("b", False, "x", dq.WARNING)
    assert dq.enforce([ok, warn]) == [ok, warn]
    with pytest.raises(dq.DataQualityError, match="c"):
        dq.enforce([ok, warn, dq.CheckResult("c", False, "boom", dq.ERROR)], stage="s")
