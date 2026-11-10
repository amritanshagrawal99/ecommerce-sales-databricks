"""Task 5 - the SQL outputs return the right roll-ups of the aggregate table."""

from decimal import Decimal

import pytest

from ecommerce_sales import sql_queries, transformations as T
from test_aggregate import enriched_orders  # noqa: F401  (fixture re-use)

VIEW = "test_profit_aggregate"


@pytest.fixture
def agg_view(spark, enriched_orders):  # noqa: F811
    T.build_profit_aggregate(enriched_orders).createOrReplaceTempView(VIEW)
    yield VIEW
    spark.catalog.dropTempView(VIEW)


def _run(spark, name):
    return [tuple(r) for r in spark.sql(sql_queries.render(name, VIEW)).collect()]


def test_profit_by_year(spark, agg_view):
    assert _run(spark, "profit_by_year") == [(2016, Decimal("145.55")), (2017, Decimal("1.10"))]


def test_profit_by_year_category(spark, agg_view):
    assert _run(spark, "profit_by_year_category") == [
        (2016, "Furniture", Decimal("-4.45")),
        (2016, "Technology", Decimal("150.00")),
        (2017, "Furniture", Decimal("1.10")),
    ]


def test_profit_by_customer(spark, agg_view):
    # ordered by profit desc; the two 'Unknown' customers stay separate
    assert _run(spark, "profit_by_customer") == [
        ("C-2", "Unknown", Decimal("100.00")),
        ("C-3", "Unknown", Decimal("50.00")),
        ("C-1", "Gary Hansen", Decimal("-3.35")),
    ]


def test_profit_by_customer_year(spark, agg_view):
    assert _run(spark, "profit_by_customer_year") == [
        ("C-1", "Gary Hansen", 2016, Decimal("-4.45")),
        ("C-1", "Gary Hansen", 2017, Decimal("1.10")),
        ("C-2", "Unknown", 2016, Decimal("100.00")),
        ("C-3", "Unknown", 2016, Decimal("50.00")),
    ]


def test_all_queries_reconcile_to_total(spark, agg_view):
    for name in sql_queries.QUERIES:
        total = sum(r[-1] for r in _run(spark, name))
        assert total == Decimal("146.65"), name


def test_sql_files_match_python_queries():
    """sql/*.sql (used by the %sql notebook) must not drift from sql_queries.py."""
    from pathlib import Path

    sql_dir = Path(__file__).resolve().parents[1] / "sql"
    for name, query in sql_queries.QUERIES.items():
        text = (sql_dir / f"{name}.sql").read_text(encoding="utf-8")
        body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--")).strip()
        expected = query.format(table="${aggregate_table}").strip()
        assert " ".join(body.rstrip(";").split()) == " ".join(expected.split()), name
