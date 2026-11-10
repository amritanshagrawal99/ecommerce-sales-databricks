"""End-to-end run of the whole pipeline on the real source files (in-memory sink).

These tests pin the reconciliation facts discovered while profiling the data,
so a regression in any stage (e.g. a join fan-out) fails loudly.
"""

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from ecommerce_sales import pipeline, transformations as T
from ecommerce_sales.config import PipelineConfig
from ecommerce_sales.data_quality import DataQualityError
from ecommerce_sales.sinks import MemorySink

pytestmark = pytest.mark.integration

SOURCE_TOTAL_PROFIT = Decimal("278417.03")   # sum of Profit in Orders.json
SOURCE_ORDER_ROWS = 9994


@pytest.fixture(scope="module")
def run(spark, data_dir):
    if not (data_dir / "Orders.json").exists():
        pytest.skip("source data not downloaded")
    cfg = PipelineConfig(source_path=str(data_dir))
    sink = MemorySink(spark)
    tables = pipeline.run_all(spark, cfg, sink)
    return cfg, sink, tables


def test_raw_tables_have_all_source_rows(run):
    cfg, _, t = run
    assert t[cfg.raw_customers].count() == 793
    assert t[cfg.raw_products].count() == 1851
    assert t[cfg.raw_orders].count() == SOURCE_ORDER_ROWS


def test_enriched_dimensions_are_unique(run):
    cfg, _, t = run
    assert t[cfg.enriched_customers].count() == 793
    assert t[cfg.enriched_products].count() == 1818          # 1851 rows - 33 duplicate ids


def test_customer_names_are_clean(run):
    cfg, _, t = run
    dirty = t[cfg.enriched_customers].filter(~F.col("customer_name").rlike(r"^\p{L}[\p{L} '\-]*$"))
    assert dirty.count() == 0
    assert t[cfg.enriched_customers].filter(F.col("customer_name") == T.UNKNOWN).count() == 8


def test_enriched_orders_reconcile_with_source(run):
    cfg, _, t = run
    orders = t[cfg.enriched_orders]
    assert orders.count() == SOURCE_ORDER_ROWS
    assert t[cfg.rejected_orders].count() == 0
    assert orders.agg(F.sum("profit")).first()[0] == SOURCE_TOTAL_PROFIT


def test_every_order_has_category_and_customer(run):
    cfg, _, t = run
    orders = t[cfg.enriched_orders]
    assert orders.filter((F.col("category") == T.UNKNOWN) | (F.col("sub_category") == T.UNKNOWN)).count() == 0
    status = {r.product_match_status: r["count"] for r in orders.groupBy("product_match_status").count().collect()}
    assert status == {T.PRODUCT_MATCHED: 9790, T.PRODUCT_INFERRED: 204}
    assert orders.filter(F.col("customer_match_status") != T.CUSTOMER_MATCHED).count() == 0


def test_years_present(run):
    cfg, _, t = run
    years = sorted(r.order_year for r in t[cfg.enriched_orders].select("order_year").distinct().collect())
    assert years == [2014, 2015, 2016, 2017]


def test_aggregate_reconciles(run):
    cfg, _, t = run
    assert t[cfg.profit_aggregate].agg(F.sum("total_profit")).first()[0] == SOURCE_TOTAL_PROFIT


def test_sql_outputs_reconcile(spark, run):
    cfg, sink, _ = run
    outputs = pipeline.run_sql_outputs(spark, sink.view_name(cfg.profit_aggregate))
    for name, df in outputs.items():
        assert df.agg(F.sum("total_profit")).first()[0] == SOURCE_TOTAL_PROFIT, name
    assert outputs["profit_by_year"].count() == 4
    assert outputs["profit_by_customer"].count() == 793


def test_pipeline_fails_fast_on_missing_source(spark, tmp_path):
    from ecommerce_sales.ingestion import IngestionError

    with pytest.raises(IngestionError):
        pipeline.run_raw(spark, PipelineConfig(source_path=str(tmp_path / "empty")), MemorySink(spark))


def test_pipeline_stops_on_dq_error(spark, run):
    """A fan-out (duplicated customer dimension) must be caught by the DQ gate."""
    cfg, sink, t = run
    bad_sink = MemorySink(spark)
    bad_sink.write(t[cfg.raw_orders], cfg.raw_orders)
    bad_sink.write(t[cfg.enriched_products], cfg.enriched_products)
    customers = t[cfg.enriched_customers]
    bad_sink.write(customers.unionByName(customers.limit(5)), cfg.enriched_customers)
    with pytest.raises(DataQualityError, match="no_join_fanout"):
        pipeline.run_enriched_orders(spark, cfg, bad_sink)
