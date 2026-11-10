"""Pipeline orchestration: raw -> enriched -> aggregate (+ SQL outputs).

Each stage reads its inputs from the sink (i.e. from the upstream tables), so
stages can be run independently as separate Databricks notebooks / job tasks
and re-run idempotently (every write is a full overwrite).
"""

from __future__ import annotations

import logging
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import data_quality as dq
from . import ingestion, sql_queries
from . import transformations as T
from .config import PipelineConfig
from .spark_utils import try_cache, try_unpersist

log = logging.getLogger("ecommerce_sales.pipeline")


# --------------------------------------------------------------------------- #
# 1. raw
# --------------------------------------------------------------------------- #
def run_raw(spark: SparkSession, cfg: PipelineConfig, sink) -> Dict[str, DataFrame]:
    sink.ensure_schemas(cfg.schemas)
    sources = {
        cfg.raw_customers: ingestion.read_customers_raw(spark, cfg.source(cfg.files.customers)),
        cfg.raw_products: ingestion.read_products_raw(spark, cfg.source(cfg.files.products)),
        cfg.raw_orders: ingestion.read_orders_raw(spark, cfg.source(cfg.files.orders)),
    }
    out = {}
    for table, df in sources.items():
        written = sink.write(df, table, comment=f"Raw copy of {df.select('_source_file').first()[0]}")
        dq.enforce([dq.check_not_empty(written, table)], stage="raw")
        out[table] = written
    return out


# --------------------------------------------------------------------------- #
# 2. enriched customers + products
# --------------------------------------------------------------------------- #
def run_enriched_dimensions(spark: SparkSession, cfg: PipelineConfig, sink) -> Dict[str, DataFrame]:
    raw_products = sink.read(cfg.raw_products)

    customers = sink.write(
        T.enrich_customers(sink.read(cfg.raw_customers)),
        cfg.enriched_customers,
        comment="Cleaned customers, one row per customer_id",
    )
    products = sink.write(
        T.enrich_products(raw_products),
        cfg.enriched_products,
        comment="Cleaned products, one row per product_id",
    )

    # does any duplicated product_id carry conflicting category / sub-category?
    conflicting = (
        raw_products.groupBy(F.upper(F.trim("product_id")).alias("product_id"))
        .agg(F.countDistinct("category", "sub_category").alias("n"))
        .filter(F.col("n") > 1)
    )
    dq.enforce(
        [
            dq.check_not_empty(customers, cfg.enriched_customers),
            dq.check_unique(customers, ["customer_id"], cfg.enriched_customers),
            dq.check_not_null(customers, ["customer_id", "customer_name", "country"], cfg.enriched_customers),
            dq.check_not_empty(products, cfg.enriched_products),
            dq.check_unique(products, ["product_id"], cfg.enriched_products),
            dq.check_not_null(products, ["product_id", "category", "sub_category"], cfg.enriched_products),
            dq.check_row_count_equal(0, conflicting, f"{cfg.raw_products}.conflicting_category", dq.WARNING),
        ],
        stage="enriched_dimensions",
    )
    return {cfg.enriched_customers: customers, cfg.enriched_products: products}


# --------------------------------------------------------------------------- #
# 3. enriched orders (master table)
# --------------------------------------------------------------------------- #
def run_enriched_orders(spark: SparkSession, cfg: PipelineConfig, sink) -> Dict[str, DataFrame]:
    raw_orders = sink.read(cfg.raw_orders)
    valid, rejected = T.validate_orders(raw_orders)
    valid = try_cache(valid)
    valid_count = valid.count()
    raw_count = raw_orders.count()

    orders = sink.write(
        T.enrich_orders(valid, sink.read(cfg.enriched_customers), sink.read(cfg.enriched_products)),
        cfg.enriched_orders,
        comment="Order master: order facts + customer name/country + product category/sub-category",
    )
    rejected = sink.write(rejected, cfg.rejected_orders, comment="Orders rejected by validation")

    dq.enforce(
        [
            # a left join that changes the row count means the join fanned out
            dq.check_row_count_equal(valid_count, orders, f"{cfg.enriched_orders}.no_join_fanout"),
            dq.check_unique(orders, ["row_id"], cfg.enriched_orders),
            dq.check_not_null(
                orders,
                ["order_id", "order_date", "order_year", "customer_name", "country",
                 "category", "sub_category", "profit"],
                cfg.enriched_orders,
            ),
            dq.check_sum_equal(
                valid.select(T.round_money(F.col("profit")).alias("profit")), "profit",
                orders, "profit", cfg.enriched_orders,
            ),
            dq.check_max_ratio(rejected, F.lit(True), raw_count, 0.01, cfg.rejected_orders),
            dq.check_max_ratio(
                orders, F.col("product_match_status") == T.PRODUCT_UNMATCHED,
                valid_count, 0.0, f"{cfg.enriched_orders}.unmatched_products",
            ),
            dq.check_max_ratio(
                orders, F.col("product_match_status") == T.PRODUCT_INFERRED,
                valid_count, 0.05, f"{cfg.enriched_orders}.inferred_products",
            ),
            dq.check_max_ratio(
                orders, F.col("customer_match_status") == T.CUSTOMER_UNMATCHED,
                valid_count, 0.0, f"{cfg.enriched_orders}.unmatched_customers",
            ),
        ],
        stage="enriched_orders",
    )
    try_unpersist(valid)
    return {cfg.enriched_orders: orders, cfg.rejected_orders: rejected}


# --------------------------------------------------------------------------- #
# 4. aggregate
# --------------------------------------------------------------------------- #
def run_aggregate(spark: SparkSession, cfg: PipelineConfig, sink) -> Dict[str, DataFrame]:
    orders = sink.read(cfg.enriched_orders)
    agg = sink.write(
        T.build_profit_aggregate(orders),
        cfg.profit_aggregate,
        comment="Profit by year, product category, product sub-category and customer",
    )
    dq.enforce(
        [
            dq.check_unique(agg, T.AGGREGATE_KEYS, cfg.profit_aggregate),
            dq.check_sum_equal(orders, "profit", agg, "total_profit", cfg.profit_aggregate),
        ],
        stage="aggregate",
    )
    return {cfg.profit_aggregate: agg}


# --------------------------------------------------------------------------- #
# 5. SQL outputs
# --------------------------------------------------------------------------- #
def run_sql_outputs(spark: SparkSession, aggregate_table: str) -> Dict[str, DataFrame]:
    return {name: spark.sql(sql_queries.render(name, aggregate_table)) for name in sql_queries.QUERIES}


def run_all(spark: SparkSession, cfg: PipelineConfig, sink) -> Dict[str, DataFrame]:
    out: Dict[str, DataFrame] = {}
    for stage in (run_raw, run_enriched_dimensions, run_enriched_orders, run_aggregate):
        log.info("running %s", stage.__name__)
        out.update(stage(spark, cfg, sink))
    return out
