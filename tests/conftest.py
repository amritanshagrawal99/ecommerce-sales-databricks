import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

# make the Spark python workers use the same interpreter as pytest
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from ecommerce_sales.schemas import ORDER_RAW_SCHEMA  # noqa: E402

INGESTED_AT = datetime(2026, 1, 1, 0, 0, 0)


ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def spark():
    if ON_DATABRICKS:  # re-use the cluster session, never stop it
        yield SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-sales-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# On Databricks (serverless) Spark cannot read the driver's local disk, so test
# files and source data live in a Unity Catalog volume instead.
DBX_VOLUME = os.environ.get("ECOM_TEST_VOLUME", "/Volumes/workspace/default/ecommerce_landing")


@pytest.fixture(scope="session")
def data_dir():
    return Path(DBX_VOLUME) if ON_DATABRICKS else ROOT / "data" / "raw"


if ON_DATABRICKS:
    import shutil
    import uuid

    @pytest.fixture
    def tmp_path():  # noqa: F811 - overrides pytest's builtin on Databricks only
        p = Path(DBX_VOLUME) / "_pytest_tmp" / uuid.uuid4().hex
        p.mkdir(parents=True, exist_ok=True)
        yield p
        shutil.rmtree(p, ignore_errors=True)


# --------------------------------------------------------------------------- #
# builders for raw-layer DataFrames (shape = output of ingestion.read_*_raw)
# --------------------------------------------------------------------------- #
CUSTOMER_COLS = [
    "customer_id", "customer_name", "email", "phone", "address", "segment",
    "country", "city", "state", "postal_code", "region",
]


def make_raw_customers(spark, rows):
    """rows: list of dicts; missing keys default to sensible values."""
    defaults = dict(
        email="a@b.com", phone="7185624866", address="1 Main St", segment="Consumer",
        country="United States", city="Austin", state="Texas", postal_code="78701", region="Central",
    )
    data = [tuple({**defaults, **r}.get(c) for c in CUSTOMER_COLS) + ("Customer.xlsx", INGESTED_AT) for r in rows]
    schema = ", ".join(f"{c} string" for c in CUSTOMER_COLS) + ", _source_file string, _ingested_at timestamp"
    return spark.createDataFrame(data, schema)


PRODUCT_COLS = ["product_id", "category", "sub_category", "product_name", "state", "price_per_product"]


def make_raw_products(spark, rows):
    defaults = dict(product_name="Thing", state="Texas", price_per_product=1.0)
    data = [tuple({**defaults, **r}.get(c) for c in PRODUCT_COLS) + (None, "Products.csv", INGESTED_AT) for r in rows]
    schema = (
        "product_id string, category string, sub_category string, product_name string, state string, "
        "price_per_product double, _corrupt_record string, _source_file string, _ingested_at timestamp"
    )
    return spark.createDataFrame(data, schema)


ORDER_COLS = [
    "row_id", "order_id", "order_date", "ship_date", "ship_mode", "customer_id",
    "product_id", "quantity", "price", "discount", "profit",
]


def make_raw_orders(spark, rows):
    defaults = dict(
        order_id="CA-2016-000001", order_date="1/1/2016", ship_date="3/1/2016",
        ship_mode="Standard Class", quantity=1, price=10.0, discount=0.0, profit=1.0,
    )
    data = [
        tuple({**defaults, **r}.get(c) for c in ORDER_COLS)
        + (r.get("_corrupt_record"), "Orders.json", r.get("_ingested_at", INGESTED_AT))
        for r in rows
    ]
    schema = (
        "row_id long, order_id string, order_date string, ship_date string, ship_mode string, "
        "customer_id string, product_id string, quantity long, price double, discount double, "
        "profit double, _corrupt_record string, _source_file string, _ingested_at timestamp"
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def raw_customers(spark):
    return make_raw_customers(
        spark,
        [
            {"customer_id": "C-1", "customer_name": "Gary567 Hansen", "country": "United States"},
            {"customer_id": "C-2", "customer_name": "Mary O'Rourke", "country": "Canada"},
            {"customer_id": "C-3", "customer_name": None, "country": "United States"},
        ],
    )


@pytest.fixture
def raw_products(spark):
    return make_raw_products(
        spark,
        [
            {"product_id": "FUR-CH-1", "category": "Furniture", "sub_category": "Chairs"},
            # duplicated product id (as in the real data) - must not fan out the join
            {"product_id": "FUR-CH-1", "category": "Furniture", "sub_category": "Chairs", "state": "Ohio"},
            {"product_id": "OFF-PA-1", "category": "Office Supplies", "sub_category": "Paper"},
            {"product_id": "TEC-PH-1", "category": "Technology", "sub_category": "Phones"},
        ],
    )


def collect_dicts(df, *cols, order_by=None):
    cols = cols or df.columns
    df = df.select(*cols)
    if order_by:
        df = df.orderBy(*order_by)
    return [r.asDict() for r in df.collect()]
