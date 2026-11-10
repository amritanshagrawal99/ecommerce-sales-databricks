"""Raw layer: readers, schema enforcement, column standardisation, error handling."""

import json
from pathlib import Path

import pandas as pd
import pytest

from ecommerce_sales import ingestion
from ecommerce_sales.ingestion import IngestionError, standardize_column_names, to_snake_case


@pytest.mark.parametrize(
    "source, expected",
    [
        ("Customer ID", "customer_id"),
        ("Sub-Category", "sub_category"),
        ("Price per product", "price_per_product"),
        ("  Postal Code ", "postal_code"),
        ("email", "email"),
        ("Row ID", "row_id"),
    ],
)
def test_to_snake_case(source, expected):
    assert to_snake_case(source) == expected


def test_standardize_column_names_keeps_metadata_columns(spark):
    df = spark.createDataFrame([(1, "x")], "`Order ID` int, `_corrupt_record` string")
    assert standardize_column_names(df).columns == ["order_id", "_corrupt_record"]


def test_to_local_path():
    assert ingestion.to_local_path("dbfs:/FileStore/a.xlsx") == "/dbfs/FileStore/a.xlsx"
    assert ingestion.to_local_path("/Volumes/c/s/v/a.xlsx") == "/Volumes/c/s/v/a.xlsx"


def test_to_local_path_file_uri(tmp_path):
    f = tmp_path / "my file.xlsx"
    assert Path(ingestion.to_local_path(f.as_uri())) == f


# --------------------------------------------------------------------------- #
# orders (JSON array, multi-line)
# --------------------------------------------------------------------------- #
def _write_orders(path, records):
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    return str(path)


def test_read_orders_raw_multiline_json(spark, tmp_path):
    path = _write_orders(
        tmp_path / "Orders.json",
        [
            {"Row ID": 1, "Order ID": "CA-2016-1", "Order Date": "21/8/2016", "Ship Date": "25/8/2016",
             "Ship Mode": "Standard Class", "Customer ID": "JK-1", "Product ID": "FUR-CH-1",
             "Quantity": 7, "Price": 573.17, "Discount": 0.3, "Profit": 63.69},
            {"Row ID": 2, "Order ID": "CA-2017-2", "Order Date": "6/10/2017", "Ship Date": "7/10/2017",
             "Ship Mode": "First Class", "Customer ID": "BD-1", "Product ID": "TEC-AC-1",
             "Quantity": 4, "Price": 291.96, "Discount": 0, "Profit": -14.92},
        ],
    )
    df = ingestion.read_orders_raw(spark, path)
    assert df.count() == 2
    for col in ["row_id", "order_id", "order_date", "customer_id", "product_id", "profit",
                "_corrupt_record", "_source_file", "_ingested_at"]:
        assert col in df.columns
    types = dict(df.dtypes)
    assert types["row_id"] == "bigint" and types["profit"] == "double" and types["order_date"] == "string"
    row = df.filter("row_id = 2").first()
    assert row.profit == -14.92 and row.discount == 0.0 and row._corrupt_record is None


def test_read_orders_raw_missing_field_becomes_null(spark, tmp_path):
    path = _write_orders(tmp_path / "Orders.json", [{"Row ID": 1, "Order ID": "X"}])
    row = ingestion.read_orders_raw(spark, path).first()
    assert row.profit is None and row.customer_id is None


def test_read_orders_raw_missing_file_raises(spark, tmp_path):
    with pytest.raises(IngestionError):
        ingestion.read_orders_raw(spark, str(tmp_path / "nope.json"))


# --------------------------------------------------------------------------- #
# products (CSV with quoted commas + non breaking spaces)
# --------------------------------------------------------------------------- #
def test_read_products_raw_handles_quotes_and_unicode(spark, tmp_path):
    p = tmp_path / "Products.csv"
    p.write_text(
        "Product ID,Category,Sub-Category,Product Name,State,Price per product\n"
        'FUR-CH-1,Furniture,Chairs,"Leather Task Chair, Black",New York,81.882\n'
        "TEC-AC-1,Technology,Accessories,Imation Secure+ USB,Oklahoma,72.99\n"
        'OFF-BI-1,Office Supplies,Binders,"13"" Binder",Colorado,4.25\n',
        encoding="utf-8",
    )
    df = ingestion.read_products_raw(spark, str(p))
    rows = {r.product_id: r for r in df.collect()}
    assert len(rows) == 3
    assert rows["FUR-CH-1"].product_name == "Leather Task Chair, Black"
    assert rows["FUR-CH-1"].price_per_product == pytest.approx(81.882)
    assert rows["TEC-AC-1"].product_name == "Imation Secure+ USB"
    assert rows["OFF-BI-1"].product_name == '13" Binder'
    assert "sub_category" in df.columns


def test_read_products_raw_missing_column_raises(spark, tmp_path):
    p = tmp_path / "Products.csv"
    p.write_text("Product ID,Category\nFUR-CH-1,Furniture\n", encoding="utf-8")
    with pytest.raises(IngestionError, match="Sub-Category"):
        ingestion.read_products_raw(spark, str(p))


def test_read_products_raw_missing_file_raises(spark, tmp_path):
    with pytest.raises(IngestionError):
        ingestion.read_products_raw(spark, str(tmp_path / "nope.csv"))


# --------------------------------------------------------------------------- #
# customers (Excel)
# --------------------------------------------------------------------------- #
def _customer_frame(**overrides):
    base = {
        "Customer ID": ["PW-19240", "KL-16555"],
        "Customer Name": ["Pierre Wener", None],
        "email": ["a@x.com", "b@x.com"],
        "phone": ["421.580.0902x9815", 7185624866],  # mixed str / int, as in the source
        "address": ["1 A St\nTown, FL 1", "2 B St"],
        "Segment": ["Consumer", "Corporate"],
        "Country": ["United States", "United States"],
        "City": ["Louisville", "Boston"],
        "State": ["Colorado", "Massachusetts"],
        "Postal Code": [80027, 2920],
        "Region": ["West", "East"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _write_xlsx(frame, path):
    """Build the workbook in memory, then write the bytes in one go. UC volume
    (FUSE) paths do not support the seeks the xlsx zip writer needs."""
    import io

    buf = io.BytesIO()
    frame.to_excel(buf, index=False)
    path.write_bytes(buf.getvalue())


def test_read_customers_raw_excel(spark, tmp_path):
    path = tmp_path / "Customer.xlsx"
    _write_xlsx(_customer_frame(), path)
    df = ingestion.read_customers_raw(spark, str(path))
    assert df.count() == 2
    assert all(t == "string" for c, t in df.dtypes if not c.startswith("_"))
    rows = {r.customer_id: r for r in df.collect()}
    assert rows["KL-16555"].phone == "7185624866"      # int preserved as text
    assert rows["KL-16555"].customer_name is None       # NaN -> null
    assert rows["KL-16555"].postal_code == "2920"       # raw keeps value as delivered


def test_read_customers_raw_missing_column_raises(spark, tmp_path):
    path = tmp_path / "Customer.xlsx"
    _write_xlsx(_customer_frame().drop(columns=["Country"]), path)
    with pytest.raises(IngestionError, match="Country"):
        ingestion.read_customers_raw(spark, str(path))


def test_read_customers_raw_missing_file_raises(spark, tmp_path):
    with pytest.raises(IngestionError, match="not found"):
        ingestion.read_customers_raw(spark, str(tmp_path / "nope.xlsx"))


def test_read_customers_raw_corrupt_file_raises(spark, tmp_path):
    path = tmp_path / "Customer.xlsx"
    path.write_bytes(b"this is not an excel file")
    with pytest.raises(IngestionError, match="Unable to read"):
        ingestion.read_customers_raw(spark, str(path))
