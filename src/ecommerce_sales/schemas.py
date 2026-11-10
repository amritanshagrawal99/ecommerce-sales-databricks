"""Explicit source schemas.

Schemas are declared up-front instead of inferred: inference costs an extra pass
over the data, silently changes types when the source drifts, and (for the Excel
file) mixes ints and strings in the same column (e.g. phone).
Field names match the source exactly; they are standardised to snake_case on ingest.
"""

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Customer.xlsx - everything is read as string in the raw layer. Excel stores
# phone numbers as int or text and drops leading zeros of postal codes, so typing
# is deferred to the enriched layer where it is done deliberately.
CUSTOMER_SOURCE_COLUMNS = [
    "Customer ID",
    "Customer Name",
    "email",
    "phone",
    "address",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region",
]
CUSTOMER_RAW_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in CUSTOMER_SOURCE_COLUMNS]
)

PRODUCT_RAW_SCHEMA = StructType(
    [
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", DoubleType(), True),
    ]
)

# Dates arrive as d/M/yyyy strings ("6/10/2016") and are parsed in the enriched layer.
ORDER_RAW_SCHEMA = StructType(
    [
        StructField("Row ID", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", LongType(), True),
        StructField("Price", DoubleType(), True),
        StructField("Discount", DoubleType(), True),
        StructField("Profit", DoubleType(), True),
    ]
)
