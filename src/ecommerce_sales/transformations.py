"""Enriched + aggregate layer transformations (pure PySpark, no SQL).

Every public function takes DataFrames and returns DataFrames, with no I/O,
so each one can be unit tested in isolation.
"""

from __future__ import annotations

from typing import Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType

from .ingestion import CORRUPT_RECORD_COL

UNKNOWN = "Unknown"
DATE_FORMAT = "d/M/yyyy"  # source dates look like 6/10/2016 and 21/8/2016
MONEY = DecimalType(18, 2)

# product_match_status values
PRODUCT_MATCHED = "matched"
PRODUCT_INFERRED = "inferred_from_product_id"
PRODUCT_UNMATCHED = "unmatched"

# customer_match_status values
CUSTOMER_MATCHED = "matched"
CUSTOMER_UNMATCHED = "unmatched"


# --------------------------------------------------------------------------- #
# generic column helpers
# --------------------------------------------------------------------------- #
def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim every string column and turn empty strings into nulls."""
    cols = []
    for field in df.schema.fields:
        name = field.name
        if isinstance(field.dataType, StringType) and name != CORRUPT_RECORD_COL:
            trimmed = F.trim(F.col(name))
            cols.append(F.when(trimmed == "", None).otherwise(trimmed).alias(name))
        else:
            cols.append(F.col(name))
    return df.select(*cols)


def collapse_whitespace(col: Column) -> Column:
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def round_money(col: Column, scale: int = 2) -> Column:
    """Round half-up to `scale` decimals and store as DECIMAL(18,2).

    Going through DECIMAL avoids binary floating point artefacts (e.g. 2.675
    becoming 2.67) and makes downstream SUMs exact.
    """
    return F.round(col.cast(DecimalType(38, 10)), scale).cast(MONEY)


# Look-alike characters that were substituted for letters inside names
# ('Bi1l' -> 'Bill', 'Wa55erman' -> 'Wasserman', 'N0ra' -> 'Nora', 'C@thy' -> 'Cathy').
LOOKALIKES = {"0": "o", "1": "l", "3": "e", "4": "a", "5": "s", "7": "t", "@": "a", "!": "i", "$": "s"}


def _restore_lookalikes(col: Column) -> Column:
    """Map a look-alike back to its letter only where it clearly replaced one:
    one character (or the same character twice) *between two letters*, the same
    character twice at the end of a word ('Ji11'), or one at the start of a word
    followed by a lower-case letter ('5chnelling').
    Runs of mixed digits/symbols ('Gary567', 'Willin0009gham', 'Brosin34a')
    are insertions, not substitutions, and are left for the noise filter."""
    for ch, letter in LOOKALIKES.items():
        e = "\\" + ch if ch in "$!@" else ch
        col = F.regexp_replace(col, rf"(?<=\p{{L}}){e}{e}(?=\p{{L}})", letter * 2)
        # doubled look-alike closing a word ('Ji11' -> 'Jill'); single ones are too ambiguous
        col = F.regexp_replace(col, rf"(?<=\p{{L}}){e}{e}(?![\p{{L}}\p{{N}}\p{{P}}\p{{S}}])", letter * 2)
        col = F.regexp_replace(col, rf"(?<=\p{{L}}){e}(?=\p{{L}})", letter)
        col = F.regexp_replace(col, rf"(?<![\p{{L}}\p{{N}}\p{{P}}\p{{S}}]){e}(?=\p{{Ll}})", letter.upper())
    return col


def clean_customer_name(col: Column) -> Column:
    """Remove noise injected into customer names.

    Source examples -> result:
        'Gary567 Hansen'            -> 'Gary Hansen'
        'Tam&^*ara Willing___)ham'  -> 'Tamara Willingham'
        "       Kristi;'[]na Nunn"  -> 'Kristina Nunn'
        'Helen Wa55erman'           -> 'Helen Wasserman'
        'Peter Bühler'              -> 'Peter Bühler'    (non-ASCII letters kept)
        "Mary O'Rourke"             -> "Mary O'Rourke"   (real apostrophe kept)
        'Corey-Lock'                -> 'Corey-Lock'      (real hyphen kept)
        'Jason Fortune-'            -> 'Jason Fortune'
    Rules:
      1. restore look-alike substitutions (see _restore_lookalikes)
      2. keep only letters (any script), spaces, apostrophes and hyphens
      3. keep an apostrophe only in the O'Rourke / D'Ascenzo pattern
         (single capital at word start, followed by a capital)
      4. keep a hyphen only between a letter and a capital (double-barrelled names)
      5. collapse repeated whitespace, trim; empty -> null
    Noise that included spaces ('Tho   12 =-.mas') leaves a split word that
    cannot be told apart from a real two-part name, so it is not re-joined.
    """
    c = _restore_lookalikes(col)
    c = F.regexp_replace(c, r"[^\p{L} '\-]", "")
    c = F.regexp_replace(c, r"(?<!\b\p{Lu})'|'(?!\p{Lu})", "")
    c = F.regexp_replace(c, r"(?<!\p{L})-|-(?!\p{Lu})", "")
    c = collapse_whitespace(c)
    return F.when(c == "", None).otherwise(c)


def clean_phone(col: Column) -> Column:
    """Normalise US phone numbers to 10 digits, null when not a valid number.

    '421.580.0902x9815' -> '4215800902'   (extension dropped)
    '001-542-415-0246'  -> '5424150246'   (international 001 prefix dropped)
    '(563)647-4830'     -> '5636474830'
    '-6181'             -> null
    """
    no_ext = F.regexp_replace(col, r"(?i)\s*(x|ext\.?)\s*\d+\s*$", "")
    digits = F.regexp_replace(no_ext, r"[^0-9]", "")
    digits = (
        F.when((F.length(digits) == 13) & digits.startswith("001"), F.substring(digits, 4, 10))
        .when((F.length(digits) == 11) & digits.startswith("1"), F.substring(digits, 2, 10))
        .otherwise(digits)
    )
    return F.when(F.length(digits) == 10, digits).otherwise(F.lit(None).cast("string"))


def clean_postal_code(col: Column) -> Column:
    """Excel drops leading zeros of US ZIP codes (e.g. 2920 -> 02920). Re-pad to 5 digits."""
    c = F.regexp_replace(F.trim(col), r"\.0+$", "")
    return F.when(c.rlike(r"^\d{1,5}$"), F.lpad(c, 5, "0")).otherwise(F.lit(None).cast("string"))


def _keep_first(df: DataFrame, keys: list[str], order_cols: list[Column]) -> DataFrame:
    """Deterministic de-duplication: keep the first row per key by `order_cols`."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# --------------------------------------------------------------------------- #
# customers
# --------------------------------------------------------------------------- #
def enrich_customers(raw_customers: DataFrame) -> DataFrame:
    """Clean customers to one row per customer_id."""
    df = trim_string_columns(raw_customers)
    df = df.withColumn("customer_id", F.upper(F.col("customer_id"))).filter(
        F.col("customer_id").isNotNull()
    )
    cleaned_name = clean_customer_name(F.col("customer_name"))
    df = df.select(
        "customer_id",
        F.coalesce(cleaned_name, F.lit(UNKNOWN)).alias("customer_name"),
        F.col("customer_name").alias("customer_name_raw"),
        F.lower(F.col("email")).alias("email"),
        clean_phone(F.col("phone")).alias("phone"),
        collapse_whitespace(F.regexp_replace(F.col("address"), r"[\r\n]+", ", ")).alias("address"),
        "segment",
        F.coalesce(F.col("country"), F.lit(UNKNOWN)).alias("country"),
        "city",
        "state",
        clean_postal_code(F.col("postal_code")).alias("postal_code"),
        "region",
        "_ingested_at",
    )
    df = _keep_first(df, ["customer_id"], [F.col("_ingested_at").desc(), F.col("customer_name")])
    return df.drop("_ingested_at").withColumn("_processed_at", F.current_timestamp())


# --------------------------------------------------------------------------- #
# products
# --------------------------------------------------------------------------- #
def product_code(col: Column) -> Column:
    """'OFF-PA-10003349' -> 'OFF-PA' (category + sub-category code)."""
    return F.regexp_extract(col, r"^([A-Z]+-[A-Z]+)-", 1)


def enrich_products(raw_products: DataFrame) -> DataFrame:
    """Clean products to one row per product_id.

    The source has 33 product_ids listed more than once (different state /
    name / price). Category and sub-category are identical across these
    duplicates, so de-duplicating loses nothing needed downstream and, more
    importantly, prevents the orders join from fanning out and double counting profit.
    """
    df = raw_products
    if CORRUPT_RECORD_COL in df.columns:
        df = df.filter(F.col(CORRUPT_RECORD_COL).isNull()).drop(CORRUPT_RECORD_COL)
    df = trim_string_columns(df)
    df = df.withColumn("product_id", F.upper(F.col("product_id"))).filter(
        F.col("product_id").isNotNull()
    )
    df = df.withColumn(
        "product_name",
        collapse_whitespace(F.regexp_replace(F.col("product_name"), "[ �]", " ")),
    )
    counts = df.groupBy("product_id").agg(F.count(F.lit(1)).alias("source_record_count"))
    df = _keep_first(
        df,
        ["product_id"],
        [F.col("product_name").asc_nulls_last(), F.col("state").asc_nulls_last()],
    )
    return (
        df.join(counts, "product_id")
        .select(
            "product_id",
            "product_name",
            F.coalesce("category", F.lit(UNKNOWN)).alias("category"),
            F.coalesce("sub_category", F.lit(UNKNOWN)).alias("sub_category"),
            "state",
            "price_per_product",
            "source_record_count",
        )
        .withColumn("_processed_at", F.current_timestamp())
    )


def product_code_mapping(enriched_products: DataFrame) -> DataFrame:
    """Map product code ('OFF-PA') -> (category, sub_category), learned from the
    product master. Only codes that map to exactly one pair are kept, so the
    fallback never guesses between alternatives."""
    pairs = (
        enriched_products.filter(F.col("category") != UNKNOWN)
        .select(product_code(F.col("product_id")).alias("product_code"), "category", "sub_category")
        .filter(F.col("product_code") != "")
        .distinct()
    )
    unambiguous = pairs.groupBy("product_code").count().filter(F.col("count") == 1).drop("count")
    return pairs.join(unambiguous, "product_code")


# --------------------------------------------------------------------------- #
# orders
# --------------------------------------------------------------------------- #
def parse_date(col: Column, fmt: str = DATE_FORMAT) -> Column:
    """Lenient date parsing: invalid strings -> null (also under ANSI mode)."""
    return F.to_date(F.try_to_timestamp(col, F.lit(fmt)))


def standardize_orders(raw_orders: DataFrame) -> DataFrame:
    df = trim_string_columns(raw_orders)
    if CORRUPT_RECORD_COL not in df.columns:
        df = df.withColumn(CORRUPT_RECORD_COL, F.lit(None).cast("string"))
    return (
        df.withColumn("customer_id", F.upper("customer_id"))
        .withColumn("product_id", F.upper("product_id"))
        .withColumn("order_date_raw", F.col("order_date"))
        .withColumn("order_date", parse_date(F.col("order_date")))
        .withColumn("ship_date", parse_date(F.col("ship_date")))
    )


def validate_orders(raw_orders: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Split orders into (valid, rejected). Rejected rows carry `reject_reason`
    (comma separated list) and are written to a quarantine table instead of
    being silently dropped. Duplicate row_ids are also rejected."""
    df = standardize_orders(raw_orders)
    rules = [
        (F.col(CORRUPT_RECORD_COL).isNotNull(), "corrupt_record"),
        (F.col("row_id").isNull(), "missing_row_id"),
        (F.col("order_id").isNull(), "missing_order_id"),
        (F.col("customer_id").isNull(), "missing_customer_id"),
        (F.col("product_id").isNull(), "missing_product_id"),
        (F.col("order_date").isNull(), "invalid_order_date"),
        (F.col("profit").isNull(), "missing_profit"),
        (F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
        (
            F.col("discount").isNotNull() & ((F.col("discount") < 0) | (F.col("discount") > 1)),
            "invalid_discount",
        ),
    ]
    reasons = F.concat_ws(",", *[F.when(cond, F.lit(label)) for cond, label in rules])
    df = df.withColumn("reject_reason", reasons)

    # duplicates: same row_id seen more than once among otherwise-valid rows ->
    # keep the first (latest ingested), reject the rest
    w = Window.partitionBy(F.col("row_id"), F.col("reject_reason") == "").orderBy(
        F.col("_ingested_at").desc(), F.col("order_id")
    )
    df = df.withColumn(
        "reject_reason",
        F.when(
            (F.col("reject_reason") == "")
            & F.col("row_id").isNotNull()
            & (F.row_number().over(w) > 1),
            F.lit("duplicate_row_id"),
        ).otherwise(F.col("reject_reason")),
    )
    valid = df.filter(F.col("reject_reason") == "").drop("reject_reason", CORRUPT_RECORD_COL)
    rejected = df.filter(F.col("reject_reason") != "").withColumn(
        "_rejected_at", F.current_timestamp()
    )
    return valid, rejected


def enrich_orders(
    valid_orders: DataFrame,
    enriched_customers: DataFrame,
    enriched_products: DataFrame,
) -> DataFrame:
    """Order master table: order facts + customer name/country + product category/sub-category.

    * left joins - an order is never dropped because its dimension row is missing
      (profit totals must reconcile with the source)
    * dimensions are small -> broadcast joins (no shuffle of the fact table)
    * products missing from the product master get category/sub-category inferred
      from the product-id code (e.g. 'OFF-EN-...' -> Office Supplies / Envelopes);
      `product_match_status` records which path was used
    """
    customers = enriched_customers.select(
        "customer_id",
        F.col("customer_name").alias("_c_name"),
        F.col("country").alias("_c_country"),
    )
    products = enriched_products.select(
        "product_id",
        F.col("category").alias("_p_category"),
        F.col("sub_category").alias("_p_sub_category"),
    )
    code_map = product_code_mapping(enriched_products).select(
        F.col("product_code").alias("_product_code"),
        F.col("category").alias("_m_category"),
        F.col("sub_category").alias("_m_sub_category"),
    )

    df = (
        valid_orders.withColumn("_product_code", product_code(F.col("product_id")))
        .join(F.broadcast(customers), "customer_id", "left")
        .join(F.broadcast(products), "product_id", "left")
        .join(F.broadcast(code_map), "_product_code", "left")
    )

    product_status = (
        F.when(F.col("_p_category").isNotNull(), F.lit(PRODUCT_MATCHED))
        .when(F.col("_m_category").isNotNull(), F.lit(PRODUCT_INFERRED))
        .otherwise(F.lit(PRODUCT_UNMATCHED))
    )
    customer_status = F.when(F.col("_c_name").isNotNull(), F.lit(CUSTOMER_MATCHED)).otherwise(
        F.lit(CUSTOMER_UNMATCHED)
    )

    return df.select(
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        F.year("order_date").alias("order_year"),
        "ship_mode",
        "customer_id",
        F.coalesce("_c_name", F.lit(UNKNOWN)).alias("customer_name"),
        F.coalesce("_c_country", F.lit(UNKNOWN)).alias("country"),
        "product_id",
        F.coalesce("_p_category", "_m_category", F.lit(UNKNOWN)).alias("category"),
        F.coalesce("_p_sub_category", "_m_sub_category", F.lit(UNKNOWN)).alias("sub_category"),
        "quantity",
        "price",
        "discount",
        round_money(F.col("profit")).alias("profit"),
        product_status.alias("product_match_status"),
        customer_status.alias("customer_match_status"),
        F.current_timestamp().alias("_processed_at"),
    )


# --------------------------------------------------------------------------- #
# aggregate
# --------------------------------------------------------------------------- #
AGGREGATE_KEYS = ["order_year", "category", "sub_category", "customer_id", "customer_name"]


def build_profit_aggregate(enriched_orders: DataFrame) -> DataFrame:
    """Profit by Year x Product Category x Product Sub-Category x Customer.

    customer_id is part of the grain (names are not unique - e.g. several
    'Unknown'); customer_name is carried for readability. Coarser aggregates
    (by year, by customer, ...) are roll-ups of this table (see sql_queries.py).
    """
    return (
        enriched_orders.groupBy(*AGGREGATE_KEYS)
        .agg(
            F.sum("profit").cast(MONEY).alias("total_profit"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
        )
        .withColumn("_processed_at", F.current_timestamp())
    )
