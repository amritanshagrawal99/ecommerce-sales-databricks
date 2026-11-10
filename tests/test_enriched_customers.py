"""Task 2 - enriched customers: name / phone / postal-code cleaning, dedup, nulls."""

import pytest
from pyspark.sql import functions as F

from conftest import make_raw_customers
from ecommerce_sales import transformations as T


def _apply(spark, fn, values):
    df = spark.createDataFrame([(v,) for v in values], "v string")
    return [r.out for r in df.select(fn(F.col("v")).alias("out")).collect()]


# Real dirty values taken from Customer.xlsx
@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Gary567 Hansen", "Gary Hansen"),
        ("Pete@#$ Takahito", "Pete Takahito"),
        ("Tam&^*ara Willing___)ham", "Tamara Willingham"),
        ("       Kristi;'[]na Nunn", "Kristina Nunn"),
        ("Mitch Willin0009gham", "Mitch Willingham"),
        ("John Hus<>><>ton", "John Huston"),
        (" &&Tracy Blumstein", "Tracy Blumstein"),
        ("Neil Knudson%^&*(", "Neil Knudson"),
        (")(*&Sung Pak", "Sung Pak"),
        ("Beth Tho098-.,;;mpson", "Beth Thompson"),
        ("[]-=;''Becky Pak", "Becky Pak"),
        ('":[]|}-=Charles Crestani', "Charles Crestani"),
        ("         =--Katharine Harms", "Katharine Harms"),
        ("Jason Fortune-", "Jason Fortune"),
        ("Brosin34a Hoffman 09", "Brosina Hoffman"),
        ("Rac5467hel Payne", "Rachel Payne"),
        ("5467Ben Wallace", "Ben Wallace"),
        ("Shahi  Hopkins", "Shahi Hopkins"),
        # look-alike substitutions are restored
        ("Bi1l Eplett", "Bill Eplett"),
        ("Ji11 Stevenson", "Jill Stevenson"),
        ("Helen Wa55erman", "Helen Wasserman"),
        ("N0ra Paige", "Nora Paige"),
        ("C@thy Armstrong", "Cathy Armstrong"),
        ("Karen Dan!els", "Karen Daniels"),
        ("Maribeth 5chnelling", "Maribeth Schnelling"),
        # non-ASCII letters are real letters
        ("Peter Bühler", "Peter Bühler"),
        ("Roy Französisch", "Roy Französisch"),
        # legitimate punctuation must survive
        ("Mary O'Rourke", "Mary O'Rourke"),
        ("Russell D'Ascenzo", "Russell D'Ascenzo"),
        ("Corey-Lock", "Corey-Lock"),
        ("Pierre Wener", "Pierre Wener"),
        # nothing left / missing
        ("1234 !!", None),
        ("", None),
        (None, None),
    ],
)
def test_clean_customer_name(spark, raw, expected):
    assert _apply(spark, T.clean_customer_name, [raw]) == [expected]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("421.580.0902x9815", "4215800902"),
        ("001-542-415-0246x314", "5424150246"),
        ("(563)647-4830x5318", "5636474830"),
        ("7185624866", "7185624866"),
        ("1-718-562-4866", "7185624866"),
        ("786.638.6820", "7866386820"),
        ("-6181", None),
        ("abc", None),
        (None, None),
    ],
)
def test_clean_phone(spark, raw, expected):
    assert _apply(spark, T.clean_phone, [raw]) == [expected]


@pytest.mark.parametrize(
    "raw, expected",
    [("80027", "80027"), ("2920", "02920"), ("2920.0", "02920"), (" 501 ", "00501"), ("ABCDE", None), (None, None)],
)
def test_clean_postal_code(spark, raw, expected):
    assert _apply(spark, T.clean_postal_code, [raw]) == [expected]


def test_enrich_customers_cleans_and_defaults(spark):
    raw = make_raw_customers(
        spark,
        [
            {"customer_id": " pw-19240 ", "customer_name": "Gary567 Hansen", "email": " A@B.COM ",
             "address": "1 Main St\nAustin, TX", "postal_code": "2920"},
            {"customer_id": "KL-1", "customer_name": None, "country": None},
            {"customer_id": "EM-1", "customer_name": "   "},
        ],
    )
    out = {r.customer_id: r for r in T.enrich_customers(raw).collect()}
    assert set(out) == {"PW-19240", "KL-1", "EM-1"}       # ids trimmed + upper-cased
    pw = out["PW-19240"]
    assert pw.customer_name == "Gary Hansen"
    assert pw.customer_name_raw == "Gary567 Hansen"       # lineage kept
    assert pw.email == "a@b.com"
    assert pw.address == "1 Main St, Austin, TX"
    assert pw.postal_code == "02920"
    assert out["KL-1"].customer_name == T.UNKNOWN
    assert out["KL-1"].country == T.UNKNOWN
    assert out["EM-1"].customer_name == T.UNKNOWN          # blank -> Unknown


def test_enrich_customers_one_row_per_customer(spark):
    raw = make_raw_customers(
        spark,
        [
            {"customer_id": "C-1", "customer_name": "Zed Last"},
            {"customer_id": "c-1 ", "customer_name": "Amy First"},   # same id after normalising
            {"customer_id": None, "customer_name": "No Id"},          # unusable -> dropped
            {"customer_id": "C-2", "customer_name": "Bob"},
        ],
    )
    out = T.enrich_customers(raw)
    assert out.count() == 2
    assert out.groupBy("customer_id").count().filter("count > 1").count() == 0
    # deterministic winner (same ingestion time -> alphabetical name)
    assert out.filter("customer_id = 'C-1'").first().customer_name == "Amy First"


def test_enrich_customers_output_schema(spark, raw_customers):
    out = T.enrich_customers(raw_customers)
    assert out.columns == [
        "customer_id", "customer_name", "customer_name_raw", "email", "phone", "address",
        "segment", "country", "city", "state", "postal_code", "region", "_processed_at",
    ]
