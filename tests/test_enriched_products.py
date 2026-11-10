"""Task 2 - enriched products: de-duplication, text cleaning, code mapping."""

from conftest import make_raw_products
from ecommerce_sales import transformations as T


def test_enrich_products_one_row_per_product(spark, raw_products):
    out = T.enrich_products(raw_products)
    assert out.count() == 3
    assert out.groupBy("product_id").count().filter("count > 1").count() == 0
    chair = out.filter("product_id = 'FUR-CH-1'").first()
    assert chair.source_record_count == 2
    assert (chair.category, chair.sub_category) == ("Furniture", "Chairs")


def test_enrich_products_dedup_is_deterministic(spark):
    rows = [
        {"product_id": "FUR-BO-1", "category": "Furniture", "sub_category": "Bookcases",
         "product_name": "Sauder Library", "state": "California"},
        {"product_id": "FUR-BO-1", "category": "Furniture", "sub_category": "Bookcases",
         "product_name": "DMI Eclipse", "state": "New York"},
    ]
    first = T.enrich_products(make_raw_products(spark, rows)).first()
    second = T.enrich_products(make_raw_products(spark, list(reversed(rows)))).first()
    assert first.product_name == second.product_name == "DMI Eclipse"


def test_enrich_products_cleans_text(spark):
    raw = make_raw_products(
        spark,
        [
            {"product_id": " tec-ac-1 ", "category": " Technology ", "sub_category": "Accessories ",
             "product_name": "Imation Secure+  USB�Drive"},
            {"product_id": None, "category": "Technology", "sub_category": "Phones"},
            {"product_id": "OFF-XX-1", "category": None, "sub_category": None},
        ],
    )
    out = {r.product_id: r for r in T.enrich_products(raw).collect()}
    assert set(out) == {"TEC-AC-1", "OFF-XX-1"}       # null id dropped
    p = out["TEC-AC-1"]
    assert (p.category, p.sub_category) == ("Technology", "Accessories")
    assert p.product_name == "Imation Secure+ USB Drive"
    assert out["OFF-XX-1"].category == T.UNKNOWN


def test_enrich_products_drops_corrupt_records(spark):
    df = make_raw_products(spark, [{"product_id": "FUR-CH-1", "category": "Furniture", "sub_category": "Chairs"}])
    from pyspark.sql import functions as F

    df = df.withColumn("_corrupt_record", F.lit("garbage,line"))
    assert T.enrich_products(df).count() == 0


def test_product_code_mapping_only_keeps_unambiguous_codes(spark):
    raw = make_raw_products(
        spark,
        [
            {"product_id": "OFF-PA-1", "category": "Office Supplies", "sub_category": "Paper"},
            {"product_id": "OFF-PA-2", "category": "Office Supplies", "sub_category": "Paper"},
            {"product_id": "XYZ-AB-1", "category": "Furniture", "sub_category": "Chairs"},
            {"product_id": "XYZ-AB-2", "category": "Technology", "sub_category": "Phones"},  # ambiguous
            {"product_id": "BADID", "category": "Technology", "sub_category": "Phones"},      # no code
        ],
    )
    mapping = {r.product_code: (r.category, r.sub_category)
               for r in T.product_code_mapping(T.enrich_products(raw)).collect()}
    assert mapping == {"OFF-PA": ("Office Supplies", "Paper")}
