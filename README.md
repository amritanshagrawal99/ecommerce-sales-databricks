# E-commerce Sales Data Processing on Databricks (PySpark)

This project builds a raw → enriched → aggregate pipeline over the e-commerce sales data (customers, products
and orders), plus SQL reporting outputs.
* All transformations are written in **PySpark DataFrame API** code. SQL is used only for the Task 5 outputs and
  for DDL.
* The logic lives in a tested Python package (`src/ecommerce_sales`). The Databricks notebooks only orchestrate it.
* **135 unit and integration tests (all passing)** (pytest + local Spark) cover every task. Results are in `test_results.txt`.

---

## 1. Task → deliverable map

| # | Requirement | Where | Output table |
|---|---|---|---|
| 1 | Raw table per source dataset | `ingestion.py`, `notebooks/01_raw_ingestion.py` | `ecommerce_raw.customers`, `ecommerce_raw.products`, `ecommerce_raw.orders` |
| 2 | Enriched customers & products | `transformations.enrich_customers / enrich_products`, `notebooks/02_…` | `ecommerce_enriched.customers`, `ecommerce_enriched.products` |
| 3 | Enriched order table: order info + profit rounded to 2 dp, customer name & country, product category & sub-category | `transformations.validate_orders / enrich_orders`, `notebooks/03_…` | `ecommerce_enriched.orders` (+ quarantine `ecommerce_enriched.orders_rejected`) |
| 4 | Aggregate: profit by Year, Category, Sub-Category, Customer | `transformations.build_profit_aggregate`, `notebooks/04_…` | `ecommerce_aggregate.profit_by_year_category_customer` |
| 5 | SQL: profit by Year / Year+Category / Customer / Customer+Year | `notebooks/05_sql_aggregates.sql`, `sql/*.sql`, `sql_queries.py` | query results |
| – | Unit tests | `tests/` | `test_results.txt` |

## 2. Architecture

```
 Customer.xlsx ─┐                     ┌─ customers (1 row / customer_id) ─┐
 Products.csv  ─┼─► RAW (Delta) ──────┼─ products  (1 row / product_id)  ─┼─► orders (master, 1 row / order line)
 Orders.json   ─┘   as delivered,     └───────────────────────────────────┘        │   + orders_rejected (quarantine)
                    explicit schema,          ENRICHED (cleaned, conformed)        ▼
                    snake_case cols,                                    AGGREGATE profit_by_year_category_customer
                    lineage cols                                                   │
                                                                                   ▼
                                                                SQL: by year | year+category | customer | customer+year
```

* Each stage reads its inputs from the upstream **tables**, not from memory, so a stage can run alone as a job task
  and re-running it is idempotent (full overwrite with `overwriteSchema`).
* The pipeline only talks to a **sink** interface (`sinks.py`). `DeltaSink` writes managed Delta tables on Databricks.
  `MemorySink` keeps temp views for local runs and tests. The same orchestration code runs in both places.
* **Data-quality gates** (`data_quality.py`) run after every stage. A failed *error* check stops the run with
  `DataQualityError`. A failed *warning* is logged.

### Repository layout
```
src/ecommerce_sales/
  config.py          table names / source paths (catalog + one schema per layer)
  schemas.py         explicit source schemas
  ingestion.py       raw readers (Excel / CSV / JSON), column standardisation, IngestionError
  transformations.py all cleaning, enrichment and aggregation logic (pure DataFrame -> DataFrame functions)
  data_quality.py    checks (not-null, unique, row-count, sum reconciliation, ratio) + enforce()
  sinks.py           DeltaSink (Databricks) / MemorySink (local + tests)
  pipeline.py        stage orchestration + DQ gates
  sql_queries.py     Task 5 SQL (same text as sql/*.sql; a test keeps the two in sync)
notebooks/           Databricks source-format notebooks 00–07 (import via Git folder / Workspace > Import)
sql/                 the four Task 5 queries as .sql files
tests/               pytest suite (unit + integration)
scripts/run_local.py run everything locally and export all tables + SQL outputs to output/
output/              CSV exports of every table and SQL result from a local run (for review without a workspace)
data/raw/            the source files as downloaded
databricks.yml       Databricks Asset Bundle: the pipeline as a multi-task job
```

## 3. How to run

> **Verified on Databricks Free Edition (serverless):** the 5-task job (01 → 05) finished with SUCCESS, and the
> tables reconcile exactly with the local run: 9,994 orders, 8,092 aggregate rows, total profit 278,417.03.
> `07_run_unit_tests` passed all 135 tests on the cluster.


### On Databricks (Free Edition, or any Unity Catalog workspace)
1. Import this folder as a **Git folder**, or upload it with *Workspace → Import*.
2. Upload `Customer.xlsx`, `Products.csv` and `Orders.json` to a volume. For example, create volume
   `workspace.default.ecommerce_landing` and upload the files to `/Volumes/workspace/default/ecommerce_landing/`.
3. Open `notebooks/06_run_all`, set the `source_path` widget (and `catalog` if needed), then *Run all*.
   You can also run `01` → `05` one at a time. The `05_sql_aggregates` SQL notebook prints the four Task 5 outputs.
4. To run the tests on the cluster, run `notebooks/07_run_unit_tests`.
5. Optional, as a scheduled job: `databricks bundle deploy -t dev && databricks bundle run ecommerce_sales_pipeline`.

> Excel is read with pandas/openpyxl on the driver (`%pip install openpyxl` is in notebook 01) because Spark has no
> built-in Excel reader. That is fine for a dimension file of this size. For very large workbooks, switch to
> `com.crealytics:spark-excel` or ask for CSV extracts.

### Locally (Python 3.9+ and Java 8/11/17)
```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements-dev.txt   # (bin/ on Linux/macOS)
.venv/Scripts/python -m pytest -v                 # all tests
.venv/Scripts/python -m pytest -m "not integration"   # unit tests only
.venv/Scripts/python scripts/run_local.py         # full pipeline -> output/
```

## 4. Data-quality findings and how they are handled

Profiling the source files (`data/raw`) turned up the following issues:

| Dataset | Finding | Handling |
|---|---|---|
| Customers | **76 names with injected noise**: `Gary567 Hansen`, `Tam&^*ara Willing___)ham`, `"       Kristi;'[]na Nunn"`, `Neil Knudson%^&*(` | `clean_customer_name`: drop everything except letters (any script), spaces, `'` and `-`. Apostrophes are kept only in the `O'Rourke`/`D'Ascenzo` pattern and hyphens only in double-barrelled names (`Corey-Lock`). Whitespace is collapsed. The original is kept in `customer_name_raw`. |
| Customers | **Look-alike substitutions**: `Bi1l`, `Ji11`, `Wa55erman`, `N0ra`, `C@thy`, `Dan!els`, `5chnelling` | These are restored (`Bill`, `Jill`, `Wasserman`, …) only where a single or doubled look-alike character sits *inside* a word. Mixed runs such as `Gary567` and `Willin0009gham` count as insertions and are removed. |
| Customers | Real non-ASCII letters (`Peter Bühler`, `Französisch`) | Kept. Letter matching uses Unicode `\p{L}`, not `[A-Za-z]`. |
| Customers | **8 missing names** | `customer_name = 'Unknown'`. The customer and their orders are kept. |
| Customers | Phone numbers in mixed formats, and mixed int/str types in Excel (`421.580.0902x9815`, `001-542-415-0246x314`, `-6181`) | Read as string. Normalised to 10 digits (extension and `001`/`1` prefix removed). Invalid numbers become null. |
| Customers | **32 ZIP codes lost their leading zero** in Excel (`2920`) | Left-padded to 5 digits (`02920`). |
| Customers | Addresses contain line breaks | Replaced with `, `. |
| Products | **33 product_ids appear twice** (different state, name or price) | De-duplicated to one row per id. The pick is deterministic: first by product name, then state. `source_record_count` keeps the number of merged rows. Category and sub-category are identical across duplicates, and a DQ check verifies this. **Without this step the orders join fans out and double counts profit.** |
| Products | Non-breaking spaces (UTF-8 ` `) and quoted commas in names | CSV read with quote/escape/UTF-8 options. Non-breaking spaces are replaced. |
| Orders | **44 product_ids (204 order lines, 2 % of rows) are missing from the product master** | The order is kept (left join). Category and sub-category are **inferred from the product-id code** (`OFF-EN-…` → Office Supplies / Envelopes). The code→category map is learned from the product master and used only when a code maps to exactly one pair (all 17 codes do). `product_match_status = 'inferred_from_product_id'` flags these rows. |
| Orders | Dates are `d/M/yyyy` strings (`6/10/2016`, `21/8/2016`) | Parsed with `try_to_timestamp` (lenient, also safe under ANSI mode). An unparseable date sends the row to quarantine. |
| Orders | 7 orders whose `Order ID` year differs from `Order Date` | **The year comes from `Order Date`.** |
| Orders | No other defects in this file, but the pipeline still guards against them | Missing keys, a bad date, a null profit, quantity ≤ 0, discount outside [0, 1], duplicate `row_id` and corrupt JSON all go to `orders_rejected` with a `reject_reason`. Nothing is dropped silently. |

### DQ gates in the pipeline (error = stop the run)
* Enriched customers and products: unique key, no nulls in key or enriched columns.
* Enriched orders: **row count equals the validated input** (catches join fan-out), unique `row_id`, no nulls in
  enriched columns, **profit total reconciles** with the input.
* Aggregate: the grain is unique and the **profit total equals the order master**.
* Warnings: rejected rows above 1 %, inferred products above 5 %, any unmatched product or customer, or conflicting
  categories for a duplicated product id.

## 5. Assumptions
1. **Profit** is the source `Profit` field, rounded **half-up to 2 dp** and stored as `DECIMAL(18,2)`. The cast to
   decimal happens before rounding, which avoids float artefacts (for example, a double `2.675` rounds to 2.68, not
   2.67). Every sum is exact.
2. **Year** means the calendar year of `Order Date`.
3. **Customer** in the aggregates means `customer_id` together with `customer_name`. Grouping by name alone would
   merge different customers (for example, the `Unknown` ones).
4. **Country** comes from the customer master. All customers are in the United States.
5. The source `Price` field is the order-line amount (`Price per product × Quantity`). It is passed through unchanged.
6. Products: the product master is modelled at `product_id` grain, because the order lines carry no state to match a
   per-state listing.
7. The raw layer keeps every source row as delivered. Cleaning is only applied in the enriched layer, so it can be
   replayed.
8. Each run is a full refresh. The source is a snapshot extract, so there are no incremental keys or watermarks.
   See §8 for the incremental design.

## 6. Results (from `scripts/run_local.py`, full outputs in `output/`)

| Check | Value |
|---|---|
| Raw rows (customers / products / orders) | 793 / 1,851 / 9,994 |
| Enriched customers / products / orders | 793 / 1,818 / 9,994 (0 rejected) |
| Orders matched to a product / inferred from the product code / unmatched | 9,790 / 204 / 0 |
| Aggregate rows (year × category × sub-category × customer) | 8,092 |
| Total profit (source = order master = aggregate = every SQL output) | **278,417.03** |

**5.1 Profit by year:** 2014: 39,185.75 · 2015: 63,073.09 · 2016: 65,073.23 · 2017: 111,084.96

**5.2 Profit by year + category**

| Year | Furniture | Office Supplies | Technology |
|---|---|---|---|
| 2014 | -5,297.36 | 22,989.76 | 21,493.35 |
| 2015 | 3,039.17 | 25,090.56 | 34,943.36 |
| 2016 | 6,926.09 | 34,923.38 | 23,223.76 |
| 2017 | 3,019.25 | 45,056.50 | 63,009.21 |

**5.3 Profit by customer:** top 3 are Frank Hawley 13,400.32, Tamara Chand 8,981.32 and Raymond Buch 6,976.36. Bottom is
Susan MacKendrick at -16,510.34. All 793 customers are in `output/sql/profit_by_customer.csv`.
**5.4 Profit by customer + year:** 2,499 rows in `output/sql/profit_by_customer_year.csv`.

## 7. Testing strategy
The tests were written alongside each transformation (TDD style). They use small hand-built DataFrames whose
expected values can be worked out by hand.

| File | Covers |
|---|---|
| `test_ingestion.py` | snake_case renaming; multi-line JSON; CSV with quoted commas, escaped quotes and NBSP; Excel with mixed int/str phones and NaN names; missing column, missing file and corrupt file → `IngestionError`; path mapping |
| `test_enriched_customers.py` | 35+ real dirty names (noise, look-alikes, Unicode, legitimate `'`/`-`, empty/null); phone and ZIP normalisation; trimming and upper-casing ids; `Unknown` defaults; deterministic de-duplication; output schema |
| `test_enriched_products.py` | duplicate ids → one row; deterministic choice regardless of input order; text cleaning; corrupt CSV rows; product-code map keeps only unambiguous codes |
| `test_enriched_orders.py` | half-up rounding incl. negatives and float edge cases; `DECIMAL(18,2)` type; `d/M/yyyy` parsing incl. invalid dates; year taken from the order date; every reject rule and multiple reasons; duplicate `row_id` (latest load wins, an invalid duplicate never displaces a valid row); id normalisation before the join; customer and product enrichment; **no fan-out from duplicate products**; inferred, unknown and unmatched products and customers kept; row count and profit preserved |
| `test_aggregate.py` | values at the grain; customers sharing a name are not merged; profit preserved; schema and types; unique grain; empty input |
| `test_sql_queries.py` | each Task 5 query against known data; every query reconciles to the total; `sql/*.sql` files equal the tested queries |
| `test_data_quality.py` | every check, and `enforce` raising only on errors |
| `test_pipeline_integration.py` | full pipeline on the real files: counts, clean names, profit reconciliation at every layer, no unknown categories, years 2014–2017; fails fast on a missing source; **DQ gate catches an injected join fan-out** |

## 8. Performance and scalability notes
* **Explicit schemas** avoid an inference pass and stop silent type drift.
* **Broadcast joins**: the dimensions (793 and 1,818 rows) are broadcast, so the order fact table is never shuffled
  for enrichment.
* Products and customers are **de-duplicated with a window**, not `dropDuplicates`, so the surviving row is
  deterministic and reproducible.
* The aggregate is built **once** at the finest reporting grain. The four SQL outputs are cheap roll-ups of it
  (8k rows instead of 10k order lines, and the gap widens at scale).
* No partitioning at this volume (≈10k rows): partitioning would only create small files. At production scale the
  plan is:
  * Liquid clustering (`CLUSTER BY (order_year, customer_id)`) on `orders`, or partitioning by `order_date` month.
  * Incremental ingestion with Auto Loader (`cloudFiles`) into raw.
  * `MERGE` into enriched keyed on `row_id`, using change data feed.
  * Recomputing only the affected years in the aggregate.
* DQ checks are each a single aggregation pass. `validate_orders` output is cached because it feeds both the write
  and the reconciliation checks.
* Rounding happens once, at row level in the enriched layer. Aggregates sum exact decimals, so rounding errors cannot
  build up.
