"""E-commerce sales data processing on Databricks (PySpark).

Layers (medallion style):
    raw       -> source data as delivered, standardised column names + ingestion metadata
    enriched  -> cleaned / conformed customers, products and orders (master table)
    aggregate -> profit aggregated by year, category, sub-category and customer

All transformation logic lives in this package as pure functions
(DataFrame in -> DataFrame out) so it can be unit tested locally with pytest
and re-used unchanged from the Databricks notebooks.
"""

__version__ = "1.0.0"
