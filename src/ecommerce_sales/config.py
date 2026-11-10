"""Pipeline configuration: source file names and target table names."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SourceFiles:
    customers: str = "Customer.xlsx"
    products: str = "Products.csv"
    orders: str = "Orders.json"


@dataclass(frozen=True)
class PipelineConfig:
    """Where data is read from and which tables are written.

    source_path : directory holding the three source files. On Databricks this is
                  typically a Unity Catalog volume, e.g. /Volumes/workspace/ecommerce/landing
    catalog     : Unity Catalog catalog (None -> current catalog / hive_metastore).
    raw_schema / enriched_schema / aggregate_schema : one schema per layer.
    """

    source_path: str = "data/raw"
    catalog: Optional[str] = None
    raw_schema: str = "ecommerce_raw"
    enriched_schema: str = "ecommerce_enriched"
    aggregate_schema: str = "ecommerce_aggregate"
    files: SourceFiles = field(default_factory=SourceFiles)

    # ---- table names ---------------------------------------------------
    def _fqn(self, schema: str, table: str) -> str:
        return f"{self.catalog}.{schema}.{table}" if self.catalog else f"{schema}.{table}"

    @property
    def raw_customers(self) -> str:
        return self._fqn(self.raw_schema, "customers")

    @property
    def raw_products(self) -> str:
        return self._fqn(self.raw_schema, "products")

    @property
    def raw_orders(self) -> str:
        return self._fqn(self.raw_schema, "orders")

    @property
    def enriched_customers(self) -> str:
        return self._fqn(self.enriched_schema, "customers")

    @property
    def enriched_products(self) -> str:
        return self._fqn(self.enriched_schema, "products")

    @property
    def enriched_orders(self) -> str:
        return self._fqn(self.enriched_schema, "orders")

    @property
    def rejected_orders(self) -> str:
        return self._fqn(self.enriched_schema, "orders_rejected")

    @property
    def profit_aggregate(self) -> str:
        return self._fqn(self.aggregate_schema, "profit_by_year_category_customer")

    @property
    def schemas(self) -> list[str]:
        names = [self.raw_schema, self.enriched_schema, self.aggregate_schema]
        return [f"{self.catalog}.{s}" if self.catalog else s for s in names]

    # ---- source paths ----------------------------------------------------
    def source(self, file_name: str) -> str:
        return f"{self.source_path.rstrip('/')}/{file_name}"
