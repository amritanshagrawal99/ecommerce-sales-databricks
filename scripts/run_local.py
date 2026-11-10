"""Run the full pipeline locally (no Databricks needed) and export every table and
SQL output to output/ as CSV, so results can be reviewed without a workspace.

    python scripts/run_local.py [--source data/raw] [--out output]

Delta tables are replaced by an in-memory sink. The transformation code is the
same code the Databricks notebooks run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from ecommerce_sales import pipeline  # noqa: E402
from ecommerce_sales.config import PipelineConfig  # noqa: E402
from ecommerce_sales.sinks import MemorySink  # noqa: E402


def export(df, path: Path) -> None:
    # toPandas avoids needing Hadoop winutils for Spark writes on Windows; data is small
    drop = [c for c in df.columns if c.startswith("_")]
    df.drop(*drop).toPandas().to_csv(path, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(ROOT / "data" / "raw"))
    ap.add_argument("--out", default=str(ROOT / "output"))
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(name)s - %(message)s")
    for noisy in ("py4j", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    spark = (
        SparkSession.builder.master("local[2]").appName("ecommerce-sales-local")
        .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    cfg = PipelineConfig(source_path=Path(args.source).resolve().as_uri())
    sink = MemorySink(spark)
    tables = pipeline.run_all(spark, cfg, sink)

    out = Path(args.out)
    (out / "tables").mkdir(parents=True, exist_ok=True)
    (out / "sql").mkdir(parents=True, exist_ok=True)
    for table, df in tables.items():
        export(df, out / "tables" / f"{table}.csv")
        print(f"{table:<55} {df.count():>6} rows")

    for name, df in pipeline.run_sql_outputs(spark, sink.view_name(cfg.profit_aggregate)).items():
        export(df, out / "sql" / f"{name}.csv")
        print(f"\n== {name} ({df.count()} rows) ==")
        df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
