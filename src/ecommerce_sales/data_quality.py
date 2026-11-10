"""Lightweight data-quality checks run between pipeline stages.

Severity "error" stops the pipeline (raise DataQualityError); "warning" is
logged and reported but lets the run continue. Each check is one Spark action,
so they are kept to the checks that protect correctness of the outputs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger("ecommerce_sales.dq")

ERROR = "error"
WARNING = "warning"


class DataQualityError(Exception):
    """Raised when one or more error-severity checks fail."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    severity: str = ERROR

    def __str__(self) -> str:
        status = "PASS" if self.passed else f"FAIL[{self.severity}]"
        return f"{status:<14} {self.name}: {self.details}"


def check_not_empty(df: DataFrame, name: str, severity: str = ERROR) -> CheckResult:
    has_rows = len(df.take(1)) > 0
    return CheckResult(f"{name}.not_empty", has_rows, "has rows" if has_rows else "0 rows", severity)


def check_not_null(
    df: DataFrame, columns: Iterable[str], name: str, severity: str = ERROR
) -> CheckResult:
    columns = list(columns)
    row = df.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]
    ).collect()[0]
    nulls = {c: int(row[c] or 0) for c in columns if row[c]}
    return CheckResult(
        f"{name}.not_null({','.join(columns)})",
        not nulls,
        f"null counts {nulls}" if nulls else "no nulls",
        severity,
    )


def check_unique(df: DataFrame, keys: Iterable[str], name: str, severity: str = ERROR) -> CheckResult:
    keys = list(keys)
    dupes = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return CheckResult(
        f"{name}.unique({','.join(keys)})",
        dupes == 0,
        f"{dupes} duplicated key(s)" if dupes else "unique",
        severity,
    )


def check_row_count_equal(
    expected: int, df: DataFrame, name: str, severity: str = ERROR
) -> CheckResult:
    actual = df.count()
    return CheckResult(
        f"{name}.row_count",
        actual == expected,
        f"expected {expected}, got {actual}",
        severity,
    )


def check_sum_equal(
    df_a: DataFrame, col_a: str, df_b: DataFrame, col_b: str, name: str,
    tolerance: Decimal = Decimal("0.00"), severity: str = ERROR,
) -> CheckResult:
    """Reconciliation: total of a measure must be preserved between layers."""
    a = df_a.agg(F.sum(col_a)).collect()[0][0] or Decimal(0)
    b = df_b.agg(F.sum(col_b)).collect()[0][0] or Decimal(0)
    diff = abs(Decimal(str(a)) - Decimal(str(b)))
    return CheckResult(
        f"{name}.sum_reconciles({col_a}={col_b})",
        diff <= tolerance,
        f"{a} vs {b} (diff {diff})",
        severity,
    )


def check_max_ratio(
    df: DataFrame, condition, total: int, max_ratio: float, name: str, severity: str = WARNING
) -> CheckResult:
    """e.g. 'no more than 5% of orders may have an unmatched product'."""
    n = df.filter(condition).count()
    ratio = (n / total) if total else 0.0
    return CheckResult(
        f"{name}.ratio<= {max_ratio:.0%}",
        ratio <= max_ratio,
        f"{n}/{total} rows ({ratio:.2%})",
        severity,
    )


def enforce(results: List[CheckResult], stage: Optional[str] = None) -> List[CheckResult]:
    """Log every result; raise if any error-severity check failed."""
    for r in results:
        (log.info if r.passed else log.warning)(str(r))
    failed = [r for r in results if not r.passed and r.severity == ERROR]
    if failed:
        msg = "\n".join(str(r) for r in failed)
        raise DataQualityError(f"Data quality checks failed{f' in {stage}' if stage else ''}:\n{msg}")
    return results
