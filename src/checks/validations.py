from __future__ import annotations

import re
from typing import Iterable, Tuple

import pandas as pd


def require_no_nulls(df: pd.DataFrame, columns: Iterable[str]) -> None:
    null_counts = df[list(columns)].isna().sum()
    bad = null_counts[null_counts > 0]
    if not bad.empty:
        raise ValueError(f"Nulls found in columns: {bad.to_dict()}")


def require_currency(df: pd.DataFrame, allowed: Tuple[str, ...] = ("EUR",)) -> None:
    bad = df.loc[~df["currency"].isin(allowed), "currency"].unique().tolist()
    if bad:
        raise ValueError(f"Unexpected currency values: {bad}")


def require_pay_period_format(df: pd.DataFrame) -> None:
    # Expect YYYY-MM
    pattern = re.compile(r"^\d{4}-\d{2}$")
    bad = df.loc[~df["pay_period"].astype(str).str.match(pattern), "pay_period"].unique().tolist()
    if bad:
        raise ValueError(f"Unexpected pay_period format values: {bad}")


def require_amount_consistency(df: pd.DataFrame, tol: float = 0.01) -> None:
    # gross = taxes + net (within tolerance)
    diff = (df["gross_amount"] - (df["taxes_amount"] + df["net_amount"])).abs()
    bad = df.loc[diff > tol, ["gross_amount", "taxes_amount", "net_amount"]]
    if not bad.empty:
        # limit to first few rows in error message
        sample = bad.head(5).to_dict(orient="records")
        raise ValueError(f"Amount consistency check failed (sample rows): {sample}")


def require_one_subsidiary_per_source_file(df: pd.DataFrame) -> None:
    # Each input file should contain one subsidiary_id
    grp = df.groupby("source_file")["subsidiary_id"].nunique()
    bad = grp[grp != 1]
    if not bad.empty:
        raise ValueError(f"Expected one subsidiary per source_file. Found: {bad.to_dict()}")
