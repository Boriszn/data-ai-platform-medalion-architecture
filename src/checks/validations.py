from __future__ import annotations

import re
from typing import Iterable, Tuple

import pandas as pd

# -----------------------------------------------------------------------------
# validations.py
#
# Purpose
# - Small validation functions used by the Silver stage.
# - These checks enforce basic data quality rules and fail fast on bad inputs.
#
# How the checks are used
# - Silver calls these functions before producing curated outputs.
# - If a check fails, a ValueError is raised and the pipeline stops.
# - Errors include a short description to help debugging.
#
# Notes
# - These checks are intentionally simple for the exercise.
# - For production, a full data quality framework and quarantining strategy
#   can be added.
# -----------------------------------------------------------------------------


def require_no_nulls(df: pd.DataFrame, columns: Iterable[str]) -> None:
    """
    Ensure that selected columns contain no null values.

    Parameters
    - df: input dataframe
    - columns: columns that must be non-null

    Raises
    - ValueError if any of the selected columns contains null values.
    """
    null_counts = df[list(columns)].isna().sum()
    bad = null_counts[null_counts > 0]
    if not bad.empty:
        raise ValueError(f"Nulls found in columns: {bad.to_dict()}")


def require_currency(df: pd.DataFrame, allowed: Tuple[str, ...] = ("EUR",)) -> None:
    """
    Ensure that currency values match the allowed list.

    Default
    - Only EUR is allowed for this exercise.

    Raises
    - ValueError if unexpected currency values are found.
    """
    bad = df.loc[~df["currency"].isin(allowed), "currency"].unique().tolist()
    if bad:
        raise ValueError(f"Unexpected currency values: {bad}")


def require_pay_period_format(df: pd.DataFrame) -> None:
    """
    Ensure that pay_period uses the expected YYYY-MM format.

    Examples
    - 2025-07
    - 2025-08

    Raises
    - ValueError if unexpected values are found.
    """
    # Expect YYYY-MM
    pattern = re.compile(r"^\\d{4}-\\d{2}$")
    bad = df.loc[~df["pay_period"].astype(str).str.match(pattern), "pay_period"].unique().tolist()
    if bad:
        raise ValueError(f"Unexpected pay_period format values: {bad}")


def require_amount_consistency(df: pd.DataFrame, tol: float = 0.01) -> None:
    """
    Ensure that gross_amount = taxes_amount + net_amount within a tolerance.

    Why this check exists
    - Payroll data should be internally consistent.
    - Small rounding differences can exist, so a tolerance is used.

    Parameters
    - tol: maximum allowed absolute difference

    Raises
    - ValueError with a small sample of failing rows.
    """
    # gross = taxes + net (within tolerance)
    diff = (df["gross_amount"] - (df["taxes_amount"] + df["net_amount"])).abs()
    bad = df.loc[diff > tol, ["gross_amount", "taxes_amount", "net_amount"]]
    if not bad.empty:
        # limit to first few rows in error message
        sample = bad.head(5).to_dict(orient="records")
        raise ValueError(f"Amount consistency check failed (sample rows): {sample}")


def require_one_subsidiary_per_source_file(df: pd.DataFrame) -> None:
    """
    Ensure that each source file contains records for a single subsidiary.

    Why this check exists
    - Each input CSV is expected to represent one subsidiary.

    Requires
    - source_file column is present (added in Bronze)
    - subsidiary_id column is present (from input)

    Raises
    - ValueError if a file contains more than one subsidiary_id.
    """
    # Each input file should contain one subsidiary_id
    grp = df.groupby("source_file")["subsidiary_id"].nunique()
    bad = grp[grp != 1]
    if not bad.empty:
        raise ValueError(f"Expected one subsidiary per source_file. Found: {bad.to_dict()}")