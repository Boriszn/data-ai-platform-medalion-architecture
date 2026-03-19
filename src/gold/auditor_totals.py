from __future__ import annotations

import pandas as pd

from ..config import COLS

# -----------------------------------------------------------------------------
# auditor_totals.py
#
# Purpose
# - Build the Gold output for auditors: monthly totals across all subsidiaries.
#
# Input
# - A Silver dataframe with:
#   - employee_key (pseudonymous identifier)
#   - subsidiary_id
#   - pay_period (YYYY-MM)
#   - gross_amount, taxes_amount, net_amount (numeric)
#
# Output
# - A dataframe grouped by pay_period with:
#   - subsidiary_count (distinct subsidiaries in the month)
#   - employee_count (distinct employees in the month)
#   - gross_total, taxes_total, net_total (monthly totals across all subsidiaries)
#
# Notes
# - The output contains aggregates only and does not contain direct identifiers.
# - Values are rounded to 2 decimals for reporting readability.
# -----------------------------------------------------------------------------


def build_auditor_cross_subsidiary_totals(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Create auditor cross-subsidiary totals.

    This function summarizes payroll data per month across all subsidiaries.
    The result is used as a Gold dataset for auditor reporting.

    Parameters
    - df_silver: cleaned and protected payroll dataset (Silver).

    Returns
    - Aggregated dataframe with one row per pay_period.
    """
    g = (
        df_silver
        .groupby([COLS.pay_period], as_index=False)
        .agg(
            # Distinct count of subsidiaries within the month.
            subsidiary_count=(COLS.subsidiary_id, "nunique"),
            # Distinct count of employees within the month.
            employee_count=(COLS.employee_key, "nunique"),
            # Monthly totals across all subsidiaries.
            gross_total=(COLS.gross_amount, "sum"),
            taxes_total=(COLS.taxes_amount, "sum"),
            net_total=(COLS.net_amount, "sum"),
        )
        .sort_values([COLS.pay_period])
    )
    # Round to 2 decimals for reporting.
    for c in ["gross_total", "taxes_total", "net_total"]:
        g[c] = g[c].round(2)
    return g