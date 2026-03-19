from __future__ import annotations

import pandas as pd

from ..config import COLS

# -----------------------------------------------------------------------------
# controller_aggregates.py
#
# Purpose
# - Build the Gold output for controllers: monthly aggregates per subsidiary.
#
# Input
# - A Silver dataframe with:
#   - employee_key (pseudonymous identifier)
#   - subsidiary_id
#   - pay_period (YYYY-MM)
#   - gross_amount, taxes_amount, net_amount (numeric)
#
# Output
#   - A dataframe grouped by subsidiary_id and pay_period with:
#   - employee_count (distinct employees)
#   - gross_total, taxes_total, net_total (monthly totals)
#
# Notes
# - The output contains aggregates only and does not contain direct identifiers.
# - Values are rounded to 2 decimals for reporting readability.
# -----------------------------------------------------------------------------

def build_controller_monthly_aggregates(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Create controller monthly aggregates.

    This function summarizes payroll data per subsidiary and pay period.
    The result is used as a Gold dataset for controller reporting.

    Parameters
    - df_silver: cleaned and protected payroll dataset (Silver).

    Returns
    - Aggregated dataframe with one row per (subsidiary_id, pay_period).
    """
    g = (
        df_silver
        .groupby([COLS.subsidiary_id, COLS.pay_period], as_index=False)
        .agg(
            # Distinct count of employees within the subsidiary and month.
            employee_count=(COLS.employee_key, "nunique"),
            # Monthly totals.
            gross_total=(COLS.gross_amount, "sum"),
            taxes_total=(COLS.taxes_amount, "sum"),
            net_total=(COLS.net_amount, "sum"),
        )
        .sort_values([COLS.subsidiary_id, COLS.pay_period])
    )
    # Round to 2 decimals for reporting.
    for c in ["gross_total", "taxes_total", "net_total"]:
        g[c] = g[c].round(2)
    return g