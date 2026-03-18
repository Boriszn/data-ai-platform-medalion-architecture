from __future__ import annotations

import pandas as pd

from ..config import COLS


def build_controller_monthly_aggregates(df_silver: pd.DataFrame) -> pd.DataFrame:
    g = (
        df_silver
        .groupby([COLS.subsidiary_id, COLS.pay_period], as_index=False)
        .agg(
            employee_count=(COLS.employee_key, "nunique"),
            gross_total=(COLS.gross_amount, "sum"),
            taxes_total=(COLS.taxes_amount, "sum"),
            net_total=(COLS.net_amount, "sum"),
        )
        .sort_values([COLS.subsidiary_id, COLS.pay_period])
    )
    # Round to 2 decimals for reporting
    for c in ["gross_total", "taxes_total", "net_total"]:
        g[c] = g[c].round(2)
    return g
