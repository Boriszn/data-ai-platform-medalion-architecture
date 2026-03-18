from __future__ import annotations

import datetime as dt
import pandas as pd

from ..config import COLS


def build_bronze(df_raw: pd.DataFrame) -> pd.DataFrame:
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    df = df_raw.copy()

    # rename input file marker to source_file
    df[COLS.source_file] = df.pop("__input_file__")
    df[COLS.ingested_at] = now

    return df
