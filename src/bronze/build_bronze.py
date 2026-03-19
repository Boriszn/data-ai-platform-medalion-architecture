from __future__ import annotations

import datetime as dt
import pandas as pd

from ..config import COLS

# -----------------------------------------------------------------------------
# build_bronze.py
#
# Purpose
# - Create the Bronze dataset from the raw input dataframe.
# - Bronze is the raw ingest layer with minimal transformation.
#
# What Bronze does in this project
# - Keeps all input columns as they are.
# - Adds ingestion metadata:
#   - source_file: file name used for traceability
#   - ingested_at: ingestion timestamp (UTC)
#
# Input expectation
# - df_raw contains a helper column "__input_file__" created during file reading.
#   This column identifies which CSV file each row came from.
# -----------------------------------------------------------------------------


def build_bronze(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Build the Bronze dataset.

    Parameters
    - df_raw: raw dataframe that includes "__input_file__"

    Returns
    - Bronze dataframe with:
      - source_file (renamed from "__input_file__")
      - ingested_at (UTC timestamp)
    """
    now = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    df = df_raw.copy()

    # Rename the input file marker to a standard column name.
    # This supports traceability of rows back to the source file.
    df[COLS.source_file] = df.pop("__input_file__")

    # Add ingestion timestamp in UTC.
    df[COLS.ingested_at] = now

    return df