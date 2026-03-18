from __future__ import annotations

import glob
import os
from typing import List

import pandas as pd

from ..config import REQUIRED_INPUT_COLUMNS

try:
    from databricks.sdk.runtime import dbutils  # available in Databricks runtime
except Exception:
    dbutils = None


def find_input_files(input_dir: str):
    if input_dir.startswith("abfss://"):
        if dbutils is None:
            raise RuntimeError("Databricks dbutils is required to list abfss paths.")
        items = dbutils.fs.ls(input_dir)
        files = sorted([x.path for x in items if x.name.startswith("payroll_transactions_") and x.name.endswith(".csv")])
        if not files:
            raise FileNotFoundError(f"No input CSV files found at: {input_dir}")
        return files

    pattern = os.path.join(input_dir, "payroll_transactions_*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No input CSV files found at: {pattern}")
    return files


def read_csv_files(files: List[str]) -> pd.DataFrame:
    frames = []
    for f in files:
        df = pd.read_csv(f)
        missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in {os.path.basename(f)}: {missing}")
        df["__input_file__"] = os.path.basename(f)
        frames.append(df)
    return pd.concat(frames, ignore_index=True)
