from __future__ import annotations

import os
from typing import Optional

import pandas as pd


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_parquet_and_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> None:
    ensure_dir(out_dir)
    parquet_path = os.path.join(out_dir, f"{base_name}.parquet")
    csv_path = os.path.join(out_dir, f"{base_name}.csv")

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)
