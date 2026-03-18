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


def find_input_files(input_dir: str) -> List[str]:
    # Local
    if not input_dir.startswith("abfss://"):
        pattern = os.path.join(input_dir, "payroll_transactions_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No input CSV files found at: {pattern}")
        return files

    # Databricks / ABFSS
    try:
        from databricks.sdk.runtime import dbutils
    except Exception:
        dbutils = None

    if dbutils is None:
        raise RuntimeError("dbutils is required to list abfss paths in Databricks.")

    items = dbutils.fs.ls(input_dir)
    files = sorted(
        [
            x.path
            for x in items
            if x.name.startswith("payroll_transactions_") and x.name.endswith(".csv")
        ]
    )
    if not files:
        raise FileNotFoundError(f"No input CSV files found at: {input_dir}")
    return files


def read_csv_files(files: List[str]) -> pd.DataFrame:
    # If files are ABFSS paths, use Spark to read (Spark respects the fs.azure.account.key config)
    if files and str(files[0]).startswith("abfss://"):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import input_file_name

        spark = SparkSession.builder.getOrCreate()

        sdf = (
            spark.read.option("header", "true")
            .csv(files)
            .withColumn("__input_file__", input_file_name())
        )

        # Convert to pandas
        df = sdf.toPandas()

        # Keep only the basename as in local mode
        df["__input_file__"] = df["__input_file__"].astype(str).apply(lambda p: os.path.basename(p))
    else:
        # Local filesystem
        frames = []
        for f in files:
            df = pd.read_csv(f)
            df["__input_file__"] = os.path.basename(f)
            frames.append(df)
        df = pd.concat(frames, ignore_index=True)

    # Validate schema after load
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input data: {missing}")

    return df