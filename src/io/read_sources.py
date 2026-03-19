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

# -----------------------------------------------------------------------------
# read_sources.py
#
# Purpose
# - Locate and load the input CSV files for the pipeline.
# - Support two execution modes:
#   1) Local runs (input_dir is a local folder path)
#   2) Databricks runs (input_dir is an abfss:// path in ADLS Gen2)
#
# Why Spark is used for ABFSS
# - Pandas + glob do not work on abfss:// paths.
# - Spark can read abfss:// paths using the cluster storage configuration.
#
# Output
# - A single pandas dataframe containing all input rows.
# - A helper column "__input_file__" is added to track the source file per row.
# -----------------------------------------------------------------------------


def find_input_files(input_dir: str) -> List[str]:
    """
    Find input CSV files in the given input_dir.

    Local mode
    - Uses glob to find payroll_transactions_*.csv in a local folder.

    Databricks mode (ABFSS)
    - Uses dbutils.fs.ls to list files in an ADLS folder.

    Parameters
    - input_dir: local path or abfss:// path

    Returns
    - List of file paths (local file paths or abfss:// paths)

    Raises
    - FileNotFoundError if no matching files are found.
    - RuntimeError if abfss:// is used but dbutils is not available.
    """
    # Local
    if not input_dir.startswith("abfss://"):
        pattern = os.path.join(input_dir, "payroll_transactions_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No input CSV files found at: {pattern}")
        return files

    # Databricks / ABFSS
    # dbutils is required to list remote storage paths.
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
    """
    Read the input files into a single pandas dataframe.

    Databricks / ABFSS mode
    - Uses Spark to read the CSV files.
    - Adds "__input_file__" using Spark input_file_name().
    - Converts the Spark dataframe to pandas for downstream processing.

    Local mode
    - Uses pandas read_csv for each file and concatenates.

    Schema validation
    - After loading, checks that all REQUIRED_INPUT_COLUMNS exist.

    Parameters
    - files: list of file paths (local or abfss://)

    Returns
    - pandas dataframe with input rows and "__input_file__" column.
    """
    # If files are ABFSS paths, use Spark to read.
    # Spark uses the cluster storage configuration and can access abfss:// locations.
    if files and str(files[0]).startswith("abfss://"):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import input_file_name

        spark = SparkSession.builder.getOrCreate()

        sdf = (
            spark.read.option("header", "true")
            .csv(files)
            .withColumn("__input_file__", input_file_name())
        )

        # Convert to pandas for the rest of the pipeline.
        df = sdf.toPandas()

        # Keep only the basename as in local mode.
        # This makes source_file values consistent across run modes.
        df["__input_file__"] = df["__input_file__"].astype(str).apply(lambda p: os.path.basename(p))
    else:
        # Local filesystem
        frames = []
        for f in files:
            df = pd.read_csv(f)
            df["__input_file__"] = os.path.basename(f)
            frames.append(df)
        df = pd.concat(frames, ignore_index=True)

    # Validate schema after load.
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in input data: {missing}")

    return df