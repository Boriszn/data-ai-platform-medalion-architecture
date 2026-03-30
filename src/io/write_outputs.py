import os
import pandas as pd

try:
    from databricks.sdk.runtime import dbutils
except Exception:
    dbutils = None

# -----------------------------------------------------------------------------
# write_outputs.py
#
# Purpose
# - Write pipeline outputs in both Parquet and CSV format.
# - Support two execution modes:
#   1) Local filesystem paths (example: data/gold)
#   2) Databricks with ADLS paths (abfss://...) where direct Python file writes
#      are not supported.
#
# Why local staging is needed for ABFSS
# - Pandas writes files using local filesystem APIs.
# - ABFSS is a remote storage path and cannot be used with os.makedirs() and
#   normal file writes.
# - In Databricks, /dbfs is a local mount that supports Python file writes.
# - The approach is:
#   1) Write files to /dbfs/tmp/...
#   2) Copy files from local DBFS path to abfss://... using dbutils.fs.cp
#
# Output
# - <base_name>.parquet
# - <base_name>.csv
# -----------------------------------------------------------------------------


_TMP_BASE = "/dbfs/tmp/af_ex2"


def _is_abfss(path: str) -> bool:
    """
    Check if a path is an ABFSS path.

    ABFSS paths are used for ADLS Gen2 access in Databricks, for example:
    abfss://<filesystem>@<storage-account>.dfs.core.windows.net/<path>
    """
    return isinstance(path, str) and path.startswith("abfss://")


def ensure_dir(path: str) -> None:
    """
    Ensure the target directory exists.

    Local mode
    - Uses os.makedirs.

    Databricks ABFSS mode
    - Uses dbutils.fs.mkdirs to create remote directories.
    """
    if _is_abfss(path):
        if dbutils is None:
            raise RuntimeError("Databricks dbutils is required to create abfss directories.")
        dbutils.fs.mkdirs(path)
    else:
        os.makedirs(path, exist_ok=True)


def write_parquet_and_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> None:
    """
    Write a dataframe as Parquet and CSV to the target output directory.

    Parameters
    - df: dataframe to write
    - out_dir: output directory (local path or abfss:// path)
    - base_name: base file name without extension

    Local mode
    - Writes directly to out_dir.

    Databricks ABFSS mode
    - Writes to a local staging directory in /dbfs/tmp
    - Copies files to abfss://... using dbutils.fs.cp
    """
    ensure_dir(out_dir)

    parquet_name = f"{base_name}.parquet"
    csv_name     = f"{base_name}.csv"

    if _is_abfss(out_dir):
        if dbutils is None:
            raise RuntimeError("Databricks dbutils is required to write to abfss.")

        # Local staging directory on Databricks.
        os.makedirs(_TMP_BASE, exist_ok=True)
        local_parquet = os.path.join(_TMP_BASE, parquet_name)
        local_csv     = os.path.join(_TMP_BASE, csv_name)

        # Write locally using pandas.
        df.to_parquet(local_parquet, index=False)
        df.to_csv(local_csv, index=False)

        # Copy from local DBFS file path to ADLS via ABFSS.
        dbutils.fs.cp(f"file:{local_parquet}", f"{out_dir}/{parquet_name}", True)
        dbutils.fs.cp(f"file:{local_csv}", f"{out_dir}/{csv_name}", True)

    else:
        # Local filesystem writes.
        df.to_parquet(os.path.join(out_dir, parquet_name), index=False)
        df.to_csv(os.path.join(out_dir, csv_name), index=False)