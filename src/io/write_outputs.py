import os
import pandas as pd

try:
    from databricks.sdk.runtime import dbutils
except Exception:
    dbutils = None

_TMP_BASE = "/dbfs/tmp/afileon_ex2"

def _is_abfss(path: str) -> bool:
    return isinstance(path, str) and path.startswith("abfss://")

def ensure_dir(path: str) -> None:
    if _is_abfss(path):
        if dbutils is None:
            raise RuntimeError("Databricks dbutils is required to create abfss directories.")
        dbutils.fs.mkdirs(path)
    else:
        os.makedirs(path, exist_ok=True)

def write_parquet_and_csv(df: pd.DataFrame, out_dir: str, base_name: str) -> None:
    ensure_dir(out_dir)

    parquet_name = f"{base_name}.parquet"
    csv_name     = f"{base_name}.csv"

    if _is_abfss(out_dir):
        if dbutils is None:
            raise RuntimeError("Databricks dbutils is required to write to abfss.")
        os.makedirs(_TMP_BASE, exist_ok=True)
        local_parquet = os.path.join(_TMP_BASE, parquet_name)
        local_csv     = os.path.join(_TMP_BASE, csv_name)

        df.to_parquet(local_parquet, index=False)
        df.to_csv(local_csv, index=False)

        dbutils.fs.cp(f"file:{local_parquet}", f"{out_dir}/{parquet_name}", True)
        dbutils.fs.cp(f"file:{local_csv}", f"{out_dir}/{csv_name}", True)
    else:
        df.to_parquet(os.path.join(out_dir, parquet_name), index=False)
        df.to_csv(os.path.join(out_dir, csv_name), index=False)