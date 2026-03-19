from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import asdict
from typing import Dict

import pandas as pd

from .config import DEFAULT_INPUT_DIR, DEFAULT_OUTPUT_DIR, BRONZE_DIRNAME, SILVER_DIRNAME, GOLD_DIRNAME, LOGS_DIRNAME
from .io.read_sources import find_input_files, read_csv_files
from .io.write_outputs import write_parquet_and_csv
from .bronze.build_bronze import build_bronze
from .silver.build_silver import build_silver
from .gold.controller_aggregates import build_controller_monthly_aggregates
from .gold.auditor_totals import build_auditor_cross_subsidiary_totals


# -----------------------------------------------------------------------------
# main.py
#
# Purpose
# - Runs Exercise 2 payroll pipeline end-to-end using a Medallion approach:
#   Bronze -> Silver -> Gold.
# - Writes outputs to the selected output location.
# - Works in two execution modes:
#   1) Local filesystem paths (example: data/raw, data/gold)
#   2) Databricks with ABFSS output paths (example: abfss://.../gold)
#
# Inputs
# - A folder that contains CSV files matching payroll_transactions_*.csv.
#
# Outputs
# - Bronze dataset: bronze_payroll_transactions (Parquet + CSV)
# - Silver dataset: silver_payroll_transactions (Parquet + CSV)
# - Gold outputs:
#   - controller_monthly_aggregates (Parquet + CSV)
#   - auditor_cross_subsidiary_totals (Parquet + CSV)
# - Run metadata: logs/run_metadata.json
# - Run log file: local file path returned by _setup_logging()
# -----------------------------------------------------------------------------


def _setup_logging(log_dir: str) -> str:
    """
    Create a log file location that works both locally and on Databricks.

    - Local runs: write logs into <output_dir>/logs/run.log
    - Databricks runs with abfss output: write logs to /dbfs/tmp/... (ABFSS is not a local FS path)

    Why this exists
    - ABFSS locations are remote storage paths.
    - Standard Python logging handlers write to a local filesystem path.
    - When output_dir is abfss://..., logs_dir also becomes abfss://.../logs.
      In that case, logging writes to /dbfs/tmp instead.
    """
    import os
    import logging
    import datetime as dt

    def _is_abfss(p: str) -> bool:
        return isinstance(p, str) and p.startswith("abfss://")

    # If logs are targeting ABFSS, switch to a local DBFS path.
    # This avoids errors from writing log files to abfss://... paths.
    if _is_abfss(log_dir):
        log_dir = "/dbfs/tmp/af_ex2_logs"

    os.makedirs(log_dir, exist_ok=True)

    # Use a timestamped filename to avoid collisions across multiple runs.
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    log_path = os.path.join(log_dir, f"run-{ts}.log")

    # Reset handlers if the notebook/job re-runs in the same Python process.
    # This prevents duplicate log lines when the module is executed multiple times.
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)sZ %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    return log_path


def run_pipeline(input_dir: str, output_dir: str) -> Dict[str, str]:
    """
    Run the full pipeline and write Bronze, Silver, and Gold outputs.

    Steps
    1) Locate input CSV files in input_dir.
    2) Read inputs into a single dataframe.
    3) Build Bronze:
       - raw ingest + ingestion metadata.
    4) Build Silver:
       - standardize types, validate rules, apply data protection.
    5) Build Gold:
       - controller_monthly_aggregates
       - auditor_cross_subsidiary_totals
    6) Write outputs to output_dir.

    Returns
    - A dictionary with key output paths for Bronze/Silver/Gold and log locations.
    """
    t0 = time.time()

    # Output folders for Medallion layers and logs.
    bronze_dir = os.path.join(output_dir, BRONZE_DIRNAME)
    silver_dir = os.path.join(output_dir, SILVER_DIRNAME)
    gold_dir = os.path.join(output_dir, GOLD_DIRNAME)
    logs_dir = os.path.join(output_dir, LOGS_DIRNAME)

    # Logging setup is done before reading files so that failures are logged.
    log_path = _setup_logging(logs_dir)

    # 1) Input file discovery
    files = find_input_files(input_dir)
    logging.info(f"Found {len(files)} input files")

    # 2) Read inputs
    df_raw = read_csv_files(files)
    logging.info(f"Raw rows: {len(df_raw)}")

    # 3) Bronze
    df_bronze = build_bronze(df_raw)
    logging.info(f"Bronze rows: {len(df_bronze)}")
    write_parquet_and_csv(df_bronze, bronze_dir, "bronze_payroll_transactions")

    # 4) Silver
    df_silver = build_silver(df_bronze)
    logging.info(f"Silver rows: {len(df_silver)}")
    write_parquet_and_csv(df_silver, silver_dir, "silver_payroll_transactions")

    # 5) Gold outputs
    df_controller = build_controller_monthly_aggregates(df_silver)
    df_auditor = build_auditor_cross_subsidiary_totals(df_silver)

    write_parquet_and_csv(df_controller, gold_dir, "controller_monthly_aggregates")
    write_parquet_and_csv(df_auditor, gold_dir, "auditor_cross_subsidiary_totals")

    # Basic runtime measurement for observability.
    elapsed = round(time.time() - t0, 2)
    logging.info(f"Pipeline completed in {elapsed}s")

    # Run metadata helps with traceability and troubleshooting.
    # It captures input, output, row counts for each stage, and log file location.
    run_meta = {
        "input_dir": input_dir,
        "output_dir": output_dir,
        "input_files": [os.path.basename(f) for f in files],
        "rows_raw": int(len(df_raw)),
        "rows_bronze": int(len(df_bronze)),
        "rows_silver": int(len(df_silver)),
        "rows_gold_controller": int(len(df_controller)),
        "rows_gold_auditor": int(len(df_auditor)),
        "elapsed_seconds": elapsed,
        "log_path": log_path,
    }

    meta_path = os.path.join(logs_dir, "run_metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(run_meta, f, indent=2)

    # Return important paths for downstream usage (jobs, notebooks, troubleshooting).
    return {
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "logs_dir": logs_dir,
        "metadata": meta_path,
        "log": log_path,
    }


def main() -> None:
    """
    Command-line entrypoint.

    Arguments
    - --input-dir: folder containing payroll CSV inputs
    - --output-dir: base folder for Bronze/Silver/Gold outputs

    Default values are defined in src/config.py.
    """
    parser = argparse.ArgumentParser(description="data-ai Exercise 2: Payroll Medallion Pipeline")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Folder containing input CSV files")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Base output folder")
    args = parser.parse_args()

    run_pipeline(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
