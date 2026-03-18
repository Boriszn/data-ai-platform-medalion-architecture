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


def _setup_logging(log_dir: str) -> str:
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "run.log")
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
    t0 = time.time()

    bronze_dir = os.path.join(output_dir, BRONZE_DIRNAME)
    silver_dir = os.path.join(output_dir, SILVER_DIRNAME)
    gold_dir = os.path.join(output_dir, GOLD_DIRNAME)
    logs_dir = os.path.join(output_dir, LOGS_DIRNAME)

    log_path = _setup_logging(logs_dir)

    files = find_input_files(input_dir)
    logging.info(f"Found {len(files)} input files")

    df_raw = read_csv_files(files)
    logging.info(f"Raw rows: {len(df_raw)}")

    df_bronze = build_bronze(df_raw)
    logging.info(f"Bronze rows: {len(df_bronze)}")
    write_parquet_and_csv(df_bronze, bronze_dir, "bronze_payroll_transactions")

    df_silver = build_silver(df_bronze)
    logging.info(f"Silver rows: {len(df_silver)}")
    write_parquet_and_csv(df_silver, silver_dir, "silver_payroll_transactions")

    df_controller = build_controller_monthly_aggregates(df_silver)
    df_auditor = build_auditor_cross_subsidiary_totals(df_silver)

    write_parquet_and_csv(df_controller, gold_dir, "controller_monthly_aggregates")
    write_parquet_and_csv(df_auditor, gold_dir, "auditor_cross_subsidiary_totals")

    elapsed = round(time.time() - t0, 2)
    logging.info(f"Pipeline completed in {elapsed}s")

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

    return {
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "logs_dir": logs_dir,
        "metadata": meta_path,
        "log": log_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="data-ai Exercise 2: Payroll Medallion Pipeline")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Folder containing input CSV files")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Base output folder")
    args = parser.parse_args()

    run_pipeline(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
