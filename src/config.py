from __future__ import annotations

import os
from dataclasses import dataclass

# -----------------------------------------------------------------------------
# config.py
#
# Purpose
# - Central place for configuration constants used across the pipeline.
# - Defines default local input/output locations.
# - Defines folder names for Bronze/Silver/Gold outputs.
# - Defines column names and the required schema for input CSV files.
# - Defines a secret key setting used to generate a pseudonymous employee key.
#
# Notes
# - DEFAULT_* paths are meant for local runs.
# - When running on Databricks with ADLS (abfss://...), input_dir and output_dir
#   are passed as parameters and override these defaults.
# -----------------------------------------------------------------------------


DEFAULT_INPUT_DIR = os.path.join("data", "raw")
DEFAULT_OUTPUT_DIR = "data"

# Output folders under the selected output_dir.
BRONZE_DIRNAME = "bronze"
SILVER_DIRNAME = "silver"
GOLD_DIRNAME = "gold"
LOGS_DIRNAME = "logs"

# Environment variable used to build a pseudonymous employee key.
# The secret key supports pseudonymization of employee_id values.
# A stable key is needed so the same employee_id maps to the same employee_key.
HMAC_KEY_ENV = "data-ai_HMAC_KEY"

# Development fallback key.
# For production, set HMAC_KEY_ENV to a strong secret value.
DEV_FALLBACK_HMAC_KEY = "dev-only-change-this"


@dataclass(frozen=True)
class Columns:
    """
    Column names used in the pipeline.

    Input columns
    - These are expected in the raw CSV files.

    Pipeline-added columns
    - These are created during processing to support traceability and reporting.
    """
    employee_id: str = "employee_id"
    employee_name: str = "employee_name"
    iban: str = "iban"
    subsidiary_id: str = "subsidiary_id"
    pay_period: str = "pay_period"
    gross_amount: str = "gross_amount"
    taxes_amount: str = "taxes_amount"
    net_amount: str = "net_amount"
    currency: str = "currency"

    # Added by pipeline
    source_file: str = "source_file"      # input file name for traceability
    ingested_at: str = "ingested_at"      # ingestion timestamp (UTC)
    employee_key: str = "employee_key"    # pseudonymous key derived from employee_id
    pay_month: str = "pay_month"          # normalized month date derived from pay_period


# Convenience instance for referencing column names.
COLS = Columns()

# Required input schema.
# The pipeline validates that each input CSV contains these columns.
REQUIRED_INPUT_COLUMNS = [
    COLS.employee_id,
    COLS.employee_name,
    COLS.iban,
    COLS.subsidiary_id,
    COLS.pay_period,
    COLS.gross_amount,
    COLS.taxes_amount,
    COLS.net_amount,
    COLS.currency,
]

# Columns allowed in Silver after protection.
# Direct identifiers are removed from Silver and replaced with employee_key.
SILVER_COLUMNS = [
    COLS.employee_key,
    COLS.subsidiary_id,
    COLS.pay_period,
    COLS.pay_month,
    COLS.gross_amount,
    COLS.taxes_amount,
    COLS.net_amount,
    COLS.currency,
    COLS.source_file,
    COLS.ingested_at,
]