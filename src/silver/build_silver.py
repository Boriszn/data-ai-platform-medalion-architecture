from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import os

import pandas as pd

from ..config import COLS, HMAC_KEY_ENV, DEV_FALLBACK_HMAC_KEY, SILVER_COLUMNS
from ..checks.validations import (
    require_amount_consistency,
    require_currency,
    require_no_nulls,
    require_one_subsidiary_per_source_file,
    require_pay_period_format,
)

# -----------------------------------------------------------------------------
# build_silver.py
#
# Purpose
# - Create the Silver dataset from the Bronze dataset.
# - Silver is the cleaned and governed version of the data.
#
# What Silver does in this project
# - Standardizes data types (amounts become numeric).
# - Validates basic business rules (format, currency, amount consistency).
# - Applies data protection:
#   - creates a pseudonymous employee_key
#   - removes direct identifiers (employee_id, employee_name, iban)
# - Produces a stable schema defined by SILVER_COLUMNS.
#
# Notes
# - HMAC_KEY_ENV controls the secret key used for pseudonymization.
# - If HMAC_KEY_ENV is not set, DEV_FALLBACK_HMAC_KEY is used.
#   For production, HMAC_KEY_ENV should be set to a strong secret.
# -----------------------------------------------------------------------------


def _hmac_key() -> bytes:
    """
    Read the HMAC key from an environment variable.

    Returns
    - The key encoded as bytes.

    Why this exists
    - The HMAC key is used to generate a pseudonymous employee_key.
    - The same key ensures the same employee_id always maps to the same employee_key.
    """
    key = os.environ.get(HMAC_KEY_ENV, DEV_FALLBACK_HMAC_KEY)
    return key.encode("utf-8")


def _employee_key(employee_id: str, key: bytes) -> str:
    """
    Create a stable pseudonymous key for an employee_id using HMAC-SHA256.

    Parameters
    - employee_id: original identifier
    - key: secret key used for HMAC

    Returns
    - Hex-encoded HMAC digest (string)

    Result properties
    - Stable: same input produces same output when key is the same.
    - Pseudonymous: original employee_id is not stored in Silver.
    """
    # HMAC-SHA256 for stable pseudonymous key
    digest = hmac.new(key, employee_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest


def build_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    """
    Build the Silver dataset.

    Input
    - Bronze dataframe with raw columns plus ingestion metadata.

    Output
    - Silver dataframe with:
      - standardized types
      - validation applied
      - direct identifiers removed
      - employee_key added
      - columns ordered according to SILVER_COLUMNS
    """
    df = df_bronze.copy()

    # Type casting first is important when data is read via Spark.
    # Spark CSV input often results in string columns when converted to pandas.
    for c in [COLS.gross_amount, COLS.taxes_amount, COLS.net_amount]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # Basic validations on raw values.
    # These checks stop the pipeline early when input data is not consistent.
    require_no_nulls(df, df.columns)
    require_pay_period_format(df)
    require_currency(df)
    require_amount_consistency(df)
    require_one_subsidiary_per_source_file(df)

    # Type casting (kept as-is).
    # This section repeats numeric conversion.
    for c in [COLS.gross_amount, COLS.taxes_amount, COLS.net_amount]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # Normalize pay_period to a real date for the month (first day of month, UTC).
    df[COLS.pay_month] = pd.to_datetime(df[COLS.pay_period] + "-01", format="%Y-%m-%d", utc=True)

    # Create a pseudonymous employee_key and then remove direct identifiers.
    key = _hmac_key()
    df[COLS.employee_key] = df[COLS.employee_id].astype(str).map(lambda x: _employee_key(x, key))

    # Remove sensitive / direct identifiers from Silver.
    df = df.drop(columns=[COLS.employee_id, COLS.employee_name, COLS.iban], errors="ignore")

    # Reorder / select columns to produce a stable Silver schema.
    df = df[SILVER_COLUMNS]

    return df