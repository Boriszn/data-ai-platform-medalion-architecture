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


def _hmac_key() -> bytes:
    key = os.environ.get(HMAC_KEY_ENV, DEV_FALLBACK_HMAC_KEY)
    return key.encode("utf-8")


def _employee_key(employee_id: str, key: bytes) -> str:
    # HMAC-SHA256 for stable pseudonymous key
    digest = hmac.new(key, employee_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest


def build_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    df = df_bronze.copy()

    # type casting FIRST (Spark->pandas often yields strings)
    for c in [COLS.gross_amount, COLS.taxes_amount, COLS.net_amount]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # basic validations on raw values
    require_no_nulls(df, df.columns)
    require_pay_period_format(df)
    require_currency(df)
    require_amount_consistency(df)
    require_one_subsidiary_per_source_file(df)

    # type casting
    for c in [COLS.gross_amount, COLS.taxes_amount, COLS.net_amount]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # pay_month = first day of pay_period month
    df[COLS.pay_month] = pd.to_datetime(df[COLS.pay_period] + "-01", format="%Y-%m-%d", utc=True)

    # pseudonymous employee_key and drop direct identifiers
    key = _hmac_key()
    df[COLS.employee_key] = df[COLS.employee_id].astype(str).map(lambda x: _employee_key(x, key))

    # Remove sensitive / direct identifiers
    df = df.drop(columns=[COLS.employee_id, COLS.employee_name, COLS.iban], errors="ignore")

    # Reorder / select columns
    df = df[SILVER_COLUMNS]

    return df
