from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_INPUT_DIR = os.path.join("data", "raw")
DEFAULT_OUTPUT_DIR = "data"

BRONZE_DIRNAME = "bronze"
SILVER_DIRNAME = "silver"
GOLD_DIRNAME = "gold"
LOGS_DIRNAME = "logs"

# Environment variable used to build a pseudonymous employee key
HMAC_KEY_ENV = "data-ai_HMAC_KEY"

# Development fallback (should be overridden in real deployments)
DEV_FALLBACK_HMAC_KEY = "dev-only-change-this"


@dataclass(frozen=True)
class Columns:
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
    source_file: str = "source_file"
    ingested_at: str = "ingested_at"
    employee_key: str = "employee_key"
    pay_month: str = "pay_month"


COLS = Columns()

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

# Columns allowed in Silver after protection
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
