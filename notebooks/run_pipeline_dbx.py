# Databricks notebook source
# data-ai Exercise 2: run payroll medallion pipeline on Databricks
#
# This notebook is meant to be used as the entrypoint for a Databricks Job.
# It calls the same pipeline code from src/main.py.

import os
from src.main import run_pipeline

# Set these paths to ADLS locations mounted or accessed via ABFSS.
# Example:
# input_dir  = "abfss://data-ai-ex2@<storage-account>.dfs.core.windows.net/raw"
# output_dir = "abfss://data-ai-ex2@<storage-account>.dfs.core.windows.net"

input_dir  = dbutils.widgets.get("input_dir") if "dbutils" in globals() else "data/raw"
output_dir = dbutils.widgets.get("output_dir") if "dbutils" in globals() else "data"

# Optional: set HMAC key via Databricks secrets and environment variables.
# os.environ["data-ai_HMAC_KEY"] = dbutils.secrets.get(scope="<scope>", key="data-ai-hmac-key")

run_pipeline(input_dir=input_dir, output_dir=output_dir)
