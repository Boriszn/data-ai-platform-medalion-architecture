# Databricks notebook source
import os
from src.main import run_pipeline

# Create widgets for Job parameters
dbutils.widgets.text("input_dir", "")
dbutils.widgets.text("output_dir", "")


# Defaults (used if widgets are empty)
default_input_dir  = "abfss://data-ai-platform@datalakecrpprdswn002.dfs.core.windows.net/raw"
default_output_dir = "abfss://data-ai-platform@datalakecrpprdswn002.dfs.core.windows.net"

if "dbutils" in globals():
    input_dir  = dbutils.widgets.get("input_dir") or default_input_dir
    output_dir = dbutils.widgets.get("output_dir") or default_output_dir
else:
    input_dir  = "data/raw"
    output_dir = "data"

# Optional: set HMAC key via Databricks secrets and environment variables.
# os.environ["AFILEON_HMAC_KEY"] = dbutils.secrets.get(scope="<scope>", key="afileon-hmac-key")

run_pipeline(input_dir=str(input_dir), output_dir=str(output_dir))
