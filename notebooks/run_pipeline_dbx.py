# Databricks notebook source

# -----------------------------------------------------------------------------
# notebooks/run_pipeline_dbx.py
#
# Purpose
# - Databricks notebook entrypoint for running the Exercise 2 pipeline as a Job.
# - Reads inputs from ADLS Gen2 using ABFSS paths.
# - Writes Bronze/Silver/Gold outputs back to ADLS Gen2.
#
# How this notebook is used
# - A Databricks Job calls this notebook.
# - The Job can pass input/output paths as parameters.
# - If Job parameters are not provided, default paths are used.
#
# Parameters (widgets)
# - input_dir: folder that contains the input CSV files
# - output_dir: base folder for Bronze/Silver/Gold outputs
#
# Notes
# - The pipeline uses the same Python code as local runs (src/main.py).
# - Secrets can be used to provide the HMAC key for pseudonymization.
# -----------------------------------------------------------------------------


import os
from src.main import run_pipeline

# Create widgets for Job parameters.
# Widgets allow Jobs to pass values into the notebook.
dbutils.widgets.text("input_dir", "")
dbutils.widgets.text("output_dir", "")

# Default ADLS Gen2 locations used when widget values are empty.
# - input_dir points to the folder that contains payroll_transactions_*.csv
# - output_dir is the base folder where bronze/, silver/, gold/, logs/ are created
default_input_dir  = "abfss://data-ai-platform@datalakecrpprdswn002.dfs.core.windows.net/raw"
default_output_dir = "abfss://data-ai-platform@datalakecrpprdswn002.dfs.core.windows.net"

# Resolve runtime paths.
# - When running in Databricks, dbutils is available and widget values can be used.
# - When running outside Databricks, local folder defaults are used.
if "dbutils" in globals():
    input_dir  = dbutils.widgets.get("input_dir") or default_input_dir
    output_dir = dbutils.widgets.get("output_dir") or default_output_dir
else:
    input_dir  = "data/raw"
    output_dir = "data"

# Optional: set HMAC key via Databricks secrets and environment variables.
# This supports a stable pseudonymous employee_key.
# os.environ["AF_HMAC_KEY"] = dbutils.secrets.get(scope="<scope>", key="afileon-hmac-key")

# Run the pipeline using the resolved paths.
run_pipeline(input_dir=str(input_dir), output_dir=str(output_dir))