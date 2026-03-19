# data-ai — Payroll Medallion Pipeline (Python)

## Project description

This project implements Exercise 2 of the data-ai assessment.

Input:
- Three payroll CSV files for subsidiaries DE001, DE002, DE003.

Output (Gold datasets):
 `controller_monthly_aggregates` — monthly totals per subsidiary**
  Payroll records are grouped by `subsidiary_id` and `pay_period`. For each group, the output calculates:

  - number of employees (distinct count)
  - total gross amount
  - total taxes amount
  - total net amount

`auditor_cross_subsidiary_totals` — monthly totals across all subsidiaries**
  Payroll records are grouped by `pay_period` only (all subsidiaries together). For each month, the output calculates:

  - number of subsidiaries (distinct count)
  - number of employees (distinct count)
  - total gross amount
  - total taxes amount
  - total net amount


The pipeline follows a simple Medallion structure:
- Bronze: ingest and add ingestion metadata.
- Silver: standardize types, validate data, and apply basic data protection.
- Gold: create the required reporting outputs.

A small Azure Databricks deployment plan is included as a bonus option. Existing ADLS Gen2 storage is reused; only the folder/filesystem structure is created.

## Project structure

- `src/`
  - `main.py` — pipeline entrypoint (Bronze → Silver → Gold)
  - `config.py` — constants and default paths
  - `io/` — read/write helpers
  - `bronze/` — Bronze stage
  - `silver/` — Silver stage (validation + protection)
  - `gold/` — Gold outputs (controller and auditor)
  - `checks/` — validation rules
- `data/`
  - `raw/` — input CSVs for local runs
  - `bronze/`, `silver/`, `gold/` — outputs for local runs
  - `logs/` — run logs (local runs)
- `infra/terraform/` — minimal Terraform for Azure Databricks + ADLS folder structure (reuse existing storage)
- `infra/databricks/` — Databricks job and cluster templates
- `notebooks/` — Databricks notebook entrypoint for running the pipeline in a Job

## Requirements

- Python 3.10+ (3.11 also works)
- Local run dependencies:
  - `pandas`
  - `pyarrow`
- Optional (tests):
  - `pytest`

## How to run locally

### 1) Install dependencies

Run:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Prepare input data

Place the three CSV files into:

- `data/raw/`

This repository already includes the example inputs in `data/raw/`.

### 3) Run the pipeline

Run:

```bash
python -m src.main --input-dir data/raw --output-dir data
```

Outputs are written to:
- `data/bronze/`
- `data/silver/`
- `data/gold/`

The two required Gold outputs are:
- `data/gold/controller_monthly_aggregates.parquet`
- `data/gold/auditor_cross_subsidiary_totals.parquet`

CSV exports are also written into the same folder.

### 4) Run tests (optional)

Install dev dependencies:

```bash
pip install -r requirements-dev.txt
```

Run:

```bash
pytest -q
```

## Data protection and governance (implemented)

- Sensitive fields are not carried into reporting outputs.
- In Silver:
  - `employee_name` and `iban` are removed.
  - `employee_id` is replaced by a pseudonymous `employee_key` derived by HMAC-SHA256.
- In Gold:
  - Only aggregates and counts are published.
  - No direct identifiers are present.

The HMAC secret key is read from `data-ai_HMAC_KEY`.
If the variable is not set, a development default is used.
For production, set `data-ai_HMAC_KEY` to a strong secret.

Example:

```bash
export data-ai_HMAC_KEY="replace-with-strong-secret"
python -m src.main --input-dir data/raw --output-dir data
```

## Bonus: deploy infrastructure and run on Azure Databricks

### Overview

- An Azure Databricks workspace is created.
- An existing ADLS Gen2 storage account is reused.
- A filesystem and folders are created for:
  - `raw/`, `bronze/`, `silver/`, `gold/`
- Storage access is granted to the Databricks workspace managed identity.
- A Databricks Job runs the pipeline on job compute (ephemeral cluster with auto-termination).

### Steps

1) Deploy Azure resources with Terraform:

- Go to `infra/terraform/`
- Set variables in `terraform.tfvars` (examples are in `terraform.tfvars.example`)
- Run:

```bash
terraform init
terraform plan
terraform apply
```

2) Import the code into Databricks using Databricks Repos:

- Connect Databricks Repos to the private GitHub repository.
- Select the branch/tag used for the exercise submission.

3) Create a Databricks Job:

- Use `infra/databricks/job.json` as a template.
- Update:
  - notebook path (Repo location)
  - storage paths (ADLS filesystem and folders)
  - cluster settings (node type and spark version)

4) Run the Job:

- Job reads inputs from ADLS `raw/`
- Job writes Bronze/Silver/Gold to ADLS `bronze/`, `silver/`, `gold/`

## Future improvements

- Add a small data catalog document (data dictionary) for Bronze/Silver/Gold.
- Add richer data quality checks (range checks, outlier detection, schema versioning).
- Add incremental processing (process only new pay periods).
- Add structured run metadata table (instead of file logs).
- Add role-based access for Gold outputs (controller vs auditor) in the platform deployment.
- Add CI workflow for linting, tests, and release packaging.

## Best-practice to-do list

- Define data retention rules for each layer (raw/bronze/silver/gold).
- Define access rules and approvals for Gold datasets.
- Store secrets (HMAC key) in a secret manager, not in code or notebooks.
- Keep job clusters ephemeral and enable auto-termination.
- Add alerts for failed jobs and data quality failures.
- Pin dependency versions for repeatable runs.
