# Terraform (Azure Databricks + existing ADLS Gen2 structure)

## Goal

- Create an Azure Databricks workspace (minimal).
- Reuse an existing ADLS Gen2 storage account.
- Create a filesystem and folder structure for:
  - raw/
  - bronze/
  - silver/
  - gold/
- Grant the Databricks workspace managed identity access to the storage account.
- Terraform state is local by default.

## Files

- `main.tf` — core resources
- `variables.tf` — input variables
- `outputs.tf` — outputs
- `terraform.tfvars.example` — example values

## Deploy

Run:

```bash
terraform init
terraform plan
terraform apply
```

## Notes

- The storage account is reused, not created.
- The filesystem and paths are created in the existing storage account.
- Node type, spark version, and job definitions are provided separately in `../databricks/`.
