resource_group_name                  = "rg-data-ai-platform-ex2"
location                             = "westeurope"
databricks_workspace_name            = "dbw-data-ai-ex2"
existing_storage_account_name        = "datalakecrpprdswn002"
existing_storage_account_resource_group = "rg-corp-data-ai-platform-001"
adls_filesystem_name                 = "data-ai-platform"
adls_paths                           = ["raw", "bronze", "silver", "gold"]
