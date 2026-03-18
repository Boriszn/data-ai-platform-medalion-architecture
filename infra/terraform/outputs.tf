output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.dbw.workspace_url
  description = "Databricks workspace URL."
}

output "databricks_workspace_resource_id" {
  value       = azurerm_databricks_workspace.dbw.id
  description = "Databricks workspace resource id."
}

output "adls_storage_account_id" {
  value       = data.azurerm_storage_account.adls.id
  description = "Existing ADLS storage account id."
}

output "adls_filesystem_name" {
  value       = azurerm_storage_data_lake_gen2_filesystem.fs.name
  description = "Created filesystem name."
}

output "adls_paths" {
  value       = [for p in azurerm_storage_data_lake_gen2_path.paths : p.path]
  description = "Created folder paths."
}
