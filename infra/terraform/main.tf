terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 3.100.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Existing storage account (reused)
data "azurerm_storage_account" "adls" {
  name                = var.existing_storage_account_name
  resource_group_name = var.existing_storage_account_resource_group
}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Azure Databricks workspace
resource "azurerm_databricks_workspace" "dbw" {
  name                = var.databricks_workspace_name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "standard"

  managed_resource_group_name = "${var.resource_group_name}-dbw-managed"

}

# Managed identity for Databricks to access ADLS (recommended pattern)
resource "azurerm_databricks_access_connector" "ac" {
  name                 = "${var.databricks_workspace_name}-ac"
  location              = azurerm_resource_group.rg.location
  resource_group_name  = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }
}

# Grant the access connector permissions on the existing ADLS account
resource "azurerm_role_assignment" "ac_storage_contributor" {
  scope                = data.azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ac.identity[0].principal_id
}

# Create filesystem and folder structure in existing ADLS Gen2
resource "azurerm_storage_data_lake_gen2_filesystem" "fs" {
  name               = var.adls_filesystem_name
  storage_account_id = data.azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_path" "paths" {
  for_each           = toset(var.adls_paths)
  path               = each.value
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.fs.name
  storage_account_id = data.azurerm_storage_account.adls.id
  resource           = "directory"
}
