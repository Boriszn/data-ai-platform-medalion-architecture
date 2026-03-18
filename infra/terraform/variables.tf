variable "resource_group_name" {
  type        = string
  description = "Azure resource group for the Databricks workspace."
}

variable "location" {
  type        = string
  description = "Azure region."
}

variable "databricks_workspace_name" {
  type        = string
  description = "Name of the Azure Databricks workspace."
}

variable "existing_storage_account_name" {
  type        = string
  description = "Existing ADLS Gen2 storage account name."
}

variable "existing_storage_account_resource_group" {
  type        = string
  description = "Resource group of the existing ADLS Gen2 storage account."
}

variable "adls_filesystem_name" {
  type        = string
  description = "Filesystem (container) name to create in the existing storage account."
  default     = "data-ai-ex2"
}

variable "adls_paths" {
  type        = list(string)
  description = "Folder paths to create in the filesystem."
  default     = ["raw", "bronze", "silver", "gold"]
}
