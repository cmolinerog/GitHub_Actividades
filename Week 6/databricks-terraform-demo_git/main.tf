terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40.0"
    }
  }
}

provider "databricks" {
  host  = var.databricks_host
  token = var.databricks_token
}

resource "databricks_notebook" "demo_nb" {
  path     = "/Users/email/databricks-terraform-demo/demo_nb"
  language = "PYTHON"
  source   = "./notebook.py"
}