resource "snowflake_warehouse" "ECOMMERCE_wh" {
  name           = "ECOMMERCE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
}

resource "snowflake_database" "ecommerce_db" {
  name = "ECOMMERCE_DB"
}

resource "snowflake_schema" "demo_schema" {
  database = snowflake_database.ecommerce_db.name
  name     = "ECOMMERCE_RAW"
}
