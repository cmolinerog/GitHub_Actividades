{{ config(materialized='table') }}

SELECT
    "REGION",
    SUM("AMOUNT") AS revenue
FROM fct_orders
GROUP BY "REGION"
ORDER BY revenue DESC