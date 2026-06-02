{{ config(materialized='table') }}

SELECT
    product,
    SUM(amount) AS total_sales
FROM fct_orders
GROUP BY product
ORDER BY total_sales DESC