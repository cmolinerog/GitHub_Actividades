{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('minute', order_time) AS minute,
    SUM(AMOUNT) AS gmv_per_minute
FROM fct_orders
GROUP BY minute
ORDER BY minute