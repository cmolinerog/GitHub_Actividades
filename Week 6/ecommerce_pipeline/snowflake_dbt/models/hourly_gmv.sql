SELECT
    DATE_TRUNC('minute', TIMESTAMP) AS minute_block,
    SUM(AMOUNT) AS gmv_per_minute
FROM orders_cleaned
GROUP BY 1
ORDER BY 1