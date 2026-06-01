SELECT
    "REGION",
    SUM("AMOUNT") AS revenue
FROM orders_cleaned
GROUP BY "REGION"
ORDER BY revenue DESC