SELECT
    customer,
    SUM(amount) AS total_sales,
    COUNT(order_id) AS total_orders,
    AVG(amount) AS avg_order_value
FROM {{ ref('sales_data') }}
GROUP BY customer
ORDER BY total_sales DESC