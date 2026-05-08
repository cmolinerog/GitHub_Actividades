SELECT
    product,
    SUM(amount) AS product_revenue,
    COUNT(order_id) AS total_orders
FROM {{ ref('sales_data') }}
GROUP BY product
ORDER BY product_revenue DESC
