SELECT
    customer,
    total_sales,
    CASE
        WHEN total_sales >= 70000 THEN 'High value'
        WHEN total_sales > 30000 THEN 'Medium value'
        ELSE 'Low value'
    END AS customer_segment
FROM {{ ref('customer_sales') }}
ORDER BY total_sales DESC