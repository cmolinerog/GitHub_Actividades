{{ config(materialized='table') }}

select 
    order_id,
    customer_name,
    product,
    amount,
    region,
    order_time
from stg_orders