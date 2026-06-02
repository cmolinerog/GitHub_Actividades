{{ config(materialized='table') }}

select
    order_id::integer as order_id,
    customer_name::varchar as customer_name,
    product::varchar as product,
    amount::float as amount,
    region::varchar as region,
    timestamp::timestamp as order_time
from raw_orders