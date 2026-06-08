-- Dimension: customer
with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    {{ generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    customer_name,
    gender,
    city,
    created_at,
    now() as loaded_at
from customers
