-- Dimension: customer (multi-tenant).
select
    {{ generate_surrogate_key(['tenant_id', 'customer_id']) }} as customer_key,
    tenant_id,
    customer_id,
    customer_name,
    gender,
    city,
    now() as loaded_at
from {{ ref('stg_customers_mt') }}
