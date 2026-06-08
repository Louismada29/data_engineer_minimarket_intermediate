-- Dimension: product (multi-tenant).
select
    {{ generate_surrogate_key(['tenant_id', 'product_id']) }} as product_key,
    tenant_id,
    product_id,
    product_name,
    category,
    brand,
    unit_price,
    is_active,
    now() as loaded_at
from {{ ref('stg_products_mt') }}
