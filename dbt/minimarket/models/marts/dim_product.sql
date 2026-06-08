-- Dimension: product (with category)
with products as (
    select * from {{ ref('stg_products') }}
)

select
    {{ generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category,
    brand,
    unit_price,
    is_active,
    now() as loaded_at
from products
