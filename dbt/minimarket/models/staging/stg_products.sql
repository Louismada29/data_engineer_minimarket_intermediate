-- Staging: products. Clean & standardise from raw landing.
with source as (
    select * from {{ source('raw', 'products') }}
)

select
    product_id,
    trim(product_name)                 as product_name,
    initCap(trim(category))            as category,
    trim(brand)                        as brand,
    unit_price,
    coalesce(is_active, 1)             as is_active
from source
