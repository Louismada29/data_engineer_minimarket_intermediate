-- Multi-tenant staging: promotions
select
    tenant_id,
    promo_id,
    trim(promo_name)        as promo_name,
    lower(trim(promo_type)) as promo_type,
    discount_pct,
    start_date,
    end_date,
    min_purchase
from {{ source('raw_mt', 'promotions') }} final
