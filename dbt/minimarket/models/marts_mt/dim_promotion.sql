-- Dimension: promotion (multi-tenant).
select
    {{ generate_surrogate_key(['tenant_id', 'promo_id']) }} as promo_key,
    tenant_id,
    promo_id,
    promo_name,
    promo_type,
    discount_pct,
    start_date,
    end_date,
    min_purchase,
    now() as loaded_at
from {{ ref('stg_promotions') }}
