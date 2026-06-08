-- Dimension: store (multi-tenant). Natural key is composite (tenant_id, store_id).
select
    {{ generate_surrogate_key(['tenant_id', 'store_id']) }} as store_key,
    tenant_id,
    store_id,
    store_name,
    city,
    province,
    store_type,
    opened_at,
    is_active,
    now() as loaded_at
from {{ ref('stg_stores') }}
