-- Multi-tenant staging: stores
select
    tenant_id,
    store_id,
    trim(store_name)      as store_name,
    initCap(trim(city))   as city,
    province,
    lower(trim(store_type)) as store_type,
    opened_at,
    coalesce(is_active, 1) as is_active
from {{ source('raw_mt', 'stores') }} final
