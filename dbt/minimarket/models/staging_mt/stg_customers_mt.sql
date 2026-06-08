-- Multi-tenant staging: customers (FINAL collapses ReplacingMergeTree versions)
select
    tenant_id,
    customer_id,
    trim(name)            as customer_name,
    phone,
    lower(trim(email))    as email,
    lower(trim(gender))   as gender,
    initCap(trim(city))   as city,
    created_at,
    updated_at
from {{ source('raw_mt', 'customers') }} final
