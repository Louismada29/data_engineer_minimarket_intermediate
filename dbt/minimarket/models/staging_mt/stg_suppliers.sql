-- Multi-tenant staging: suppliers
select
    tenant_id,
    supplier_id,
    trim(supplier_name) as supplier_name,
    contact_name,
    initCap(trim(city)) as city,
    country
from {{ source('raw_mt', 'suppliers') }} final
