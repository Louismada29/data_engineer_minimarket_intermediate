-- Multi-tenant staging: transaction <-> promotion junction
select
    tenant_id,
    id,
    transaction_id,
    promo_id,
    discount_applied
from {{ source('raw_mt', 'transaction_promotions') }} final
