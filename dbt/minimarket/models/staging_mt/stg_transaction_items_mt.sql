-- Multi-tenant staging: transaction items
select
    tenant_id,
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount,
    subtotal
from {{ source('raw_mt', 'transaction_items') }} final
