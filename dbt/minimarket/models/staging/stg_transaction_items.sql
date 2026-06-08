-- Staging: transaction_items. One row per sold line.
with source as (
    select * from {{ source('raw', 'transaction_items') }}
)

select
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    discount,
    subtotal
from source
