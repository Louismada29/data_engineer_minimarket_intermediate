-- Fact: sales at transaction-item grain (multi-tenant).
-- Enriched with tenant_id and store_id. INNER JOIN drops non-completed txns.
with items as (
    select * from {{ ref('stg_transaction_items_mt') }}
),
txns as (
    select * from {{ ref('stg_transactions_mt') }}
)
select
    {{ generate_surrogate_key(['i.tenant_id', 'i.item_id']) }}        as sale_key,
    i.tenant_id        as tenant_id,
    i.item_id          as item_id,
    i.transaction_id   as transaction_id,
    t.customer_id      as customer_id,
    i.product_id       as product_id,
    t.store_id         as store_id,
    t.transaction_day  as transaction_day,
    t.payment_method   as payment_method,
    i.quantity         as quantity,
    i.unit_price       as unit_price,
    i.discount         as discount,
    i.subtotal         as subtotal,
    now()              as loaded_at
from items as i
inner join txns as t
    on i.tenant_id = t.tenant_id and i.transaction_id = t.transaction_id
