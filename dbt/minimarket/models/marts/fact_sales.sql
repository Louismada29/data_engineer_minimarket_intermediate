-- Fact: sales at transaction-item grain.
-- INNER JOIN to stg_transactions drops items belonging to
-- cancelled/pending transactions (status filtered in staging).
with items as (
    select * from {{ ref('stg_transaction_items') }}
),

txns as (
    select * from {{ ref('stg_transactions') }}
)

select
    -- surrogate / natural keys
    {{ generate_surrogate_key(['i.item_id']) }}          as sale_key,
    i.item_id                                            as item_id,
    i.transaction_id                                     as transaction_id,
    t.customer_id                                        as customer_id,
    i.product_id                                         as product_id,
    {{ generate_surrogate_key(['t.customer_id']) }}      as customer_key,
    {{ generate_surrogate_key(['i.product_id']) }}       as product_key,
    {{ generate_surrogate_key(['t.transaction_day']) }}  as date_key,
    t.transaction_day                                    as transaction_day,

    -- degenerate dimensions
    t.store_id                                           as store_id,
    t.payment_method                                     as payment_method,

    -- measures
    i.quantity                                           as quantity,
    i.unit_price                                         as unit_price,
    i.discount                                           as discount,
    i.subtotal                                           as subtotal,
    now()                                                as loaded_at
from items as i
inner join txns as t
    on i.transaction_id = t.transaction_id
