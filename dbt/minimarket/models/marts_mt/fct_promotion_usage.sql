-- Fact: promotion usage at (transaction x promo) grain (multi-tenant).
-- Carries the transaction total so promo effectiveness can compare avg
-- transaction value with vs without promotions.
with tp as (
    select * from {{ ref('stg_transaction_promotions') }}
),
txns as (
    select * from {{ ref('stg_transactions_mt') }}
)
select
    {{ generate_surrogate_key(['tp.tenant_id', 'tp.id']) }} as promo_usage_key,
    tp.tenant_id        as tenant_id,
    tp.id               as junction_id,
    tp.transaction_id   as transaction_id,
    tp.promo_id         as promo_id,
    t.store_id          as store_id,
    t.customer_id       as customer_id,
    t.transaction_day   as transaction_day,
    tp.discount_applied as discount_applied,
    t.total_amount      as transaction_total,
    now()               as loaded_at
from tp
inner join txns as t
    on tp.tenant_id = t.tenant_id and tp.transaction_id = t.transaction_id
