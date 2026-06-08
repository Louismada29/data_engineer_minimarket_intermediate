-- Staging: transactions. Keep only completed sales (business rule).
with source as (
    select * from {{ source('raw', 'transactions') }}
)

select
    transaction_id,
    customer_id,
    store_id,
    transaction_date,
    toDate(transaction_date)           as transaction_day,
    total_amount,
    lower(trim(payment_method))        as payment_method,
    lower(trim(status))                as status
from source
where lower(trim(status)) = 'completed'
